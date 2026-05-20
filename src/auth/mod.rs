//! Authentication and authorization subsystem.
//!
//! Provides RabbitMQ-compatible multi-user credential management
//! and per-vhost permission enforcement.

pub mod credentials;
pub mod permissions;

use std::net::SocketAddr;
use std::path::Path;

use dashmap::DashMap;
use tracing::{info, warn};

pub use credentials::{UserEntry, UserTag};
pub use permissions::Permission;

/// Central authentication and authorization backend.
///
/// Manages users and their per-vhost permissions.
/// Thread-safe via DashMap for concurrent access from connection handlers.
pub struct AuthBackend {
    /// username → UserEntry
    users: DashMap<String, UserEntry>,
    /// (username, vhost) → Permission
    permissions: DashMap<(String, String), Permission>,
}

impl AuthBackend {
    /// Create a new auth backend seeded with the default `guest` user.
    pub fn new() -> Self {
        let backend = Self {
            users: DashMap::new(),
            permissions: DashMap::new(),
        };

        // Seed default guest user (like RabbitMQ)
        let guest = UserEntry::new("guest", "guest", vec![UserTag::Administrator]);
        backend.users.insert("guest".to_string(), guest);

        // Grant guest full access to default vhost "/"
        backend.permissions.insert(
            ("guest".to_string(), "/".to_string()),
            Permission::full_access("guest", "/"),
        );

        // Seed admin user
        let admin = UserEntry::new("admin", "1234", vec![UserTag::Administrator]);
        backend.users.insert("admin".to_string(), admin);

        // Grant admin full access to default vhost "/"
        backend.permissions.insert(
            ("admin".to_string(), "/".to_string()),
            Permission::full_access("admin", "/"),
        );

        backend
    }

    // ── Authentication ────────────────────────────────────

    /// Authenticate a user with username + password.
    /// Returns Ok(()) on success, Err(reason) on failure.
    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
        peer_addr: SocketAddr,
    ) -> Result<(), String> {
        let entry = self
            .users
            .get(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;

        // RabbitMQ rule: guest can only connect from localhost
        if username == "guest" && !is_loopback(&peer_addr) {
            return Err("user 'guest' can only connect via localhost".to_string());
        }

        if !entry.verify_password(password) {
            return Err(format!("authentication failed for user '{}'", username));
        }

        Ok(())
    }

    /// Check if a user has access to a vhost.
    pub fn check_vhost_access(&self, username: &str, vhost: &str) -> bool {
        self.permissions
            .contains_key(&(username.to_string(), vhost.to_string()))
    }

    // ── Authorization ─────────────────────────────────────

    /// Check if a user has 'configure' permission for a resource in a vhost.
    /// Configure = declare/delete queues and exchanges.
    pub fn check_configure(&self, username: &str, vhost: &str, resource: &str) -> bool {
        self.check_permission(username, vhost, resource, |p| &p.configure)
    }

    /// Check if a user has 'write' permission for a resource in a vhost.
    /// Write = publish to exchange, bind queue to exchange.
    pub fn check_write(&self, username: &str, vhost: &str, resource: &str) -> bool {
        self.check_permission(username, vhost, resource, |p| &p.write)
    }

    /// Check if a user has 'read' permission for a resource in a vhost.
    /// Read = consume from queue, basic.get, queue.purge.
    pub fn check_read(&self, username: &str, vhost: &str, resource: &str) -> bool {
        self.check_permission(username, vhost, resource, |p| &p.read)
    }

    fn check_permission(
        &self,
        username: &str,
        vhost: &str,
        resource: &str,
        extractor: impl Fn(&Permission) -> &str,
    ) -> bool {
        match self
            .permissions
            .get(&(username.to_string(), vhost.to_string()))
        {
            Some(perm) => {
                let pattern = extractor(&perm);
                permissions::matches_resource(pattern, resource)
            }
            None => false,
        }
    }

    // ── User management ───────────────────────────────────

    /// Add a new user. Returns Err if user already exists.
    pub fn add_user(
        &self,
        username: &str,
        password: &str,
        tags: Vec<UserTag>,
    ) -> Result<(), String> {
        if self.users.contains_key(username) {
            return Err(format!("user '{}' already exists", username));
        }
        let entry = UserEntry::new(username, password, tags);
        self.users.insert(username.to_string(), entry);
        info!(user = username, "user created");
        Ok(())
    }

    /// Delete a user and all their permissions.
    pub fn delete_user(&self, username: &str) -> Result<(), String> {
        if username == "guest" {
            return Err("cannot delete the default guest user".to_string());
        }
        self.users
            .remove(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;

        // Remove all permissions for this user
        self.permissions.retain(|key, _| key.0 != username);

        info!(user = username, "user deleted");
        Ok(())
    }

    /// Change a user's password.
    pub fn change_password(&self, username: &str, new_password: &str) -> Result<(), String> {
        let mut entry = self
            .users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        entry.set_password(new_password);
        info!(user = username, "password changed");
        Ok(())
    }

    /// Set permissions for a user on a vhost.
    pub fn set_permissions(
        &self,
        username: &str,
        vhost: &str,
        configure: &str,
        write: &str,
        read: &str,
    ) -> Result<(), String> {
        if !self.users.contains_key(username) {
            return Err(format!("user '{}' not found", username));
        }
        let perm = Permission::new(username, vhost, configure, write, read);
        self.permissions
            .insert((username.to_string(), vhost.to_string()), perm);
        info!(user = username, vhost, "permissions set");
        Ok(())
    }

    /// List all users.
    pub fn list_users(&self) -> Vec<(String, Vec<UserTag>)> {
        self.users
            .iter()
            .map(|e| (e.key().clone(), e.value().tags.clone()))
            .collect()
    }

    /// List all permissions for a user.
    pub fn list_user_permissions(&self, username: &str) -> Vec<Permission> {
        self.permissions
            .iter()
            .filter(|e| e.key().0 == username)
            .map(|e| e.value().clone())
            .collect()
    }

    // ── Persistence ───────────────────────────────────────

    /// Save user database to a JSON file.
    pub fn save_to_file(&self, path: &Path) -> Result<(), String> {
        let data = credentials::UserStore {
            users: self
                .users
                .iter()
                .map(|e| e.value().to_serializable())
                .collect(),
            permissions: self.permissions.iter().map(|e| e.value().clone()).collect(),
        };
        let json =
            serde_json::to_string_pretty(&data).map_err(|e| format!("serialize error: {}", e))?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir error: {}", e))?;
        }

        std::fs::write(path, json).map_err(|e| format!("write error: {}", e))?;
        info!(path = %path.display(), "user database saved");
        Ok(())
    }

    /// Load user database from a JSON file.
    /// If the file doesn't exist, keep the default seed (guest user).
    pub fn load_from_file(&self, path: &Path) -> Result<(), String> {
        if !path.exists() {
            info!(path = %path.display(), "no user database found, using defaults");
            return Ok(());
        }

        let json = std::fs::read_to_string(path).map_err(|e| format!("read error: {}", e))?;
        let data: credentials::UserStore =
            serde_json::from_str(&json).map_err(|e| format!("parse error: {}", e))?;

        self.users.clear();
        self.permissions.clear();

        for su in data.users {
            self.users
                .insert(su.username.clone(), UserEntry::from_serializable(su));
        }

        for perm in data.permissions {
            self.permissions
                .insert((perm.username.clone(), perm.vhost.clone()), perm);
        }

        let user_count = self.users.len();
        let perm_count = self.permissions.len();
        info!(
            path = %path.display(),
            user_count,
            perm_count,
            "user database loaded"
        );
        Ok(())
    }
}

fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn localhost() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345)
    }

    fn remote() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 12345)
    }

    #[test]
    fn default_guest_auth() {
        let auth = AuthBackend::new();
        assert!(auth.authenticate("guest", "guest", localhost()).is_ok());
    }

    #[test]
    fn guest_wrong_password() {
        let auth = AuthBackend::new();
        assert!(auth.authenticate("guest", "wrong", localhost()).is_err());
    }

    #[test]
    fn guest_remote_rejected() {
        let auth = AuthBackend::new();
        assert!(auth.authenticate("guest", "guest", remote()).is_err());
    }

    #[test]
    fn unknown_user() {
        let auth = AuthBackend::new();
        assert!(auth.authenticate("nobody", "pass", localhost()).is_err());
    }

    #[test]
    fn guest_has_vhost_access() {
        let auth = AuthBackend::new();
        assert!(auth.check_vhost_access("guest", "/"));
        assert!(!auth.check_vhost_access("guest", "/staging"));
    }

    #[test]
    fn guest_full_permissions() {
        let auth = AuthBackend::new();
        assert!(auth.check_configure("guest", "/", "my-queue"));
        assert!(auth.check_write("guest", "/", "my-exchange"));
        assert!(auth.check_read("guest", "/", "my-queue"));
    }

    #[test]
    fn add_user_and_auth() {
        let auth = AuthBackend::new();
        auth.add_user("ops", "s3cret", vec![UserTag::Administrator])
            .unwrap();
        auth.set_permissions("ops", "/", ".*", ".*", ".*").unwrap();
        assert!(auth.authenticate("ops", "s3cret", remote()).is_ok());
        assert!(auth.check_vhost_access("ops", "/"));
    }

    #[test]
    fn restricted_permissions() {
        let auth = AuthBackend::new();
        auth.add_user("app", "pass", vec![UserTag::None]).unwrap();
        // Can only configure queues starting with "app."
        // Can write to any exchange, can read from "app.*" queues
        auth.set_permissions("app", "/", "^app\\..*", ".*", "^app\\..*")
            .unwrap();

        assert!(auth.check_configure("app", "/", "app.orders"));
        assert!(!auth.check_configure("app", "/", "system.internal"));
        assert!(auth.check_write("app", "/", "anything"));
        assert!(auth.check_read("app", "/", "app.events"));
        assert!(!auth.check_read("app", "/", "admin.logs"));
    }

    #[test]
    fn delete_user_removes_permissions() {
        let auth = AuthBackend::new();
        auth.add_user("temp", "pass", vec![]).unwrap();
        auth.set_permissions("temp", "/", ".*", ".*", ".*").unwrap();
        assert!(auth.check_vhost_access("temp", "/"));
        auth.delete_user("temp").unwrap();
        assert!(!auth.check_vhost_access("temp", "/"));
    }

    #[test]
    fn cannot_delete_guest() {
        let auth = AuthBackend::new();
        assert!(auth.delete_user("guest").is_err());
    }

    #[test]
    fn change_password() {
        let auth = AuthBackend::new();
        auth.add_user("bob", "old", vec![]).unwrap();
        auth.set_permissions("bob", "/", ".*", ".*", ".*").unwrap();
        assert!(auth.authenticate("bob", "old", localhost()).is_ok());
        auth.change_password("bob", "new").unwrap();
        assert!(auth.authenticate("bob", "old", localhost()).is_err());
        assert!(auth.authenticate("bob", "new", localhost()).is_ok());
    }

    #[test]
    fn list_users() {
        let auth = AuthBackend::new();
        auth.add_user("alice", "pass", vec![UserTag::Monitoring])
            .unwrap();
        let users = auth.list_users();
        assert_eq!(users.len(), 3); // guest + admin + alice
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = std::env::current_dir().unwrap().join("data");
        let path = dir.join("test_users.json");

        let auth1 = AuthBackend::new();
        auth1
            .add_user("persist-test", "pass123", vec![UserTag::Management])
            .unwrap();
        auth1
            .set_permissions("persist-test", "/", "^app\\..*", ".*", "^app\\..*")
            .unwrap();
        auth1.save_to_file(&path).unwrap();

        let auth2 = AuthBackend::new();
        auth2.load_from_file(&path).unwrap();

        assert!(auth2.authenticate("guest", "guest", localhost()).is_ok());
        assert!(
            auth2
                .authenticate("persist-test", "pass123", localhost())
                .is_ok()
        );
        assert!(auth2.check_configure("persist-test", "/", "app.queue"));
        assert!(!auth2.check_configure("persist-test", "/", "system.queue"));

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }
}
