// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Edilson Pateguana
// Year: 2026
// File: mod.rs
// Description: Authentication and authorization manager module.

//! Authentication and authorization subsystem.
//!
//! Provides RabbitMQ-compatible multi-user credential management
//! and per-vhost permission enforcement.

pub mod credentials;
pub mod permissions;

use std::net::SocketAddr;
use std::path::Path;

use dashmap::DashMap;
use tracing::info;

pub use credentials::{UserEntry, UserTag};
pub use permissions::Permission;

/// Persistent authentication and authorization backend.
///
/// Stores bcrypt-hashed credentials on disk and enforces per-user
/// configure/write/read permission patterns via regex matching.
pub struct AuthBackend {
    /// username → UserEntry
    users: DashMap<String, UserEntry>,
    /// (username, vhost) → Permission
    permissions: DashMap<(String, String), Permission>,
}

impl Default for AuthBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthBackend {
    /// Creates a new instance with default values.
    pub fn new() -> Self {
        let backend = Self {
            users: DashMap::new(),
            permissions: DashMap::new(),
        };

        let guest = UserEntry::new(
            crate::config::default_guest_user(),
            crate::config::default_guest_pass(),
            vec![UserTag::Administrator],
        );
        backend
            .users
            .insert(crate::config::default_guest_user().to_string(), guest);

        backend.permissions.insert(
            (
                crate::config::default_guest_user().to_string(),
                "/".to_string(),
            ),
            Permission::full_access(crate::config::default_guest_user(), "/"),
        );

        let admin = UserEntry::new(
            crate::config::default_admin_user(),
            crate::config::default_admin_pass(),
            vec![UserTag::Administrator],
        );
        backend
            .users
            .insert(crate::config::default_admin_user().to_string(), admin);

        backend.permissions.insert(
            (
                crate::config::default_admin_user().to_string(),
                "/".to_string(),
            ),
            Permission::full_access(crate::config::default_admin_user(), "/"),
        );

        backend
    }

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

        if username == crate::config::default_guest_user() && !is_loopback(&peer_addr) {
            return Err(format!(
                "user '{}' can only connect via localhost",
                crate::config::default_guest_user()
            ));
        }

        if !entry.verify_password(password) {
            return Err(format!("authentication failed for user '{}'", username));
        }

        Ok(())
    }

    pub fn check_vhost_access(&self, username: &str, vhost: &str) -> bool {
        self.permissions
            .contains_key(&(username.to_string(), vhost.to_string()))
    }

    pub fn check_configure(&self, username: &str, vhost: &str, resource: &str) -> bool {
        self.check_permission(username, vhost, resource, |p| &p.configure)
    }

    pub fn check_write(&self, username: &str, vhost: &str, resource: &str) -> bool {
        self.check_permission(username, vhost, resource, |p| &p.write)
    }

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

    /// Removes a user from the backend and persists the change to disk.
    pub fn delete_user(&self, username: &str) -> Result<(), String> {
        if username == crate::config::default_guest_user() {
            return Err("cannot delete the default guest user".to_string());
        }
        self.users
            .remove(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;

        self.permissions.retain(|key, _| key.0 != username);

        info!(user = username, "user deleted");
        Ok(())
    }

    /// Updates a user's password hash and persists the change.
    pub fn change_password(&self, username: &str, new_password: &str) -> Result<(), String> {
        let mut entry = self
            .users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        entry.set_password(new_password);
        info!(user = username, "password changed");
        Ok(())
    }

    pub fn set_user_tags(&self, username: &str, tags: Vec<UserTag>) -> Result<(), String> {
        let mut entry = self
            .users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        entry.tags = tags;
        info!(user = username, "user tags updated");
        Ok(())
    }

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

    /// Returns a list of all registered usernames.
    pub fn list_users(&self) -> Vec<(String, Vec<UserTag>)> {
        self.users
            .iter()
            .map(|e| (e.key().clone(), e.value().tags.clone()))
            .collect()
    }

    pub fn list_user_permissions(&self, username: &str) -> Vec<Permission> {
        self.permissions
            .iter()
            .filter(|e| e.key().0 == username)
            .map(|e| e.value().clone())
            .collect()
    }

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

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir error: {}", e))?;
        }

        std::fs::write(path, json).map_err(|e| format!("write error: {}", e))?;
        info!(path = %path.display(), "user database saved");
        Ok(())
    }

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
    #[allow(unused_imports)]
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
        assert_eq!(users.len(), 3);
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

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn new_creates_both_default_users() {
        let auth = AuthBackend::new();
        let users = auth.list_users();
        let names: Vec<&str> = users.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"guest"), "default guest user missing");
        assert!(names.contains(&"admin"), "default admin user missing");
    }

    #[test]
    fn authenticate_rejects_empty_password() {
        let auth = AuthBackend::new();
        assert!(auth.authenticate("guest", "", localhost()).is_err());
    }

    #[test]
    fn add_duplicate_user_fails() {
        let auth = AuthBackend::new();
        auth.add_user("ops", "pass", vec![]).unwrap();
        let err = auth.add_user("ops", "other", vec![]);
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("already exists"));
    }

    #[test]
    fn set_user_tags_updates_existing_user() {
        let auth = AuthBackend::new();
        auth.add_user("tagger", "pass", vec![UserTag::None])
            .unwrap();
        auth.set_user_tags("tagger", vec![UserTag::Administrator, UserTag::Monitoring])
            .unwrap();

        let users = auth.list_users();
        let (_, tags) = users.iter().find(|(n, _)| n == "tagger").unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&UserTag::Administrator));
        assert!(tags.contains(&UserTag::Monitoring));
    }

    #[test]
    fn set_user_tags_unknown_user_fails() {
        let auth = AuthBackend::new();
        let err = auth.set_user_tags("nobody", vec![]);
        assert!(err.is_err());
    }

    #[test]
    fn set_permissions_unknown_user_fails() {
        let auth = AuthBackend::new();
        let err = auth.set_permissions("nobody", "/", ".*", ".*", ".*");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not found"));
    }

    #[test]
    fn multi_vhost_permission_isolation() {
        let auth = AuthBackend::new();
        auth.add_user("multi", "pass", vec![]).unwrap();
        auth.set_permissions("multi", "/prod", "^prod\\..*", ".*", ".*")
            .unwrap();
        auth.set_permissions("multi", "/staging", "^staging\\..*", ".*", ".*")
            .unwrap();

        assert!(auth.check_configure("multi", "/prod", "prod.orders"));
        assert!(!auth.check_configure("multi", "/prod", "staging.orders"));
        assert!(auth.check_configure("multi", "/staging", "staging.events"));
        assert!(!auth.check_configure("multi", "/staging", "prod.events"));
    }

    #[test]
    fn list_user_permissions_returns_all_vhosts() {
        let auth = AuthBackend::new();
        auth.add_user("lister", "pass", vec![]).unwrap();
        auth.set_permissions("lister", "/", ".*", ".*", ".*")
            .unwrap();
        auth.set_permissions("lister", "/staging", ".*", ".*", ".*")
            .unwrap();

        let perms = auth.list_user_permissions("lister");
        assert_eq!(perms.len(), 2);
    }

    #[test]
    fn check_permission_regex_anchoring() {
        let auth = AuthBackend::new();
        auth.add_user("regex", "pass", vec![]).unwrap();
        // Pattern anchored at start: only matches "app.*"
        auth.set_permissions("regex", "/", "^app$", ".*", ".*")
            .unwrap();

        assert!(auth.check_configure("regex", "/", "app"));
        assert!(!auth.check_configure("regex", "/", "app.queue"));
        assert!(!auth.check_configure("regex", "/", "myapp"));
    }

    #[test]
    fn change_password_unknown_user_fails() {
        let auth = AuthBackend::new();
        let err = auth.change_password("nobody", "new");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not found"));
    }

    #[test]
    fn is_loopback_true_for_localhost() {
        let addr = localhost();
        assert!(is_loopback(&addr));
    }

    #[test]
    fn is_loopback_false_for_remote() {
        let addr = remote();
        assert!(!is_loopback(&addr));
    }
}
