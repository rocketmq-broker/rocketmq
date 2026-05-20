//! User credential management with bcrypt password hashing.

use serde::{Deserialize, Serialize};

/// Tags that define a user's administrative role.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UserTag {
    Administrator,
    Management,
    Monitoring,
    None,
}

/// A user entry in the credential store.
pub struct UserEntry {
    pub username: String,
    /// bcrypt hash of the password.
    password_hash: String,
    pub tags: Vec<UserTag>,
}

impl UserEntry {
    /// Create a new user with a plaintext password (hashed immediately).
    pub fn new(username: &str, password: &str, tags: Vec<UserTag>) -> Self {
        Self {
            username: username.to_string(),
            password_hash: hash_password(password),
            tags,
        }
    }

    /// Verify a plaintext password against the stored hash.
    pub fn verify_password(&self, password: &str) -> bool {
        bcrypt::verify(password, &self.password_hash).unwrap_or(false)
    }

    /// Change the password (re-hashes).
    pub fn set_password(&mut self, password: &str) {
        self.password_hash = hash_password(password);
    }

    /// Convert to a serializable form for persistence.
    pub fn to_serializable(&self) -> SerializableUser {
        SerializableUser {
            username: self.username.clone(),
            password_hash: self.password_hash.clone(),
            tags: self.tags.clone(),
        }
    }

    /// Reconstruct from a serialized form.
    pub fn from_serializable(su: SerializableUser) -> Self {
        Self {
            username: su.username,
            password_hash: su.password_hash,
            tags: su.tags,
        }
    }
}

/// Serializable user entry for JSON persistence.
#[derive(Serialize, Deserialize)]
pub struct SerializableUser {
    pub username: String,
    pub password_hash: String,
    pub tags: Vec<UserTag>,
}

/// Serializable user store for JSON persistence.
#[derive(Serialize, Deserialize)]
pub struct UserStore {
    pub users: Vec<SerializableUser>,
    pub permissions: Vec<super::permissions::Permission>,
}

fn hash_password(password: &str) -> String {
    // Cost factor 10 is the standard default (fast enough for auth, slow enough for brute-force)
    bcrypt::hash(password, 10).expect("bcrypt hash should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hash_verify() {
        let user = UserEntry::new("alice", "s3cret", vec![UserTag::Administrator]);
        assert!(user.verify_password("s3cret"));
        assert!(!user.verify_password("wrong"));
    }

    #[test]
    fn password_change() {
        let mut user = UserEntry::new("bob", "old", vec![]);
        assert!(user.verify_password("old"));
        user.set_password("new");
        assert!(!user.verify_password("old"));
        assert!(user.verify_password("new"));
    }

    #[test]
    fn serialization_roundtrip() {
        let user = UserEntry::new(
            "carol",
            "pass",
            vec![UserTag::Management, UserTag::Monitoring],
        );
        let ser = user.to_serializable();
        let json = serde_json::to_string(&ser).unwrap();
        let deser: SerializableUser = serde_json::from_str(&json).unwrap();
        let restored = UserEntry::from_serializable(deser);
        assert_eq!(restored.username, "carol");
        assert!(restored.verify_password("pass"));
        assert_eq!(restored.tags.len(), 2);
    }

    #[test]
    fn hash_is_unique() {
        // Two hashes of the same password should differ (bcrypt uses random salt)
        let h1 = hash_password("same");
        let h2 = hash_password("same");
        assert_ne!(h1, h2);
        // But both verify
        assert!(bcrypt::verify("same", &h1).unwrap());
        assert!(bcrypt::verify("same", &h2).unwrap());
    }
}
