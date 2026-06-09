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
// File: credentials.rs
// Description: User credential validation, password hashing, and user storage.

//! User credential management with bcrypt password hashing.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UserTag {
    Administrator,
    Management,
    Monitoring,
    None,
}

/// A single user record containing the username, bcrypt password hash,
/// and the set of permission tags (e.g. `administrator`, `management`).
pub struct UserEntry {
    pub username: String,
    /// bcrypt hash of the password.
    password_hash: String,
    pub tags: Vec<UserTag>,
}

impl UserEntry {
    /// Creates a new instance with the given username, password, tags.
    pub fn new(username: &str, password: &str, tags: Vec<UserTag>) -> Self {
        Self {
            username: username.to_string(),
            password_hash: hash_password(password),
            tags,
        }
    }

    /// Verifies a plaintext password against a bcrypt hash.
    pub fn verify_password(&self, password: &str) -> bool {
        bcrypt::verify(password, &self.password_hash).unwrap_or(false)
    }

    pub fn set_password(&mut self, password: &str) {
        self.password_hash = hash_password(password);
    }

    pub fn to_serializable(&self) -> SerializableUser {
        SerializableUser {
            username: self.username.clone(),
            password_hash: self.password_hash.clone(),
            tags: self.tags.clone(),
        }
    }

    pub fn from_serializable(su: SerializableUser) -> Self {
        Self {
            username: su.username,
            password_hash: su.password_hash,
            tags: su.tags,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerializableUser {
    pub username: String,
    pub password_hash: String,
    pub tags: Vec<UserTag>,
}

#[derive(Serialize, Deserialize)]
pub struct UserStore {
    pub users: Vec<SerializableUser>,
    pub permissions: Vec<super::permissions::Permission>,
}

/// Hashes a plaintext password using bcrypt with the default cost factor.
fn hash_password(password: &str) -> String {
    bcrypt::hash(password, crate::config::bcrypt_cost()).expect("bcrypt hash should not fail")
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
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
        let h1 = hash_password("same");
        let h2 = hash_password("same");
        assert_ne!(h1, h2);

        assert!(bcrypt::verify("same", &h1).unwrap());
        assert!(bcrypt::verify("same", &h2).unwrap());
    }
}
