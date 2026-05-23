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

/// Defines the various states or variants of user tag.
///
/// Defines details for user tag inside the broker ecosystem.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UserTag {
    Administrator,
    Management,
    Monitoring,
    None,
}

/// Represents the schema or state for user entry.
///
/// Defines details for user entry inside the broker ecosystem.
pub struct UserEntry {
    pub username: String,
    /// bcrypt hash of the password.
    password_hash: String,
    pub tags: Vec<UserTag>,
}

impl UserEntry {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `username` - `&str`: The unique identifier string of the resource.
    /// * `password` - `&str`: The `password` argument.
    /// * `tags` - `Vec<UserTag>`: The `tags` argument.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new(username: &str, password: &str, tags: Vec<UserTag>) -> Self {
        Self {
            username: username.to_string(),
            password_hash: hash_password(password),
            tags,
        }
    }

    /// Executes the standard verify password lifecycle step.
    ///
    /// Executes the required business logic for verify password.
    ///
    /// # Arguments
    ///
    /// * `password` - `&str`: The `password` argument.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub fn verify_password(&self, password: &str) -> bool {
        bcrypt::verify(password, &self.password_hash).unwrap_or(false)
    }

    /// Executes the standard set password lifecycle step.
    ///
    /// Executes the required business logic for set password.
    ///
    /// # Arguments
    ///
    /// * `password` - `&str`: The `password` argument.
    pub fn set_password(&mut self, password: &str) {
        self.password_hash = hash_password(password);
    }

    /// Executes the standard to serializable lifecycle step.
    ///
    /// Executes the required business logic for to serializable.
    ///
    /// # Returns
    ///
    /// * `SerializableUser` - The evaluated outcome or operation handle.
    pub fn to_serializable(&self) -> SerializableUser {
        SerializableUser {
            username: self.username.clone(),
            password_hash: self.password_hash.clone(),
            tags: self.tags.clone(),
        }
    }

    /// Executes the standard from serializable lifecycle step.
    ///
    /// Executes the required business logic for from serializable.
    ///
    /// # Arguments
    ///
    /// * `su` - `SerializableUser`: The `su` argument.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn from_serializable(su: SerializableUser) -> Self {
        Self {
            username: su.username,
            password_hash: su.password_hash,
            tags: su.tags,
        }
    }
}

/// Represents the schema or state for serializable user.
///
/// Defines details for serializable user inside the broker ecosystem.
#[derive(Serialize, Deserialize)]
pub struct SerializableUser {
    pub username: String,
    pub password_hash: String,
    pub tags: Vec<UserTag>,
}

/// Represents the schema or state for user store.
///
/// Defines details for user store inside the broker ecosystem.
#[derive(Serialize, Deserialize)]
pub struct UserStore {
    pub users: Vec<SerializableUser>,
    pub permissions: Vec<super::permissions::Permission>,
}

/// Executes the standard hash password lifecycle step.
///
/// Executes the required business logic for hash password.
///
/// # Arguments
///
/// * `password` - `&str`: The `password` argument.
///
/// # Returns
///
/// * `String` - The evaluated outcome or operation handle.
fn hash_password(password: &str) -> String {
    bcrypt::hash(password, crate::config::bcrypt_cost()).expect("bcrypt hash should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Executes the standard password hash verify lifecycle step.
    ///
    /// Executes the required business logic for password hash verify.
    #[test]
    fn password_hash_verify() {
        let user = UserEntry::new("alice", "s3cret", vec![UserTag::Administrator]);
        assert!(user.verify_password("s3cret"));
        assert!(!user.verify_password("wrong"));
    }

    /// Executes the standard password change lifecycle step.
    ///
    /// Executes the required business logic for password change.
    #[test]
    fn password_change() {
        let mut user = UserEntry::new("bob", "old", vec![]);
        assert!(user.verify_password("old"));
        user.set_password("new");
        assert!(!user.verify_password("old"));
        assert!(user.verify_password("new"));
    }

    /// Executes the standard serialization roundtrip lifecycle step.
    ///
    /// Executes the required business logic for serialization roundtrip.
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

    /// Executes the standard hash is unique lifecycle step.
    ///
    /// Executes the required business logic for hash is unique.
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
