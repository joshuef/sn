// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::super::{Error, PublicKey, Result};
use super::Action;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, hash::Hash};

/// Wrapper type for permissions, which can be public or private.
#[derive(Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub enum Policy {
    /// Public permissions.
    Public(PublicPolicy),
    /// Private permissions.
    Private(PrivatePolicy),
}

impl Policy {
    /// Returns true if `action` is allowed for the provided user.
    pub fn is_action_allowed(&self, requester: User, action: Action) -> Result<()> {
        match self {
            Policy::Public(policy) => policy.is_action_allowed(requester, action),
            Policy::Private(policy) => policy.is_action_allowed(requester, action),
        }
    }

    /// Gets the permissions for a user if applicable.
    pub fn permissions(&self, user: User) -> Option<Permissions> {
        match self {
            Policy::Public(policy) => policy.permissions(user),
            Policy::Private(policy) => policy.permissions(user),
        }
    }

    /// Returns the owner.
    pub fn owner(&self) -> &User {
        match self {
            Policy::Public(policy) => policy.owner(),
            Policy::Private(policy) => policy.owner(),
        }
    }
}

impl From<PrivatePolicy> for Policy {
    fn from(policy: PrivatePolicy) -> Self {
        Policy::Private(policy)
    }
}

impl From<PublicPolicy> for Policy {
    fn from(policy: PublicPolicy) -> Self {
        Policy::Public(policy)
    }
}

/// Wrapper type for permissions set, which can be public or private.
#[derive(Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub enum Permissions {
    /// Public permissions set.
    Public(PublicPermissions),
    /// Private permissions set.
    Private(PrivatePermissions),
}

impl From<PrivatePermissions> for Permissions {
    fn from(permission_set: PrivatePermissions) -> Self {
        Permissions::Private(permission_set)
    }
}

impl From<PublicPermissions> for Permissions {
    fn from(permission_set: PublicPermissions) -> Self {
        Permissions::Public(permission_set)
    }
}

/// Set of public permissions for a user.
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub struct PublicPermissions {
    /// `Some(true)` if the user can write.
    /// `Some(false)` explicitly denies this permission (even if `Anyone` has permissions).
    /// Use permissions for `Anyone` if `None`.
    write: Option<bool>,
}

impl PublicPermissions {
    /// Constructs a new public permission set.
    pub fn new(write: impl Into<Option<bool>>) -> Self {
        Self {
            write: write.into(),
        }
    }

    /// Sets permissions.
    pub fn set_perms(&mut self, write: impl Into<Option<bool>>) {
        self.write = write.into();
    }

    /// Returns `Some(true)` if `action` is allowed and `Some(false)` if it's not permitted.
    /// `None` means that default permissions should be applied.
    pub fn is_allowed(self, action: Action) -> Option<bool> {
        match action {
            Action::Read => Some(true), // It's public data, so it's always allowed to read it.
            Action::Write => self.write,
        }
    }
}

/// Set of private permissions for a user.
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub struct PrivatePermissions {
    /// `true` if the user can read.
    read: bool,
    /// `true` if the user can write.
    write: bool,
}

impl PrivatePermissions {
    /// Constructs a new private permission set.
    pub fn new(read: bool, write: bool) -> Self {
        Self { read, write }
    }

    /// Sets permissions.
    pub fn set_perms(&mut self, read: bool, write: bool) {
        self.read = read;
        self.write = write;
    }

    /// Returns `true` if `action` is allowed.
    pub fn is_allowed(self, action: Action) -> bool {
        match action {
            Action::Read => self.read,
            Action::Write => self.write,
        }
    }
}

/// User that can access a Register.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub enum User {
    /// Any user.
    Anyone,
    /// User identified by its public key.
    Key(PublicKey),
}

/// Public permissions.
#[derive(Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub struct PublicPolicy {
    /// An owner could represent an individual user, or a group of users,
    /// depending on the `public_key` type.
    pub owner: User,
    /// Map of users to their public permission set.
    pub permissions: BTreeMap<User, PublicPermissions>,
}

impl PublicPolicy {
    /// Returns `Ok(())` if `action` is allowed for the provided user and `Err(AccessDenied)` if
    /// this action is not permitted.
    pub fn is_action_allowed(&self, requester: User, action: Action) -> Result<()> {
        // First checks if the requester is the owner.
        if action == Action::Read || requester == self.owner {
            Ok(())
        } else {
            match self
                .is_action_allowed_by_user(&requester, action)
                .or_else(|| self.is_action_allowed_by_user(&User::Anyone, action))
            {
                Some(true) => Ok(()),
                Some(false) => Err(Error::AccessDenied(requester)),
                None => Err(Error::AccessDenied(requester)),
            }
        }
    }

    /// Returns `Some(true)` if `action` is allowed for the provided user and `Some(false)` if it's
    /// not permitted. `None` means that default permissions should be applied.
    fn is_action_allowed_by_user(&self, user: &User, action: Action) -> Option<bool> {
        self.permissions
            .get(user)
            .and_then(|perms| perms.is_allowed(action))
    }

    /// Gets the permissions for a user if applicable.
    pub fn permissions(&self, user: User) -> Option<Permissions> {
        if user == self.owner {
            // i.e. it won't be possible to circumvent the semantics of `owner`
            // by setting some other permissions for the user.
            // the permissions can still be kept in the state though, so that switching owners gives an immediate permission update as well
            Some(Permissions::Public(PublicPermissions::new(true)))
        } else {
            self.permissions.get(&user).map(|p| Permissions::Public(*p))
        }
    }

    /// Returns the owner.
    pub fn owner(&self) -> &User {
        &self.owner
    }
}

/// Private permissions.
#[derive(Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub struct PrivatePolicy {
    /// An owner could represent an individual user, or a group of users,
    /// depending on the `public_key` type.
    pub owner: User,
    /// Map of users to their private permission set.
    pub permissions: BTreeMap<User, PrivatePermissions>,
}

impl PrivatePolicy {
    /// Returns `Ok(())` if `action` is allowed for the provided user and `Err(AccessDenied)` if
    /// this action is not permitted.
    pub fn is_action_allowed(&self, requester: User, action: Action) -> Result<()> {
        // First checks if the requester is the owner.
        if requester == self.owner {
            Ok(())
        } else {
            match self.permissions.get(&requester) {
                Some(perms) => {
                    if perms.is_allowed(action) {
                        Ok(())
                    } else {
                        Err(Error::AccessDenied(requester))
                    }
                }
                None => Err(Error::AccessDenied(requester)),
            }
        }
    }

    /// Gets the permissions for a user if applicable.
    pub fn permissions(&self, user: User) -> Option<Permissions> {
        if user == self.owner {
            // i.e. it won't be possible to circumvent the semantics of `owner`
            // by setting some other permissions for the user.
            // the permissions can still be kept in the state though, so that switching owners gives an immediate permission update as well
            Some(Permissions::Private(PrivatePermissions::new(true, true)))
        } else {
            match user {
                User::Anyone => None,
                user @ User::Key(_) => self
                    .permissions
                    .get(&user)
                    .map(|p| Permissions::Private(*p)),
            }
        }
    }

    /// Returns the owner.
    pub fn owner(&self) -> &User {
        &self.owner
    }
}
