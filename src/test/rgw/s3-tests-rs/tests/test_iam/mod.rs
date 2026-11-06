//! Identity:  IAM, STS and accounts.
//!
//! Translated from the vendored Python s3-tests suite in the Ceph tree:
//!
//!   s3tests/functional/test_iam.py  -> most of this module
//!   s3tests/functional/test_sts.py  -> one test, in sts.rs
//!
//! Python names are preserved for the translated tests.  Much of this
//! module has no Python counterpart:  `test_iam.py` was only partly
//! ported and the account tests are new.

mod group;
mod oidc;
mod role;
mod sts;
mod user;
mod user_policy;
