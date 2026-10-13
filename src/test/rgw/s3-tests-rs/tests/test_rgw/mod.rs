//! RGW's own surface:  the extensions this project added, and one
//! translated workunit.
//!
//! Original to this project, with no external source -- Admin Ops
//! coverage, the features query, driver hints, bucket profiles, the
//! nsfs identity records, impersonation.
//!
//! The exception is `quota.rs`, translated from the Ceph qa workunit
//! `qa/workunits/rgw/s3_user_quota.pl`.  See its own header.

mod admin;
mod bucket_profile;
mod driver_hint;
mod features;
mod identity;
mod impersonation;
mod key_limit;
mod key_separators;
mod quota;
mod reserved_names;
mod root_object;
