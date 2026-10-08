//! The S3 API surface.
//!
//! Translated from the Python s3-tests suite, vendored in the Ceph
//! tree at `src/test/rgw/s3-tests/`:
//!
//!   s3tests/functional/test_s3.py       -> most of this module
//!   s3tests/functional/test_headers.py  -> headers.rs
//!   src/test/rgw/rgw_multi/tests.py     -> two tests, in acl.rs and
//!                                          object_ops.rs
//!
//! Names are kept identical to the Python ones wherever a test was
//! translated, which is what makes the correspondence recoverable by
//! grep.  Tests with no Python counterpart are this suite's own.

mod acl;
mod bucket_list;
mod bucket_logging;
mod bucket_policy;
mod conditional;
mod copy;
mod cors;
mod encryption;
mod headers;
mod lifecycle;
mod signing;
mod multipart;
mod object_lock;
mod post_object;
mod object_ops;
mod stress;
mod tagging;
mod versioning;
