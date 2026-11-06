//! The conversion corpus:  artifacts and a manifest, not assertions.
//!
//! A modality of this suite rather than a separate tool, because the
//! suite already is an enumeration of S3 features and already carries
//! the client, the config and the recipe primitives.  Its output is a
//! set of artifacts on a gateway and a line-oriented manifest of what
//! S3 says about them;  comparing two gateways is a later pass.
//!
//! `docs/NSFS_CORPUS.md` in the nsfs design notes is the design.
//!
//! **Nothing it produces is committed.**  The manifests and
//! inventories are output, regenerable by definition, and there is no
//! expected copy in the repository to diff against.  A golden manifest
//! was considered and rejected:  it records what this implementation
//! happened to do, so it would have enshrined the defects that were in
//! the first run's output as the expected result.  What the run asserts
//! instead are properties true of any S3 implementation -- see
//! `check_listing_invariants` -- and comparisons are between two
//! captures, never against a file in the tree.
//!
//! Driven by the environment, as `test_bench` is.  The variables are
//! listed, with their defaults, in `s3_tests_rs::corpus::env`.  The
//! manifest and inventory formats are `s3_tests_rs::corpus::manifest`
//! and `::inventory`, in the library so a comparison of two captures
//! reads them with the code that wrote them.

mod names;
mod shapes;
mod lock;
use s3_tests_rs::corpus::{env, inventory, manifest};
mod buckets;
mod copy;
mod generate;
mod identity;
mod integrity;
mod multipart;
mod objects;
mod storage;
mod versions;
