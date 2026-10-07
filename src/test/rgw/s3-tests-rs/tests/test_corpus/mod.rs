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
//! Driven by the environment, as `test_bench` is:
//!
//! ```text
//! CORPUS_TIER   coverage (default) | interaction | scale
//! CORPUS_OUT    where to write the manifest;  default target/corpus
//! ```

mod names;
mod manifest;
mod generate;
