//! The conversion corpus's artifact formats, and the environment that
//! drives it.
//!
//! The corpus itself -- the axes that write objects and record what S3
//! returns -- is the `test_corpus` test target.  What lives here is what
//! something other than the corpus has to read:  the manifest and
//! inventory formats, writing and parsing both, so a comparison of two
//! captures reads them with the code that wrote them;  and the names of
//! the environment variables a workflow driver such as
//! `adoption/standup.rs` sets to run it.

pub mod compare;
pub mod env;
pub mod inventory;
pub mod manifest;
