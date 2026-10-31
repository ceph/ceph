//! The environment variables that drive the corpus.
//!
//! The one list of them.  `adoption/standup.rs` sets them and has no
//! dependencies to import these from, so it names them as strings and
//! cites this file.

/// `coverage` (default), `interaction` or `scale`.
pub const TIER: &str = "CORPUS_TIER";

/// Where the captures are written.  Default `target/corpus`, which is
/// build output.
pub const OUT: &str = "CORPUS_OUT";

/// `1`, `on`, `true` or `yes`:  observe mode.  The corpus writes
/// nothing and records what a tree another gateway wrote looks like;
/// assertions about its own writing are dropped.  Unset is generate
/// mode, which writes the corpus and records what happened.
pub const OBSERVE: &str = "CORPUS_OBSERVE";

/// The gateway's data root.  Set only when the run is local to it;
/// without it the disk inventory is not taken and the S3 half still
/// works.
pub const DATA_ROOT: &str = "CORPUS_DATA_ROOT";

/// A config store, for a backend that keeps a bucket's configuration in
/// a record rather than in the bucket directory's attributes (NooBaa).
/// Read generically -- a name and a digest per field, no schema -- and
/// unset for an nsfs or posix tree.
pub const CONFIG_ROOT: &str = "CORPUS_CONFIG_ROOT";

/// The mode every object must be published with, octal, default 0660;
/// `any` turns the check off.  An assertion about the tree rather than
/// about S3, so a run with a different `rgw_nsfs_umask` says so here.
pub const OBJECT_MODE: &str = "CORPUS_OBJECT_MODE";

/// The mode every directory under a bucket must have, octal, default
/// 0770;  `any` turns the check off.
pub const DIR_MODE: &str = "CORPUS_DIR_MODE";
