//! The capability query itself.
//!
//! There is no test here whose *expectation* yet depends on the backend --
//! features are added when a caller needs one, and the first callers are in
//! the librgw suite.  What this covers is the wiring: that the deployment
//! answers, that what comes back is a capability document rather than a
//! 200 from something else, and that the values a backend claims are
//! self-consistent.  Without it the accessors would return None forever and
//! nothing would say so.

use s3_tests_rs::features::features;

#[tokio::test]
async fn test_features_reported_are_coherent() {
    let Some(f) = features().await else {
        /* Not a failure.  A backend that does not report, or credentials
         * without info=read, is the normal case against anything but a
         * development gateway -- including AWS, which this suite is also
         * run against. */
        eprintln!("no features reported;  nothing to check");
        return;
    };

    let backend = f.backend.as_deref().unwrap_or("");
    assert!(!backend.is_empty(),
        "a capability document without a backend name");

    if backend != "nsfs" {
        return;
    }

    /* nsfs reports these, and their meanings are fixed in
     * NSFSDriver::get_features().  Absent would mean the endpoint answered
     * for a driver that does not know itself. */
    assert_eq!(f.get_bool("fsio"), Some(true),
        "nsfs implements the FSIO interfaces and should say so");
    assert!(f.get_str("strategy").is_some(),
        "nsfs selects an FSStrategy at startup and should report which");
    assert!(f.get_bool("can_rename").is_some(), "can_rename not reported");
    assert!(f.get_bool("shares_extents").is_some(),
        "shares_extents not reported");

    /* GPFS shares no extents at any granularity reachable from the VFS
     * (probes/results-cowprobe-2026-09-22.txt) and deliberately declines
     * to move names.  If either of these ever reads true on gpfs, it is
     * news and not a test bug. */
    if f.get_str("strategy") == Some("gpfs") {
        assert_eq!(f.get_bool("shares_extents"), Some(false),
            "gpfs reported extent sharing;  copy_file_range used to return \
             EOPNOTSUPP there");
        assert_eq!(f.get_bool("can_rename"), Some(false),
            "gpfs reported that it moves names;  it declines to, by \
             decision rather than by accident");
    }
}
