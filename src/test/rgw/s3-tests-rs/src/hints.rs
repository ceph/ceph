//! Driver hints that cannot outlive the test that set one.
//!
//! A hint is gateway-wide.  `inject-etag-as-gateway` or
//! `access-model` applies to every request the daemon serves, not to
//! the connection that asked for it -- so a test which dies between
//! setting one and putting it back leaves the whole run measuring a
//! gateway nobody configured.  That is not hypothetical:  on
//! 2026-10-17 a test panicked inside an injected window, the flag
//! stayed on, and the *next* run failed on its first object for a
//! reason nothing in that run had done.
//!
//! [`with_hint`] owns the window instead of asking each test to
//! remember.  The body runs between the set and the clear, and the
//! clear happens on the panicking path too.
//!
//! **What it cannot cover**, and why [`clear_hint`] exists as well:
//! a process that dies without unwinding -- a harness timeout kill,
//! an abort -- never runs the restore at all.  No client-side guard
//! can fix that, so a test which uses a hint also clears it on the
//! way in, and a leak from somebody else's death heals rather than
//! compounding.
//!
//! The real fix is not here:  an injection scoped in the driver, to
//! the authenticated user rather than to the daemon, would need no
//! discipline from the suite at all.

use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};

use futures_util::FutureExt;

use crate::admin::driver_hint_results;

/// Turn a hint on, run `body`, turn it off -- including when `body`
/// panics, after which the panic continues on its way.
///
/// The return value is the body's, so a test reads as it would
/// without the hint.  A gateway with no such hint is not an error
/// here:  `driver_hint_results` returns `None` and the body still
/// runs, which is what lets a test of a hinted behaviour skip on a
/// backend that has none.
pub async fn with_hint<F, T>(hint: &str, params: &[(&str, &str)], body: F) -> T
where
    F: Future<Output = T>,
{
    let _ = driver_hint_results(hint, params).await;
    let out = AssertUnwindSafe(body).catch_unwind().await;
    let _ = driver_hint_results(hint, &[("enable", "false")]).await;
    match out {
        Ok(v) => v,
        Err(panic) => resume_unwind(panic),
    }
}

/// Turn an injection hint off without caring whether it was on.
///
/// Called by a test on the way *in*, so that a hint some other test
/// leaked -- by being killed rather than by panicking, which
/// [`with_hint`] already handles -- does not decide this test's
/// result.
pub async fn clear_hint(hint: &str) {
    let _ = driver_hint_results(hint, &[("enable", "false")]).await;
}
