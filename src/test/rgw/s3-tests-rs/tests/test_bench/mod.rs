//! Calibrated workload runs, not conformance tests.
//!
//! Automation for an `elbencho` workload suite:  the point is a
//! repeatable measurement rather than a pass or a fail, so these do
//! not translate anything and have no Python counterpart.  They live
//! in this suite because the harness is here -- the client, the INI
//! config, nextest's process isolation and the profile mechanism.
//!
//! Requires `elbencho` on the host.

mod elbencho;
mod multipart;
