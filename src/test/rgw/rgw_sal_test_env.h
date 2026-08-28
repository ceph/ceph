// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 *
 * Test-only helper that brings up a ceph context and a SAL driver, so that
 * test harnesses which do not own main() -- notably Rust's libtest, used by
 * the lancedb-rgw-store `cargo test` binary -- can still exercise the
 * rgw_sal_wrapper C API against a live cluster.
 *
 * Built into libceph_rgw_sal_test_env.so, which re-exports the whole
 * rgw_sal_wrapper API alongside the entry points below.  See
 * src/rgw/lancedb-rgw-store/tests/object_store.rs for the consumer.
 */

#pragma once

#include "rgw/rgw_sal_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Bring up the ceph context and SAL driver.
 *
 * Idempotent and thread-safe: concurrent callers block until the first
 * caller finishes, and every caller observes the same result.  This matters
 * because libtest runs tests on several threads with no global setup hook.
 *
 * There is no argv to parse -- libtest owns it -- so configuration comes
 * from the environment: $CEPH_CONF for the config file and $CEPH_ARGS for
 * anything else (e.g. CEPH_ARGS="--rgw-backend-store dbstore").
 *
 * Registers an atexit() handler that removes every bucket created through
 * rgw_test_env_create_bucket() and shuts the driver down.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_test_env_init(void);

/**
 * SAL driver handle, or NULL if rgw_test_env_init() has not succeeded.
 */
CRgwDriver* rgw_test_env_driver(void);

/**
 * DoutPrefixProvider handle, or NULL if rgw_test_env_init() has not succeeded.
 */
const CRgwDoutPrefix* rgw_test_env_dpp(void);

/**
 * Name of the backend in use ("rados", "dbstore", "posix", ...), or NULL if
 * rgw_test_env_init() has not succeeded.  Lets a test skip cases the backend
 * does not implement.
 */
const char* rgw_test_env_backend(void);

/**
 * Create a bucket, remembering it for teardown at exit.
 *
 * Succeeds if the bucket already exists.  Giving each test its own bucket is
 * what the arrow-rs conformance suite assumes, and it keeps libtest's
 * thread-parallel execution safe.
 *
 * @param name   bucket name, null-terminated
 * @param tenant tenant, null-terminated (NULL for the default tenant)
 * @return 0 on success, negative errno on failure
 */
int rgw_test_env_create_bucket(const char* name, const char* tenant);

/**
 * Remove a bucket and everything in it, and forget it for teardown purposes.
 *
 * Optional: any bucket left behind by a panicking test is removed at exit.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_test_env_remove_bucket(const char* name, const char* tenant);

#ifdef __cplusplus
}
#endif
