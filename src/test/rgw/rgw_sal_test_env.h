// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

/*
 * Shared test environment for the LanceDB ObjectStore integration tests.
 *
 * The ObjectStore tests are Rust `cargo test` functions, and libtest owns
 * main() -- so the ceph context and SAL driver cannot be brought up from the
 * test itself the way our old C++ harness (test_rgw_lancedb_object_store.cc)
 * did.  Instead the same bring-up lives here behind a small extern "C" API and
 * is compiled into libceph_rgw_sal_test_env.so, which the cargo test binary
 * links against.  Because that library links rgw_common it also satisfies the
 * rgw_sal_wrapper symbols the Rust FFI needs, so the test binary links a single
 * .so rather than the whole RGW C++ link line.
 *
 * Consumer: src/rgw/lancedb-rgw-store/tests/object_store.rs
 */

#pragma once

#include "rgw/rgw_sal_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initialize the ceph context and SAL driver.  Safe to call from any number
 * of threads: the first call performs the work, the rest wait for it and see
 * the same result.  libtest has no global setup hook, so every test calls this
 * and only the first one pays for it.
 *
 * Configuration comes from the environment ($CEPH_CONF, $CEPH_ARGS) since there
 * is no argv to parse.  An atexit() handler is registered to drop any buckets
 * still tracked and shut the driver down.
 *
 * Returns 0 on success, negative errno otherwise.
 */
int rgw_test_env_init(void);

/* SAL driver handle, or NULL before a successful rgw_test_env_init(). */
CRgwDriver* rgw_test_env_driver(void);

/* DoutPrefixProvider handle, or NULL before a successful rgw_test_env_init(). */
const CRgwDoutPrefix* rgw_test_env_dpp(void);

/* Backend name ("rados", "dbstore", "posix", ...), or NULL if uninitialized. */
const char* rgw_test_env_backend(void);

/*
 * Create a bucket and remember it for teardown.  Succeeds if it already
 * exists.  Each test uses its own bucket so libtest can run them in parallel.
 * tenant may be NULL for the default tenant.
 *
 * Returns 0 on success, negative errno otherwise.
 */
int rgw_test_env_create_bucket(const char* name, const char* tenant);

/*
 * Remove a bucket (and its contents) and stop tracking it.  Any bucket left
 * behind by a failing test is cleaned up at exit instead.
 *
 * Returns 0 on success, negative errno otherwise.
 */
int rgw_test_env_remove_bucket(const char* name, const char* tenant);

#ifdef __cplusplus
}
#endif
