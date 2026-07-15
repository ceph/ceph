/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

/*
 * LanceDB RGW Backend Integration - C API
 *
 * This header provides the C API for creating a custom ObjectStoreProvider
 * that routes S3 operations through Ceph RGW's native SAL API.
 *
 * All s3:// URLs will automatically be routed through RGW SAL.
 */

#ifndef LANCEDB_RGW_STORE_H
#define LANCEDB_RGW_STORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declaration - defined in lancedb-c/lancedb.h */
struct LanceDBObjectStoreProvider;

/**
 * Create an ObjectStoreProvider that routes S3 operations through RGW SAL.
 *
 * The returned provider can be inserted into an ObjectStoreRegistry via
 * lancedb_registry_insert_provider() from lancedb.h.
 *
 * @param driver  Non-null pointer to rgw::sal::Driver (env.driver in RGW handlers)
 * @param dpp     Non-null pointer to DoutPrefixProvider for logging
 *
 * @return Non-null pointer to LanceDBObjectStoreProvider on success,
 *         NULL if either driver or dpp is NULL
 *
 * @note Caller must either:
 *   - Pass to lancedb_registry_insert_provider() (transfers ownership), OR
 *   - Free with rgw_lancedb_free_provider()
 * @note Both driver and dpp must remain valid for provider lifetime
 */
struct LanceDBObjectStoreProvider* rgw_lancedb_create_provider(void* driver, const void* dpp);

/**
 * Free a provider created by rgw_lancedb_create_provider.
 *
 * Only call this if the provider was NOT passed to lancedb_registry_insert_provider().
 * If it was inserted into a registry, the registry owns it.
 *
 * @param provider  Provider pointer to free (safe to pass NULL)
 */
void rgw_lancedb_free_provider(struct LanceDBObjectStoreProvider* provider);

/*===========================================================================
 * Version Information
 *===========================================================================*/

/**
 * Get the version string for this library.
 *
 * @return Null-terminated version string (e.g., "0.1.0")
 */
const char* lancedb_rgw_store_version(void);

#ifdef __cplusplus
}
#endif

#endif /* LANCEDB_RGW_STORE_H */
