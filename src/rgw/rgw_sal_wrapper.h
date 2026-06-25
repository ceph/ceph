// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 *
 * This file provides C wrapper functions for RGW SAL that are mainly
 * called by Rust crate (defined in rust/rgw/lancedb-rgw-store) via FFI.
 * Any updates to these functions should reflected in the FFI bindings defined
 * in the corresponding Rust code as well.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#define RGW_SAL_WRAPPER_VERSION_MAJOR 1
#define RGW_SAL_WRAPPER_VERSION_MINOR 0

#ifdef __cplusplus
extern "C" {
#endif

/*==========================================================================
 * Opaque handle types for C++ objects passed through FFI.
 * These provide basic type safety instead of void*.
 *=========================================================================*/
typedef struct CRgwDriver CRgwDriver;       /* rgw::sal::Driver */
typedef struct CRgwDoutPrefix CRgwDoutPrefix; /* DoutPrefixProvider */
typedef struct CRgwYieldContext CRgwYieldContext; /* optional_yield; NULL = null_yield (blocking) */

/**
 * Bucket identifier (name + optional tenant)
 */
typedef struct CRgwBucket {
  const char* name;      /* Bucket name, null-terminated */
  const char* tenant;    /* Tenant, null-terminated (NULL for default tenant) */
} CRgwBucket;

/**
 * Object identifier (key + optional version)
 */
typedef struct CRgwObject {
  const char* key;         /* Object key, null-terminated */
  const char* version_id;  /* Version ID, null-terminated (NULL for current version) */
} CRgwObject;

/**
 * Buffer for receiving data from RGW
 */
typedef struct CRgwBuffer {
  uint8_t* data;      /* Pointer to data (allocated by RGW) */
  size_t len;         /* Length of data */
} CRgwBuffer;

/**
 * Object metadata
 */
typedef struct CRgwObjectMeta {
  uint64_t size;          /* Object size in bytes */
  char* etag;             /* ETag (MD5 hash), null-terminated */
  char* content_type;     /* Content type, null-terminated */
  int64_t last_modified;  /* Last modified timestamp */
} CRgwObjectMeta;

/**
 * Single entry in a list operation result
 */
typedef struct CRgwListEntry {
  char* key;              /* Object key, null-terminated */
  char* version_id;       /* Version ID, null-terminated (NULL for current version) */
  uint64_t size;          /* Object size in bytes */
  int64_t last_modified;  /* Last modified timestamp */
} CRgwListEntry;

/**
 * Result of a list objects operation
 */
typedef struct CRgwListResult {
  CRgwListEntry* entries;  /* Array of list entries */
  size_t count;           /* Number of entries */
  int is_truncated;       /* True (1) if there are more results */
  char* next_marker;      /* Marker for next page, null-terminated */
} CRgwListResult;

/*==========================================================================
 * Memory Ownership
 *=========================================================================
 * Input parameters:
 *   - All string parameters (bucket, key, etc.) are borrowed (caller retains
 *     ownership). They must remain valid for the duration of the call.
 *
 * Output parameters:
 *   - CRgwBuffer: Allocated by rgw_get_object, caller must free with
 *     rgw_free_buffer().
 *   - CRgwObjectMeta: Allocated by rgw_head_object, caller must free with
 *     rgw_free_object_meta() (frees etag and content_type strings).
 *   - CRgwListResult: Allocated by rgw_list_objects, caller must free with
 *     rgw_free_list_result() (frees all entry keys and next_marker).
 *   - upload_id/etag strings: Allocated by rgw_init_multipart / rgw_multipart_put_part,
 *     caller must free with free().
 *=========================================================================*/

/*==========================================================================
 * Core Object Operations
 *
 * All functions return 0 on success, negative errno on failure (e.g. -ENOENT,
 * -EINVAL, -ENOMEM, -EIO).
 *
 * Common parameters (present on every function):
 *   driver    - CRgwDriver* (opaque handle to rgw::sal::Driver)
 *   dpp       - CRgwDoutPrefix* (opaque handle to DoutPrefixProvider, for logging)
 *   yield_ctx - CRgwYieldContext* (opaque handle to optional_yield, NULL for blocking)
 *=========================================================================*/

/**
 * Write an object to storage (unconditional overwrite).
 *
 * Creates the object if it doesn't exist, or replaces it if it does.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_put_object( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, const CRgwBuffer* buffer);

/**
 * Write an object with conditional preconditions.
 *
 * Atomically writes data only if the specified precondition is met.
 * Exactly one of if_match/if_nomatch should be non-NULL.
 *
 * @return 0 on success (check *canceled for precondition result), negative errno on failure
 */
int rgw_put_object_conditional( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, const CRgwBuffer* buffer,
  const char* if_match, const char* if_nomatch, int* canceled);

/**
 * Read an object (or a byte range) from storage.
 *
 * @param buffer    [out] Receives allocated data; caller must free with rgw_free_buffer()
 *
 * @return 0 on success, -ENOENT if object not found, negative errno on failure
 */
int rgw_get_object( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, uint64_t offset, uint64_t length,
  CRgwBuffer* buffer);

/**
 * Delete a single object from storage.
 *
 * Deleting a non-existent object is treated as success (returns 0).
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_object( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj);

/**
 * Get object metadata without reading content.
 *
 * @param meta      [out] Populated on success; caller must free with rgw_free_object_meta()
 *
 * @return 0 on success, -ENOENT if object not found, negative errno on failure
 */
int rgw_head_object( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, CRgwObjectMeta* meta);

/**
 * List objects in a bucket by prefix, with optional delimiter for hierarchy.
 *
 * @param result    [out] Populated on success; caller must free with rgw_free_list_result()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_list_objects( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const char* prefix, const char* delimiter, const char* marker,
  uint32_t max_keys, CRgwListResult* result);

/**
 * Copy an object within or across buckets (unconditional overwrite at destination).
 *
 * @return 0 on success, -ENOENT if source not found, negative errno on failure
 */
int rgw_copy_object( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* src_bucket, const CRgwObject* src_obj, const CRgwBucket* dst_bucket,
  const CRgwObject* dst_obj);

/**
 * Copy an object with preconditions on the destination.
 *
 * @return 0 on success, -EEXIST if destination exists (when if_nomatch="*"),
 *         negative errno on failure
 */
int rgw_copy_object_conditional( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* src_bucket, const CRgwObject* src_obj, const CRgwBucket* dst_bucket,
  const CRgwObject* dst_obj, const char* if_match, const char* if_nomatch);

/**
 * Delete multiple objects in one call.
 *
 * Attempts to delete all keys. Individual failures are logged and counted.
 * Deleting a non-existent object is treated as success.
 *
 * @return 0 if all deletes succeeded, -EIO if any failed
 */
int rgw_delete_objects( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const char* const* keys, size_t count);

/*==========================================================================
 * Multipart Upload Operations
 *
 * Multipart uploads allow writing large objects in parts. The workflow is:
 *   1. rgw_init_multipart()       - start upload, get upload_id
 *   2. rgw_multipart_put_part()   - upload each part (1-indexed), get part ETags
 *   3. rgw_multipart_complete()   - finalize with ordered ETags, assembles the object
 *   On failure at any step, call rgw_multipart_abort() to clean up.
 *=========================================================================*/

/**
 * Start a multipart upload and obtain an upload ID.
 *
 * @param upload_id  [out] Populated on success; caller must free with free()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_init_multipart( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, char** upload_id);

/**
 * Upload one part of a multipart upload.
 *
 * @param etag  [out] Populated on success; caller must free with free()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_put_part( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, const char* upload_id, uint32_t part_num,
  const uint8_t* data, size_t len, char** etag);

/**
 * Complete a multipart upload, assembling parts into the final object.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_complete( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, const char* upload_id,
  const char* const* etags, size_t count);

/**
 * Abort a multipart upload and discard all uploaded parts.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_abort( CRgwDriver* driver, const CRgwDoutPrefix* dpp, CRgwYieldContext* yield_ctx,
  const CRgwBucket* bucket, const CRgwObject* obj, const char* upload_id);

/**
 * Free a buffer allocated by rgw_get_object
 */
void rgw_free_buffer(CRgwBuffer* buffer);

/**
 * Free metadata allocated by rgw_head_object
 */
void rgw_free_object_meta(CRgwObjectMeta* meta);

/**
 * Free list result allocated by rgw_list_objects
 */
void rgw_free_list_result(CRgwListResult* result);

/**
 * Get the configured rgw_max_chunk_size (in bytes)
 */
uint64_t rgw_get_max_chunk_size(CRgwDriver* driver);

/**
 * Get the SAL wrapper API version string.
 *
 * @return Null-terminated version string (e.g., "1.0")
 */
const char* rgw_sal_wrapper_version(void);

#ifdef __cplusplus
}
#endif
