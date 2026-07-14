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
typedef struct RGWSalDriver RGWSalDriver;
typedef struct RGWDoutPrefix RGWDoutPrefix;
typedef struct RGWYieldContext RGWYieldContext;

/**
 * Object identifier (key + optional version)
 */
typedef struct RGWObject {
  const char* key;         /* Object key, null-terminated */
  const char* version_id;  /* Version ID, null-terminated (NULL for current version) */
} RGWObject;

/**
 * Heap-allocated string with length
 */
typedef struct RGWString {
  char* str;        /* Null-terminated string (allocated by RGW) */
  size_t len;       /* Length excluding null terminator */
} RGWString;

/**
 * Buffer for receiving data from RGW
 */
typedef struct RGWBuffer {
  uint8_t* data;      /* Pointer to data (allocated by RGW) */
  size_t len;         /* Length of data */
} RGWBuffer;

/**
 * Object metadata
 */
typedef struct RGWObjectMeta {
  uint64_t size;          /* Object size in bytes */
  char* etag;             /* ETag (MD5 hash), null-terminated */
  char* content_type;     /* Content type, null-terminated */
  int64_t last_modified;  /* Last modified timestamp */
} RGWObjectMeta;

/**
 * Single entry in a list operation result
 */
typedef struct RGWListEntry {
  char* key;              /* Object key, null-terminated */
  uint64_t size;          /* Object size in bytes */
  int64_t last_modified;  /* Last modified timestamp */
} RGWListEntry;

/**
 * Result of a list objects operation
 */
typedef struct RGWListResult {
  RGWListEntry* entries;  /* Array of list entries */
  size_t count;           /* Number of entries */
  int is_truncated;       /* True (1) if there are more results */
  char* next_marker;      /* Marker for next page, null-terminated */
} RGWListResult;

/*==========================================================================
 * Memory Ownership
 *=========================================================================
 * Input parameters:
 *   - All string parameters (bucket, key, etc.) are borrowed (caller retains
 *     ownership). They must remain valid for the duration of the call.
 *
 * Output parameters:
 *   - RGWBuffer: Allocated by rgw_get_object, caller must free with
 *     rgw_free_buffer().
 *   - RGWObjectMeta: Allocated by rgw_head_object, caller must free with
 *     rgw_free_object_meta() (frees etag and content_type strings).
 *   - RGWListResult: Allocated by rgw_list_objects, caller must free with
 *     rgw_free_list_result() (frees all entry keys and next_marker).
 *   - RGWString: Allocated by rgw_init_multipart / rgw_multipart_put_part,
 *     caller must free with rgw_free_string().
 *=========================================================================*/

/*==========================================================================
 * Core Object Operations
 *
 * All functions return 0 on success, negative errno on failure (e.g. -ENOENT,
 * -EINVAL, -ENOMEM, -EIO).
 *
 * Common parameters (present on every function):
 *   driver    - RGWSalDriver* (opaque handle to rgw::sal::Driver)
 *   dpp       - RGWDoutPrefix* (opaque handle to DoutPrefixProvider, for logging)
 *   yield_ctx - RGWYieldContext* (opaque handle to optional_yield, NULL for blocking)
 *=========================================================================*/

/**
 * Write an object to storage (unconditional overwrite).
 *
 * Creates the object if it doesn't exist, or replaces it if it does.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_put_object( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, const uint8_t* data, size_t len,
  const char* content_type);

/**
 * Write an object with conditional preconditions.
 *
 * Atomically writes data only if the specified precondition is met.
 * Exactly one of if_match/if_nomatch should be non-NULL.
 *
 * @return 0 on success (check *canceled for precondition result), negative errno on failure
 */
int rgw_put_object_conditional( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, const uint8_t* data, size_t len,
  const char* content_type, const char* if_match, const char* if_nomatch, int* canceled);

/**
 * Read an object (or a byte range) from storage.
 *
 * @param buffer    [out] Receives allocated data; caller must free with rgw_free_buffer()
 *
 * @return 0 on success, -ENOENT if object not found, negative errno on failure
 */
int rgw_get_object( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, uint64_t offset, uint64_t length,
  RGWBuffer* buffer);

/**
 * Delete a single object from storage.
 *
 * Deleting a non-existent object is treated as success (returns 0).
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_object( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj);

/**
 * Get object metadata without reading content.
 *
 * @param meta      [out] Populated on success; caller must free with rgw_free_object_meta()
 *
 * @return 0 on success, -ENOENT if object not found, negative errno on failure
 */
int rgw_head_object( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, RGWObjectMeta* meta);

/**
 * List objects in a bucket by prefix, with optional delimiter for hierarchy.
 *
 * @param result    [out] Populated on success; caller must free with rgw_free_list_result()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_list_objects( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const char* prefix, const char* delimiter, const char* marker,
  uint32_t max_keys, RGWListResult* result);

/**
 * Copy an object within or across buckets (unconditional overwrite at destination).
 *
 * @return 0 on success, -ENOENT if source not found, negative errno on failure
 */
int rgw_copy_object( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* src_bucket, const RGWObject* src_obj, const char* dst_bucket,
  const RGWObject* dst_obj);

/**
 * Copy an object with preconditions on the destination.
 *
 * @return 0 on success, -EEXIST if destination exists (when if_nomatch="*"),
 *         negative errno on failure
 */
int rgw_copy_object_conditional( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* src_bucket, const RGWObject* src_obj, const char* dst_bucket,
  const RGWObject* dst_obj, const char* if_match, const char* if_nomatch);

/**
 * Delete multiple objects in one call.
 *
 * Best-effort: individual object failures are silently ignored.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_objects( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const char* const* keys, size_t count);

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
 * @param upload_id  [out] Populated on success; caller must free with rgw_free_string()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_init_multipart( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, RGWString* upload_id);

/**
 * Upload one part of a multipart upload.
 *
 * @param etag  [out] Populated on success; caller must free with rgw_free_string()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_put_part( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, const char* upload_id, uint32_t part_num,
  const uint8_t* data, size_t len, RGWString* etag);

/**
 * Complete a multipart upload, assembling parts into the final object.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_complete( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, const char* upload_id,
  const char* const* etags, size_t count);

/**
 * Abort a multipart upload and discard all uploaded parts.
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_abort( RGWSalDriver* driver, const RGWDoutPrefix* dpp, RGWYieldContext* yield_ctx,
  const char* bucket, const RGWObject* obj, const char* upload_id);

/**
 * Free a buffer allocated by rgw_get_object
 */
void rgw_free_buffer(RGWBuffer* buffer);

/**
 * Free metadata allocated by rgw_head_object
 */
void rgw_free_object_meta(RGWObjectMeta* meta);

/**
 * Free list result allocated by rgw_list_objects
 */
void rgw_free_list_result(RGWListResult* result);

/**
 * Free a string allocated by rgw_init_multipart or rgw_multipart_put_part
 */
void rgw_free_string(RGWString* str);

/**
 * Get the configured rgw_max_chunk_size (in bytes)
 */
uint64_t rgw_get_max_chunk_size(RGWSalDriver* driver);

/**
 * Get the SAL wrapper API version string.
 *
 * @return Null-terminated version string (e.g., "1.0")
 */
const char* rgw_sal_wrapper_version(void);

#ifdef __cplusplus
}
#endif
