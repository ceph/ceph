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

/**
 * Object identifier (key + optional version)
 */
typedef struct RGWObject {
  const char* key;         /* Object key, null-terminated */
  const char* version_id;  /* Version ID, null-terminated (NULL for current version) */
} RGWObject;

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
 * Thread Safety and Memory Ownership
 *=========================================================================
 * These functions are NOT thread-safe for concurrent operations on the same
 * bucket/object. The caller (like LANCEDB) must serialize operations
 * when multiple threads access the same objects. Different threads may
 * safely operate on different objects/buckets concurrently.
 *
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
 *   - upload_id/etag out-buffers in multipart: Caller provides the buffer,
 *     function writes into it. No separate free needed.
 *=========================================================================*/

/*==========================================================================
 * Core Object Operations
 *
 * All functions return 0 on success, negative errno on failure (e.g. -ENOENT,
 * -EINVAL, -ENOMEM, -EIO). Exceptions from SAL are caught and mapped to -EIO.
 *
 * Common parameters (present on every function):
 *   driver    - rgw::sal::Driver* (the SAL backend handle)
 *   dpp       - DoutPrefixProvider* (for logging)
 *   yield_ctx - optional_yield* (NULL for blocking)
 *=========================================================================*/

/**
 * Write an object to storage (unconditional overwrite).
 *
 * Creates the object if it doesn't exist, or replaces it if it does.
 *
 * @param driver       rgw::sal::Driver*
 * @param dpp          DoutPrefixProvider*
 * @param yield_ctx    optional_yield* (NULL for blocking)
 * @param bucket       Bucket name
 * @param obj          Object identifier
 * @param data         Object content
 * @param len          Length of data in bytes
 * @param content_type MIME type
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_put_object( void* driver, const void* dpp, void* yield_ctx,
  const char* bucket, const RGWObject* obj, const uint8_t* data, size_t len,
  const char* content_type);

/**
 * Write an object with conditional preconditions.
 *
 * Atomically writes data only if the specified precondition is met.
 * Exactly one of if_match/if_nomatch should be non-NULL.
 *
 * @param driver       rgw::sal::Driver*
 * @param dpp          DoutPrefixProvider*
 * @param yield_ctx    optional_yield* (NULL for blocking)
 * @param bucket       Bucket name
 * @param obj          Object identifier
 * @param data         Object content
 * @param len          Length of data in bytes
 * @param content_type MIME type
 * @param if_match     Only write if existing ETag matches (NULL to skip)
 * @param if_nomatch   Set to "*" for create-if-not-exists (NULL to skip)
 * @param canceled     [out] Set to 1 if precondition failed, 0 otherwise
 *
 * @return 0 on success (check *canceled for precondition result), negative errno on failure
 */
int rgw_put_object_conditional( void* driver, const void* dpp, void* yield_ctx,
  const char* bucket, const RGWObject* obj, const uint8_t* data, size_t len,
  const char* content_type, const char* if_match, const char* if_nomatch, int* canceled);

/**
 * Read an object (or a byte range) from storage.
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param obj       Object identifier
 * @param offset    Byte offset to start reading from
 * @param length    Number of bytes to read
 * @param buffer    [out] Receives allocated data; caller must free with rgw_free_buffer()
 *
 * @return 0 on success, -ENOENT if object not found, negative errno on failure
 */
int rgw_get_object( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj, uint64_t offset, uint64_t length, RGWBuffer* buffer);

/**
 * Delete a single object from storage.
 *
 * Deleting a non-existent object is treated as success (returns 0).
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param obj       Object identifier
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_object( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj);

/**
 * Get object metadata without reading content.
 *
 * Populates size, etag, content_type, and last_modified. The etag and
 * content_type strings are heap-allocated and may be NULL if not set.
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param obj       Object identifier
 * @param meta      [out] Populated on success; caller must free with rgw_free_object_meta()
 *
 * @return 0 on success, -ENOENT if object not found, negative errno on failure
 */
int rgw_head_object( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj, RGWObjectMeta* meta);

/**
 * List objects in a bucket by prefix, with optional delimiter for hierarchy.
 *
 * With delimiter="/", returns one level of hierarchy: objects at the current
 * level plus common prefixes (directory-like groupings with trailing '/').
 * With delimiter="", returns all objects recursively (flat enumeration).
 *
 * Results are paginated. If is_truncated==1, pass next_marker as the marker
 * parameter in the next call to continue. Listing is lexically ordered.
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param prefix    Only return keys starting with this prefix
 * @param delimiter Hierarchy separator, typically "/" or ""
 * @param marker    Start listing after this key for pagination
 * @param max_keys  Maximum number of entries to return per call
 * @param result    [out] Populated on success; caller must free with rgw_free_list_result()
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_list_objects( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const char* prefix, const char* delimiter, const char* marker, uint32_t max_keys,
  RGWListResult* result);

/**
 * Copy an object within or across buckets (unconditional overwrite at destination).
 *
 * @param driver     rgw::sal::Driver*
 * @param dpp        DoutPrefixProvider*
 * @param yield_ctx  optional_yield* (NULL for blocking)
 * @param src_bucket Source bucket name
 * @param src_obj    Source object identifier
 * @param dst_bucket Destination bucket name
 * @param dst_obj    Destination object identifier
 *
 * @return 0 on success, -ENOENT if source not found, negative errno on failure
 */
int rgw_copy_object( void* driver, const void* dpp, void* yield_ctx, const char* src_bucket,
  const RGWObject* src_obj, const char* dst_bucket, const RGWObject* dst_obj);

/**
 * Copy an object with preconditions on the destination.
 *
 * For copy-if-not-exists (if_nomatch="*"), checks destination existence first
 * via head, then copies. There is a small race window between the check and
 * the copy. Returns -EEXIST if the destination already exists.
 *
 * @param driver     rgw::sal::Driver*
 * @param dpp        DoutPrefixProvider*
 * @param yield_ctx  optional_yield* (NULL for blocking)
 * @param src_bucket Source bucket name
 * @param src_obj    Source object identifier
 * @param dst_bucket Destination bucket name
 * @param dst_obj    Destination object identifier
 * @param if_match   Only copy if source ETag matches (NULL to skip)
 * @param if_nomatch Set to "*" for copy-if-destination-not-exists (NULL to skip)
 *
 * @return 0 on success, -EEXIST if destination exists (when if_nomatch="*"),
 *         negative errno on failure
 */
int rgw_copy_object_conditional( void* driver, const void* dpp, void* yield_ctx, const char* src_bucket,
  const RGWObject* src_obj, const char* dst_bucket, const RGWObject* dst_obj,
  const char* if_match, const char* if_nomatch);

/**
 * Delete multiple objects in one call.
 *
 * Best-effort: individual object failures are silently ignored.
 * NULL entries in the keys array are skipped.
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param keys      Array of null-terminated object keys
 * @param count     Number of keys in the array
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_delete_objects( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const char* const* keys, size_t count);

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
 * @param driver        rgw::sal::Driver*
 * @param dpp           DoutPrefixProvider*
 * @param yield_ctx     optional_yield* (NULL for blocking)
 * @param bucket        Bucket name
 * @param obj           Object identifier
 * @param upload_id     [out] Caller-provided buffer; receives null-terminated upload ID
 * @param upload_id_len Size of the upload_id buffer
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_init_multipart( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj, char* upload_id, size_t upload_id_len);

/**
 * Upload one part of a multipart upload.
 *
 * The part's ETag (MD5 hex digest) is written into the caller-provided etag buffer.
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param obj       Object identifier
 * @param upload_id Upload ID from rgw_init_multipart
 * @param part_num  Part number (1-indexed)
 * @param data      Part content
 * @param len       Length of data in bytes
 * @param etag      [out] Caller-provided buffer; receives null-terminated MD5 hex ETag
 * @param etag_len  Size of the etag buffer
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_put_part( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj, const char* upload_id, uint32_t part_num, const uint8_t* data,
  size_t len, char* etag, size_t etag_len);

/**
 * Complete a multipart upload, assembling parts into the final object.
 *
 * The etags array must contain the ETags returned by rgw_multipart_put_part(),
 * in part-number order (index 0 = part 1, index 1 = part 2, etc.).
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param obj       Object identifier
 * @param upload_id Upload ID from rgw_init_multipart
 * @param etags     Array of null-terminated part ETags in order
 * @param count     Number of ETags/parts
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_complete( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj, const char* upload_id, const char* const* etags, size_t count);

/**
 * Abort a multipart upload and discard all uploaded parts.
 *
 * @param driver    rgw::sal::Driver*
 * @param dpp       DoutPrefixProvider*
 * @param yield_ctx optional_yield* (NULL for blocking)
 * @param bucket    Bucket name
 * @param obj       Object identifier
 * @param upload_id Upload ID from rgw_init_multipart
 *
 * @return 0 on success, negative errno on failure
 */
int rgw_multipart_abort( void* driver, const void* dpp, void* yield_ctx, const char* bucket,
  const RGWObject* obj, const char* upload_id);

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
 * Get the configured rgw_max_chunk_size (in bytes)
 */
uint64_t rgw_get_max_chunk_size(void* driver);

/**
 * Get the SAL wrapper API version string.
 *
 * @return Null-terminated version string (e.g., "1.0")
 */
const char* rgw_sal_wrapper_version(void);

#ifdef __cplusplus
}
#endif
