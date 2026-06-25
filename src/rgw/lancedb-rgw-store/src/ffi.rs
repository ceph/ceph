/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! FFI bindings to Ceph's rgw_sal_wrapper.cc
//!
//! These functions are implemented in Ceph's C++ code and resolved at runtime.
//! The Rust code calls these functions to perform actual storage operations.
//!
//! TODO: add an in-memory HashMap-backed mock store under #[cfg(test)] so that
//! unit tests can exercise put/get/delete/list round-trips through the Rust
//! ObjectStore trait with meaningful functional coverage.

use std::os::raw::{c_char, c_int};

/// Opaque handle types matching C typedefs in rgw_sal_wrapper.h
#[repr(C)]
pub struct CRgwDriver {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct CRgwDoutPrefix {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct CRgwYieldContext {
    _opaque: [u8; 0],
}

/// Bucket identifier (name + optional tenant)
#[repr(C)]
pub struct CRgwBucket {
    pub name: *const c_char,
    pub tenant: *const c_char,
}

impl CRgwBucket {
    pub fn from_name(name: *const c_char) -> Self {
        Self {
            name,
            tenant: std::ptr::null(),
        }
    }

    pub fn new(name: *const c_char, tenant: *const c_char) -> Self {
        Self { name, tenant }
    }
}

impl Default for CRgwBucket {
    fn default() -> Self {
        Self {
            name: std::ptr::null(),
            tenant: std::ptr::null(),
        }
    }
}

/// Object identifier (key + optional version)
#[repr(C)]
pub struct CRgwObject {
    /// Object key, null-terminated
    pub key: *const c_char,
    /// Version ID, null-terminated (NULL for current version)
    pub version_id: *const c_char,
}

impl CRgwObject {
    /// Create an CRgwObject for the current version (no version_id)
    pub fn from_key(key: *const c_char) -> Self {
        Self {
            key,
            version_id: std::ptr::null(),
        }
    }
}

impl Default for CRgwObject {
    fn default() -> Self {
        Self {
            key: std::ptr::null(),
            version_id: std::ptr::null(),
        }
    }
}

/// Buffer for receiving data from RGW
#[repr(C)]
pub struct CRgwBuffer {
    /// Pointer to data (allocated by RGW, freed by rgw_free_buffer)
    pub data: *mut u8,
    /// Length of data
    pub len: usize,
}

impl Default for CRgwBuffer {
    fn default() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
        }
    }
}

/// Object metadata returned by head operations
#[repr(C)]
pub struct CRgwObjectMeta {
    /// Object size in bytes
    pub size: u64,
    /// ETag (MD5 hash), null-terminated string
    pub etag: *mut c_char,
    /// Content type, null-terminated string
    pub content_type: *mut c_char,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
}

impl Default for CRgwObjectMeta {
    fn default() -> Self {
        Self {
            size: 0,
            etag: std::ptr::null_mut(),
            content_type: std::ptr::null_mut(),
            last_modified: 0,
        }
    }
}

/// Single entry in a list operation result
#[repr(C)]
pub struct CRgwListEntry {
    /// Object key, null-terminated string
    pub key: *mut c_char,
    /// Version ID, null-terminated (NULL for current version)
    pub version_id: *mut c_char,
    /// Object size in bytes
    pub size: u64,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
}

/// Result of a list objects operation
/// Note: is_truncated is c_int (not bool) to match the C struct exactly
#[repr(C)]
pub struct CRgwListResult {
    /// Array of list entries
    pub entries: *mut CRgwListEntry,
    /// Number of entries
    pub count: usize,
    /// True (1) if there are more results, 0 otherwise
    pub is_truncated: c_int,
    /// Marker for next page, null-terminated string
    pub next_marker: *mut c_char,
}

impl Default for CRgwListResult {
    fn default() -> Self {
        Self {
            entries: std::ptr::null_mut(),
            count: 0,
            is_truncated: 0,
            next_marker: std::ptr::null_mut(),
        }
    }
}

extern "C" {
    //=========================================================================
    // Core Object Operations
    //=========================================================================

    /// Write an object to RGW storage
    pub fn rgw_put_object(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        buffer: *const CRgwBuffer,
    ) -> c_int;

    /// Write an object with conditional preconditions
    pub fn rgw_put_object_conditional(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        buffer: *const CRgwBuffer,
        if_match: *const c_char,
        if_nomatch: *const c_char,
        canceled: *mut c_int,
    ) -> c_int;

    /// Read an object from RGW storage
    pub fn rgw_get_object(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        offset: u64,
        length: u64,
        buffer: *mut CRgwBuffer,
    ) -> c_int;

    /// Delete an object from RGW storage
    pub fn rgw_delete_object(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
    ) -> c_int;

    /// Get object metadata without reading content
    pub fn rgw_head_object(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        meta: *mut CRgwObjectMeta,
    ) -> c_int;

    /// List objects in a bucket
    pub fn rgw_list_objects(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        prefix: *const c_char,
        delimiter: *const c_char,
        marker: *const c_char,
        max_keys: u32,
        result: *mut CRgwListResult,
    ) -> c_int;

    /// Copy an object within or between buckets
    pub fn rgw_copy_object(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        src_bucket: *const CRgwBucket,
        src_obj: *const CRgwObject,
        dst_bucket: *const CRgwBucket,
        dst_obj: *const CRgwObject,
    ) -> c_int;

    /// Copy an object with conditional preconditions
    pub fn rgw_copy_object_conditional(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        src_bucket: *const CRgwBucket,
        src_obj: *const CRgwObject,
        dst_bucket: *const CRgwBucket,
        dst_obj: *const CRgwObject,
        if_match: *const c_char,
        if_nomatch: *const c_char,
    ) -> c_int;

    //=========================================================================
    // Multipart Upload Operations
    //=========================================================================

    /// Initialize a multipart upload
    pub fn rgw_init_multipart(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        upload_id: *mut *mut c_char,
    ) -> c_int;

    /// Upload a part in a multipart upload
    pub fn rgw_multipart_put_part(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        upload_id: *const c_char,
        part_num: u32,
        data: *const u8,
        len: usize,
        etag: *mut *mut c_char,
    ) -> c_int;

    /// Complete a multipart upload
    pub fn rgw_multipart_complete(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        upload_id: *const c_char,
        etags: *const *const c_char,
        count: usize,
    ) -> c_int;

    /// Abort a multipart upload
    pub fn rgw_multipart_abort(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        yield_ctx: *mut CRgwYieldContext,
        bucket: *const CRgwBucket,
        obj: *const CRgwObject,
        upload_id: *const c_char,
    ) -> c_int;

    //=========================================================================
    // Memory Management
    //=========================================================================

    /// Free a buffer allocated by rgw_get_object
    pub fn rgw_free_buffer(buffer: *mut CRgwBuffer);

    /// Free metadata allocated by rgw_head_object
    pub fn rgw_free_object_meta(meta: *mut CRgwObjectMeta);

    /// Free list result allocated by rgw_list_objects
    pub fn rgw_free_list_result(result: *mut CRgwListResult);

    //=========================================================================
    // Configuration
    //=========================================================================

    /// Get the configured rgw_max_chunk_size (in bytes)
    pub fn rgw_get_max_chunk_size(driver: *mut CRgwDriver) -> u64;

    /// Get the RGW wrapper API version string
    pub fn rgw_sal_wrapper_version() -> *const c_char;
}

//=============================================================================
// Safe Rust Wrappers
//=============================================================================

/// RAII wrapper for CRgwBuffer that automatically frees on drop
pub struct OwnedRGWBuffer(pub CRgwBuffer);

impl Drop for OwnedRGWBuffer {
    fn drop(&mut self) {
        if !self.0.data.is_null() {
            unsafe {
                rgw_free_buffer(&mut self.0);
            }
        }
    }
}

impl OwnedRGWBuffer {
    /// Convert to Bytes, copying the data
    pub fn to_bytes(&self) -> bytes::Bytes {
        if self.0.data.is_null() || self.0.len == 0 {
            bytes::Bytes::new()
        } else {
            unsafe {
                let slice = std::slice::from_raw_parts(self.0.data, self.0.len);
                bytes::Bytes::copy_from_slice(slice)
            }
        }
    }
}

/// RAII wrapper for CRgwObjectMeta
pub struct OwnedRGWObjectMeta(pub CRgwObjectMeta);

impl Drop for OwnedRGWObjectMeta {
    fn drop(&mut self) {
        unsafe {
            rgw_free_object_meta(&mut self.0);
        }
    }
}

/// RAII wrapper for CRgwListResult
pub struct OwnedRGWListResult(pub CRgwListResult);

impl Drop for OwnedRGWListResult {
    fn drop(&mut self) {
        unsafe {
            rgw_free_list_result(&mut self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_buffer_default() {
        let buf = CRgwBuffer::default();
        assert!(buf.data.is_null());
        assert_eq!(buf.len, 0);
    }

    #[test]
    fn test_object_default() {
        let obj = CRgwObject::default();
        assert!(obj.key.is_null());
        assert!(obj.version_id.is_null());
    }

    #[test]
    fn test_object_from_key() {
        let key = CString::new("test-key").unwrap();
        let obj = CRgwObject::from_key(key.as_ptr());
        assert_eq!(obj.key, key.as_ptr());
        assert!(obj.version_id.is_null());
    }

    #[test]
    fn test_object_meta_default() {
        let meta = CRgwObjectMeta::default();
        assert_eq!(meta.size, 0);
        assert!(meta.etag.is_null());
        assert!(meta.content_type.is_null());
        assert_eq!(meta.last_modified, 0);
    }

    #[test]
    fn test_list_result_default() {
        let result = CRgwListResult::default();
        assert!(result.entries.is_null());
        assert_eq!(result.count, 0);
        assert_eq!(result.is_truncated, 0);
        assert!(result.next_marker.is_null());
    }
}
