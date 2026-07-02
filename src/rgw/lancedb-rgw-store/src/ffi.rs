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
pub struct RGWSalDriver {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct RGWDoutPrefix {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct RGWYieldContext {
    _opaque: [u8; 0],
}

/// Heap-allocated string with length, freed by rgw_free_string
#[repr(C)]
pub struct RGWString {
    pub str_ptr: *mut c_char,
    pub len: usize,
}

impl Default for RGWString {
    fn default() -> Self {
        Self {
            str_ptr: std::ptr::null_mut(),
            len: 0,
        }
    }
}

/// Object identifier (key + optional version)
#[repr(C)]
pub struct RGWObject {
    /// Object key, null-terminated
    pub key: *const c_char,
    /// Version ID, null-terminated (NULL for current version)
    pub version_id: *const c_char,
}

impl RGWObject {
    /// Create an RGWObject for the current version (no version_id)
    pub fn from_key(key: *const c_char) -> Self {
        Self {
            key,
            version_id: std::ptr::null(),
        }
    }
}

impl Default for RGWObject {
    fn default() -> Self {
        Self {
            key: std::ptr::null(),
            version_id: std::ptr::null(),
        }
    }
}

/// Buffer for receiving data from RGW
#[repr(C)]
pub struct RGWBuffer {
    /// Pointer to data (allocated by RGW, freed by rgw_free_buffer)
    pub data: *mut u8,
    /// Length of data
    pub len: usize,
}

impl Default for RGWBuffer {
    fn default() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
        }
    }
}

/// Object metadata returned by head operations
#[repr(C)]
pub struct RGWObjectMeta {
    /// Object size in bytes
    pub size: u64,
    /// ETag (MD5 hash), null-terminated string
    pub etag: *mut c_char,
    /// Content type, null-terminated string
    pub content_type: *mut c_char,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
}

impl Default for RGWObjectMeta {
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
pub struct RGWListEntry {
    /// Object key, null-terminated string
    pub key: *mut c_char,
    /// Object size in bytes
    pub size: u64,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified: i64,
}

/// Result of a list objects operation
/// Note: is_truncated is c_int (not bool) to match the C struct exactly
#[repr(C)]
pub struct RGWListResult {
    /// Array of list entries
    pub entries: *mut RGWListEntry,
    /// Number of entries
    pub count: usize,
    /// True (1) if there are more results, 0 otherwise
    pub is_truncated: c_int,
    /// Marker for next page, null-terminated string
    pub next_marker: *mut c_char,
}

impl Default for RGWListResult {
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
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        data: *const u8,
        len: usize,
        content_type: *const c_char,
    ) -> c_int;

    /// Write an object with conditional preconditions
    pub fn rgw_put_object_conditional(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        data: *const u8,
        len: usize,
        content_type: *const c_char,
        if_match: *const c_char,
        if_nomatch: *const c_char,
        canceled: *mut c_int,
    ) -> c_int;

    /// Read an object from RGW storage
    pub fn rgw_get_object(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        offset: u64,
        length: u64,
        buffer: *mut RGWBuffer,
    ) -> c_int;

    /// Delete an object from RGW storage
    pub fn rgw_delete_object(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
    ) -> c_int;

    /// Get object metadata without reading content
    pub fn rgw_head_object(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        meta: *mut RGWObjectMeta,
    ) -> c_int;

    /// List objects in a bucket
    pub fn rgw_list_objects(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        prefix: *const c_char,
        delimiter: *const c_char,
        marker: *const c_char,
        max_keys: u32,
        result: *mut RGWListResult,
    ) -> c_int;

    /// Copy an object within or between buckets
    pub fn rgw_copy_object(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        src_bucket: *const c_char,
        src_obj: *const RGWObject,
        dst_bucket: *const c_char,
        dst_obj: *const RGWObject,
    ) -> c_int;

    /// Copy an object with conditional preconditions
    pub fn rgw_copy_object_conditional(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        src_bucket: *const c_char,
        src_obj: *const RGWObject,
        dst_bucket: *const c_char,
        dst_obj: *const RGWObject,
        if_match: *const c_char,
        if_nomatch: *const c_char,
    ) -> c_int;

    //=========================================================================
    // Multipart Upload Operations
    //=========================================================================

    /// Initialize a multipart upload
    pub fn rgw_init_multipart(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *mut RGWString,
    ) -> c_int;

    /// Upload a part in a multipart upload
    pub fn rgw_multipart_put_part(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *const c_char,
        part_num: u32,
        data: *const u8,
        len: usize,
        etag: *mut RGWString,
    ) -> c_int;

    /// Complete a multipart upload
    pub fn rgw_multipart_complete(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *const c_char,
        etags: *const *const c_char,
        count: usize,
    ) -> c_int;

    /// Abort a multipart upload
    pub fn rgw_multipart_abort(
        driver: *mut RGWSalDriver,
        dpp: *const RGWDoutPrefix,
        yield_ctx: *mut RGWYieldContext,
        bucket: *const c_char,
        obj: *const RGWObject,
        upload_id: *const c_char,
    ) -> c_int;

    //=========================================================================
    // Memory Management
    //=========================================================================

    /// Free a buffer allocated by rgw_get_object
    pub fn rgw_free_buffer(buffer: *mut RGWBuffer);

    /// Free metadata allocated by rgw_head_object
    pub fn rgw_free_object_meta(meta: *mut RGWObjectMeta);

    /// Free list result allocated by rgw_list_objects
    pub fn rgw_free_list_result(result: *mut RGWListResult);

    /// Free a string allocated by rgw_init_multipart or rgw_multipart_put_part
    pub fn rgw_free_string(str: *mut RGWString);

    //=========================================================================
    // Configuration
    //=========================================================================

    /// Get the configured rgw_max_chunk_size (in bytes)
    pub fn rgw_get_max_chunk_size(driver: *mut RGWSalDriver) -> u64;

    /// Get the SAL wrapper API version string
    pub fn rgw_sal_wrapper_version() -> *const c_char;
}

//=============================================================================
// Safe Rust Wrappers
//=============================================================================

/// RAII wrapper for RGWBuffer that automatically frees on drop
pub struct OwnedRGWBuffer(pub RGWBuffer);

impl Drop for OwnedRGWBuffer {
    fn drop(&mut self) {
        if !self.0.data.is_null() {
            unsafe { rgw_free_buffer(&mut self.0); }
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

/// RAII wrapper for RGWString
pub struct OwnedRGWString(pub RGWString);

impl Drop for OwnedRGWString {
    fn drop(&mut self) {
        if !self.0.str_ptr.is_null() {
            unsafe { rgw_free_string(&mut self.0); }
        }
    }
}

impl OwnedRGWString {
    pub fn to_string(&self) -> String {
        if self.0.str_ptr.is_null() || self.0.len == 0 {
            String::new()
        } else {
            unsafe {
                let slice = std::slice::from_raw_parts(self.0.str_ptr as *const u8, self.0.len);
                String::from_utf8_lossy(slice).into_owned()
            }
        }
    }
}

/// RAII wrapper for RGWObjectMeta
pub struct OwnedRGWObjectMeta(pub RGWObjectMeta);

impl Drop for OwnedRGWObjectMeta {
    fn drop(&mut self) {
        unsafe { rgw_free_object_meta(&mut self.0); }
    }
}

/// RAII wrapper for RGWListResult
pub struct OwnedRGWListResult(pub RGWListResult);

impl Drop for OwnedRGWListResult {
    fn drop(&mut self) {
        unsafe { rgw_free_list_result(&mut self.0); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_buffer_default() {
        let buf = RGWBuffer::default();
        assert!(buf.data.is_null());
        assert_eq!(buf.len, 0);
    }

    #[test]
    fn test_string_default() {
        let s = RGWString::default();
        assert!(s.str_ptr.is_null());
        assert_eq!(s.len, 0);
    }

    #[test]
    fn test_object_default() {
        let obj = RGWObject::default();
        assert!(obj.key.is_null());
        assert!(obj.version_id.is_null());
    }

    #[test]
    fn test_object_from_key() {
        let key = CString::new("test-key").unwrap();
        let obj = RGWObject::from_key(key.as_ptr());
        assert_eq!(obj.key, key.as_ptr());
        assert!(obj.version_id.is_null());
    }

    #[test]
    fn test_object_meta_default() {
        let meta = RGWObjectMeta::default();
        assert_eq!(meta.size, 0);
        assert!(meta.etag.is_null());
        assert!(meta.content_type.is_null());
        assert_eq!(meta.last_modified, 0);
    }

    #[test]
    fn test_list_result_default() {
        let result = RGWListResult::default();
        assert!(result.entries.is_null());
        assert_eq!(result.count, 0);
        assert_eq!(result.is_truncated, 0);
        assert!(result.next_marker.is_null());
    }

    #[test]
    fn test_owned_string_empty() {
        let s = OwnedRGWString(RGWString::default());
        assert_eq!(s.to_string(), "");
    }
}
