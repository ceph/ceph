/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! ObjectStore trait implementation using RGW SAL
//!
//! This implements Apache Arrow's `object_store::ObjectStore` trait,
//! routing all I/O operations through Ceph's RGW SAL C API.

use crate::ffi::{
    self, CRgwBucket, CRgwDoutPrefix, CRgwDriver, CRgwObject, OwnedRGWBuffer, OwnedRGWListResult,
    OwnedRGWObjectMeta, OwnedRGWString,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    path::Path, Attributes, CopyMode, CopyOptions, Error as ObjectStoreError, GetOptions, GetRange,
    GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    UpdateVersion,
};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::{Arc, Mutex};

/// Send+Sync wrapper for driver pointer.
///
/// Methods that return `BoxStream<'static, ...>` (e.g., `delete_stream`, `list`)
/// cannot capture `&self` because the stream outlives the borrow.  The raw
/// pointers must be copied out, but they do not implement `Send`,
/// which async streams require.  This wrapper adds `Send + Sync`.
///
/// Safety: The RGW driver pointer is safe to use from multiple threads
#[derive(Clone, Copy, Debug)]
struct SendPtr(*mut CRgwDriver);

unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

impl SendPtr {
    fn new(ptr: *mut CRgwDriver) -> Self {
        Self(ptr)
    }
    fn as_ptr(&self) -> *mut CRgwDriver {
        self.0
    }
}

/// Send+Sync wrapper for dpp pointer (see [`SendPtr`]).
#[derive(Clone, Copy, Debug)]
struct SendConstPtr(*const CRgwDoutPrefix);

unsafe impl Send for SendConstPtr {}
unsafe impl Sync for SendConstPtr {}

impl SendConstPtr {
    fn new(ptr: *const CRgwDoutPrefix) -> Self {
        Self(ptr)
    }
    fn as_ptr(&self) -> *const CRgwDoutPrefix {
        self.0
    }
}

const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// Convert a string to CString, returning an ObjectStore error on failure.
fn str_to_cstring(s: &str) -> ObjectStoreResult<CString> {
    CString::new(s).map_err(|e| object_store::Error::Generic {
        store: "rgw",
        source: Box::new(e),
    })
}

/// ObjectStore implementation that uses RGW directly
///
/// This store holds raw pointers to Ceph's RGW driver and DoutPrefixProvider.
/// These pointers must remain valid for the lifetime of this store.
pub struct RGWObjectStore {
    driver: *mut CRgwDriver,
    dpp: *const CRgwDoutPrefix,
    bucket: String,
    tenant: String,
    /// Path prefix for debugging/logging (not functionally needed due to
    /// 1:1 bucket-to-db mapping, kept for debug/logging only)
    prefix: String,
    chunk_size: u64,
}

// Safety: The raw pointers reference RGW driver and DoutPrefixProvider which
// are thread-safe -- they are shared across RGW request threads and RGW
// provides atomic readers/writers for concurrent object access.
unsafe impl Send for RGWObjectStore {}
unsafe impl Sync for RGWObjectStore {}

/// Helper struct to hold CStrings for attributes (ensures they live long enough for FFI call)
/// Holds CStrings alive during FFI calls to prevent dangling pointers.
/// Fields are not directly read but must remain in scope while their
/// raw pointers are passed to C code.
struct AttributesCStrings {
    _content_type: Option<CString>,
    _content_encoding: Option<CString>,
    _content_disposition: Option<CString>,
    _content_language: Option<CString>,
    _cache_control: Option<CString>,
    _metadata_json: Option<CString>,
}

impl RGWObjectStore {
    /// Create a new RGWObjectStore
    ///
    /// Reads `rgw_max_chunk_size` from the driver's config to set the
    /// streaming read chunk size.
    ///
    /// # Safety
    /// The caller must ensure that `driver` and `dpp` pointers remain valid
    /// for the lifetime of this store and any clones.
    pub unsafe fn new(
        driver: *mut CRgwDriver,
        dpp: *const CRgwDoutPrefix,
        bucket: &str,
        tenant: &str,
        prefix: &str,
    ) -> Self {
        let chunk_size = ffi::rgw_get_max_chunk_size(driver);
        Self {
            driver,
            dpp,
            bucket: bucket.to_string(),
            tenant: tenant.to_string(),
            prefix: prefix.to_string(),
            chunk_size: if chunk_size > 0 {
                chunk_size
            } else {
                DEFAULT_CHUNK_SIZE
            },
        }
    }

    /// Get bucket name as C string
    fn bucket_cstr(&self) -> ObjectStoreResult<CString> {
        str_to_cstring(&self.bucket)
    }

    /// Get tenant as C string (empty string for default tenant)
    fn tenant_cstr(&self) -> ObjectStoreResult<CString> {
        str_to_cstring(&self.tenant)
    }

    /// Build an CRgwBucket from pre-constructed CStrings
    fn make_bucket(bucket_c: &CString, tenant_c: &CString) -> CRgwBucket {
        let tenant_ptr = if tenant_c.as_bytes().is_empty() {
            std::ptr::null()
        } else {
            tenant_c.as_ptr()
        };
        CRgwBucket::new(bucket_c.as_ptr(), tenant_ptr)
    }


    /// Convert object_store Attributes to C-compatible format
    /// Returns (CStrings holder, CRgwObjectMeta with pointers into the CStrings)
    fn attributes_to_c_meta(
        attributes: &object_store::Attributes,
    ) -> ObjectStoreResult<(AttributesCStrings, ffi::CRgwObjectMeta)> {
        use std::collections::HashMap;

        let mut content_type: Option<CString> = None;
        let mut content_encoding: Option<CString> = None;
        let mut content_disposition: Option<CString> = None;
        let mut content_language: Option<CString> = None;
        let mut cache_control: Option<CString> = None;
        let mut custom_metadata: HashMap<String, String> = HashMap::new();

        for (attr, value) in attributes.iter() {
            let value_str = value.as_ref();
            match attr {
                object_store::Attribute::ContentType => {
                    content_type = Some(str_to_cstring(value_str)?);
                }
                object_store::Attribute::ContentEncoding => {
                    content_encoding = Some(str_to_cstring(value_str)?);
                }
                object_store::Attribute::ContentDisposition => {
                    content_disposition = Some(str_to_cstring(value_str)?);
                }
                object_store::Attribute::ContentLanguage => {
                    content_language = Some(str_to_cstring(value_str)?);
                }
                object_store::Attribute::CacheControl => {
                    cache_control = Some(str_to_cstring(value_str)?);
                }
                object_store::Attribute::Metadata(key) => {
                    custom_metadata.insert(key.to_string(), value_str.to_string());
                }
                _ => {
                    // Ignore unknown attributes (Attribute is non-exhaustive)
                }
            }
        }

        // Serialize custom metadata to JSON
        let metadata_json = if !custom_metadata.is_empty() {
            Some(str_to_cstring(
                &serde_json::to_string(&custom_metadata).map_err(|e| {
                    ObjectStoreError::Generic {
                        store: "RGW",
                        source: Box::new(e),
                    }
                })?,
            )?)
        } else {
            None
        };

        let meta = ffi::CRgwObjectMeta {
            size: 0,
            etag: std::ptr::null_mut(),
            content_type: content_type
                .as_ref()
                .map_or(std::ptr::null_mut(), |s| s.as_ptr() as *mut _),
            last_modified: 0,
            last_modified_ns: 0,
            content_encoding: content_encoding
                .as_ref()
                .map_or(std::ptr::null_mut(), |s| s.as_ptr() as *mut _),
            content_disposition: content_disposition
                .as_ref()
                .map_or(std::ptr::null_mut(), |s| s.as_ptr() as *mut _),
            content_language: content_language
                .as_ref()
                .map_or(std::ptr::null_mut(), |s| s.as_ptr() as *mut _),
            cache_control: cache_control
                .as_ref()
                .map_or(std::ptr::null_mut(), |s| s.as_ptr() as *mut _),
            metadata: metadata_json
                .as_ref()
                .map_or(std::ptr::null_mut(), |s| s.as_ptr() as *mut _),
        };

        let cstrings = AttributesCStrings {
            _content_type: content_type,
            _content_encoding: content_encoding,
            _content_disposition: content_disposition,
            _content_language: content_language,
            _cache_control: cache_control,
            _metadata_json: metadata_json,
        };

        Ok((cstrings, meta))
    }

    /// Convert path to C string key
    fn path_to_cstr(&self, path: &Path) -> ObjectStoreResult<CString> {
        str_to_cstring(path.as_ref())
    }

    fn make_obj(key: &CString) -> CRgwObject {
        CRgwObject::from_key(key.as_ptr())
    }

    /// Get the prefix for this store
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Build the metadata of a listed object.
    ///
    /// The etag and the full resolution timestamp are what let a caller tell two
    /// objects with the same key apart: an object that is deleted and written again
    /// keeps its key, and its size and its mtime in seconds may be unchanged.
    unsafe fn entry_meta(entry: &ffi::CRgwListEntry) -> ObjectMeta {
        let key = CStr::from_ptr(entry.key).to_string_lossy().into_owned();
        let e_tag = if entry.etag.is_null() {
            None
        } else {
            Some(CStr::from_ptr(entry.etag).to_string_lossy().into_owned())
        };
        // Keys written through this store are already percent-encoded
        let location = Path::parse(&key).unwrap_or_else(|_| Path::from(key));
        ObjectMeta {
            location,
            last_modified: Self::timestamp(entry.last_modified, entry.last_modified_ns),
            size: entry.size,
            e_tag,
            version: None,
        }
    }

    /// Convert an RGW timestamp to a chrono one, keeping the sub-second part
    fn timestamp(seconds: i64, nanoseconds: i32) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(seconds, nanoseconds.max(0) as u32)
            .unwrap_or_else(chrono::Utc::now)
    }

    /// Convert errno to ObjectStore error (test-only public accessor)
    #[cfg(test)]
    pub fn errno_to_error_for_test(
        &self,
        errno: i32,
        path: &Path,
        op: &str,
    ) -> object_store::Error {
        Self::errno_to_error(errno, path, op)
    }

    /// Convert errno to ObjectStore error.
    /// Common RGW/RADOS codes are mapped explicitly; unmapped codes fall
    /// through to Generic with the raw errno in the message.
    fn errno_to_error(errno: i32, path: &Path, op: &str) -> object_store::Error {
        match errno {
            -2 => object_store::Error::NotFound {
                // ENOENT
                path: path.to_string(),
                source: format!("{} failed: object not found", op).into(),
            },
            -1 => object_store::Error::Generic {
                // EPERM
                store: "rgw",
                source: format!("{} failed: operation not permitted", op).into(),
            },
            -13 => object_store::Error::Generic {
                // EACCES
                store: "rgw",
                source: format!("{} failed: permission denied", op).into(),
            },
            -17 => object_store::Error::AlreadyExists {
                // EEXIST
                path: path.to_string(),
                source: format!("{} failed: object already exists", op).into(),
            },
            -22 => object_store::Error::Generic {
                // EINVAL
                store: "rgw",
                source: format!("{} failed: invalid argument", op).into(),
            },
            -28 => object_store::Error::Generic {
                // ENOSPC
                store: "rgw",
                source: format!("{} failed: no space left on device", op).into(),
            },
            -36 => object_store::Error::Generic {
                // ENAMETOOLONG
                store: "rgw",
                source: format!("{} failed: object key too long", op).into(),
            },
            -95 => object_store::Error::NotSupported {
                // ENOTSUP
                source: format!("{} not supported by RGW backend", op).into(),
            },
            -2015 => object_store::Error::Precondition {
                // ERR_PRECONDITION_FAILED
                path: path.to_string(),
                source: format!("{} failed: precondition failed", op).into(),
            },
            -2016 => object_store::Error::NotModified {
                // ERR_NOT_MODIFIED
                path: path.to_string(),
                source: format!("{} failed: not modified", op).into(),
            },
            _ => object_store::Error::Generic {
                store: "rgw",
                source: format!("{} failed with errno {}", op, errno).into(),
            },
        }
    }
}

impl std::fmt::Display for RGWObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.prefix.is_empty() {
            write!(f, "RGWObjectStore(bucket={})", self.bucket)
        } else {
            write!(
                f,
                "RGWObjectStore(bucket={}, prefix={})",
                self.bucket, self.prefix
            )
        }
    }
}

impl std::fmt::Debug for RGWObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RGWObjectStore")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("driver", &format!("{:p}", self.driver))
            .finish()
    }
}

#[async_trait]
impl ObjectStore for RGWObjectStore {
    /// Write an object to RGW.
    ///
    /// Supports three modes:
    /// - `Overwrite`: unconditional write (creates or replaces)
    /// - `Create`: write only if the object does not exist (if-none-match: *)
    /// - `Update`: write only if the existing ETag matches (if-match)
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let bucket = self.bucket_cstr()?;
        let tenant = self.tenant_cstr()?;
        let rgw_bucket = Self::make_bucket(&bucket, &tenant);
        let key = self.path_to_cstr(location)?;
        let obj = Self::make_obj(&key);
        let bytes: Bytes = payload.into();

        // Convert attributes to C strings (must live for duration of FFI call)
        let (attrs_cstrings, attrs_meta) = Self::attributes_to_c_meta(&opts.attributes)?;
        let attrs_ptr = if opts.attributes.is_empty() {
            std::ptr::null()
        } else {
            &attrs_meta as *const ffi::CRgwObjectMeta
        };

        match opts.mode {
            PutMode::Overwrite => {
                let buf = ffi::CRgwBuffer {
                    data: bytes.as_ptr() as *mut u8,
                    len: bytes.len(),
                };
                let mut etag_ptr: *mut c_char = std::ptr::null_mut();
                let result = unsafe {
                    ffi::rgw_put_object(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        &obj,
                        &buf,
                        attrs_ptr,
                        &mut etag_ptr,
                    )
                };

                // Keep attrs_cstrings alive until after FFI call
                drop(attrs_cstrings);

                if result != 0 {
                    return Err(Self::errno_to_error(result, location, "put"));
                }
                let e_tag = OwnedRGWString(etag_ptr).as_string();
                Ok(PutResult {
                    e_tag,
                    version: None,
                })
            }
            PutMode::Create => {
                let if_nomatch = str_to_cstring("*")?;
                let mut canceled: c_int = 0;
                let buf = ffi::CRgwBuffer {
                    data: bytes.as_ptr() as *mut u8,
                    len: bytes.len(),
                };
                let mut etag_ptr: *mut c_char = std::ptr::null_mut();

                let result = unsafe {
                    ffi::rgw_put_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        &obj,
                        &buf,
                        std::ptr::null(),
                        if_nomatch.as_ptr(),
                        &mut canceled,
                        attrs_ptr,
                        &mut etag_ptr,
                    )
                };

                // Keep attrs_cstrings alive until after FFI call
                drop(attrs_cstrings);

                if result != 0 {
                    return Err(Self::errno_to_error(result, location, "put (create)"));
                }
                if canceled != 0 {
                    return Err(ObjectStoreError::AlreadyExists {
                        path: location.to_string(),
                        source: "object already exists (conditional create failed)".into(),
                    });
                }
                let e_tag = OwnedRGWString(etag_ptr).as_string();
                Ok(PutResult {
                    e_tag,
                    version: None,
                })
            }
            PutMode::Update(UpdateVersion { e_tag, .. }) => {
                let etag_str = e_tag.ok_or_else(|| ObjectStoreError::Generic {
                    store: "rgw",
                    source: "PutMode::Update requires e_tag".into(),
                })?;
                let if_match =
                    CString::new(etag_str.as_str()).map_err(|_| ObjectStoreError::Generic {
                        store: "rgw",
                        source: "invalid etag string".into(),
                    })?;
                let mut canceled: c_int = 0;
                let buf = ffi::CRgwBuffer {
                    data: bytes.as_ptr() as *mut u8,
                    len: bytes.len(),
                };
                let mut etag_ptr: *mut c_char = std::ptr::null_mut();

                let result = unsafe {
                    ffi::rgw_put_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        &obj,
                        &buf,
                        if_match.as_ptr(),
                        std::ptr::null(),
                        &mut canceled,
                        attrs_ptr,
                        &mut etag_ptr,
                    )
                };

                // Keep attrs_cstrings alive until after FFI call
                drop(attrs_cstrings);

                if result != 0 {
                    return Err(Self::errno_to_error(result, location, "put (update)"));
                }
                if canceled != 0 {
                    return Err(ObjectStoreError::Precondition {
                        path: location.to_string(),
                        source: "conditional update failed (etag mismatch or object not found)"
                            .into(),
                    });
                }
                let e_tag = OwnedRGWString(etag_ptr).as_string();
                Ok(PutResult {
                    e_tag,
                    version: None,
                })
            }
        }
    }

    /// Read an object (or byte range) from RGW.
    ///
    /// Two read paths depending on the requested size:
    /// - **Small read** (`total_len <= chunk_size`): single `rgw_get_object` call,
    ///   data copied into Rust-owned `Bytes` via `OwnedRGWBuffer::to_bytes()`.
    /// - **Chunked read** (`total_len > chunk_size`): returns a lazy stream that
    ///   reads one chunk per `rgw_get_object` call.  Uses `SendPtr`/`SendConstPtr`
    ///   because the returned stream is `'static` and cannot borrow `&self`.
    async fn get_opts(&self, location: &Path, opts: GetOptions) -> ObjectStoreResult<GetResult> {
        let (meta, attributes) = self.head_opts(location).await?;

        // HEAD request
        if opts.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async { Ok(Bytes::new()) }).boxed()),
                range: 0..0,
                meta,
                attributes,
            });
        }

        let has_conditionals = opts.if_match.is_some()
            || opts.if_none_match.is_some()
            || opts.if_modified_since.is_some()
            || opts.if_unmodified_since.is_some();

        let if_match_c = opts
            .if_match
            .as_deref()
            .map(str_to_cstring)
            .transpose()?;
        let if_nomatch_c = opts
            .if_none_match
            .as_deref()
            .map(str_to_cstring)
            .transpose()?;
        let if_mod_since = opts.if_modified_since.map(|t| t.timestamp());
        let if_unmod_since = opts.if_unmodified_since.map(|t| t.timestamp());

        let obj_size = meta.size;

        // Resolve the byte range to read
        let (range_start, range_end) = match &opts.range {
            Some(GetRange::Bounded(range)) => {
                if range.start >= obj_size {
                    return Err(ObjectStoreError::Generic {
                        store: "rgw",
                        source: format!(
                            "range start {} exceeds object size {}",
                            range.start, obj_size
                        )
                        .into(),
                    });
                }
                (range.start, range.end.min(obj_size))
            }
            Some(GetRange::Offset(start)) => {
                if *start >= obj_size {
                    return Err(ObjectStoreError::Generic {
                        store: "rgw",
                        source: format!("offset {} exceeds object size {}", start, obj_size).into(),
                    });
                }
                (*start, obj_size)
            }
            Some(GetRange::Suffix(len)) => {
                let start = obj_size.saturating_sub(*len);
                (start, obj_size)
            }
            None => (0, obj_size),
        };

        let total_len = range_end - range_start;

        if total_len == 0 {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async { Ok(Bytes::new()) }).boxed()),
                meta,
                range: range_start..range_end,
                attributes,
            });
        }

        // For small reads (<= one chunk), use a single FFI call -- no overhead
        if total_len <= self.chunk_size {
            let bucket = self.bucket_cstr()?;
            let tenant = self.tenant_cstr()?;
            let rgw_bucket = Self::make_bucket(&bucket, &tenant);
            let key = self.path_to_cstr(location)?;
            let obj = Self::make_obj(&key);

            let bytes = {
                let mut buffer = ffi::CRgwBuffer::default();
                let result = if has_conditionals {
                    unsafe {
                        ffi::rgw_get_object_conditional(
                            self.driver,
                            self.dpp,
                            std::ptr::null_mut(),
                            &rgw_bucket,
                            &obj,
                            range_start,
                            total_len,
                            if_match_c.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
                            if_nomatch_c
                                .as_ref()
                                .map_or(std::ptr::null(), |c| c.as_ptr()),
                            if_mod_since
                                .as_ref()
                                .map_or(std::ptr::null(), |v| v as *const i64),
                            if_unmod_since
                                .as_ref()
                                .map_or(std::ptr::null(), |v| v as *const i64),
                            &mut buffer,
                        )
                    }
                } else {
                    unsafe {
                        ffi::rgw_get_object(
                            self.driver,
                            self.dpp,
                            std::ptr::null_mut(),
                            &rgw_bucket,
                            &obj,
                            range_start,
                            total_len,
                            &mut buffer,
                        )
                    }
                };
                if result != 0 {
                    return Err(Self::errno_to_error(result, location, "get"));
                }
                OwnedRGWBuffer(buffer).to_bytes()
            };

            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
                meta,
                range: range_start..range_end,
                attributes,
            });
        }

        // Chunked read: stream that yields one chunk per rgw_get_object call.
        // Uses SendPtr/SendConstPtr because the stream is 'static (outlives &self).
        let bucket_name = self.bucket.clone();
        let tenant_name = self.tenant.clone();
        let key_str: String = location.as_ref().to_string();
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let chunk_size = self.chunk_size;

        let chunk_stream = stream::unfold(range_start, move |offset| {
            let bucket_name = bucket_name.clone();
            let tenant_name = tenant_name.clone();
            let key_str = key_str.clone();

            async move {
                if offset >= range_end {
                    return None;
                }

                let chunk_len = chunk_size.min(range_end - offset);
                let bucket_c = match CString::new(bucket_name.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        return Some((
                            Err(object_store::Error::Generic {
                                store: "rgw",
                                source: Box::new(e),
                            }),
                            range_end,
                        ))
                    }
                };
                let key_c = match CString::new(key_str.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        return Some((
                            Err(object_store::Error::Generic {
                                store: "rgw",
                                source: Box::new(e),
                            }),
                            range_end,
                        ))
                    }
                };
                let tenant_c = match CString::new(tenant_name.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        return Some((
                            Err(object_store::Error::Generic {
                                store: "rgw",
                                source: Box::new(e),
                            }),
                            range_end,
                        ))
                    }
                };
                let rgw_bucket = RGWObjectStore::make_bucket(&bucket_c, &tenant_c);
                let obj = CRgwObject::from_key(key_c.as_ptr());

                let mut buffer = ffi::CRgwBuffer::default();
                let result = unsafe {
                    ffi::rgw_get_object(
                        driver.as_ptr(),
                        dpp.as_ptr(),
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        &obj,
                        offset,
                        chunk_len,
                        &mut buffer,
                    )
                };

                if result != 0 {
                    return Some((
                        Err(object_store::Error::Generic {
                            store: "rgw",
                            source: format!(
                                "get chunk at offset {} failed with errno {}",
                                offset, result
                            )
                            .into(),
                        }),
                        range_end,
                    ));
                }

                let bytes = OwnedRGWBuffer(buffer).to_bytes();
                if bytes.is_empty() {
                    // the object was truncated after its size was read. the
                    // offset would not advance,
                    return Some((
                        Err(object_store::Error::Generic {
                            store: "rgw",
                            source: format!(
                                "read at offset {} returned no data, before the end of the range ({})",
                                offset, range_end
                            )
                            .into(),
                        }),
                        range_end,
                    ));
                }
                let next_offset = offset + bytes.len() as u64;
                Some((Ok(bytes), next_offset))
            }
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(chunk_stream.boxed()),
            meta,
            range: range_start..range_end,
            attributes,
        })
    }

    /// Delete objects from a stream of paths, up to 10 concurrently.
    ///
    /// The returned stream owns all its data (no borrowed references from
    /// `&self`).  Driver/dpp pointers are copied into Send+Sync wrappers.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let bucket = self.bucket.clone();
        let tenant = self.tenant.clone();

        locations
            .map(move |location_result| {
                let bucket = bucket.clone();
                let tenant = tenant.clone();
                async move {
                    let location = location_result?;
                    let bucket_c = str_to_cstring(&bucket)?;
                    let tenant_c = str_to_cstring(&tenant)?;
                    let rgw_bucket = Self::make_bucket(&bucket_c, &tenant_c);
                    let key_c = str_to_cstring(location.as_ref())?;
                    let obj = CRgwObject::from_key(key_c.as_ptr());

                    let result = unsafe {
                        ffi::rgw_delete_object(
                            driver.as_ptr(),
                            dpp.as_ptr(),
                            std::ptr::null_mut(),
                            &rgw_bucket,
                            &obj,
                        )
                    };

                    // ENOENT is already mapped to 0 by rgw_delete_object
                    if result == 0 {
                        Ok(location)
                    } else {
                        Err(object_store::Error::Generic {
                            store: "rgw",
                            source: format!("delete failed with errno {}", result).into(),
                        })
                    }
                }
            })
            .buffered(10)
            .boxed()
    }

    /// List all objects under prefix (flat listing, no delimiter).
    ///
    /// The returned stream owns all its data (see `delete_stream`).
    /// Each page fetches up to 1000 entries via `rgw_list_objects`.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix_str = prefix
            .map(|p| {
                let s = p.to_string();
                if s.is_empty() || s.ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            })
            .unwrap_or_default();
        let bucket = self.bucket.clone();
        let tenant = self.tenant.clone();
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);

        stream::unfold((String::new(), false), move |(marker, done)| {
            let bucket = bucket.clone();
            let tenant = tenant.clone();
            let prefix_str = prefix_str.clone();

            async move {
                if done {
                    return None;
                }

                macro_rules! try_cstring {
                    ($s:expr) => {
                        match CString::new($s) {
                            Ok(c) => c,
                            Err(e) => {
                                return Some((
                                    vec![Err(object_store::Error::Generic {
                                        store: "rgw",
                                        source: Box::new(e),
                                    })],
                                    (String::new(), true),
                                ))
                            }
                        }
                    };
                }

                let bucket_c = try_cstring!(bucket.as_str());
                let tenant_c = try_cstring!(tenant.as_str());
                let rgw_bucket = RGWObjectStore::make_bucket(&bucket_c, &tenant_c);
                let prefix_c = try_cstring!(prefix_str.as_str());
                let marker_c = try_cstring!(marker.as_str());
                let delimiter_c = try_cstring!("");

                let mut result = ffi::CRgwListResult::default();

                let ret = unsafe {
                    ffi::rgw_list_objects(
                        driver.as_ptr(),
                        dpp.as_ptr(),
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        prefix_c.as_ptr(),
                        delimiter_c.as_ptr(),
                        marker_c.as_ptr(),
                        1000,
                        &mut result,
                    )
                };

                if ret != 0 {
                    // the errno is mapped to a typed error, so that a caller could tell a
                    // missing bucket (ENOENT) apart from any other listing failure.
                    // the bucket is part of the path, since it is the only part of the
                    // error that lance keeps when it converts a "not found"
                    let path = Path::from(format!("{}/{}", bucket, prefix_str).as_str());
                    return Some((
                        vec![Err(RGWObjectStore::errno_to_error(ret, &path, "list"))],
                        (String::new(), true),
                    ));
                }

                let owned_result = OwnedRGWListResult(result);
                let entries: Vec<ObjectStoreResult<ObjectMeta>> = unsafe {
                    if owned_result.0.entries.is_null() || owned_result.0.count == 0 {
                        vec![]
                    } else {
                        let slice = std::slice::from_raw_parts(
                            owned_result.0.entries,
                            owned_result.0.count,
                        );
                        slice
                            .iter()
                            .map(|e| Ok(RGWObjectStore::entry_meta(e)))
                            .collect()
                    }
                };

                let next_marker =
                    if owned_result.0.is_truncated != 0 && !owned_result.0.next_marker.is_null() {
                        unsafe {
                            CStr::from_ptr(owned_result.0.next_marker)
                                .to_string_lossy()
                                .into_owned()
                        }
                    } else {
                        String::new()
                    };

                let is_done = owned_result.0.is_truncated == 0;
                let is_done = is_done || entries.is_empty();

                Some((entries, (next_marker, is_done)))
            }
        })
        .flat_map(stream::iter)
        .boxed()
    }

    /// List one level of hierarchy using "/" as delimiter.
    ///
    /// Returns objects at the current level and common prefixes (directory-like
    /// groupings).  Uses `self.driver`/`self.dpp` directly since this is an
    /// `async fn(&self)` -- the borrow covers the entire paginated loop.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix_str = match prefix {
            Some(p) => {
                let s = p.to_string();
                if s.is_empty() || s.ends_with('/') {
                    s
                } else {
                    format!("{}/", s)
                }
            }
            None => String::new(),
        };

        let mut objects: Vec<ObjectMeta> = Vec::new();
        let mut common_prefixes: Vec<Path> = Vec::new();
        let mut marker = String::new();

        loop {
            let bucket_c = self.bucket_cstr()?;
            let tenant_c = self.tenant_cstr()?;
            let rgw_bucket = Self::make_bucket(&bucket_c, &tenant_c);
            let prefix_c = str_to_cstring(&prefix_str)?;
            let marker_c = str_to_cstring(&marker)?;
            let delimiter_c = str_to_cstring("/")?;

            let mut result = ffi::CRgwListResult::default();

            let ret = unsafe {
                ffi::rgw_list_objects(
                    self.driver,
                    self.dpp,
                    std::ptr::null_mut(),
                    &rgw_bucket,
                    prefix_c.as_ptr(),
                    delimiter_c.as_ptr(),
                    marker_c.as_ptr(),
                    1000,
                    &mut result,
                )
            };

            if ret != 0 {
                // the errno is mapped to a typed error, so that a caller could tell a
                // missing bucket (ENOENT) apart from any other listing failure.
                // the bucket is part of the path, since it is the only part of the
                // error that lance keeps when it converts a "not found"
                let path = Path::from(format!("{}/{}", self.bucket, prefix_str).as_str());
                return Err(Self::errno_to_error(ret, &path, "list_with_delimiter"));
            }

            let owned_result = OwnedRGWListResult(result);

            unsafe {
                if !owned_result.0.entries.is_null() && owned_result.0.count > 0 {
                    let slice =
                        std::slice::from_raw_parts(owned_result.0.entries, owned_result.0.count);
                    for e in slice.iter() {
                        let key = CStr::from_ptr(e.key).to_string_lossy().into_owned();

                        if key.ends_with('/') {
                            let prefix_path = key.trim_end_matches('/');
                            if !prefix_path.is_empty() {
                                common_prefixes.push(
                                    Path::parse(prefix_path)
                                        .unwrap_or_else(|_| Path::from(prefix_path)),
                                );
                            }
                        } else if !key.is_empty() {
                            objects.push(Self::entry_meta(e));
                        }
                    }
                }
            }

            if owned_result.0.is_truncated == 0 {
                break;
            }

            if !owned_result.0.next_marker.is_null() {
                marker = unsafe {
                    CStr::from_ptr(owned_result.0.next_marker)
                        .to_string_lossy()
                        .into_owned()
                };
            } else {
                break;
            }

            if owned_result.0.count == 0 {
                break;
            }
        }

        common_prefixes.sort();
        common_prefixes.dedup();

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    /// Copy an object within the same bucket via RGW.
    ///
    /// Supports `Overwrite` (unconditional) and `Create` (copy-if-not-exists).
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        let bucket = self.bucket_cstr()?;
        let tenant = self.tenant_cstr()?;
        let rgw_bucket = Self::make_bucket(&bucket, &tenant);
        let from_key = self.path_to_cstr(from)?;
        let to_key = self.path_to_cstr(to)?;
        let src_obj = Self::make_obj(&from_key);
        let dst_obj = Self::make_obj(&to_key);

        match options.mode {
            CopyMode::Overwrite => {
                let result = unsafe {
                    ffi::rgw_copy_object(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        &src_obj,
                        &rgw_bucket,
                        &dst_obj,
                    )
                };

                if result == 0 {
                    Ok(())
                } else {
                    Err(Self::errno_to_error(result, from, "copy"))
                }
            }
            CopyMode::Create => {
                let if_nomatch = str_to_cstring("*")?;

                let result = unsafe {
                    ffi::rgw_copy_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        &rgw_bucket,
                        &src_obj,
                        &rgw_bucket,
                        &dst_obj,
                        std::ptr::null(),
                        if_nomatch.as_ptr(),
                    )
                };

                if result == 0 {
                    Ok(())
                } else if result == -17 {
                    Err(ObjectStoreError::AlreadyExists {
                        path: to.to_string(),
                        source: "destination already exists".into(),
                    })
                } else {
                    Err(Self::errno_to_error(result, from, "copy_if_not_exists"))
                }
            }
        }
    }

    /// Start a multipart upload via RGW.
    ///
    /// Returns an `RGWMultipartUpload` handle. Caller uploads parts with
    /// `put_part()`, then finalizes with `complete()` or cancels with `abort()`.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let bucket = self.bucket_cstr()?;
        let tenant = self.tenant_cstr()?;
        let rgw_bucket = Self::make_bucket(&bucket, &tenant);
        let key = self.path_to_cstr(location)?;
        let obj = Self::make_obj(&key);

        // Convert attributes to C strings (must live for duration of FFI call)
        let (attrs_cstrings, attrs_meta) = Self::attributes_to_c_meta(&opts.attributes)?;
        let attrs_ptr = if opts.attributes.is_empty() {
            std::ptr::null()
        } else {
            &attrs_meta as *const ffi::CRgwObjectMeta
        };

        let mut upload_id_ptr: *mut c_char = std::ptr::null_mut();

        let result = unsafe {
            ffi::rgw_multipart_init(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                &rgw_bucket,
                &obj,
                attrs_ptr,
                &mut upload_id_ptr,
            )
        };

        // Keep attrs_cstrings alive until after FFI call
        drop(attrs_cstrings);

        if result != 0 {
            return Err(Self::errno_to_error(result, location, "init_multipart"));
        }

        let upload_id_str = OwnedRGWString(upload_id_ptr).as_string().unwrap_or_default();

        Ok(Box::new(RGWMultipartUpload {
            driver: self.driver,
            dpp: self.dpp,
            bucket: self.bucket.clone(),
            tenant: self.tenant.clone(),
            key: location.as_ref().to_string(),
            upload_id: upload_id_str,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }
}

/// Internal helpers
impl RGWObjectStore {
    /// Convert C metadata structure to Rust Attributes
    ///
    /// Extracts standard HTTP headers and custom metadata from CRgwObjectMeta
    fn c_meta_to_attributes(c_meta: &ffi::CRgwObjectMeta) -> Attributes {
        let mut attributes = Attributes::new();

        if !c_meta.content_type.is_null() {
            let content_type = unsafe {
                CStr::from_ptr(c_meta.content_type)
                    .to_string_lossy()
                    .into_owned()
            };
            attributes.insert(object_store::Attribute::ContentType, content_type.into());
        }

        if !c_meta.content_encoding.is_null() {
            let content_encoding = unsafe {
                CStr::from_ptr(c_meta.content_encoding)
                    .to_string_lossy()
                    .into_owned()
            };
            attributes.insert(object_store::Attribute::ContentEncoding, content_encoding.into());
        }

        if !c_meta.content_disposition.is_null() {
            let content_disposition = unsafe {
                CStr::from_ptr(c_meta.content_disposition)
                    .to_string_lossy()
                    .into_owned()
            };
            attributes.insert(object_store::Attribute::ContentDisposition, content_disposition.into());
        }

        if !c_meta.content_language.is_null() {
            let content_language = unsafe {
                CStr::from_ptr(c_meta.content_language)
                    .to_string_lossy()
                    .into_owned()
            };
            attributes.insert(object_store::Attribute::ContentLanguage, content_language.into());
        }

        if !c_meta.cache_control.is_null() {
            let cache_control = unsafe {
                CStr::from_ptr(c_meta.cache_control)
                    .to_string_lossy()
                    .into_owned()
            };
            attributes.insert(object_store::Attribute::CacheControl, cache_control.into());
        }

        // Parse custom metadata from JSON
        if !c_meta.metadata.is_null() {
            let metadata_json = unsafe {
                CStr::from_ptr(c_meta.metadata)
                    .to_string_lossy()
                    .into_owned()
            };
            if let Ok(metadata_map) = serde_json::from_str::<std::collections::HashMap<String, String>>(&metadata_json) {
                for (key, value) in metadata_map {
                    attributes.insert(object_store::Attribute::Metadata(key.into()), value.into());
                }
            }
        }

        attributes
    }

    /// Get object metadata (size, etag, mtime) without reading content.
    ///
    /// Called by `get_opts` and by the default `head` trait method.
    /// Uses `rgw_head_object` -> `load_obj_state`.
    async fn head_opts(&self, location: &Path) -> ObjectStoreResult<(ObjectMeta, Attributes)> {
        let bucket = self.bucket_cstr()?;
        let tenant = self.tenant_cstr()?;
        let rgw_bucket = Self::make_bucket(&bucket, &tenant);
        let key = self.path_to_cstr(location)?;
        let obj = Self::make_obj(&key);

        let mut meta = ffi::CRgwObjectMeta::default();

        let result = unsafe {
            ffi::rgw_head_object(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                &rgw_bucket,
                &obj,
                &mut meta,
            )
        };

        if result != 0 {
            return Err(Self::errno_to_error(result, location, "head"));
        }

        let owned_meta = OwnedRGWObjectMeta(meta);

        let etag = if !owned_meta.0.etag.is_null() {
            Some(unsafe {
                CStr::from_ptr(owned_meta.0.etag)
                    .to_string_lossy()
                    .into_owned()
            })
        } else {
            None
        };

        // Extract attributes using helper function
        let attributes = Self::c_meta_to_attributes(&owned_meta.0);

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Self::timestamp(
                owned_meta.0.last_modified,
                owned_meta.0.last_modified_ns,
            ),
            size: owned_meta.0.size,
            e_tag: etag,
            version: None,
        };
        
        Ok((meta, attributes))
    }
}

/// Multipart upload state for RGW.
///
/// Holds raw driver/dpp pointers (not `SendPtr`) because `MultipartUpload`
/// methods take `&mut self`, so the borrow covers each call.  The `parts`
/// vec collects ETags returned by each `put_part`; `complete` passes them
/// in order to `rgw_multipart_complete` to assemble the final object.
#[derive(Debug)]
struct RGWMultipartUpload {
    driver: *mut CRgwDriver,
    dpp: *const CRgwDoutPrefix,
    bucket: String,
    tenant: String,
    key: String,
    upload_id: String,
    parts: Arc<Mutex<Vec<String>>>,
}

unsafe impl Send for RGWMultipartUpload {}

impl RGWMultipartUpload {
    fn make_bucket(bucket_c: &CString, tenant_c: &CString) -> CRgwBucket {
        let tenant_ptr = if tenant_c.as_bytes().is_empty() {
            std::ptr::null()
        } else {
            tenant_c.as_ptr()
        };
        CRgwBucket::new(bucket_c.as_ptr(), tenant_ptr)
    }
}

#[async_trait]
impl MultipartUpload for RGWMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let bucket = self.bucket.clone();
        let tenant = self.tenant.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let parts = self.parts.clone();

        let part_index = {
            let mut parts_guard = parts.lock().unwrap();
            parts_guard.push(String::new());
            parts_guard.len() - 1
        };
        let part_num = (part_index + 1) as u32;

        Box::pin(async move {
            let bucket_c = str_to_cstring(&bucket)?;
            let tenant_c = str_to_cstring(&tenant)?;
            let rgw_bucket = Self::make_bucket(&bucket_c, &tenant_c);
            let key_c = str_to_cstring(&key)?;
            let obj = CRgwObject::from_key(key_c.as_ptr());
            let upload_id_c = str_to_cstring(&upload_id)?;

            let bytes: Bytes = data.into();
            let mut etag_ptr: *mut c_char = std::ptr::null_mut();

            let result = unsafe {
                ffi::rgw_multipart_put_part(
                    driver.as_ptr(),
                    dpp.as_ptr(),
                    std::ptr::null_mut(),
                    &rgw_bucket,
                    &obj,
                    upload_id_c.as_ptr(),
                    part_num,
                    bytes.as_ptr(),
                    bytes.len(),
                    &mut etag_ptr,
                )
            };

            if result != 0 {
                return Err(object_store::Error::Generic {
                    store: "rgw",
                    source: format!("put_part failed with errno {}", result).into(),
                });
            }

            let etag_str = OwnedRGWString(etag_ptr).as_string().unwrap_or_default();

            {
                let mut parts_guard = parts.lock().unwrap();
                parts_guard[part_index] = etag_str;
            }

            Ok(())
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let bucket_c = str_to_cstring(&self.bucket)?;
        let tenant_c = str_to_cstring(&self.tenant)?;
        let rgw_bucket = RGWMultipartUpload::make_bucket(&bucket_c, &tenant_c);
        let key_c = str_to_cstring(&self.key)?;
        let obj = CRgwObject::from_key(key_c.as_ptr());
        let upload_id_c = str_to_cstring(&self.upload_id)?;

        let parts_guard = self
            .parts
            .lock()
            .map_err(|e| object_store::Error::Generic {
                store: "rgw",
                source: format!("failed to lock parts: {}", e).into(),
            })?;

        let etag_cstrings: Vec<CString> = parts_guard
            .iter()
            .map(|s| str_to_cstring(s))
            .collect::<ObjectStoreResult<Vec<_>>>()?;
        let etag_ptrs: Vec<*const c_char> = etag_cstrings.iter().map(|s| s.as_ptr()).collect();

        let result = unsafe {
            ffi::rgw_multipart_complete(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                &rgw_bucket,
                &obj,
                upload_id_c.as_ptr(),
                etag_ptrs.as_ptr(),
                etag_ptrs.len(),
            )
        };

        drop(parts_guard);

        if result != 0 {
            return Err(object_store::Error::Generic {
                store: "rgw",
                source: format!("complete_multipart failed with errno {}", result).into(),
            });
        }

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        let bucket_c = str_to_cstring(&self.bucket)?;
        let tenant_c = str_to_cstring(&self.tenant)?;
        let rgw_bucket = RGWMultipartUpload::make_bucket(&bucket_c, &tenant_c);
        let key_c = str_to_cstring(&self.key)?;
        let obj = CRgwObject::from_key(key_c.as_ptr());
        let upload_id_c = str_to_cstring(&self.upload_id)?;

        let result = unsafe {
            ffi::rgw_multipart_abort(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                &rgw_bucket,
                &obj,
                upload_id_c.as_ptr(),
            )
        };

        if result != 0 {
            return Err(object_store::Error::Generic {
                store: "rgw",
                source: format!("abort_multipart failed with errno {}", result).into(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_display_with_prefix() {
        let store = unsafe {
            RGWObjectStore::new(
                std::ptr::null_mut(),
                std::ptr::null(),
                "test-bucket",
                "",
                "my-prefix/",
            )
        };
        assert_eq!(
            format!("{}", store),
            "RGWObjectStore(bucket=test-bucket, prefix=my-prefix/)"
        );
    }

    // ========================================================================
    // Unit tests for c_meta_to_attributes and attributes_to_c_meta
    // ========================================================================

    /// Helper to create CRgwObjectMeta with test data
    /// Returns (CStrings to keep alive, CRgwObjectMeta)
    fn create_test_c_meta(
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        content_disposition: Option<&str>,
        content_language: Option<&str>,
        cache_control: Option<&str>,
        metadata_json: Option<&str>,
    ) -> (Vec<CString>, ffi::CRgwObjectMeta) {
        let mut cstrings = Vec::new();
        let mut meta = ffi::CRgwObjectMeta::default();

        if let Some(ct) = content_type {
            let cs = CString::new(ct).unwrap();
            meta.content_type = cs.as_ptr() as *mut _;
            cstrings.push(cs);
        }

        if let Some(ce) = content_encoding {
            let cs = CString::new(ce).unwrap();
            meta.content_encoding = cs.as_ptr() as *mut _;
            cstrings.push(cs);
        }

        if let Some(cd) = content_disposition {
            let cs = CString::new(cd).unwrap();
            meta.content_disposition = cs.as_ptr() as *mut _;
            cstrings.push(cs);
        }

        if let Some(cl) = content_language {
            let cs = CString::new(cl).unwrap();
            meta.content_language = cs.as_ptr() as *mut _;
            cstrings.push(cs);
        }

        if let Some(cc) = cache_control {
            let cs = CString::new(cc).unwrap();
            meta.cache_control = cs.as_ptr() as *mut _;
            cstrings.push(cs);
        }

        if let Some(md) = metadata_json {
            let cs = CString::new(md).unwrap();
            meta.metadata = cs.as_ptr() as *mut _;
            cstrings.push(cs);
        }

        (cstrings, meta)
    }

    #[test]
    fn test_c_meta_to_attributes_all_headers() {
        let (_cstrings, meta) = create_test_c_meta(
            Some("application/json"),
            Some("gzip"),
            Some("attachment; filename=\"test.txt\""),
            Some("en-US"),
            Some("max-age=604800"),
            None,
        );

        let attrs = RGWObjectStore::c_meta_to_attributes(&meta);

        assert_eq!(attrs.len(), 5);
        assert_eq!(
            attrs.get(&object_store::Attribute::ContentType),
            Some(&"application/json".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::ContentEncoding),
            Some(&"gzip".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::ContentDisposition),
            Some(&"attachment; filename=\"test.txt\"".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::ContentLanguage),
            Some(&"en-US".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::CacheControl),
            Some(&"max-age=604800".into())
        );
    }

    #[test]
    fn test_attributes_to_c_meta_all_headers() {
        let mut attrs = Attributes::new();
        attrs.insert(
            object_store::Attribute::ContentType,
            "application/json".into(),
        );
        attrs.insert(object_store::Attribute::ContentEncoding, "gzip".into());
        attrs.insert(
            object_store::Attribute::ContentDisposition,
            "attachment; filename=\"test.txt\"".into(),
        );
        attrs.insert(object_store::Attribute::ContentLanguage, "en-US".into());
        attrs.insert(
            object_store::Attribute::CacheControl,
            "max-age=604800".into(),
        );

        let result = RGWObjectStore::attributes_to_c_meta(&attrs);
        assert!(result.is_ok());

        let (_holder, meta) = result.unwrap();

        // Verify all pointers are non-null
        assert!(!meta.content_type.is_null());
        assert!(!meta.content_encoding.is_null());
        assert!(!meta.content_disposition.is_null());
        assert!(!meta.content_language.is_null());
        assert!(!meta.cache_control.is_null());

        // Verify content
        unsafe {
            assert_eq!(
                CStr::from_ptr(meta.content_type).to_str().unwrap(),
                "application/json"
            );
            assert_eq!(CStr::from_ptr(meta.content_encoding).to_str().unwrap(), "gzip");
            assert_eq!(
                CStr::from_ptr(meta.content_disposition).to_str().unwrap(),
                "attachment; filename=\"test.txt\""
            );
            assert_eq!(
                CStr::from_ptr(meta.content_language).to_str().unwrap(),
                "en-US"
            );
            assert_eq!(
                CStr::from_ptr(meta.cache_control).to_str().unwrap(),
                "max-age=604800"
            );
        }
    }

    #[test]
    fn test_c_meta_to_attributes_with_custom_metadata() {
        let json = r#"{"key1":"value1","key2":"value2","x-custom":"test"}"#;
        let (_cstrings, meta) = create_test_c_meta(
            Some("text/html"),
            None,
            None,
            None,
            None,
            Some(json),
        );

        let attrs = RGWObjectStore::c_meta_to_attributes(&meta);

        assert_eq!(attrs.len(), 4);
        assert_eq!(
            attrs.get(&object_store::Attribute::ContentType),
            Some(&"text/html".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::Metadata("key1".into())),
            Some(&"value1".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::Metadata("key2".into())),
            Some(&"value2".into())
        );
        assert_eq!(
            attrs.get(&object_store::Attribute::Metadata("x-custom".into())),
            Some(&"test".into())
        );
    }

    #[test]
    fn test_round_trip_mixed_headers_and_metadata() {
        let mut original = Attributes::new();
        original.insert(
            object_store::Attribute::ContentType,
            "text/html".into(),
        );
        original.insert(
            object_store::Attribute::ContentDisposition,
            "inline".into(),
        );
        original.insert(
            object_store::Attribute::Metadata("user-id".into()),
            "12345".into(),
        );
        original.insert(
            object_store::Attribute::Metadata("session".into()),
            "abc-def".into(),
        );

        // Convert to C
        let result = RGWObjectStore::attributes_to_c_meta(&original);
        assert!(result.is_ok());
        let (_holder, c_meta) = result.unwrap();

        // Convert back to Rust
        let round_trip = RGWObjectStore::c_meta_to_attributes(&c_meta);

        // Verify equality
        assert_eq!(original.len(), round_trip.len());
        assert_eq!(
            original.get(&object_store::Attribute::ContentType),
            round_trip.get(&object_store::Attribute::ContentType)
        );
        assert_eq!(
            original.get(&object_store::Attribute::ContentDisposition),
            round_trip.get(&object_store::Attribute::ContentDisposition)
        );
        assert_eq!(
            original.get(&object_store::Attribute::Metadata("user-id".into())),
            round_trip.get(&object_store::Attribute::Metadata("user-id".into()))
        );
        assert_eq!(
            original.get(&object_store::Attribute::Metadata("session".into())),
            round_trip.get(&object_store::Attribute::Metadata("session".into()))
        );
    }

}
