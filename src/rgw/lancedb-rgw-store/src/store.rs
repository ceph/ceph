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

use crate::ffi::{self, OwnedRGWBuffer, OwnedRGWListResult, OwnedRGWObjectMeta, OwnedRGWString,
    RGWObject, RGWSalDriver, RGWDoutPrefix, RGWString};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    path::Path, Attributes, CopyMode, CopyOptions, Error as ObjectStoreError, GetOptions,
    GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult, UpdateVersion,
};
use std::ffi::{CStr, CString};
use std::os::raw::c_int;
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
struct SendPtr(*mut RGWSalDriver);

unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

impl SendPtr {
    fn new(ptr: *mut RGWSalDriver) -> Self {
        Self(ptr)
    }
    fn as_ptr(&self) -> *mut RGWSalDriver {
        self.0
    }
}

/// Send+Sync wrapper for dpp pointer (see [`SendPtr`]).
#[derive(Clone, Copy, Debug)]
struct SendConstPtr(*const RGWDoutPrefix);

unsafe impl Send for SendConstPtr {}
unsafe impl Sync for SendConstPtr {}

impl SendConstPtr {
    fn new(ptr: *const RGWDoutPrefix) -> Self {
        Self(ptr)
    }
    fn as_ptr(&self) -> *const RGWDoutPrefix {
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

/// ObjectStore implementation that uses RGW SAL directly
///
/// This store holds raw pointers to Ceph's RGW driver and DoutPrefixProvider.
/// These pointers must remain valid for the lifetime of this store.
pub struct RGWObjectStore {
    driver: *mut RGWSalDriver,
    dpp: *const RGWDoutPrefix,
    bucket: String,
    /// Path prefix for debugging/logging (not functionally needed due to
    /// 1:1 bucket-to-db mapping, kept for debug/logging only)
    prefix: String,
    chunk_size: u64,
}

// Safety: The raw pointers reference RGW driver and DoutPrefixProvider which
// are thread-safe — they are shared across RGW request threads and SAL
// provides atomic readers/writers for concurrent object access.
unsafe impl Send for RGWObjectStore {}
unsafe impl Sync for RGWObjectStore {}

impl RGWObjectStore {
    /// Create a new RGWObjectStore
    ///
    /// Reads `rgw_max_chunk_size` from the driver's config to set the
    /// streaming read chunk size.
    ///
    /// # Safety
    /// The caller must ensure that `driver` and `dpp` pointers remain valid
    /// for the lifetime of this store and any clones.
    pub unsafe fn new(driver: *mut RGWSalDriver, dpp: *const RGWDoutPrefix, bucket: &str, prefix: &str) -> Self {
        let chunk_size = ffi::rgw_get_max_chunk_size(driver);
        Self {
            driver,
            dpp,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            chunk_size: if chunk_size > 0 { chunk_size } else { DEFAULT_CHUNK_SIZE },
        }
    }

    /// Get bucket as C string
    fn bucket_cstr(&self) -> ObjectStoreResult<CString> {
        str_to_cstring(&self.bucket)
    }

    /// Convert path to C string key
    fn path_to_cstr(&self, path: &Path) -> ObjectStoreResult<CString> {
        str_to_cstring(&path.to_string())
    }

    fn make_obj(key: &CString) -> RGWObject {
        RGWObject::from_key(key.as_ptr())
    }

    /// Get the prefix for this store
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Convert errno to ObjectStore error (test-only public accessor)
    #[cfg(test)]
    pub fn errno_to_error_for_test(&self, errno: i32, path: &Path, op: &str) -> object_store::Error {
        self.errno_to_error(errno, path, op)
    }

    /// Convert errno to ObjectStore error
    fn errno_to_error(&self, errno: i32, path: &Path, op: &str) -> object_store::Error {
        match errno {
            -2 => object_store::Error::NotFound { // ENOENT
                path: path.to_string(),
                source: format!("{} failed: object not found", op).into(),
            },
            -1 => object_store::Error::Generic { // EPERM
                store: "rgw",
                source: format!("{} failed: operation not permitted", op).into(),
            },
            -13 => object_store::Error::Generic { // EACCES
                store: "rgw",
                source: format!("{} failed: permission denied", op).into(),
            },
            -17 => object_store::Error::AlreadyExists { // EEXIST
                path: path.to_string(),
                source: format!("{} failed: object already exists", op).into(),
            },
            -22 => object_store::Error::Generic { // EINVAL
                store: "rgw",
                source: format!("{} failed: invalid argument", op).into(),
            },
            -28 => object_store::Error::Generic { // ENOSPC
                store: "rgw",
                source: format!("{} failed: no space left on device", op).into(),
            },
            -36 => object_store::Error::Generic { // ENAMETOOLONG
                store: "rgw",
                source: format!("{} failed: object key too long", op).into(),
            },
            -38 => object_store::Error::NotSupported { // ENOSYS
                source: format!("{} not supported by SAL backend", op).into(),
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
            write!(f, "RGWObjectStore(bucket={}, prefix={})", self.bucket, self.prefix)
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
    /// Write an object to RGW via SAL.
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
        let key = self.path_to_cstr(location)?;
        let obj = Self::make_obj(&key);
        let content_type = str_to_cstring("application/octet-stream")?;

        let bytes: Bytes = payload.into();

        match opts.mode {
            PutMode::Overwrite => {
                let result = unsafe {
                    ffi::rgw_put_object(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        bucket.as_ptr(),
                        &obj,
                        bytes.as_ptr(),
                        bytes.len(),
                        content_type.as_ptr(),
                    )
                };

                if result == 0 {
                    Ok(PutResult {
                        e_tag: None,
                        version: None,
                    })
                } else {
                    Err(self.errno_to_error(result, location, "put"))
                }
            }
            PutMode::Create => {
                let if_nomatch = str_to_cstring("*")?;
                let mut canceled: c_int = 0;

                let result = unsafe {
                    ffi::rgw_put_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        bucket.as_ptr(),
                        &obj,
                        bytes.as_ptr(),
                        bytes.len(),
                        content_type.as_ptr(),
                        std::ptr::null(),
                        if_nomatch.as_ptr(),
                        &mut canceled,
                    )
                };

                if result != 0 {
                    return Err(self.errno_to_error(result, location, "put (create)"));
                }
                if canceled != 0 {
                    return Err(ObjectStoreError::AlreadyExists {
                        path: location.to_string(),
                        source: "object already exists (conditional create failed)".into(),
                    });
                }
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
            PutMode::Update(UpdateVersion { e_tag, .. }) => {
                let etag_str = e_tag.ok_or_else(|| ObjectStoreError::Generic {
                    store: "rgw",
                    source: "PutMode::Update requires e_tag".into(),
                })?;
                let if_match = CString::new(etag_str.as_str()).map_err(|_| {
                    ObjectStoreError::Generic {
                        store: "rgw",
                        source: "invalid etag string".into(),
                    }
                })?;
                let mut canceled: c_int = 0;

                let result = unsafe {
                    ffi::rgw_put_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        bucket.as_ptr(),
                        &obj,
                        bytes.as_ptr(),
                        bytes.len(),
                        content_type.as_ptr(),
                        if_match.as_ptr(),
                        std::ptr::null(),
                        &mut canceled,
                    )
                };

                if result != 0 {
                    return Err(self.errno_to_error(result, location, "put (update)"));
                }
                if canceled != 0 {
                    return Err(ObjectStoreError::Precondition {
                        path: location.to_string(),
                        source: "object ETag does not match (conditional update failed)".into(),
                    });
                }
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
        }
    }

    /// Read an object (or byte range) from RGW via SAL.
    ///
    /// Two read paths depending on the requested size:
    /// - **Small read** (`total_len <= chunk_size`): single `rgw_get_object` call,
    ///   data copied into Rust-owned `Bytes` via `OwnedRGWBuffer::to_bytes()`.
    /// - **Chunked read** (`total_len > chunk_size`): returns a lazy stream that
    ///   reads one chunk per `rgw_get_object` call.  Uses `SendPtr`/`SendConstPtr`
    ///   because the returned stream is `'static` and cannot borrow `&self`.
    async fn get_opts(&self, location: &Path, opts: GetOptions) -> ObjectStoreResult<GetResult> {
        let meta = self.head_opts(location).await?;

        // HEAD request
        if opts.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async { Ok(Bytes::new()) }).boxed(),
                ),
                range: 0..0,
                meta,
                attributes: Attributes::new(),
            });
        }

        let obj_size = meta.size;

        // Resolve the byte range to read
        let (range_start, range_end) = match &opts.range {
            Some(GetRange::Bounded(range)) => (range.start, range.end.min(obj_size)),
            Some(GetRange::Offset(start)) => (*start, obj_size),
            Some(GetRange::Suffix(len)) => {
                let start = obj_size.saturating_sub(*len);
                (start, obj_size)
            }
            None => (0, obj_size),
        };

        let total_len = range_end.saturating_sub(range_start);

        if total_len == 0 {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async { Ok(Bytes::new()) }).boxed(),
                ),
                meta,
                range: range_start..range_end,
                attributes: Attributes::new(),
            });
        }

        // For small reads (≤ one chunk), use a single FFI call — no overhead
        if total_len <= self.chunk_size {
            let bucket = self.bucket_cstr()?;
            let key = self.path_to_cstr(location)?;
            let obj = Self::make_obj(&key);

            let bytes = {
                let mut buffer = ffi::RGWBuffer::default();
                let result = unsafe {
                    ffi::rgw_get_object(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        bucket.as_ptr(),
                        &obj,
                        range_start,
                        total_len,
                        &mut buffer,
                    )
                };
                if result != 0 {
                    return Err(self.errno_to_error(result, location, "get"));
                }
                OwnedRGWBuffer(buffer).to_bytes()
            };

            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async move { Ok(bytes) }).boxed(),
                ),
                meta,
                range: range_start..range_end,
                attributes: Attributes::new(),
            });
        }

        // Chunked read: stream that yields one chunk per rgw_get_object call.
        // Uses SendPtr/SendConstPtr because the stream is 'static (outlives &self).
        let bucket_name = self.bucket.clone();
        let key_str = location.to_string();
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let chunk_size = self.chunk_size;

        let chunk_stream = stream::unfold(range_start, move |offset| {
            let bucket_name = bucket_name.clone();
            let key_str = key_str.clone();

            async move {
                if offset >= range_end {
                    return None;
                }

                let chunk_len = chunk_size.min(range_end - offset);
                let bucket_c = match CString::new(bucket_name.as_str()) {
                    Ok(c) => c,
                    Err(e) => return Some((
                        Err(object_store::Error::Generic {
                            store: "rgw",
                            source: Box::new(e),
                        }),
                        range_end,
                    )),
                };
                let key_c = match CString::new(key_str.as_str()) {
                    Ok(c) => c,
                    Err(e) => return Some((
                        Err(object_store::Error::Generic {
                            store: "rgw",
                            source: Box::new(e),
                        }),
                        range_end,
                    )),
                };
                let obj = RGWObject::from_key(key_c.as_ptr());

                let mut buffer = ffi::RGWBuffer::default();
                let result = unsafe {
                    ffi::rgw_get_object(
                        driver.as_ptr(),
                        dpp.as_ptr(),
                        std::ptr::null_mut(),
                        bucket_c.as_ptr(),
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
                let next_offset = offset + bytes.len() as u64;
                Some((Ok(bytes), next_offset))
            }
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(chunk_stream.boxed()),
            meta,
            range: range_start..range_end,
            attributes: Attributes::new(),
        })
    }


    /// Delete objects from a stream of paths, up to 10 concurrently.
    ///
    /// Returns a `'static` stream, so `&self` cannot be captured.  The driver
    /// and dpp pointers are copied into `SendPtr`/`SendConstPtr` wrappers to
    /// satisfy the `Send` bound required by async streams.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let bucket = self.bucket.clone();

        locations
            .map(move |location_result| {
                let bucket = bucket.clone();
                async move {
                    let location = location_result?;
                    let bucket_c = str_to_cstring(&bucket)?;
                    let key_c = str_to_cstring(&location.to_string())?;
                    let obj = RGWObject::from_key(key_c.as_ptr());

                    let result = unsafe {
                        ffi::rgw_delete_object(
                            driver.as_ptr(),
                            dpp.as_ptr(),
                            std::ptr::null_mut(),
                            bucket_c.as_ptr(),
                            &obj,
                        )
                    };

                    // Treat "not found" as success for delete operations
                    if result == 0 || result == -2 {
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

    /// List objects with optional prefix, paginated via markers.
    ///
    /// Returns a `'static` stream — uses `SendPtr`/`SendConstPtr` (see `delete_stream`).
    /// Each page fetches up to 1000 entries via `rgw_list_objects` with empty delimiter
    /// (flat/recursive listing).
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let bucket = self.bucket.clone();
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);

        stream::unfold(
            (String::new(), false),
            move |(marker, done)| {
                let bucket = bucket.clone();
                let prefix_str = prefix_str.clone();

                async move {
                    if done {
                        return None;
                    }

                    macro_rules! try_cstring {
                        ($s:expr) => {
                            match CString::new($s) {
                                Ok(c) => c,
                                Err(e) => return Some((
                                    vec![Err(object_store::Error::Generic {
                                        store: "rgw",
                                        source: Box::new(e),
                                    })],
                                    (String::new(), true),
                                )),
                            }
                        };
                    }

                    let bucket_c = try_cstring!(bucket.as_str());
                    let prefix_c = try_cstring!(prefix_str.as_str());
                    let marker_c = try_cstring!(marker.as_str());
                    let delimiter_c = try_cstring!("");

                    let mut result = ffi::RGWListResult::default();

                    let ret = unsafe {
                        ffi::rgw_list_objects(
                            driver.as_ptr(),
                            dpp.as_ptr(),
                            std::ptr::null_mut(),
                            bucket_c.as_ptr(),
                            prefix_c.as_ptr(),
                            delimiter_c.as_ptr(),
                            marker_c.as_ptr(),
                            1000,
                            &mut result,
                        )
                    };

                    if ret != 0 {
                        return Some((
                            vec![Err(object_store::Error::Generic {
                                store: "rgw",
                                source: format!("list failed with errno {}", ret).into(),
                            })],
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
                                .map(|e| {
                                    let key = CStr::from_ptr(e.key).to_string_lossy().into_owned();
                                    Ok(ObjectMeta {
                                        location: Path::from(key),
                                        last_modified: chrono::DateTime::from_timestamp(
                                            e.last_modified,
                                            0,
                                        )
                                        .unwrap_or_else(chrono::Utc::now),
                                        size: e.size,
                                        e_tag: None,
                                        version: None,
                                    })
                                })
                                .collect()
                        }
                    };

                    let next_marker = if owned_result.0.is_truncated != 0
                        && !owned_result.0.next_marker.is_null()
                    {
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
            },
        )
        .flat_map(|results| stream::iter(results))
        .boxed()
    }

    /// List one level of hierarchy using "/" as delimiter.
    ///
    /// Returns objects at the current level and common prefixes (directory-like
    /// groupings).  Uses `self.driver`/`self.dpp` directly since this is an
    /// `async fn(&self)` — the borrow covers the entire paginated loop.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let prefix_str = match prefix {
            Some(p) => {
                let s = p.to_string();
                if s.is_empty() {
                    s
                } else if s.ends_with('/') {
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
            let prefix_c = str_to_cstring(&prefix_str)?;
            let marker_c = str_to_cstring(&marker)?;
            let delimiter_c = str_to_cstring("/")?;

            let mut result = ffi::RGWListResult::default();

            let ret = unsafe {
                ffi::rgw_list_objects(
                    self.driver,
                    self.dpp,
                    std::ptr::null_mut(),
                    bucket_c.as_ptr(),
                    prefix_c.as_ptr(),
                    delimiter_c.as_ptr(),
                    marker_c.as_ptr(),
                    1000,
                    &mut result,
                )
            };

            if ret != 0 {
                return Err(object_store::Error::Generic {
                    store: "rgw",
                    source: format!("list_with_delimiter failed with errno {}", ret).into(),
                });
            }

            let owned_result = OwnedRGWListResult(result);

            unsafe {
                if !owned_result.0.entries.is_null() && owned_result.0.count > 0 {
                    let slice = std::slice::from_raw_parts(
                        owned_result.0.entries,
                        owned_result.0.count,
                    );
                    for e in slice.iter() {
                        let key = CStr::from_ptr(e.key).to_string_lossy().into_owned();

                        if key.ends_with('/') {
                            let prefix_path = key.trim_end_matches('/');
                            if !prefix_path.is_empty() {
                                common_prefixes.push(Path::from(prefix_path));
                            }
                        } else if !key.is_empty() {
                            objects.push(ObjectMeta {
                                location: Path::from(key.clone()),
                                last_modified: chrono::DateTime::from_timestamp(
                                    e.last_modified,
                                    0,
                                )
                                .unwrap_or_else(chrono::Utc::now),
                                size: e.size,
                                e_tag: None,
                                version: None,
                            });
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

    /// Copy an object within the same bucket via SAL.
    ///
    /// Supports `Overwrite` (unconditional) and `Create` (copy-if-not-exists).
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        let bucket = self.bucket_cstr()?;
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
                        bucket.as_ptr(),
                        &src_obj,
                        bucket.as_ptr(),
                        &dst_obj,
                    )
                };

                if result == 0 {
                    Ok(())
                } else {
                    Err(self.errno_to_error(result, from, "copy"))
                }
            }
            CopyMode::Create => {
                let if_nomatch = str_to_cstring("*")?;

                let result = unsafe {
                    ffi::rgw_copy_object_conditional(
                        self.driver,
                        self.dpp,
                        std::ptr::null_mut(),
                        bucket.as_ptr(),
                        &src_obj,
                        bucket.as_ptr(),
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
                    Err(self.errno_to_error(result, from, "copy_if_not_exists"))
                }
            }
        }
    }

    /// Start a multipart upload via SAL.
    ///
    /// Returns an `RGWMultipartUpload` handle. Caller uploads parts with
    /// `put_part()`, then finalizes with `complete()` or cancels with `abort()`.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let bucket = self.bucket_cstr()?;
        let key = self.path_to_cstr(location)?;
        let obj = Self::make_obj(&key);

        let mut upload_id_out = RGWString::default();

        let result = unsafe {
            ffi::rgw_init_multipart(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                bucket.as_ptr(),
                &obj,
                &mut upload_id_out,
            )
        };

        if result != 0 {
            return Err(self.errno_to_error(result, location, "init_multipart"));
        }

        let owned = OwnedRGWString(upload_id_out);
        let upload_id_str = owned.to_string();

        Ok(Box::new(RGWMultipartUpload {
            driver: self.driver,
            dpp: self.dpp,
            bucket: self.bucket.clone(),
            key: location.to_string(),
            upload_id: upload_id_str,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }
}

/// Internal helpers
impl RGWObjectStore {
    /// Get object metadata (size, etag, mtime) without reading content.
    ///
    /// Called by `get_opts` and by the default `head` trait method.
    /// Uses `rgw_head_object` → `load_obj_state`.
    async fn head_opts(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let bucket = self.bucket_cstr()?;
        let key = self.path_to_cstr(location)?;
        let obj = Self::make_obj(&key);

        let mut meta = ffi::RGWObjectMeta::default();

        let result = unsafe {
            ffi::rgw_head_object(self.driver, self.dpp, std::ptr::null_mut(), bucket.as_ptr(), &obj, &mut meta)
        };

        if result != 0 {
            return Err(self.errno_to_error(result, location, "head"));
        }

        let owned_meta = OwnedRGWObjectMeta(meta);

        let etag = if !owned_meta.0.etag.is_null() {
            Some(unsafe { CStr::from_ptr(owned_meta.0.etag).to_string_lossy().into_owned() })
        } else {
            None
        };

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: chrono::DateTime::from_timestamp(owned_meta.0.last_modified, 0)
                .unwrap_or_else(chrono::Utc::now),
            size: owned_meta.0.size,
            e_tag: etag,
            version: None,
        })
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
    driver: *mut RGWSalDriver,
    dpp: *const RGWDoutPrefix,
    bucket: String,
    key: String,
    upload_id: String,
    parts: Arc<Mutex<Vec<String>>>,
}

unsafe impl Send for RGWMultipartUpload {}

#[async_trait]
impl MultipartUpload for RGWMultipartUpload {
    fn put_part(
        &mut self,
        data: PutPayload,
    ) -> object_store::UploadPart {
        let driver = SendPtr::new(self.driver);
        let dpp = SendConstPtr::new(self.dpp);
        let bucket = self.bucket.clone();
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
            let key_c = str_to_cstring(&key)?;
            let obj = RGWObject::from_key(key_c.as_ptr());
            let upload_id_c = str_to_cstring(&upload_id)?;

            let bytes: Bytes = data.into();
            let mut etag_out = RGWString::default();

            let result = unsafe {
                ffi::rgw_multipart_put_part(
                    driver.as_ptr(),
                    dpp.as_ptr(),
                    std::ptr::null_mut(),
                    bucket_c.as_ptr(),
                    &obj,
                    upload_id_c.as_ptr(),
                    part_num,
                    bytes.as_ptr(),
                    bytes.len(),
                    &mut etag_out,
                )
            };

            if result != 0 {
                return Err(object_store::Error::Generic {
                    store: "rgw",
                    source: format!("put_part failed with errno {}", result).into(),
                });
            }

            let owned = OwnedRGWString(etag_out);
            let etag_str = owned.to_string();

            {
                let mut parts_guard = parts.lock().unwrap();
                parts_guard[part_index] = etag_str;
            }

            Ok(())
        })
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let bucket_c = str_to_cstring(&self.bucket)?;
        let key_c = str_to_cstring(&self.key)?;
        let obj = RGWObject::from_key(key_c.as_ptr());
        let upload_id_c = str_to_cstring(&self.upload_id)?;

        let parts_guard = self.parts.lock().map_err(|e| {
            object_store::Error::Generic {
                store: "rgw",
                source: format!("failed to lock parts: {}", e).into(),
            }
        })?;

        let etag_cstrings: Vec<CString> = parts_guard
            .iter()
            .map(|s| str_to_cstring(s))
            .collect::<ObjectStoreResult<Vec<_>>>()?;
        let etag_ptrs: Vec<*const i8> = etag_cstrings.iter().map(|s| s.as_ptr()).collect();

        let result = unsafe {
            ffi::rgw_multipart_complete(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                bucket_c.as_ptr(),
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
        let key_c = str_to_cstring(&self.key)?;
        let obj = RGWObject::from_key(key_c.as_ptr());
        let upload_id_c = str_to_cstring(&self.upload_id)?;

        let result = unsafe {
            ffi::rgw_multipart_abort(
                self.driver,
                self.dpp,
                std::ptr::null_mut(),
                bucket_c.as_ptr(),
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
        let store = unsafe { RGWObjectStore::new(std::ptr::null_mut(), std::ptr::null(), "test-bucket", "my-prefix/") };
        assert_eq!(format!("{}", store), "RGWObjectStore(bucket=test-bucket, prefix=my-prefix/)");
    }
}
