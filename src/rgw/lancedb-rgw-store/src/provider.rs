/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! ObjectStoreProvider that creates RGWObjectStore instances
//!
//! This provider is registered in the ObjectStoreRegistry to handle
//! specific URL schemes (e.g., "s3").

use crate::ffi::{CRgwDoutPrefix, CRgwDriver};
use crate::store::RGWObjectStore;
use async_trait::async_trait;
use lance_core::Result;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, StorageOptions, DEFAULT_CLOUD_IO_PARALLELISM,
};
use object_store::path::Path;
use std::sync::Arc;
use url::Url;

/// Provider that creates RGWObjectStore instances for rgw:// URLs
///
/// This provider routes all object operations through Ceph RGW's native
/// SAL API instead of the S3 HTTP protocol.
#[derive(Debug)]
pub struct RGWStoreProvider {
    driver: *mut CRgwDriver,
    dpp: *const CRgwDoutPrefix,
}

// To guarantee that driver and dpp are pointers are safe to share across threads
unsafe impl Send for RGWStoreProvider {}
unsafe impl Sync for RGWStoreProvider {}

impl RGWStoreProvider {
    /// Create a new RGWStoreProvider
    ///
    /// # Safety
    /// The caller must ensure that `driver` and `dpp` pointers remain valid
    /// for the lifetime of this provider and all stores it creates.
    ///
    /// # Arguments
    /// * `driver` - Pointer to rgw::sal::Driver (typically env.driver in RGW handlers)
    /// * `dpp` - Pointer to DoutPrefixProvider for logging
    pub unsafe fn new(driver: *mut CRgwDriver, dpp: *const CRgwDoutPrefix) -> Self {
        Self { driver, dpp }
    }

    /// Get the driver pointer
    pub fn driver(&self) -> *mut CRgwDriver {
        self.driver
    }

    /// Get the dpp pointer
    pub fn dpp(&self) -> *const CRgwDoutPrefix {
        self.dpp
    }
}

/// Trait that defines how to create ObjectStore instances for a given URL scheme.
/// This trait is defined in lance-io::object_store::providers
#[async_trait]
impl lance_io::object_store::ObjectStoreProvider for RGWStoreProvider {
    /// Create a new ObjectStore for the given URL and extrcts bucket name
    /// and path prefix
    /// For example: s3://bucket/vector-bucket/ -> bucket="bucket", prefix="vector-bucket/"
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        // Extract bucket from URL: s3://bucket/path -> bucket
        let bucket = match base_path.host_str() {
            Some(b) => b,
            None => {
                return Err(lance_core::Error::io(format!(
                    "URL '{}' must have a bucket/host component",
                    base_path
                )));
            }
        };

        // Extract path prefix from URL: s3://bucket/path/ -> "path/"
        // The path includes the leading slash, so we trim it
        let path = base_path.path().trim_start_matches('/');
        // Ensure the prefix ends with a slash if non-empty (for proper path concatenation)
        let prefix = if path.is_empty() {
            String::new()
        } else if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };

        // Extract tenant from URL query param: rgw://bucket/?tenant=xxx
        let tenant = base_path
            .query_pairs()
            .find(|(k, _)| k == "tenant")
            .map(|(_, v)| v.to_string())
            .unwrap_or_default();

        // Create RGW ObjectStore with bucket and prefix
        // Note: The prefix is stored in RGWObjectStore but NOT used for path manipulation
        // as Lance uses base_path (provided below) directly for its I/O operations.
        //
        // We keep the prefix for debugging/logging and troubleshooting purposes.
        let inner = Arc::new(unsafe {
            RGWObjectStore::new(self.driver, self.dpp, bucket, &tenant, &prefix)
        });

        let storage_options =
            StorageOptions::new(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();

        Ok(ObjectStore::new(
            inner,
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            // Must be true: marker-based pagination requires ordered listing
            // to guarantee no duplicates or missing entries across pages.
            // RGW's unordered listing is cheaper but markers can skip/duplicate
            // entries across shard boundaries.
            params.list_is_lexically_ordered.unwrap_or(true),
            // Max concurrent I/O ops (64). Can be overridden via LANCE_IO_THREADS env var.
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        ))
    }

    /// Extract the path relative to the bucket
    ///
    /// For s3://bucket/path/to/file, returns "path/to/file"
    fn extract_path(&self, url: &Url) -> Result<Path> {
        let path = url.path().trim_start_matches('/');
        Path::parse(path)
            .map_err(|e| lance_core::Error::io(format!("Invalid path in URL '{}': {}", url, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_io::object_store::ObjectStoreProvider;

    #[test]
    fn test_extract_path() {
        let provider = unsafe {
            RGWStoreProvider::new(
                std::ptr::null_mut::<CRgwDriver>(),
                std::ptr::null::<CRgwDoutPrefix>(),
            )
        };

        let url = Url::parse("s3://mybucket/path/to/file.lance").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "path/to/file.lance");

        let url = Url::parse("s3://mybucket/").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.as_ref(), "");
    }
}
