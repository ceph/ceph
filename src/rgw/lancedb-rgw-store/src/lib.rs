/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2026 IBM
 *
 * See file COPYING for licensing information.
 */

//! LanceDB RGW Backend Integration
//!
//! This crate provides integration between LanceDB and Ceph RGW, allowing
//! LanceDB to use RGW's native SAL API instead of the S3 HTTP protocol.
//!
//! # Architecture
//!
//! All S3 operations are intercepted at the ObjectStoreRegistry level and
//! routed through RGW's SAL C API. This provides:
//!
//! - Lower latency (no HTTP overhead)
//! - Direct access to storage
//!
//! This crate requires NO modifications to lancedb, lance, or lance-io.
//! It uses the existing extension points:
//! - `ObjectStoreProvider` trait to implement custom storage
//!
//! ```

/// FFI bindings to Ceph's rgw_sal_wrapper.cc
pub mod ffi;
mod provider;
mod store;

pub use provider::RGWStoreProvider;
pub use store::RGWObjectStore;

use ffi::{CRgwDoutPrefix, CRgwDriver};
use std::sync::Arc;

/// Opaque handle to an ObjectStoreProvider
///
/// This struct MUST have the same `#[repr(C)]` layout as lancedb-c's
/// `LanceDBObjectStoreProvider` (in `src/lancedb-c/src/connection.rs`).
/// Pointers returned by this crate are passed directly to
/// `lancedb_registry_insert_provider()` from lancedb-c at runtime.
#[repr(C)]
pub struct LanceDBObjectStoreProvider {
    pub inner: Option<Arc<dyn lance_io::object_store::ObjectStoreProvider>>,
}

/// Create an ObjectStoreProvider that routes S3 operations through RGW
///
/// The returned provider can be inserted into an ObjectStoreRegistry via
/// lancedb_registry_insert_provider() from lancedb-c.
///
/// # Safety
/// - `driver` must be a valid, non-null pointer to rgw::sal::Driver
/// - `dpp` must be a valid, non-null pointer to DoutPrefixProvider
/// - Both pointers must remain valid for the lifetime of the provider
///
/// # Returns
/// Non-null pointer to LanceDBObjectStoreProvider on success, NULL if
/// either `driver` or `dpp` is NULL.
/// Caller must either:
/// - Pass to lancedb_registry_insert_provider() (transfers ownership), OR
/// - Free with rgw_lancedb_store_free_provider()
#[no_mangle]
pub unsafe extern "C" fn rgw_lancedb_store_create_provider(
    driver: *mut CRgwDriver,
    dpp: *const CRgwDoutPrefix,
) -> *mut LanceDBObjectStoreProvider {
    if driver.is_null() || dpp.is_null() {
        return std::ptr::null_mut();
    }

    // TODO: convert to compile-time check (e.g., via build.rs with constexpr
    // version constants from the C++ header) instead of runtime verification
    match check_sal_wrapper_version() {
        Ok(_) => {}
        Err(_) => return std::ptr::null_mut(),
    }

    let provider: Arc<dyn lance_io::object_store::ObjectStoreProvider> =
        Arc::new(RGWStoreProvider::new(driver, dpp));

    Box::into_raw(Box::new(LanceDBObjectStoreProvider {
        inner: Some(provider),
    }))
}

/// Free a provider created by rgw_lancedb_store_create_provider
///
/// Only call this if the provider was NOT passed to lancedb_registry_insert_provider().
/// If it was inserted into a registry, the registry owns it.
///
/// # Safety
/// - `provider` must be a valid pointer returned by rgw_lancedb_store_create_provider
/// - Must not be called if provider was passed to lancedb_registry_insert_provider()
#[no_mangle]
pub unsafe extern "C" fn rgw_lancedb_store_free_provider(
    provider: *mut LanceDBObjectStoreProvider,
) {
    if !provider.is_null() {
        let _ = Box::from_raw(provider);
    }
}

//=============================================================================
// Version compatibility
//=============================================================================

/// Expected RGW wrapper major version — must match at runtime
const EXPECTED_RGW_SAL_WRAPPER_MAJOR: u32 = 1;

/// Check that the RGW wrapper version is compatible.
/// Returns the version string on success, or an error message on mismatch.
fn check_sal_wrapper_version() -> Result<String, String> {
    let ver_ptr = unsafe { ffi::rgw_sal_wrapper_version() };
    if ver_ptr.is_null() {
        return Err("rgw_sal_wrapper_version() returned null".to_string());
    }
    let ver_str = unsafe { std::ffi::CStr::from_ptr(ver_ptr) }
        .to_str()
        .map_err(|e| format!("invalid version string: {}", e))?
        .to_string();

    let major: u32 = ver_str
        .split('.')
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("cannot parse major version from '{}'", ver_str))?;

    if major != EXPECTED_RGW_SAL_WRAPPER_MAJOR {
        return Err(format!(
            "RGW wrapper version mismatch: expected major {}, got '{}'",
            EXPECTED_RGW_SAL_WRAPPER_MAJOR, ver_str
        ));
    }

    Ok(ver_str)
}

//=============================================================================
// Version information
//=============================================================================

/// Library version from Cargo.toml (null-terminated for C compatibility)
const VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();

/// Get the version string for this library
#[no_mangle]
pub extern "C" fn rgw_lancedb_store_version() -> *const std::os::raw::c_char {
    VERSION.as_ptr() as *const std::os::raw::c_char
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let version = rgw_lancedb_store_version();
        assert!(!version.is_null());
        let version_str = unsafe { std::ffi::CStr::from_ptr(version).to_str().unwrap() };
        assert_eq!(version_str, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_create_provider_null_args() {
        assert!(unsafe {
            rgw_lancedb_store_create_provider(std::ptr::null_mut(), std::ptr::null())
        }
        .is_null());
    }

    #[test]
    fn test_provider_lifecycle() {
        let fake_driver = 0x1234usize as *mut CRgwDriver;
        let fake_dpp = 0x5678usize as *const CRgwDoutPrefix;

        let provider = unsafe { rgw_lancedb_store_create_provider(fake_driver, fake_dpp) };
        assert!(!provider.is_null());

        // Free should not crash
        unsafe { rgw_lancedb_store_free_provider(provider) };
    }

    #[test]
    fn test_provider_free_null_safe() {
        unsafe { rgw_lancedb_store_free_provider(std::ptr::null_mut()) };
    }
}
