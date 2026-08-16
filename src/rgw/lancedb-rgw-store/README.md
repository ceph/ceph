# lancedb-rgw-store

Rust crate that provides a SAL (Storage Abstraction Layer) wrapper for LanceDB to access Ceph RGW storage directly via the SAL API instead of going through S3 HTTP protocol.

## Overview

This crate implements the `object_store::ObjectStore` trait from Apache Arrow, routing all I/O operations through Ceph's RGW SAL C API. This allows LanceDB to store vector indexes directly in Ceph without the overhead of S3 HTTP requests.

## Building

### During Ceph Build

The crate is built automatically as part of the Ceph build process when `WITH_RADOSGW_LANCEDB` is enabled:

```bash
cd ceph/build
ninja radosgw
```

### Manual Rebuild (Development)

When making changes to this crate during development:

This crate is not built on its own: it is compiled into the `rgw-lancedb`
umbrella crate, which is built by an ExternalProject of the same name, and
produces the `librgw_lancedb.a` static library that the RGW is linked with.

```bash
cd ceph/build
rm -f src/rgw/rgw-lancedb-prefix/src/rgw-lancedb-stamp/rgw-lancedb-build
ninja rgw-lancedb

```

After rebuilding, restart the RGW daemon to pick up the changes.

## Testing

### Unit Tests

Unit tests run within the Ceph build system (via `ninja`). Standalone `cargo test`
requires Ceph libraries to resolve FFI symbols at link time.

### Integration Tests

Integration tests require a running Ceph cluster with RGW. See `src/test/rgw/s3vectors/` for the test suite.

## Architecture

- `src/lib.rs` - Crate entry point and C FFI exports
- `src/store.rs` - `RGWObjectStore` implementation of `ObjectStore` trait
- `src/provider.rs` - `RGWStoreProvider` for creating stores from S3 URLs
- `src/ffi.rs` - FFI bindings to C SAL wrapper functions
- `include/lancedb_rgw_store.h` - C header for FFI interface
