# lancedb-rgw-store

Rust crate that provides a SAL (Storage Abstraction Layer) wrapper for LanceDB to access Ceph RGW storage directly via the SAL API instead of going through S3 HTTP protocol.

## Overview

This crate implements the `object_store::ObjectStore` trait from Apache Arrow, routing all I/O operations through Ceph's RGW SAL C API. This allows LanceDB to store vector indexes directly in Ceph without the overhead of S3 HTTP requests.

## Building

### During Ceph Build

The crate is built automatically as part of the Ceph build process when
`WITH_RADOSGW_LANCEDB` is enabled. It is compiled through the `rgw-lancedb`
umbrella crate (which combines `lancedb-c` and `lancedb-rgw-store` into a
single `librgw_lancedb.a` static library linked into `rgw_common`).

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

### C++ SAL Wrapper Tests

Tests the C wrapper API directly against a live SAL driver:

```bash
cd ceph/build
ninja ceph_test_rgw_sal_wrapper
./bin/ceph_test_rgw_sal_wrapper -c ./ceph.conf
```

The backend (rados, dbstore, posix) is read from ceph.conf.

### LanceDB ObjectStore Integration Tests

Tests the `ObjectStore` trait implementation end-to-end through the
real FFI boundary:

```bash
cd ceph/build
# Force Rust rebuild if sources changed
ninja ceph_test_rgw_lancedb_object_store
./bin/ceph_test_rgw_lancedb_object_store -c ./ceph.conf
```

### S3 Vector Integration Tests

Full end-to-end tests via S3 protocol. See `src/test/rgw/s3vectors/`.

## Architecture

- `src/lib.rs` - Crate entry point and C FFI exports
- `src/store.rs` - `RGWObjectStore` implementation of `ObjectStore` trait
- `src/provider.rs` - `RGWStoreProvider` for creating stores from S3 URLs
- `src/ffi.rs` - FFI bindings to C SAL wrapper functions
- `include/lancedb_rgw_store.h` - C header for FFI interface
