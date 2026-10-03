# kv-rgw POC

S3-compatible object storage over FoundationDB with bucket versioning.

## Features

- Full bucket versioning (Enable / Suspend / Disabled)
- Null version handling (max_uint32), delete markers, version-specific GET/HEAD/DELETE
- ListObjectVersions with pagination
- Byte-range GET, multi-delete, bucket policy
- 63/63 ceph-rgw S3 compatibility tests passing (see `ceph_rgw_tests.md`)

## Prerequisites

- FoundationDB (server + client libraries)
- CMake, GCC/Clang with C++23
- Go 1.22+
- protoc (protobuf compiler)
- nginx

## Build

```bash
bash scripts/build_from_tar.sh
```

This installs Go protoc plugins if missing, builds the C++ backend and Go frontend.

## Run

```bash
pkill -f kv-rgw-frontend; pkill -f kv-rgw-backend; pkill nginx; sleep 3
bash scripts/reload.sh --clean 1
```

The gateway listens on `http://127.0.0.1:9080`. Use any S3 client with:
- Access key: `test`
- Secret key: `test`
- Endpoint: `http://127.0.0.1:9080`

## Tests

```bash
# Our versioning tests
bash scripts/test_version_get_put.sh
bash scripts/test_delete_bucket_versioning.sh
bash scripts/test_list_object_versions.sh

# Ceph-rgw S3 compatibility (requires s3tests repo)
cd ~/clean/ceph/src/test/rgw/s3-tests
S3TEST_CONF=/home/gbenhano/kv_poc/s3tests.conf python3.11 -m pytest \
  s3tests/functional/test_s3.py -k "<test_expression>" --tb=line -q

# Full test plan (build + unit + smoke + stress + chaos)
bash scripts/run_test_plan.sh --quick   # fast (4 phases)
bash scripts/run_test_plan.sh           # full (10 phases, ~40min)
```

## Architecture

- **C++ backend** (`backend/`): FDB transactions, object storage, versioning logic
- **Go frontend** (`frontend/`): versitygw-based S3 gateway, gRPC client to backend
- **Proto** (`proto/kvrgw.proto`): gRPC interface between frontend and backend
- **nginx**: reverse proxy + load balancer in front of the gateway

## Key Format

| Namespace | Format | Purpose |
|---|---|---|
| O: | `S<header><object_name>` | Current object version |
| V: | `S<header><object_name>\x00<vid_be32>` | Historical versions |
| B: | `B<tenant_id><bucket_name>` | Bucket metadata |
| P: | `P<header><object_name><ref_tag>` | Pending uploads |
| G: | `G<header><ref_tag><size>` | GC queue |
| D: | `D<header><ref_tag>` | KV-store data tier |
