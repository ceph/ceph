# src/rgw/ — RADOS Gateway

## Purpose

The RADOS Gateway (RGW) provides S3-compatible and Swift-compatible HTTP object storage APIs on top of RADOS. It is the **largest subsystem** in Ceph (~360 files). The architecture is layered: HTTP frontend (Boost.Beast/Asio) → REST operation routing → **Storage Abstraction Layer (SAL)** → backend driver (usually RADOS).

RGW supports multisite replication, IAM policies, server-side encryption, lifecycle management, bucket notifications (Kafka, AMQP), and many S3 features. The SAL abstraction allows pluggable storage backends beyond RADOS.

The subsystem also includes **`radosgw-admin`** (CLI tool for bucket, user, zone, and key management) and **`librgw`** (embeddable C API for embedding RGW in other processes).

## Key Files

### Core
| File | Role |
|------|------|
| `rgw_main.cc` / `rgw_appmain.cc` | Daemon startup and initialization. |
| `rgw_process.cc/h` | Request processing pipeline. |
| `rgw_rest.cc/h` | REST request routing — maps HTTP method + URL to handler class. |
| `rgw_op.cc/h` | Base operation classes (`RGWOp` and subclasses for GET, PUT, DELETE, etc.). |
| `rgw_common.h/cc` | Common definitions, utilities, error codes. |
| `librgw.cc` | Embeddable RGW library (`librgw`) — C API for embedding RGW in other processes. |
| `radosgw-admin/` | `radosgw-admin` CLI tool — bucket, user, zone, and key management commands. |

### S3/Swift Protocol
| File | Role |
|------|------|
| `rgw_rest_s3.cc/h` | S3 API implementation — all S3-specific request/response handling. |
| `rgw_rest_swift.cc/h` | Swift API implementation. |
| `rgw_auth.h/cc` | Authentication framework (pluggable auth engines). |
| `rgw_auth_s3.h/cc` | S3 signature verification (v2 and v4 signing). |
| `rgw_acl.h/cc` / `rgw_acl_s3.h/cc` | Access control list implementation. |
| `rgw_iam_policy.h/cc` | IAM policy evaluation engine. |

### Storage Abstraction Layer
| File | Role |
|------|------|
| `rgw_sal.h` | SAL interface — `rgw::sal::Driver`, `rgw::sal::Bucket`, `rgw::sal::Object`. All storage goes through this. |
| `rgw_sal_fwd.h` | Forward declarations for SAL types. |

### Features
| File | Role |
|------|------|
| `rgw_bucket.h/cc` | Bucket operations. |
| `rgw_user.h/cc` | User management. |
| `rgw_lc.h/cc` | Lifecycle management (expiration, transitions). |
| `rgw_sync.h/cc` / `rgw_data_sync.cc` | Multisite data sync engine. |
| `rgw_crypt.h/cc` | Server-side encryption (SSE-S3, SSE-KMS, SSE-C). |
| `rgw_compression.h/cc` | Object compression. |
| `rgw_coroutine.h/cc` | Legacy coroutine framework for async operations. |
| `rgw_cache.h/cc` | Object caching layer. |
| `rgw_bucket_logging.h/cc` | Bucket access logging. |

## Directory Structure

| Subdirectory | Contents |
|---|---|
| `driver/rados/` | RADOS backend for SAL — the production backend (~127 files). |
| `driver/posix/` | POSIX filesystem backend for SAL. |
| `driver/d4n/` | D4N (Data for Nodes) caching backend. |
| `driver/daos/` | DAOS backend. |
| `driver/dbstore/` | Database-backed store (SQLite). |
| `driver/motr/` | Motr backend. |
| `services/` | Service layer (`svc_*`) — modular backend-agnostic services (zone, user, bucket, quota, etc.). |
| `radosgw-admin/` | `radosgw-admin` CLI tool source. |
| `librgw.cc` | Embeddable RGW library entry point — see also `include/rados/librgw.h`. |

## Patterns and Idioms

### SAL (Storage Abstraction Layer)
All storage operations go through the SAL interface:
- `rgw::sal::Driver` — top-level driver (create bucket, get user, etc.)
- `rgw::sal::Bucket` — bucket operations (list objects, get/set attrs)
- `rgw::sal::Object` — object operations (read, write, delete)

New features should use SAL interfaces, not talk directly to RADOS.

### REST Operation Dispatch
Each S3/Swift operation has a handler class inheriting from `RGWOp`:
1. HTTP request arrives at frontend
2. `rgw_rest.cc` matches URL pattern → selects `RGWHandler` subclass
3. Handler creates appropriate `RGWOp` subclass
4. `RGWOp::execute()` performs the operation via SAL

### Service Layer
Services in `services/` provide modular, backend-agnostic functionality:
- `svc_zone.h/cc` — zone/realm/period management
- `svc_user.h/cc` — user metadata
- `svc_bucket.h/cc` — bucket metadata
- `svc_mdlog.h/cc` — metadata change log
- `svc_datalog.h/cc` — data change log

### Coroutines
Legacy async operations (especially multisite sync) use `RGWCoroutine`. New async code should prefer boost::asio coroutines.

## Dependencies

Uses `librados/` for RADOS I/O, `common/`, `include/`. Key RADOS classes: `cls_rgw`, `cls_rgw_gc`, `cls_log`, `cls_lock`, `cls_user`.

## Navigation Hints

- To understand how a specific S3 API call is handled: search for its handler class in `rgw_rest_s3.cc` (e.g., `RGWPutObj_ObjStore_S3`)
- To add a new S3 operation: create handler in `rgw_rest_s3.cc`, create `RGWOp` subclass in `rgw_op.cc`
- To understand multisite sync: start at `driver/rados/rgw_data_sync.cc`
- To understand the RADOS backend: look in `driver/rados/`
- To understand IAM policy evaluation: see `rgw_iam_policy.cc`

## Gotchas

- Almost all files have the `rgw_` prefix. The sheer number makes grep noisy — use the SAL/driver/services layering to narrow your search.
- Multisite sync is extremely complex. The data sync engine uses coroutines extensively and spans many files.
- The SAL interface is relatively new. Some code still bypasses SAL and talks directly to RADOS. This is being cleaned up — new code must use SAL.
- `rgw_rest_s3.cc` is very large. Search by S3 operation name to find the relevant handler.
- RGW uses its own HTTP server (Beast/Asio frontend), not the Ceph Messenger. Networking patterns differ from other daemons.
- See `MAINTAINERS.md` in this directory for RGW-specific maintainer contacts.
