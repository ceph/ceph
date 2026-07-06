# src/rgw/ — RADOS Gateway

S3/Swift-compatible HTTP object storage API on RADOS. Largest subsystem (~360 files).

Architecture: HTTP frontend (Beast/Asio) → REST routing (`rgw_rest.cc`) → `RGWOp` handlers (`rgw_op.cc`) → SAL interface (`rgw_sal.h`) → backend driver (`driver/rados/`).

New code must use the SAL interface, not talk to RADOS directly. `radosgw-admin` CLI is in `radosgw-admin/`. Most RGW tests are in the separate `s3-tests` repo.
