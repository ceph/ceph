# AWS S3 Files API — Smithy model

Vendored Smithy 2.0 model for the AWS S3 Files API service. RGW's
implementation targets the **full** API surface, so the model is held
**verbatim** with no Ceph-side curation.

## Tracked version

`2025-05-05` — see `2025-05-05/s3files-2025-05-05.json`.

## Source

`https://github.com/aws/api-models-aws/blob/main/models/s3files/service/2025-05-05/s3files-2025-05-05.json`

License: Apache-2.0 (see `../LICENSE` and `../NOTICE`).

## Reconciliation policy

The model is the **design target** for the RGW S3 Files API
implementation. Where Ceph diverges from AWS semantics — for
example, reinterpreting `subnetId` as a Ceph zone-id, ignoring
caller-supplied IP fields, putting access-point ids in the NFS
pseudo-path — those divergences are documented in
`doc/dev/radosgw/s3_files_api.rst`, **not** in the model itself.
The model stays verbatim; the divergences are described in prose
alongside.

## Consumers

- `doc/dev/radosgw/s3_files_api.rst` — design doc references this
  model as the reference target.
- `doc/dev/radosgw/s3_files_api_ops.rst` (planned) — op-mapping
  table; eventually a candidate for generation from the model.
- Future: conformance-test fixture generator (when the broader
  Smithy-driven test workflow lands).
