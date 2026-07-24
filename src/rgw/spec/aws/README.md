# Vendored AWS Smithy API models

This directory holds AWS Smithy API models that RGW tracks for
control-plane shape compatibility with AWS-shape services. The models
are vendored from `aws/api-models-aws` (Apache-2.0; see `LICENSE` and
`NOTICE`). They serve as:

- A pinned reference for in-progress design work (the model at the
  time of implementation, immune to upstream-side mutation).
- A source artifact for documentation and conformance-test
  generation as that tooling lands.

## Layout

```
<service>/
  README.md
  <version>/
    <service>-<version>.json
```

Where `<version>` matches the AWS Smithy `service` shape's
`version` trait (a date string like `2025-05-05`). Multiple
versions can coexist under one service; the *currently-tracked*
version is documented in each service's `README.md`.

## Currently vendored

| Service   | Tracked version | Notes                                   |
|-----------|-----------------|-----------------------------------------|
| s3files   | 2025-05-05      | AWS S3 Files API; full surface targeted |

## Future direction

A separate design proposal will cover broader vendoring for AWS-shape
services that RGW already implements (`s3`, `iam`, `sts`, etc.). For
those services, RGW does not implement the entire AWS surface, and
the vendored model would be **curated** against what Ceph actually
supports — pruning unimplemented operations and shapes — rather than
held verbatim. That curation effort is out of scope for this directory
in its initial form; only verbatim-tracked services live here today.

## Updating

When bumping a tracked service to a newer upstream version:

1. Fetch the new model from
   `https://raw.githubusercontent.com/aws/api-models-aws/main/models/<service>/service/<new-version>/<service>-<new-version>.json`.
2. Drop it into `<service>/<new-version>/`. Leave the previously
   tracked version in place to preserve diff visibility for at least
   one cycle.
3. Update the `Tracked version` row above and the service's
   `README.md`.
4. Audit downstream references (design docs, op-mapping tables,
   generated artifacts) for shape changes and update accordingly.

## License and attribution

Models in this directory are distributed under Apache-2.0. The
upstream `LICENSE` and `NOTICE` files are reproduced alongside this
README. Ceph contributions to this directory (READMEs, curation
overlays, tooling) are licensed under Ceph's standard terms.
