# s3-tests-rs — status

Measured 2026-10-08.  Every number here is counted from the tree, and
the counting is mechanical enough to redo:  Rust tests by the `#[test]`
and `#[tokio::test]` attributes, Python tests by `^def test_`, and the
correspondence between them by set intersection of the names.

## What is here

Four kinds of work, six modules.

| module | tests | source |
|---|---|---|
| `test_s3` | 752 | `test_s3.py`, `test_headers.py`, `rgw_multi/tests.py` |
| `test_iam` | 74 | `test_iam.py`, `test_sts.py`, plus new account coverage |
| `test_rgw` | 31 | original, except `quota.rs` from `s3_user_quota.pl` |
| `test_sns` | 26 | `test_sns.py`, plus the Kafka subset of `test_bn.py` |
| `test_bench` | 6 | original — `elbencho` workload automation |
| `test_corpus` | 12 | original — the conversion corpus |
| **total** | **901** | |

The Python sources are the s3-tests suite vendored in the Ceph tree at
`src/test/rgw/s3-tests/`;  `test_bn.py` is
`src/test/rgw/bucket_notification/test_bn.py`;  `s3_user_quota.pl` is
`qa/workunits/rgw/s3_user_quota.pl`.  Each module header names its own
source.

## Provenance, and how it is recoverable

**681 of 901 Rust tests carry a name that appears verbatim in a Ceph
Python file.**  Translated tests keep their Python names, which is what
makes the correspondence greppable rather than remembered:

| Python file | tests | ported | remaining |
|---|---|---|---|
| `test_s3.py` | 760 | 598 | 162 |
| `test_iam.py` | 89 | 65 | 24 |
| `test_headers.py` | 48 | 13 | 35 |
| `test_sts.py` | 34 | 1 | 33 |
| `test_s3select.py` | 34 | 0 | 34 |
| `test_sns.py` | 5 | 4 | 1 |
| `test_s3control.py` | 1 | 0 | 1 |
| `test_utils.py` | 1 | 0 | 1 |
| **total** | **972** | **681** | **291** |

Two translations were renamed and so are not findable by name:

- `test_sns.rs`'s `test_notification_kafka_*` are the Kafka subset of
  `test_bn.py`'s 87 tests.  AMQP, persistent topics and multisite are
  deliberately not ported.
- `test_rgw/quota.rs` follows `s3_user_quota.pl`, user scope only.  The
  companion `s3_bucket_quota.pl` — bucket-scope quota, the zero and
  negative cases, enable and disable — is not ported.

## What does not run, and why

Three different things get called "ignored" and only the first is
unconditional.

**Unconditionally ignored: 14.**  13 in `test_s3`, 1 in `test_iam`.
These do not run anywhere.

**Conditionally gated: 335** carry at least one `cfg_attr(…, ignore)`.
A test may carry several.

| gate | tests | meaning |
|---|---|---|
| `fails_on_dbstore` | 204 | known to fail on that backend |
| `fails_on_posix` | 85 | |
| `fails_on_aws` | 84 | |
| `fails_on_nsfs` | 72 | |
| `fails_on_rgw` | 11 | on by default |
| `fails_on_noobaa` | 1 | NooBaa, for a capture from their gateway |
| `rgw_admin` | 30 | **requires** the feature — Admin Ops endpoints |
| `has_vault` | 22 | **requires** a Vault sidecar |

The `fails_on_*` gates are permanent annotations, not a to-do list:
the suite is meant to run against implementations it cannot
interrogate, so recording "this one does not pass here" is what works
against a black box.  `fails_on_rgw` is in `default`.

**The column counts whole tests ignored, and that is not the only
form a gate takes.**  A gate may instead sit inside a test body, as
`#[cfg(not(feature = …))]` around one assertion, so that the test
still runs and still guards everything else it checks.  Those do not
appear in a `nextest list` and so cannot appear above.  There are 53
of them for `fails_on_aws`, 2 for `fails_on_noobaa` and 1 for
`fails_on_posix`.

`fails_on_noobaa` is set by `interchange/standup.rs capture`, which
runs the conversion corpus against a NooBaa endpoint rather than
against RGW.  Its three gates are the multipart ETag, which NooBaa
returns as `mtime-<base36>-ino-<base36>` rather than as a digest with
a part count;  `PutObjectLockConfiguration`, which answers
`NotImplemented` at the pinned commit;  and three scaffolding key
names NooBaa accepts and then hides from its own listings.

`has_kafka` is **not** a test gate — it is a build feature that pulls
in `rdkafka`.  The Kafka tests check for a broker at run time and fail
with a message naming `kafka-vstart.sh`.

## Remaining from `test_s3.py`, by area

162 tests.  Grouped by what they exercise, measured rather than
estimated:

| area | tests |
|---|---|
| bucket logging | 58 |
| SSE-KMS / SSE-S3 | 23 |
| versioning | 23 |
| conditional requests | 12 |
| lifecycle | 12 |
| bucket policy / IAM | 4 |
| multipart | 4 |
| cloud tier / restore | 4 |
| object attributes | 1 |
| usage / stats | 1 |
| other | 20 |

## Priorities, for the filesystem-backed drivers

Worth porting, in order:

1. **Conditional requests** (12).  Ten of the twenty in "other" are
   also `if_match`/`if_none_match` variants, so this area is larger
   than its row suggests and it exercises a driver-level decision
   point — `If-None-Match: *` has to be decided at the publish, not
   before it.
2. **Versioning** (23).  The largest area that nsfs and posix
   implement themselves rather than inherit.
3. **Multipart** (4).  Resend and reorder resilience, which is
   driver-level idempotency.
4. **Object attributes** (1).  A newer API over the same metadata
   paths.

Needing infrastructure or a feature these drivers lack:

- **bucket logging** (58) — configuration is stored on nsfs and posix,
  but delivery is not implemented on either, so these cannot pass
- **SSE-KMS / SSE-S3** (23) — needs Vault;  SSE-C is already covered
- **STS** (33) and **S3 Select** (34) — need an OIDC IdP and the
  s3select module
- **cloud tier / restore** (4) — needs tiering
