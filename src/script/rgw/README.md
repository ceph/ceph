# RGW Development Scripts

Helper scripts for running filesystem-backed RGW vstart clusters.
These are for **nsfs** and **posix** backends only — rados-backed
clusters use `vstart.sh` directly.

## Quick start

```bash
# 1. Build
cd build
ninja -j$(nproc) radosgw radosgw-admin ceph-conf

# 2. Start cluster (nsfs, clean state, with sidecars)
../src/script/rgw/rgw-vstart.sh --store nsfs --clean --with-keycloak --with-kafka

# 3. Run tests (the script prints this command with correct paths)
cd ../qa/workunits/rgw/s3tests-rs
S3TEST_CONF=$BUILD/s3tests.conf \
  cargo nextest run -P all --test-threads=1 --features fails_on_nsfs
```

The script prints the exact test command with absolute paths when it
finishes — copy-paste from there.

## rgw-vstart.sh

Single entry point for the entire dev test environment.  Wraps
`vstart.sh` to handle cleanup, user/account bootstrap, test config
generation, optional GPFS integration, and optional sidecar services
(Keycloak, and in the future Vault and Kafka).
`rgw-vstart.sh` is the authoritative list of options, with the detail;
the table below lists every one.

Run from the **build directory**:

```bash
# nsfs on local filesystem (simplest)
../src/script/rgw/rgw-vstart.sh --store nsfs

# posix on local filesystem
../src/script/rgw/rgw-vstart.sh --store posix

# nsfs with fresh state (wipes config DB + data)
../src/script/rgw/rgw-vstart.sh --store nsfs --clean

# nsfs with Keycloak for WebIdentity/STS testing
../src/script/rgw/rgw-vstart.sh --store nsfs --clean --with-keycloak

# nsfs with data on GPFS mount
../src/script/rgw/rgw-vstart.sh --gpfs

# nsfs with GPFS CoW clones for CopyObject
../src/script/rgw/rgw-vstart.sh --clone

# nsfs with GPFS + LWE locking (print command for root)
../src/script/rgw/rgw-vstart.sh --clone --lwe --foreground
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--store nsfs\|posix` | `nsfs` | Backend store |
| `--clean` | off | Wipe config DB and all data before starting |
| `--debug-rgw N` | `20` | RGW debug log level |
| `--port N` | `8000` | The port radosgw serves on;  the one your s3tests.conf names |
| `--bucket-profile shared\|strong` | driver's, `shared` | The profile new nsfs buckets are marked with;  librgw and the librgw_file tests need `strong` |
| `--with-impersonation` | off | Serve each request as its own POSIX identity;  creates the fixture (below).  Refuses root |
| `--no-fixture` | off | With `--with-impersonation`:  the users, groups and capabilities only, for a `--data-root` another gateway wrote |
| `--access-model posix-strict\|s3-native` | driver's, `posix-strict` | What nsfs does when S3 authorises an operation the filesystem denies (`rgw_nsfs_access_model`) |
| `--user-identity-override` | off | An account user's own identity row overrides its account's (`rgw_nsfs_user_identity_override`) |
| `--role-identity-override` | off | A role given an identity row runs as it, not as its account (`rgw_nsfs_role_identity_override`) |
| `--data-root DIR` | build directory | Serve objects from `DIR/root`;  the LMDB cache and userdb stay in the build directory.  Rejects `--clean` |
| `--adoption-mode command\|automatic` | driver's, `command` | `command` adopts only when asked through Admin Ops;  `automatic` adopts the whole store from startup |
| `--with-lifecycle` | off | `rgw_lc_debug_interval=10`, so lifecycle expiry takes seconds;  uncomments `lc_debug_interval` in s3tests.conf |
| `--with-non-md5-etag` | off | `rgw_non_md5_etag=true`:  the etag is the mtime-ino string the version ids use |
| `--with-keycloak` | off | Start a local Keycloak container (below) |
| `--with-kafka` | off | Start a local Kafka broker (below) |
| `--with-vault` | off | Start a local HashiCorp Vault, for SSE-KMS and SSE-S3 |
| `--with-inotify` | off | Watch bucket directories for files written from outside |
| `--ramdisk` | off | tmpfs on the data root and LMDB cache directories |
| `--gpfs` | off | Redirect data root to GPFS mount (nsfs only) |
| `--clone` | off | Enable GPFS clone_snap+clone_copy (implies `--gpfs`) |
| `--lwe` | off | Enable GPFS LWE cluster-wide locking (implies `--gpfs`) |
| `--gpfs-root DIR` | `/mnt/rgw/nsfs` | GPFS data directory |
| `--debug-rgw N` | `20` | RGW debug log level |
| `-o 'key=value'` | none | Pass a config option through to `vstart.sh` |
| `--foreground` | off | Print the radosgw command instead of running it |

### What it does

1. Stops every radosgw this build directory runs, whatever else of
   yours holds the port, and the sidecar containers
2. Cleans data directories (and config DB if `--clean`)
3. Runs `vstart.sh` to bootstrap users, accounts, and `ceph.conf`
4. For GPFS, `--data-root`, `--perf` and `--with-impersonation`:  kills
   the vstart daemon, patches `ceph.conf`, and starts radosgw itself
5. Refuses the start unless the radosgw it started is alive and the
   port is listened on (below)
6. With `--with-impersonation`:  checks the machine (below), creates
   the fixture's users and group, applies the capabilities, and seeds
   the fixture
7. Starts sidecar services (`--with-keycloak`, `--with-kafka`,
   `--with-vault`)
8. Prints cluster status and a copy-paste test command

With `--restart` it does none of that:  it stops the running radosgw,
patches `--adoption-mode` into `ceph.conf` if given, and starts radosgw
again, so the userdb, the LMDB cache and the tree are as they were.

### s3tests.conf

The script generates `build/s3tests.conf` automatically from
`qa/workunits/rgw/s3tests-rs/s3tests.conf.SAMPLE`.  All credentials
match the users that `vstart.sh` creates — no manual editing needed.

Set `S3TEST_CONF` when running tests:

```bash
S3TEST_CONF=/path/to/build/s3tests.conf cargo nextest run -P all ...
```

The script prints the exact command at the end.

### Accounts and IAM

`vstart.sh` creates two test accounts with root users, matching the
`[iam root]` and `[iam alt root]` sections in s3tests.conf.  These
enable the full IAM test suite: user CRUD, groups, roles, OIDC
providers, STS AssumeRole, and cross-account access.

### --clean

Use `--clean` to wipe the config DB (`dev/rgw/dbstore/config.db`)
and all data directories.  This gives a fresh IAM state — important
when switching between test runs that create users, roles, or groups,
since stale rows from interrupted runs can cause false test failures.

### Keycloak (--with-keycloak)

Starts a local Keycloak container via podman for WebIdentity testing.
Provisions a realm, client, and test user, extracts tokens and
thumbprint, and writes the `[webidentity]` section to the generated
`build/s3tests.conf`.

**Prerequisites:** podman, jq, openssl, curl

The container is named `keycloak-vstart` and is automatically stopped
and removed on the next `rgw-vstart.sh` invocation.

To stop Keycloak manually:

```bash
../src/script/rgw/keycloak-vstart.sh --stop
```

### Kafka (--with-kafka)

Starts a single-node Kafka broker via podman (KRaft mode, no
ZooKeeper) for S3 bucket notification testing.  Writes the `[kafka]`
without one, the suite's notification tests find the broker at

**Prerequisites:** podman

Create topics with the `kafka://localhost:9092` push endpoint:

```bash
aws sns create-topic --name my-topic \
  --attributes '{"push-endpoint":"kafka://localhost:9092","kafka-ack-level":"broker"}' \
  --endpoint-url http://localhost:8000
```

The container is named `kafka-vstart` and is automatically stopped
and removed on the next `rgw-vstart.sh` invocation.

To stop Kafka manually:

```bash
../src/script/rgw/kafka-vstart.sh --stop
```

### Running as root (for LWE)

LWE locking requires root for DMAPI handle operations.  Use
`--foreground` to have the script do all the setup (vstart, cleanup,
ceph.conf patching) as your normal user, then print the radosgw
command for you to run as root:

```bash
../src/script/rgw/rgw-vstart.sh --clone --lwe --foreground
sudo <printed command>
```

Without root, LWE falls back to OFD locks automatically.

### Stopping

```bash
kill $(cat out/radosgw.<port>.pid)     # out/radosgw.8000.pid by default
# if Keycloak is running:
../src/script/rgw/keycloak-vstart.sh --stop
```

## Running tests

After starting a cluster, run the s3tests-rs suite from the source
tree:

```bash
cd qa/workunits/rgw/s3tests-rs
S3TEST_CONF=/path/to/build/s3tests.conf \
  cargo nextest run -P all --test-threads=1 --no-fail-fast \
  --features fails_on_nsfs
```
`scripts/validate.rs`, which starts each gateway through this script
on the port your s3tests.conf names:  `--preset full` for every
configuration, `--preset quick` or `--preset cover` for a compact
subset.  The suite's `docs/validation.md` describes it and the per-user
config file it reads.

### Profiles

Run a specific test category with `-P <profile>`:

**S3 data-path profiles:**
`bucket-list`, `object-ops`, `multipart`, `versioning`, `copy`,
`acl`, `tagging`, `conditional`, `encryption`, `bucket-policy`,
`object-lock`, `cors`, `lifecycle`, `post-object`, `bucket-logging`,
`headers`, `signing`, `stress`

**IAM/STS profiles:**
`iam` (all IAM), `role`, `oidc`, `sts`

**Composite profiles:**
`baseline` (what a gateway is held to;  the quick start's), `basic`
(core S3 operations), `all` (everything)

**Daemon-wide switches**, each run alone after `baseline` against the
same gateway, since it switches a setting by driver hint:
`access-model-switch`, `dynamic-groups`, `dynamic-groups-membership`,
`role-identity`

**Benchmarks:**
`bench-smoke`, `bench-soak`

### Feature gates

| Feature | Use when |
|---------|----------|
| `fails_on_nsfs` | Testing nsfs backend |
| `fails_on_posix` | Testing posix backend |

These gate tests that hit known filesystem-backend limitations (e.g.
file-as-prefix on nsfs).  Always pass the appropriate feature flag.

### Single test

```bash
S3TEST_CONF=/path/to/build/s3tests.conf \
  cargo nextest run --features fails_on_nsfs,rgw_admin \
  -E 'test(conditional::test_put_object_if_match)' --test-threads=1
```

## Why the two-phase GPFS dance?

`vstart.sh` bootstraps test users and writes `ceph.conf` with all
paths pointing into the build directory (`build/dev/rgw/nsfs/`).
This works fine on local filesystems, but GPFS-specific operations
(`gpfs_linkat`, `gpfs_clone_snap`, etc.) require the data root to
actually be on a GPFS filesystem — they return `EINVAL` on ext4/xfs.

The script handles this by:

1. Running `vstart.sh` normally (bootstraps users into `build/dev/`)
2. Killing the daemon vstart spawned
3. Patching `ceph.conf` to redirect `rgw nsfs base path` to the
   GPFS mount (e.g. `/mnt/rgw/nsfs/root`)
4. Restarting the daemon with the patched config

The userdb and LMDB cache stay in the build directory — they don't
need GPFS, and keeping them avoids re-bootstrapping users.

## GPFS clone lifecycle

When `--clone` is enabled, CopyObject uses GPFS copy-on-write clones
instead of copying data:

1. **`gpfs_clone_snap(src, parent)`** — creates an immutable snapshot
   of the source file as a hidden `.clone_parent.<name>` file in the
   destination directory.

2. **`gpfs_clone_copy(parent, dst)`** — creates a mutable clone child
   from the snapshot.  The child gets its own inode with independent
   xattrs, sharing only data blocks with the parent.

3. The clone parent must persist as long as the child exists.  When
   the child is deleted or overwritten, **`gpfs_clone_unsnap(fd)`**
   breaks the relationship, and then the parent can be unlinked.

### Cleaning up clone parents manually

Clone parents are immutable — `rm` fails with `EROFS` ("Read-only
file system").  To remove them:

```bash
# mmclone is the GPFS CLI for clone management
# (/usr/lpp/mmfs/bin/mmclone)

mmclone split /mnt/rgw/nsfs/root/bucket/.clone_parent.objname
rm /mnt/rgw/nsfs/root/bucket/.clone_parent.objname
```

The `rgw-vstart.sh` script handles this automatically during cleanup.
