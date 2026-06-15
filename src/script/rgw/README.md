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
| `--with-keycloak` | off | Start a local Keycloak container (see below) |
| `--with-kafka` | off | Start a local Kafka broker (see below) |
| `--gpfs` | off | Redirect data root to GPFS mount (nsfs only) |
| `--clone` | off | Enable GPFS clone_snap+clone_copy (implies `--gpfs`) |
| `--lwe` | off | Enable GPFS LWE cluster-wide locking (implies `--gpfs`) |
| `--gpfs-root DIR` | `/mnt/rgw/nsfs` | GPFS data directory |
| `--debug-rgw N` | `20` | RGW debug log level |
| `--foreground` | off | Print the radosgw command instead of running it |

### What it does

1. Kills any existing radosgw and Keycloak container
2. Cleans data directories (and config DB if `--clean`)
3. Runs `vstart.sh` to bootstrap users, accounts, and `ceph.conf`
4. For GPFS: kills the vstart daemon, patches `ceph.conf` to redirect
   the data root to the GPFS mount, and restarts the daemon
5. Generates `build/s3tests.conf` from the in-tree SAMPLE template
6. Starts sidecar services (`--with-keycloak`, `--with-kafka`)
7. Prints cluster status and a copy-paste test command

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
section to `build/s3tests.conf` with the broker endpoint.

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
kill $(cat out/radosgw.8000.pid)
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
`basic` (core S3 operations), `all` (everything)

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
  cargo nextest run -E 'test(conditional::test_put_object_if_match)' \
  --test-threads=1
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
