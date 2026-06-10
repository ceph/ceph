# RGW Development Scripts

Helper scripts for running filesystem-backed RGW vstart clusters.
These are for **nsfs** and **posix** backends only — rados-backed
clusters use `vstart.sh` directly.

## rgw-vstart.sh

Convenience wrapper around `vstart.sh` that handles cleanup, user
bootstrap, and optional GPFS integration in a single command.

Run from the **build directory**:

```bash
# nsfs on local filesystem (simplest)
../src/script/rgw/rgw-vstart.sh --store nsfs

# posix on local filesystem
../src/script/rgw/rgw-vstart.sh --store posix

# nsfs with data on GPFS mount
../src/script/rgw/rgw-vstart.sh --store nsfs --gpfs

# nsfs with GPFS CoW clones for CopyObject
../src/script/rgw/rgw-vstart.sh --store nsfs --clone

# nsfs with GPFS + LWE locking (print command for root)
../src/script/rgw/rgw-vstart.sh --clone --lwe --foreground
# then run the printed command as root in another terminal
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--store nsfs\|posix` | `nsfs` | Backend store |
| `--gpfs` | off | Redirect data root to GPFS mount (nsfs only) |
| `--clone` | off | Enable GPFS clone_snap+clone_copy (implies `--gpfs`) |
| `--lwe` | off | Enable GPFS LWE cluster-wide locking (implies `--gpfs`) |
| `--gpfs-root DIR` | `/mnt/rgw/nsfs` | GPFS data directory |
| `--debug-rgw N` | `20` | RGW debug log level |
| `--foreground` | off | Print the radosgw command instead of running it |

### Running as root (for LWE)

LWE locking requires root for DMAPI handle operations.  Use
`--foreground` to have the script do all the setup (vstart, cleanup,
ceph.conf patching) as your normal user, then print the radosgw
command for you to run as root:

```bash
../src/script/rgw/rgw-vstart.sh --clone --lwe --foreground
# copy the printed command and run it as root
sudo <printed command>
```

Without root, LWE falls back to OFD locks automatically.

### Running tests

After starting a cluster, run the s3tests-rs suite from the source
tree:

```bash
cd qa/workunits/rgw/s3tests-rs
S3TEST_CONF=~/dev/s3tests.conf \
  cargo nextest run -P copy --test-threads=1 --no-fail-fast \
  --features fails_on_nsfs
```

Available profiles: `basic`, `copy`, `conditional`, `versioning`,
`object-ops`, `multipart`, `bucket-list`, `tagging`, `headers`,
`acl`, `cors`, `encryption`, `bucket-policy`, `object-lock`,
`lifecycle`, `post-object`, `bucket-logging`, `signing`, `stress`,
`all`.

The `fails_on_nsfs` feature gates tests that hit known
filesystem-backend limitations (e.g. file-as-prefix).  Always pass
it when testing nsfs.

### Stopping

```bash
kill $(cat out/radosgw.8000.pid)
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

# Show clone status of a file
mmclone show /mnt/rgw/nsfs/root/bucket/.clone_parent.objname

# Break parent-child relationship (copies shared blocks into child)
mmclone split /mnt/rgw/nsfs/root/bucket/.clone_parent.objname

# Now rm works
rm /mnt/rgw/nsfs/root/bucket/.clone_parent.objname
```

The `rgw-vstart.sh` script handles this automatically during cleanup.
