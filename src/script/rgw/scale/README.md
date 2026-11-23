# RGW on IBM Storage Scale — deployment (first drop)

Deploys `rgw-standalone` on the **nsfs** backend as the S3 endpoint for a
Scale filesystem. No Ceph cluster: no mon, no OSD, no RADOS. The Scale
filesystem is the only storage, buckets are directories under the nsfs
root, and objects are real POSIX files.

This is the plumbing layer. `mms3`, CES IP handling, `mmhealth`,
`mmperfmon` and `gpfs.snap` integration are **not** in this drop — see
"Not in this drop" below for where they attach.

## Instance model

Instances are systemd template units, so each starts and stops
independently:

```console
# systemctl start  rgw-on-scale@s1       # one endpoint
# systemctl start  rgw-on-scale@s2       # two endpoints, same port
# systemctl stop   rgw-on-scale@s1       # back to one
# systemctl status 'rgw-on-scale@*'      # all of them
# systemctl stop   rgw-on-scale.target   # all at once
```

Every instance binds the **same** address and port using `SO_REUSEPORT`,
and the kernel load balances new connections across whichever instances
are up. No front-end proxy, and no per-instance port arithmetic. This
matches the multi-endpoint model CES already expects from NooBaa.

Verified on a two-instance run: 102 requests spread 53/49 across `s1` and
`s2`, and killing `s1` left `s2` serving reads and writes uninterrupted.

## Quick start

`install` is required once, as root. It is what renders and installs the
unit files, writes `/etc/ceph/rgw-on-scale.conf`, and creates the shared
root and per-instance state directories. `systemctl start
rgw-on-scale@s1` on a machine that has not been through `install` fails
with "Unit not found".

The nsfs root must already exist — it is a directory or fileset in the
Scale filesystem, so the script will not create it for you.

```console
# src/script/rgw/scale/rgw-on-scale.sh install \
      --nsfs-root /gpfs/fs1/s3 \
      --shared-root /gpfs/fs1/ces/s3 \
      --endpoint 0.0.0.0:8000 \
      --instances "s1 s2" \
      --build-dir /path/to/ceph/build

# src/script/rgw/scale/rgw-on-scale.sh bootstrap
# systemctl start rgw-on-scale@s1
```

`bootstrap` prints the access and secret key. It is idempotent: run
against a user that already exists it reports the existing credentials
rather than failing. To print them again later:

```console
# src/script/rgw/scale/rgw-on-scale.sh credentials
```

### SELinux

On RHEL with SELinux enforcing, binaries in a build tree under `/home`
are labeled `user_home_t`, which systemd (`init_t`) may not execute —
the unit fails with `203/EXEC "Permission denied"` despite the file
being mode 0755. `install` relabels them when `--build-dir` is used.
Packaged installs under `/usr/bin` are unaffected.

`install` records its options in `/etc/ceph/rgw-on-scale.env`, so
`bootstrap`, `start`, `stop` and `status` do not need them repeated.
Command-line flags still override. Add `--dry-run` to any command to see
what it would do without touching anything.

**After `install`, plain `systemctl` is the whole interface** — the
script is not needed to start or stop anything.

From the RPM the units are already installed; drop `--build-dir` and use
`rgw-on-scale` from `$PATH`.

Add `--gpfs` to enable the GPFS-native filesystem strategy (batch xattrs
via `gpfs_fcntl`, and LWE for cluster-wide version locking). Without it
the driver uses the portable POSIX strategy, which is what you want on a
machine with no GPFS mount.

## Layout on disk

| Path | Scope | Contents |
|---|---|---|
| `/etc/ceph/rgw-on-scale.conf` | shared | generated config |
| `<shared-root>/userdb/` | **shared** | SQLite user/account DB |
| `<shared-root>/config.db` | **shared** | dbstore config store |
| `<nsfs-root>/` | **shared** | bucket data (the Scale filesystem) |
| `/var/lib/ceph/rgw-on-scale/<id>/` | **per instance** | LMDB listing cache |
| `/var/log/ceph/rgw-on-scale.<id>.log` | per instance | log |
| `/run/ceph/rgw-on-scale.<id>.asok` | per instance | admin socket |

`--shared-root` is where the CES shared root goes in a real deployment,
so every CES node sees the same accounts and configuration.

## Constraints baked into the design

**The LMDB listing cache must never be shared between instances.**
`BucketCache::Lmdbs` purges `<database_root>/rgw_posix_lmdbs` in its
constructor, so a second instance starting against a shared root would
delete the first instance's live LMDB environments. The unit derives
`--rgw_nsfs_database_root` from the systemd instance name (`%i`) so this
cannot be misconfigured from the config file. `bootstrap` gives
`rgw-standalone-admin` its own root for the same reason.

**`RuntimeDirectoryPreserve=yes` is load-bearing.** Without it systemd
removes `/run/ceph` when *any* instance stops, taking the surviving
instances' admin sockets with it. (`global_init()` also aborts outright
if it cannot create that directory, so it must exist before `ExecStart`.)

**Lifecycle runs on exactly one instance.** The LC tables live in the
shared SQLite userdb and each instance would otherwise start its own
`RGWLC` processor against it. `install` pins LC to `--lc-instance`
(default: the first instance) via a `[client.rgw.<id>]` config section.
If that instance is stopped, lifecycle does not run until it returns.

**Instances run as root**, unlike `ceph-radosgw@.service` which uses
`--setuser ceph`. `setfsuid`/`setfsgid` identity impersonation needs
`CAP_SETUID`, and the GPFS LWE/DMAPI paths need root. Three of the stock
hardening settings are relaxed to match: `PrivateDevices=no` (GPFS needs
`/dev/ss0`), `ProtectHome=no` (the Scale mount may live under `/home`),
and `ProtectSystem=full` rather than `strict`.

## Known gaps

**SQLite concurrency.** `sqliteDB.cc` opens the userdb with
`SQLITE_OPEN_FULLMUTEX` only — that is thread safety within one process,
not across processes. There is no WAL mode and no `busy_timeout`, so
concurrent writes from two instances can fail with `SQLITE_BUSY`. Reads
dominate at runtime (the in-memory user cache plus `UserDBWatcher`
inotify invalidation absorb most of it), and single admin writes against
two running instances work in practice, but this needs WAL plus a busy
timeout before scaling past a couple of instances. This is the
"per-RGW SQLite database design" item on the project plan.

**Cache invalidation is node-local.** `UserDBWatcher` uses inotify, which
does not cross nodes. The config sets `rgw_nsfs_inotify = true` so that
instances on the *same* node see each other's writes; a multi-node CES
deployment needs the Kafka/LWE consistency work.

**No metrics endpoint.** RGW exposes perf counters over the admin socket
only, and the `rgw-standalone` RPM `Conflicts: ceph-common`, so there is
no `ceph --admin-daemon` on the node. `mmperfmon` integration will need
either a small asok client shipped in the subpackage or a real exporter.

## Not in this drop

`mms3` wrappers, CES IP-move hooks, `mmhealth` health reporting,
`mmperfmon` counters, `gpfs.snap` collection, and NooBaa→RGW migration.
The attachment points are `rgw-on-scale.sh bootstrap` (accounts, for
`mms3 account`) and `rgw-on-scale.sh status` (per-instance state plus an
endpoint probe, for `mmhealth`).

## Files

| File | Purpose |
|---|---|
| `rgw-on-scale.sh` | install / bootstrap / start / stop / status / uninstall |
| `systemd/rgw-on-scale@.service.in` | per-instance template unit |
| `systemd/rgw-on-scale.target` | group target |
