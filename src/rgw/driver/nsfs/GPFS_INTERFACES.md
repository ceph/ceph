# GPFS interfaces: what nsfs uses, what NooBaa uses, what neither uses

Measured from source on 2026-09-25.  Ours is `fs_strategy.{h,cc}`;
NooBaa's is `src/native/fs/fs_napi.cpp` at `68ca22d33`, the revision
`NOOBAA_VARIANCE.md` pins.  Header references are to
`src/rgw/driver/nsfs/gpfs/gpfs.h` unless stated.

This is an API-usage comparison.  On-disk format is `NOOBAA_VARIANCE.md`.

## Bound by both

| Interface | NooBaa | nsfs |
|-----------|--------|------|
| `gpfs_linkat`, `gpfs_linkatif`, `gpfs_unlinkat` | yes | yes |
| `gpfs_fcntl` | one attribute per call, GET only | batched, LIST + GET + SET |

### The attribute row is not parity

`get_fd_gpfs_xattr()` (`fs_napi.cpp:495-520`) builds one `gpfsRequest_t`
per key and calls `gpfs_fcntl` in a loop over a fixed list of names.
`GPFS_FCNTL_SET_XATTR` does not appear anywhere in their tree -- the
batched interface is used only to read, and only names they already
expect.

`GPFSStrategy::get_xattrs()` lists names with `GPFS_FCNTL_LIST_XATTR`
first, then packs as many `GPFS_FCNTL_GET_XATTR` requests as fit,
flushing when the 64 KB buffer fills.  `set_xattrs()` does the same with
`GPFS_FCNTL_SET_XATTR`.  The saving is per object and grows with the
number of attributes an object carries.

## Bound only by nsfs

- **`gpfs_clone_snap` / `gpfs_clone_copy` / `gpfs_clone_unsnap`** --
  whole-file copy-on-write.  Range-granular clone is unavailable on
  Scale;  `copy_file_range`, `FICLONE` and `FICLONERANGE` all return
  `EOPNOTSUPP`, which is why five paths fall back to buffered copies.
- **`gpfs_lwe_create_session` / `request_right` / `release_right` /
  `destroy_session`** -- cluster-wide locking.
  `FSStrategy::version_lock()` returns an RAII handle:  an OFD lock on
  POSIX, an LWE right on GPFS.  A file lock does not span nodes, and
  NooBaa binds no cluster-wide primitive.

## Bound only by NooBaa

- **`gpfs_ganesha`** with opcode 157, defined in their own source as
  `OPENHANDLE_REGISTER_NOOBAA` (`fs_napi.cpp:188-199`), called at startup
  with `{int version; int delay; int flags;}` (`:2694-2715`).  The opcode
  is in no GPFS header they include.  `EOPNOTSUPP` is tolerated with a
  warning;  any other failure is fatal.  What GPFS does in response is
  not visible from their source.
- **`gpfs_rdma_pread` / `gpfs_rdma_pwrite` /
  `gpfs_rdma_shadow_buffer_size`** behind `gpfs_rdma_experimental.h`.
  Live code, dlsym'd, with fabric selection (`:186-187`, `:1863`,
  `:1916-2000`).

## Bound by neither

### `gpfs_lwe_get_events` / `gpfs_lwe_respond_event`

The notification half of Light Weight Events, on the same session handle
`GPFSStrategy` already creates for locking.  We use the token half only.

Event classes (`:3382-3398`):  FILEOPEN, FILECLOSE, FILEREAD, FILEWRITE,
FILEDESTROY, FILEEVICT, BUFFERFLUSH, POOLTHRESHOLD, FILEDATA,
FILERENAME, FILEUNLINK, FILERMDIR.

Two properties matter for the listing cache.  An inotify watch sees only
the node it runs on;  LWE is cluster-wide.  And `gpfs_lwe_event_t`
carries `isSync` (`:3537`), so some events expect a response through
`gpfs_lwe_respond_event` -- a consumer of those sits in the writer's I/O
path, which decides whether an invalidation feed is a background thread
or a latency-critical service.

**Events carry their origin.**  The data items (`:3427-3451`) include
`processId`, `nodeName`, `clusterName`, `clientUserId`, `clientGroupId`
and `clientIp`, so a consumer can discard events it caused, and can tell
that a write arrived over NFS from a named client.  The `LWE_DATA_*` and
`LWE_EVENT_*` masks are consumed by no function in the header and their
comments are policy-language names (`"op_close"`, `"pathName"`), which
suggests event selection is GPFS policy configuration rather than a
C-API subscription;  that is an inference from the naming, not something
the header states.

### `gpfs_getacl_fd` / `gpfs_putacl_fd`

Native NFSv4 ACLs (`:310-360`, buffer mapping at `:126-150`).  nsfs
stores an encoded `RGW_ATTR_ACL` attribute that only the gateway honours
and that anything with write access to the file can alter;  NooBaa stores
no ACL at all.  A filesystem ACL is enforced by Scale and is honoured for
NFS and SMB clients.

### `gpfs_open_inodescan_with_xattrs` / `gpfs_next_inode_with_xattrs`

Enumerate inodes and their attributes in one scan (`:1849-1880`), with an
incremental mode returning only inodes changed since a previous snapshot.
This is the shape a listing path wants instead of readdir plus stat plus
getxattr per object.

Three constraints decide whether a gateway can use it.  The input is an
`fssnapHandle` -- a filesystem or snapshot handle, not a directory, so
scoping to one bucket is an open question.  The documented errno list
includes `EPERM caller must have superuser privilege`.  And whether it
works against a live filesystem or requires a snapshot is not stated.

### `gpfs_set_share`, `gpfs_set_lease`

Share reservations (`:553-560`) and leases.  Share-deny is what SMB
semantics require;  see the locking work in the librgw design notes.

### `gpfs_fgetattrs` / `gpfs_fputattrs`

Bulk get and put of a file's whole extended attribute set.  The nsfs copy
path currently loops `fgetxattr`/`fsetxattr` per attribute
(`rgw_sal_nsfs.cc:1139-1148`).

### Lower value, noted for completeness

`gpfs_prealloc`, `gpfs_ioprio_set`, `gpfs_qos_*`,
`gpfs_register_cifs_export`, `gpfs_stat_x` / `gpfs_lstatlite`, and the
LWE rights calls beyond request and release -- `upgrade_right`,
`downgrade_right`.  The last of those is relevant to a content-lock
downgrade in the two-phase readdir work.

## Also unused:  inode immutability

`GPFS_IAFLAG_IMMUTABLE`, `GPFS_IAFLAG_INDEFRETENT` and
`GPFS_IAFLAG_APPENDONLY` (`:1060-1068`), with
`GPFS_LWE_ATTRCHANGEEVENT_IMMUTABILITY` (`:3525`) to notify on change,
and `gpfs_fcntl` error codes spelling out the interlocks -- no immutable
directories, immutable and indefinite-retention coupled, neither settable
on a snapshot (`gpfs_fcntl.h:637-653`).

Both gateways store S3 object-lock state in extended attributes, where it
is advisory:  anything with write access to the file can alter it.  These
flags are the filesystem-enforced alternative.
