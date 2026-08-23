# GPFS Clone Parent Leak on Versioned Demote/Delete

## Summary

When `--clone` is enabled, `CopyObject` creates a GPFS CoW clone parent
(`.clone_parent.<name>`) in the bucket directory.  Two code paths fail
to clean up these parents, causing them to accumulate on disk.

## Background

`GPFSStrategy::clone_file()` (`fs_strategy.cc:635`) creates an immutable
snapshot `.clone_parent.<dst_name>` and a mutable clone child.  Cleanup
is handled by `cleanup_clone()` (`fs_strategy.cc:710`), which calls
`gpfs_clone_unsnap()` to break the clone relationship, then `unlinkat()`
to remove the parent file.

`cleanup_clone()` is safe to call on non-cloned objects (returns
silently on ENOENT).  It looks for `.clone_parent.<name>` relative to
the directory fd passed to it.

## Leak 1 — Demote without cleanup

When a cloned current-version object is demoted to `.versions/` (by
versioned PUT, CopyObject, or DELETE-creating-delete-marker), none of
the three demote paths call `cleanup_clone()`:

- Versioned PUT (`rgw_sal_nsfs.cc:6999`): `safe_link` to `.versions/`
- Versioned CopyObject (`rgw_sal_nsfs.cc:4142`): same pattern
- Versioned DELETE without versionId (`rgw_sal_nsfs.cc:5841`): same

The `.clone_parent.<name>` remains in the bucket directory.  A
subsequent `clone_file()` to the same object name self-cleans (it calls
`cleanup_clone` before creating a new parent), but a regular PUT or
DELETE does not.

## Leak 2 — Permanent version delete

When a specific version in `.versions/` is permanently deleted
(`DELETE ?versionId=`), the code at `rgw_sal_nsfs.cc:5648` uses raw
`unlinkat()`.  This does not go through `File::remove()` and does not
call `cleanup_clone()`.

Even if `cleanup_clone()` were called in this path, it would fail:
`cleanup_clone` searches for `.clone_parent.<name>` relative to the
directory the child lives in.  After demote, the child is in
`.versions/` but the clone parent remains in the **bucket directory**.
The lookup would target the wrong directory and find nothing.

## Self-copy missed optimization

Self-copy (same-key CopyObject, MetadataDirective=REPLACE) at
`rgw_sal_nsfs.cc:4197` skips GPFS cloning entirely, falling back to a
data copy via `O_TMPFILE`.  `GPFSStrategy::clone_fd()` delegates to
`POSIXStrategy::clone_fd()` because `clone_file()` takes pathnames and
after demote the source pathname is unlinked.  The fd is still valid
(same inode, now linked in `.versions/`), but there is no
clone-from-fd implementation.  This is not a correctness issue but a
missed performance opportunity.

## RCA dependency — inode vs path tracking

The severity of the leak depends on how GPFS tracks clone relationships
internally:

**If by inode:** The clone relationship survives the rename into
`.versions/`.  The parent continues to hold shared data blocks
referenced by the demoted child.  The parent cannot be removed (even
manually) until the relationship is broken via `gpfs_clone_unsnap`.
This makes the leak **correctness-critical**: shared blocks are pinned
indefinitely, and disk usage grows without bound.

**If by path:** The clone relationship is implicitly broken when the
child's original link is removed during demote.  The parent becomes an
inert orphan — an immutable regular file consuming its own disk space
but not holding shared blocks.  The leak is a **space leak only**: the
orphan files accumulate but don't pin additional data.

### Plan to determine tracking model

Inspect the GPFS kernel module source (`mmfs26/`-era or equivalent),
specifically:

1. `gpfs_clone_snap` / `gpfs_clone_copy` implementation — what
   structure records the parent-child relationship?  Is it keyed by
   inode number or by (parent-dir, name) pair?
2. `gpfs_clone_unsnap` — does it take an fd and resolve to an inode,
   or does it operate on a path?  (The userspace wrapper takes an fd,
   suggesting inode-level tracking, but the ioctl may differ.)
3. What happens to shared blocks when a clone child is hardlinked to a
   new path and the original link is removed?  Does the refcount on
   shared blocks reference the inode or the path?

If source inspection is inconclusive, an empirical test on a GPFS
filesystem:

```bash
# create a file, clone it, move the child, delete original link
echo "test data" > /mnt/gpfs/src
gpfs_clone_snap /mnt/gpfs/src /mnt/gpfs/.clone_parent.dst
gpfs_clone_copy /mnt/gpfs/.clone_parent.dst /mnt/gpfs/dst

# demote: hardlink child to new name, remove original
ln /mnt/gpfs/dst /mnt/gpfs/versions/dst_v1
rm /mnt/gpfs/dst

# check: can we still unsnap?  does the parent still hold blocks?
gpfs_clone_unsnap /mnt/gpfs/.clone_parent.dst   # or via fd
mmclone show /mnt/gpfs/versions/dst_v1           # still shows as clone?
```

## Fix plan

### Phase 1 — After RCA, fix cleanup paths

**If inode-tracked (shared blocks pinned):**

Add `cleanup_clone(dpp, bucket_dir_fd, object_name)` calls to:

1. Each demote path (versioned PUT, CopyObject, DELETE) — call
   **before** `safe_link`, while the child still lives in the bucket
   dir.  `cleanup_clone` is safe on non-cloned objects (ENOENT → noop).

2. Permanent version delete — call with the **bucket directory fd**
   (not the `.versions/` fd) and the **original object name** (not
   the version-suffixed name in `.versions/`).  This requires either
   passing the bucket dir fd + original name into the delete path, or
   recovering the original name by stripping the version suffix.

**If path-tracked (inert orphans):**

Same cleanup calls, but the fix is lower priority.  The orphan parents
are small (snapshot metadata, no data blocks) but accumulate without
bound.  A periodic sweep (`find bucket -name '.clone_parent.*'`) could
serve as a stopgap.

### Phase 2 — Self-copy clone optimization (optional)

Implement `GPFSStrategy::clone_fd()` using the child's open fd to
derive its current path (via `/proc/self/fd/<N>` or `gpfs_fcntl`
path lookup), then call the existing `clone_file()` path.  This
avoids the data copy on same-key CopyObject with MetadataDirective.

### Phase 3 — Test coverage

Add tests exercising:

- CopyObject → versioned PUT → verify no orphan `.clone_parent.*`
- CopyObject → DELETE (delete marker) → verify cleanup
- CopyObject → DELETE ?versionId= (permanent delete) → verify cleanup
- CopyObject → CopyObject same key → verify self-cleaning (existing)
- Count `.clone_parent.*` files before/after each sequence

These tests require a GPFS filesystem and `--clone` enabled.
