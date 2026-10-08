# `driver/nsfs/adopt/` — the adopt and convert path

Code that runs **at adopt or convert time**, not on the request path.

The distinction is the one that decides what belongs here.  A file in
this directory reads a NooBaa deployment, plans an import, or rewrites
a tree;  nothing here is reached while serving an S3 request.  The
strategies one level up — `xattr_strategy`, `path_strategy`,
`mpu_strategy`, `fs_strategy`, `bucket_state_strategy` — are the other
case:  they are selected per bucket and consulted per request, and
`bucket_state_strategy`'s NooBaa arm reads their bucket record on the
request path in the `serve` mode of `NSFS_ADOPTION.md` §7.  So they
stay where they are even though they are format-specific.

| | |
|---|---|
| `noobaa_config_store` | reads their configuration store;  writes nothing.  I1 |
| `noobaa_master_keys` | their master keys, and the secrets they open.  I2 |
| `noobaa_import_plan` | what a store becomes on the RGW side, and the collision gate.  I3 |

`docs/ACCOUNT_IMPORT.md` §9 is the stage list these are lettered
against, and `docs/NSFS_ADOPTION_WORK_PLAN.md` is the ordered one.

**The store is an import source, not a backing store.**  It is read
once and what the import produces is RGW's own state;  after a bucket
is adopted nothing in the request path consults their store again.
That is what makes this an adoption rather than a permanent bridge,
and it is why this code has a directory of its own.
