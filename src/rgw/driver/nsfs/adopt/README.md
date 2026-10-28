# `driver/nsfs/adopt/` — the adopt and convert path

Code that runs **at adopt or convert time**, not on the request path.

The distinction is the one that decides what belongs here.  A file in
this directory reads a NooBaa deployment, plans an import, or rewrites
a tree;  nothing here is reached while serving an S3 request.  The
strategies one level up — `xattr_strategy`, `path_strategy`,
`mpu_strategy`, `fs_strategy`, `bucket_state_strategy` — are the other
case:  they are selected per bucket and consulted per request, and
`bucket_state_strategy`'s NooBaa branch reads their bucket record on the
request path for a bucket not yet adopted (`NSFS_ADOPTION.md` §7).  So they
stay where they are even though they are format-specific.

| | |
|---|---|
| `noobaa_config_store` | reads their configuration store;  writes nothing.  I1 |
| `noobaa_master_keys` | their master keys, and the secrets they open.  I2 |
| `noobaa_import_plan` | what a store becomes on the RGW side, and the collision gate.  I3 |
| `noobaa_schema` | the fields and object attributes the reviewed NooBaa releases write, and the checks that refuse anything else.  X4 |
| `noobaa_import` | the store's identities into RGW:  accounts, IAM users, roles and the anonymous identity, with their rows.  I4, I7 to I9 |
| `noobaa_bucket_import` | one bucket's record into RGW's bucket state and its placement row.  I5, P6 |

The letters are the stages of `docs/NSFS_WORK_PLAN.md`.

**The store is an import source, not a backing store.**  It is read
once and what the import produces is RGW's own state;  after a bucket
is adopted nothing in the request path consults their store again.
That is what makes this an adoption rather than a permanent bridge,
and it is why this code has a directory of its own.
