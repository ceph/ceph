# P models of RGW protocols

These are [P](https://p-org.github.io/P/) models of correctness arguments
in RGW. Each one states properties and models the code as it is. Some
configurations remove a mechanism the code relies on, or break an
assumption; each of those must produce a counterexample. That shows the
model can see the failure, and makes the configuration a regression test
for the mechanism.

| Model | Covers | Properties |
|---|---|---|
| [`rgw_overwrite`](rgw_overwrite/README.md) | PutObject, DeleteObject, CopyObject with a shared tail, dedup and multipart completion over existing keys: head-object races, the bucket index entry and stats, listings, resharding, `cls_refcount`, part re-uploads, abort, lifecycle's abort, GC; `If-Match` and `If-None-Match: *` on PutObject, completion and DeleteObject | no head's data is deleted; the index matches the head; the stats match the index; nothing leaks; every request answered; a conditional request is answered as some order of the requests would answer it |
| [`rgw_iam_sts`](rgw_iam_sts/README.md) | IAM roles, users, access keys and groups in one account, with their inline and managed policies. AssumeRole, GetSessionToken, and the S3 requests that use their credentials | A delete removes only an empty entity, and takes effect. The indexes match the entities. Limits hold, and names are unique. No update is lost. Only active keys and trusted callers get in. A session dies with its role. Each answer is one that the Smithy model of IAM or STS lists |

## Shared components

The models share the files in `common/`. A model lists the ones it uses in
its `.pproj` file. P has no generics, so each file names the types and
machines that a model must define.

- `Common.p` holds RGW's return codes (`tRc`), the Driver, and the helpers
  that write a scenario's script (`One`, `Two`, `Three`). The Driver runs a
  script in phases. The requests of a phase run concurrently. Once every
  request of a phase is answered or its RGW is dead, the next phase
  starts. An answer can fill a slot that later requests take as input,
  for example the credentials that an STS request issues. At the end, the
  Driver asks the Store to finish. A model defines the types `tCfg`, `tReq`,
  `tInit` and `tOut`, and the functions `InSlot` and `OutSlot`. It also
  defines the machines `Store` and `Rgw`, with the constructor payloads
  that `Common.p` gives.
- `Rados.p` holds RADOS semantics as pure functions, which a Store calls
  from its handlers. They cover metadata objects under an
  `RGWObjVersionTracker` or an exclusive create, and the `cls_user`
  account resource omaps. A model that includes this file defines `tOid`
  and `tObj`, and `tObj` has a field `ver`.

`rgw_overwrite` uses `Common.p`. `rgw_iam_sts` uses both files.

## Running

The models are not part of the Ceph build: they need the .NET 8 SDK and
the P tool, not the C++ toolchain.

Install the .NET 8 SDK (a distribution package, or `brew install
dotnet@8` on macOS), then P:

```
dotnet tool install --global P

./run.sh <model> [schedules] [jobs]       # every case against <model>/expect.txt
./deep.sh <model> <test case> [schedules] # one case under random, PCT and POS
```

The scripts find P in `~/.dotnet/tools`, and a Homebrew `dotnet@8` or a
.NET in `~/.dotnet` by themselves; otherwise set `DOTNET_ROOT`. `run.sh` runs `jobs` cases at a
time; `rgw_overwrite` has 167, at 20,000 schedules each, and
`rgw_iam_sts` has 123.

`run.sh` counts a case as violated if *any* of P's summaries reports a bug.
`p check -tc` matches test names by prefix and runs every match, so no
test name may be a prefix of another.

## What the models found

`rgw_overwrite` finds fourteen gaps on main, detailed in its README, which
also lists the tracker issues and the proposed fixes:

- **The bucket index can keep a stale entry.** A stale or canceled
  completion still overwrites the entry's version. Three overlapping
  PutObjects can leave the index listing the wrong one, and a
  delete-put-delete sequence can leave it listing a deleted key.
- **Lifecycle's abort can delete a completing upload's data.** It aborts
  without the completion lock that AbortMultipartUpload takes.
- **A completion that leaves its meta object behind can lose the object's
  data later.** A later abort, or a retried completion, sends the
  completed object's parts to GC.
- **A completion that loses the head race leaks its parts.**
- **DeleteObject no longer checks that it removes the head it read.** A
  delete racing an overwrite leaks the new object's tail.
- **A copy that loses the head race leaks the source's tail**, through a
  reference no head carries.
- **A copy onto itself can write a deleted tail back into the head.** An
  overwrite between the copy's read and its write loses the object's data.
- **A failed index completion undoes a write that already happened.** On
  a FIFO-bilog bucket the bilog flush can fail after the head write; the
  write's tail is then deleted, or a copy's references dropped.
- **A writer that read a head before dedup rewrote it leaks the source's
  tail.** Dedup changes the manifest but not the ID tag writers guard on.
- **Dedup and a copy onto itself can delete the object's data.** Dedup
  frees the old tail at once, and the copy writes it back.
- **A write that stalls past the pending-op expiry is lost from the
  index.** A listing rewrites the entry from the old head.
- **A conditional DeleteObject can delete an object that fails its
  condition.** It checks `If-Match` against the head it read, and the
  removal does not check it again.
- **A conditional request that loses the race is answered success, even
  when the write that beat it needed the head it read.** `If-Match: *`
  does so on main; so does a conditional delete once its removal is
  guarded, and a lease release and a takeover of the same lease then both
  succeed.
- **A completion refused after it lost the race drops its parts from the
  bucket index**, while the upload stays.

Resharding holds, and each of its mechanisms is needed.

`rgw_iam_sts` finds sixteen problems on main, detailed in its README with
a proposed fix for most of them. Its README also maps the filed tracker
issues to their fixes:

- DeleteRole can delete a role that has policies. It checks the role it
  loaded, then deletes whatever role holds the name.
- A role update that races DeleteRole can be answered EntityAlreadyExists.
- Concurrent creates can exceed the account's limits on roles, users and
  groups, and can give two users, or two groups, the same name.
- DeleteUser can answer success while the user survives with a new access
  key, unreachable by name.
- A deactivated access key can still authenticate. A user policy write can
  restore its index entry, and authentication does not check the flag.
- DeleteGroup can delete a group that has members: one added
  concurrently, or one hidden behind a stale member entry that a rename
  leaves.
- AssumeRole can issue credentials for a role whose trust policy it never
  checked. For this, the role is deleted and created again under its name
  while the request runs. In the same account, AssumeRole does not require
  the trust policy at all.
- GetSessionToken accepts session credentials, and the credentials it
  issues to a role session survive the role's deletion.
