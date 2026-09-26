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

The scripts find P in `~/.dotnet/tools`, and a Homebrew `dotnet@8` by
themselves; otherwise set `DOTNET_ROOT`. `run.sh` runs `jobs` cases at a
time; `rgw_overwrite` has 167, at 20,000 schedules each.

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
