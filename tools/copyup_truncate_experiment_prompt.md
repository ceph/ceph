# Copyup + Truncate Same-Shard Experiment Loop

You are working in the Ceph source tree at [`/work/ceph`](../README.md). Your goal is to discover a workload that causes librbd copyup to generate an OSD transaction on an EC shard that contains both:

- a write
- a truncate

for the same clone object and on the same shard.

## Existing setup
- The cluster lifecycle is managed externally by the user.
- The user runs:
  - `~/restart_ceph_ec_cluster`
  - `~/lee_start_script.sh`
- The workload script is [`tools/rbd_discard_workload.py`](tools/rbd_discard_workload.py).
- The relevant librbd copyup code is in [`CopyupRequest::copyup()`](src/librbd/io/CopyupRequest.cc:412).
- Discard-to-object-op translation is in [`ObjectDiscardRequest::add_write_ops()`](src/librbd/io/ObjectRequest.cc:674).

## Your loop
Repeat the following until success or you run out of plausible new ideas:

1. Run the test workload using [`tools/rbd_discard_workload.py`](tools/rbd_discard_workload.py).
2. Inspect the OSD logs in [`build/out/`](build/out) and find the transactions associated with the most recent clone copyup.
3. Inspect the relevant RBD/librbd code paths to explain what happened and why.
4. Invent one new experiment by minimally changing the workload script.
5. Run the new experiment.

## Success condition
Stop with success only if you find an EC shard transaction for the clone object that contains both:
- a write
- a truncate

and both operations are on the same shard transaction.

## Failure/stop condition
Stop if:
- you run out of credible new experiments, or
- the evidence strongly suggests the requested same-shard write+truncate transaction cannot be produced through this path.

## Constraints
- Make only minimal, targeted changes to [`tools/rbd_discard_workload.py`](tools/rbd_discard_workload.py).
- Prefer changing workload shape over changing Ceph source code.
- Always use the clone object identifiers printed by the workload script (`id` and `block_name_prefix`) when grepping logs.
- Do not assume image names appear directly in OSD logs.
- After each experiment, record:
  - workload shape used
  - clone id / block_name_prefix
  - observed shard transactions
  - why the experiment did or did not advance toward success

## Important code facts discovered so far
- [`CopyupRequest::copyup()`](src/librbd/io/CopyupRequest.cc:464) appends pending write-like requests into the shared `write_op` via [`req->add_copyup_ops(&write_op)`](src/librbd/io/CopyupRequest.cc:468).
- [`ObjectWriteRequest::add_write_ops()`](src/librbd/io/ObjectRequest.cc:664) contributes `write` / `write_full`.
- [`ObjectDiscardRequest::add_write_ops()`](src/librbd/io/ObjectRequest.cc:674) contributes `remove`, `truncate`, or `zero` depending on discard shape.
- A tail discard where `object_off + object_len == object_size` should map to [`truncate`](src/librbd/io/ObjectRequest.cc:683).
- Prior experiments have shown cases where the final EC shard transaction contained only a `write`, with no visible `truncate`.
