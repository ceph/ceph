# GET producers / consumers

Locked design. Not implemented as specified. PUT / PUT-overwrite stay on the current model until a later expansion.

Style: defensive checks, braces on every block, non-cuddled else.

## Placement

- **Producers** live in the perf-driver. They generate keys and push one request at a time.
- **Consumers** live in the library, same idea as PUT `BatchCommitQueue`. Perf-driver must not duplicate library GET / storage-tier / MD-complete logic.
- Runner still starts one `--perf` process per instance. That process’s producers only enqueue keys for `g_instance_id` (`make_object_name` already embeds instance). GET instance count still matches PUT metadata. GET does not use `c=`; do not require GET thread count to match PUT `c=`.

## Config (GET `.md` / `.txt`)

Required fields (fail if missing or invalid):

- `producers:` — P feeder threads in the perf-driver
- `consumers:` — C service threads in the library
- `max_futures:` — per-consumer in-flight window; must be in `1 … GET_FUTURE_MAX`

Named constexprs (no magic numbers):

- `GET_REQUESTS_QUEUE_SIZE = 1024` — bounded shared request queue
- `GET_FUTURE_MAX = 1024` — cap for `max_futures:`
- 10 µs idle sleep: named constexpr

GET command line drops `c=`. Example: `get-test producers=N consumers=M max_futures=K [duration=|count=] [--all-versions] [--progress-sec=]`.

Wire `scripts/gen_perf_test.sh` and `scripts/run_perf_from_def.sh`. Update GET defs such as `Perf-Test-Read-4.md` / `.txt`.

## Library API

Library exposes an enqueue API. The caller (producer thread) passes a **pointer** to a producer-allocated request-entry (all command fields; **no data pointer**). The library pushes that pointer onto the shared consumer queue. If the queue is full, the caller sleeps on a semaphore.

## Producer (perf-driver)

- Pre-allocates **M** fixed-size request-entries on a **free list**.
- Fixed field sizes include `bucket_name` = 64, `object_name` = 1024, and the rest of the request-entry layout below.
- Allocate from the free list, fill the entry, then push to the library API.
- Partition the PUT `(thread_id, seq)` space evenly, disjoint, full coverage. P need not equal PUT `c=`.
- `--all-versions`: **one queue item per GET** (current + each other version). No expanding one key into `nver` Gets in the consumer.
- `duration` / `count` / one-pass: stop generating the same way as today. **Last producer to finish** (atomic) enqueues **exactly C** EOF tokens. No EOF until every producer has finished its last push.
- Producer-assigned **req-id** on each entry.
- After ACK (see below), return that entry to the free list.

## Request-entry

Allocated by the producer; never copied by the consumer (pointer only).

Fields:

- `op` — GET, PUT, LIST, …
- `bucket_name` (64)
- `object_name` (1024)
- `version-id`
- timestamps (`creation` at producer push; `serviced` at consumer pop)
- flags (`has-version`, `is EOF`, …)
- producer-id, req-id
- **linkable interface** — enough storage in the entry to be an intrusive list node; no extra allocation to enqueue
- **response** — status and data-pointer (filled by the library/consumer)

## Shared request queue (library)

Classic bounded buffer. Capacity `GET_REQUESTS_QUEUE_SIZE`.

- Structure: **intrusive linked list** of request-entries (the entry is the node).
- Queue full → producer sleeps on the full semaphore.
- Queue empty:
  - consumer local pending-tasks empty → wait on empty semaphore
  - in-flight remain → do **not** wait on empty semaphore; poll / 10 µs
- Completions do not come from this queue. Completions come from the consumer’s pending futures (and then storage-tier read).

## Reply queues (library)

Library holds an **array of per-producer reply queues**, indexed by producer-id.

When the consumer finishes a request (FDB MD future done **and** storage-tier read done), it pushes a **reply** onto that producer’s queue using producer-id and req-id. The producer then returns the request-entry to the free list.

## Consumer (library)

Container: **pending-tasks**. Each slot: **`pend_task_t`**.

Per consumer: VLA `pend_task_t array[max_futures]` from the function parameter. Issue overlapping async Gets (`kv_async_get` / `is_ready` / wait) — same overlap idea as PUT `put_prepare` / `put_finalize`. **Do not** call blocking `get_object()` on this path.

Loop:

1. Async pass over pending-tasks: complete every ready `pend_task_t`; free slots; keep `free_count`.
2. If `free_count > 0` and not in post-EOF drain: pop request **pointers** (no copy), issue async GET until `free_count == 0` or the queue has nothing to take without blocking.
3. If `free_count == 0`: sleep 10 µs, goto 1.
4. Queue empty: as above (empty semaphore vs poll).
5. Pop **EOF** → never touch the shared request queue again; drain local pending-tasks only; exit.

`pend_task_t` holds the request pointer, the FDB future (and live transaction; future must not outlive the txn), and occupied state.

## MD vs object data

- The FDB future is **metadata only**, not object payload.
- KV MD holds the **data-reference**. Payload is not read before that value is available.
- **Data-buffer** for object bytes is allocated by the consumer **after** the MD future lands. Not provided by the producer.
- After MD is valid, the consumer performs the **storage-tier / external-store read** into that buffer.
- ACK / reply happens only after both MD complete **and** storage-tier read complete.

On GET future ready (for latency accounting):

- `queue_latency` = serviced − creation
- `fdb_latency` = MD-ready − serviced
- `total` includes queue + FDB + storage-tier as recorded

All GET IOPS / hits / missing / errors / those latencies / progress come from **consumers**. Producer reports full-semaphore block percent (blocked / producer elapsed).

Progress stays on consumer thread 0 using existing `progress_tick` shape; latency field is **total** until a later format change.

EOF is not a GET (no latency/IOPS).

## Stop / correctness

- One-pass: after drain, hits vs produced items; `missing > 0` still `GET_ERR`.
- Duration: partial keyspace is allowed (same as today).
- Validate P, C, `max_futures`.

## Current code (gap)

Today’s perf-driver `get_complete_task` waits the MD get, parses it, and on a live object only increments hits. It does not check data-tier and does not read payload (inline, `D:`, or storage-tier). That is **not** the target behavior.

PUT `BatchCommitQueue` already sits on `KvRgwServiceImpl` in the library. GET consumers belong there too.

## Out of scope

PUT / PUT-overwrite new model (later). List-test. Changing FDB C API wait-any.
