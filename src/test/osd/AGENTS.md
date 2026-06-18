# src/test/osd/ — EC Peering & Failover Test Framework

## Overview

This directory contains a full-stack simulation harness for testing EC (erasure-coded) placement
group behaviour **without a real OSD daemon**. The harness wires together real production
components — `PeeringState`, `ECSwitch`, `PGBackend`, `PGLog`, `ScrubBackend` — against a set of
thin test stubs, driven by a deterministic single-threaded event loop.

The primary test file is
[`TestECFailoverWithPeering.cc`](TestECFailoverWithPeering.cc) which uses the
[`ECPeeringTestFixture`](ECPeeringTestFixture.h) to exercise peering, OSD failures, recovery, and
scrub across many EC configurations simultaneously.

---

## Class Hierarchy

```
::testing::Test
  └── PGBackendTestFixture          PGBackendTestFixture.h / .cc
        └── ECPeeringTestFixture    ECPeeringTestFixture.h / .cc
              ├── TestECFailoverWithPeering  (also: ::testing::WithParamInterface<BackendConfig>)
              └── ECCrushTestFixture         ECCrushTestFixture.h / .cc
                    └── TestECWithCRUSH      (also: ::testing::WithParamInterface<BackendConfig>)
```

---

## Key Components

### [`EventLoop`](EventLoop.h)

A deterministic, single-threaded queue that serialises all asynchronous work.  Every operation in
the harness — ObjectStore transactions, OSD-to-OSD messages, peering messages, peering state
events — is a callback in this queue.

**Event types:**
| Type | Meaning |
|---|---|
| `GENERIC` | Generic callback with no OSD association |
| `OSD_MESSAGE` | Message from the EC backend routed to a remote OSD |
| `TRANSACTION` | ObjectStore transaction callback on a specific OSD |
| `PEERING_MESSAGE` | Protocol message (PGNotify, PGLog, …) routed between OSDs |
| `CLUSTER_MESSAGE` | Higher-level cluster-state message |
| `PEERING_EVENT` | boost::statechart event injected into a `PeeringState` |

**Thread-local context** — every event executes with two statics set:

| Variable | Accessor | What it is |
|---|---|---|
| `current_executing_osd` | `EventLoop::get_current_executing_osd()` | OSD whose event is running |
| `current_test_pg` | `EventLoop::get_current_test_pg()` | TestPG that owns the event |

Code running inside a callback (including production EC code) reads these via the statics to
determine "which OSD am I?" — mimicking what a real OSD thread would provide via its thread-local
state.

**Suspension** — individual links between OSDs can be frozen and unfrozen:

```cpp
suspend_to_osd(osd)           // block all events destined for osd
suspend_from_to_osd(from,to)  // block messages from one OSD to another
unsuspend_to_osd(osd)         // release and re-queue held events
```

Events arrive at the suspended queue in order and are flushed back to the main queue on unsuspend.
This is used to reproduce races: issue writes, block an OSD's responses, trigger a failure, then
unblock.

**Stats:**
```cpp
event_loop->reset_stats();
event_loop->get_stats_by_type()  // std::map<EventType,int>
```
Tests can assert on exact message counts (e.g., "k*2 OSD messages after degraded read").

**Running:**
```cpp
event_loop->run_until_idle();      // drain until empty + no idle work
event_loop->run_one();             // process a single event
event_loop->run_many(n);           // process up to n events
```

---

### [`OsdTestFixture`](OsdTestFixture.h) and [`TestPG`](OsdTestFixture.h)

`OsdTestFixture` is a plain struct that owns all per-OSD state:

| Field | Type | Purpose |
|---|---|---|
| `store` | `shared_ptr<MockStore>` | In-memory ObjectStore for this OSD |
| `lru` | `unique_ptr<ECExtentCache::LRU>` | Extent cache LRU |
| `pgs` | `map<spg_t, unique_ptr<TestPG>>` | PGs on this OSD |
| `coll` / `ch` | `coll_t` / `CollectionHandle` | ObjectStore collection |

`TestPG` is a plain struct that owns all per-PG state:

| Field | Type | Purpose |
|---|---|---|
| `peering_state` | `unique_ptr<PeeringState>` | Real PeeringState instance |
| `peering_listener` | `unique_ptr<MockPeeringListener>` | Shim for PeeringState callbacks |
| `peering_ctx` | `unique_ptr<PeeringCtx>` | Buffered messages/transactions from PeeringState |
| `backend` | `unique_ptr<PGBackend>` | Real `ECSwitch` instance |
| `backend_listener` | Owned by `peering_listener` | `MockPGBackendListener` |
| `dpp` | `unique_ptr<DoutPrefixProvider>` | Log prefix using PeeringState's state |
| `object_contexts` | `map<hobject_t, ObjectContextRef>` | OBC cache per PG |
| `outstanding_writes` | `map<hobject_t, int>` | In-flight write count per object |

**Destruction order matters.** In `TestPG`, member fields are destroyed bottom-up (reverse
declaration order). `dpp` is declared before `peering_listener` which is declared before
`peering_state` so the raw pointers held by the state machine are always valid during teardown.

---

### [`MockStore`](MockStore.h)

Wraps `MemStore` (the in-memory ObjectStore) with error injection:

```cpp
store->inject_read_error(ghoid, -EIO);   // one-shot: cleared after first read
store->clear_read_error(ghoid);
store->clear_all_read_errors();
```

Each OSD gets its own independent `MockStore`. A fork-based cleanup daemon deletes the temp
directory even if the test crashes.  Call via `MockStore::create(cct, osd_id)`.

---

### [`MockMessenger`](MockMessenger.h)

Routes messages between OSD stubs without a real network.  Handlers are registered by message type:

```cpp
messenger->register_typed_handler<MOSDPeeringOp>(MSG_OSD_PG_NOTIFY2, handler);
```

When a `PeeringState` buffers a message in `PeeringCtx::message_map`, the idle callback in
`ECPeeringTestFixture::SetUp()` drains the map and calls `messenger->send_message()`, which
dispatches the handler as an `OSD_MESSAGE` event on the `EventLoop`.

---

### [`MockPeeringListener`](MockPeeringListener.h)

Implements `PeeringState::PeeringListener`. The listener bridges the PeeringState state machine to
the test infrastructure:

- Holds the `MockPGBackendListener` (`backend_listener`) that owns the pool, shard set, and
  missing location maps used by `ECSwitch`.
- Forwards `queue_transaction()` calls to the `EventLoop` as `TRANSACTION` events.
- Implements `request_local_background_io_reservation()` and friends as no-ops (or stubs).
- Tracks `activate_complete_called` so tests can assert `on_activate_complete` was invoked.
- Exposes `pg_temp_wanted` / `next_acting` for the monitor-simulation loop in `new_epoch()`.

---

### [`MockPGBackendListener`](MockPGBackendListener.h)

Implements `PGBackend::Listener` and `ECListener`. Owns:

- `pool` — `PGPool` wrapper, updated when OSDMap changes.
- `shardset` / `acting_recovery_backfill_shard_id_set` — the set of active shards.
- `shard_info` / `shard_missing` — per-peer `pg_info_t` and `pg_missing_t` used by `ECSwitch`.
- `local_missing` — the primary's own missing set.
- `RecoveryCallbackTracker recovery_tracker` — counts `on_local_recover`, `on_peer_recover`,
  `on_global_recover` calls.
- `sent_messages` — all messages sent by the backend (for inspection).

---

### [`PGBackendTestFixture`](PGBackendTestFixture.h)

Base fixture providing pool setup, write/read helpers, scrub, and object-tracking.

**Pool configuration fields** (set before `SetUp()`)

| Field | Default | Meaning |
|---|---|---|
| `pool_type` | `EC` | `EC` or `REPLICATED` |
| `k` | `4` | EC data chunk count |
| `m` | `2` | EC coding chunk count |
| `stripe_unit` | `4096` | Bytes per chunk (= chunk_size) |
| `ec_plugin` | `"isa"` | Plugin name (`"isa"`, `"jerasure"`) |
| `ec_technique` | `"reed_sol_van"` | Erasure technique |
| `num_zones` | `1` | Number of fault-domains; total OSD count = `num_zones * (k + m)` |
| `pool_flags` | `FLAG_EC_OVERWRITES \| FLAG_EC_OPTIMIZATIONS` | Pool flags |

**IO helpers** — all helpers are synchronous from the test's perspective (they call
`run_until_idle()` internally):

| Method | What it does |
|---|---|
| `create_and_write(obj, data)` | Create + write, return result code |
| `create_and_write_verify(obj, data)` | Same, but also asserts success and reads back |
| `write(obj, offset, data, size)` | Overwrite at offset; may return `-EINPROGRESS` if blocked |
| `write_verify(obj, offset, data, size)` | Write + assert success + read back |
| `read_object(obj, offset, len, bl, size)` | Read into bufferlist |
| `verify_object(obj)` | Read back and compare against `ObjectTracker` expected state |
| `write_attribute(obj, key, value, force_all_shards)` | Write an xattr |
| `delete_object(obj)` | Delete |
| `read_shard_object_info(obj, shard)` | Read `object_info_t` directly from disk for a shard |

**Scrub helpers:**

| Method | What it does |
|---|---|
| `scrub_object(obj)` | Full scrub of one object; returns `true` if corruption found |
| `scrub_all_objects()` | Called automatically in `TearDown()` for optimised EC pools |
| `corrupt_shard_data(hoid, shard)` | Overwrite a shard's data with zeros |

**ObjectStore access:**
```cpp
OsdTestFixture* osd_fixture = get_osd_fixture(osd_id);
osd_fixture->store->getattr(osd_fixture->ch, ghoid, "key", attr_value);
```

**Object tracking** — enabled automatically in `SetUp()`:
```cpp
enable_object_tracking();       // also called by SetUp() — already on
ObjectTracker* t = get_object_tracker();
t->object_exists("my_obj");
```

---

### [`ECPeeringTestFixture`](ECPeeringTestFixture.h)

Extends `PGBackendTestFixture` with a full peering infrastructure. `TestPG` objects are created
**lazily** via `ensure_test_pg_exists()` as OSDs appear in the OSDMap's acting set, not upfront.

**Setup flow:**

1. `PGBackendTestFixture::SetUp()` — creates `EventLoop`, `MockMessenger`, pool, OSDs.
2. `ECPeeringTestFixture::SetUp()` — optionally adds pg_upmap (when `use_upmap()` returns true,
   which is the default), registers peering message handlers and the idle callback for `PeeringCtx`
   flushing, calls `pre_peering_hook()`, then calls `new_epoch_loop()`.
3. `new_epoch_loop()` runs: `event_advance_map()` → `event_activate_map()` → `new_epoch(true)`,
   repeating until no more up_thru or pg_temp requests are outstanding.
4. After `SetUp()` all shards should be Active/Clean.

**Monitor simulation — `new_epoch_loop()`:**

Real Ceph has a monitor that bumps the OSDMap epoch when OSDs request `up_thru` or `pg_temp`
changes.  The fixture simulates this with a tight loop:

```
do {
    event_advance_map();    // advance_map() on every shard
    event_activate_map();   // activate_map() on every shard
} while (new_epoch(true));  // bump epoch if any shard needs up_thru or pg_temp
```

`new_epoch()` inspects every `TestPG::get_peering_state()->get_need_up_thru()` and the primary's
`listener->pg_temp_wanted`, applies them to a new `OSDMap::Incremental`, and advances the epoch.
For pools with `FLAG_EC_OPTIMIZATIONS`, acting sets are stored in `primaryfirst` order (coding
shards first) using `pgtemp_primaryfirst()`, matching what the real monitor does.

---

## OSD Failure and Recovery API

All of these methods create a new OSDMap epoch and call `new_epoch_loop()` to drive peering to
completion before returning.

| Method | Effect |
|---|---|
| `mark_osd_down(osd)` | OSD marked down in OSDMap |
| `mark_osd_up(osd)` | OSD marked up in OSDMap |
| `mark_osds_down(vector<int>)` | Multiple OSDs down atomically |
| `set_pool_min_size(n)` | Change pool `min_size` |
| `advance_epoch()` | Bump epoch with no state change |
| `set_config(option, value)` | Modify `g_ceph_context->_conf` at runtime (restored in `TearDown`) |

**Message suspension — simulating races:**
```cpp
suspend_primary_to_osd(shard);         // block messages from primary to that shard
unsuspend_primary_to_osd(shard);       // unblock; held messages are re-queued
```
These call `EventLoop::suspend_from_to_osd(primary, shard)` internally.

**Read error injection:**
```cpp
inject_read_error_for_shard(obj_name, shard, -EIO);
```
One-shot: clears automatically after the first read.

---

## Recovery API

### `run_recovery(obj_name, recover_primary, expected_data)`

Recovers a single object:

1. Checks consistency between `peer_missing_map` (primary's view) and each peer's actual
   `PeeringState::get_pg_log().get_missing()` — asserts they agree.
2. Calls `PGBackend::open_recovery_op()`, then `recover_object()`, then `run_recovery_op()`.
3. Drains the event loop.
4. Calls `check_recovery_completion_impl()` for each acting OSD, which posts
   `AllReplicasRecovered` or `RequestBackfill` peering events as appropriate (mimicking
   `PrimaryLogPG::start_recovery_ops()`).

### `run_parallel_recovery(obj_names, recover_primary, expected_data)`

Same as above but queues **all objects into a single `RecoveryHandle`** before calling
`run_recovery_op()` once. This is used to reproduce Bug 75432 (assertion in
`do_read_op()` when some sub-reads complete while others need resend).

### `recover_primary` flag

| Value | Meaning |
|---|---|
| `true` | The primary OSD's own shard needs recovery (it was down during a write); checks `ps->get_pg_log().get_missing()` |
| `false` | A non-primary peer needs recovery; walks `ps->get_peer_missing()` |

---

## PG State Inspection

```cpp
get_primary_test_pg()->get_peering_state()->is_active()
get_primary_test_pg()->get_peering_state()->is_clean()
get_primary_test_pg()->get_peering_state()->get_pg_log().get_log()
get_primary_test_pg()->get_peering_state()->get_peer_missing()
get_state_name(shard)      // human-readable state string
all_shards_active()        // all acting shards are Active
primary_is_clean()         // primary's PG_STATE_CLEAN is set
```

**TestPG accessors:**

| Method | What it returns |
|---|---|
| `get_primary_test_pg()` | Primary's `TestPG*` from `OSDMap::get_primary_shard()` |
| `get_test_pg_by_shard(n)` | `TestPG*` at acting-set position n |
| `get_first_test_pg_for_osd(osd)` | `TestPG*` when OSD has exactly one PG (assert otherwise) |
| `get_primary_shard_from_osdmap()` | `int` OSD number of the current acting primary |
| `get_pool()` | `const pg_pool_t&` from the live OSDMap |

---

## BackendConfig — Test Parameterisation

[`TestCommon.h`](TestCommon.h) defines `BackendConfig`:

```cpp
struct BackendConfig {
    PGBackendTestFixture::PoolType pool_type;   // EC or REPLICATED
    std::string ec_plugin;                      // "isa" or "jerasure"
    std::string ec_technique;                   // e.g. "reed_sol_van"
    uint64_t    pool_flags;                     // FLAG_EC_OVERWRITES | ...
    uint64_t    stripe_unit;                    // bytes per shard (4096, 8192, 16384)
    int         k;                              // data shards
    int         m;                              // coding shards
    int         num_zones;                      // 1 = single-zone, 2 = two-zone
    std::string label;                          // test parameter name
};
```

`TestECFailoverWithPeering` reads `GetParam()` in its constructor and copies the fields into the
fixture before `SetUp()` runs.  Parameters are declared in `kECPeeringConfigs` (a
`vector<BackendConfig>` in an anonymous namespace in `TestECFailoverWithPeering.cc`) and wired up
with `INSTANTIATE_TEST_SUITE_P`.

---

## Writing a New Test

### Minimal test body

```cpp
TEST_P(TestECFailoverWithPeering, MyNewTest) {
    ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

    // Write
    create_and_write_verify("my_obj", std::string(stripe_unit * k, 'A'));

    // Fail a shard
    mark_osd_down(1);

    // Write degraded
    write_verify("my_obj", 0, std::string(stripe_unit * k, 'B'), stripe_unit * k);

    // Recover
    mark_osd_up(1);
    run_recovery("my_obj", false, std::string(stripe_unit * k, 'B'));

    // Optionally check peering state
    ASSERT_TRUE(primary_is_clean());
}
```

### Race scenario (using message suspension)

```cpp
// Prevent the primary from delivering writes to shard k+1
suspend_primary_to_osd(k + 1);

int r = write("obj", 0, data_b, data_b.size());
ASSERT_EQ(-EINPROGRESS, r);

// While shard k+1 is isolated, fail shard 2 to trigger rollback
mark_osd_down(2);

// Unblock — shard k+1 will now process the queued messages
unsuspend_primary_to_osd(k + 1);
event_loop->run_until_idle();
```

### Skipping configurations that don't apply

```cpp
if (m < 2) {
    GTEST_SKIP() << "Requires m >= 2";
}
if (num_zones <= 1) {
    GTEST_SKIP() << "Requires multi-zone configuration";
}
```

### Adding a new parameter configuration

Add a `BackendConfig` entry to `kECPeeringConfigs` in `TestECFailoverWithPeering.cc`:

```cpp
{PGBackendTestFixture::EC, "isa", "reed_sol_van",
 pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,
 4096, 4, 2, 1, "EC_ISA_Opt_k4m2_su4k"},
```

The label becomes the GTest parameter suffix in test names.

### Runtime configuration overrides

```cpp
set_config("osd_async_recovery_min_cost", "0");
// ... test body ...
// Restored automatically in TearDown(); or restore manually:
set_config("osd_async_recovery_min_cost", "100");
```

### Direct OSDMap manipulation

For scenarios beyond the helpers (e.g., adding a brand-new OSD):

```cpp
auto new_osdmap = std::make_shared<OSDMap>();
new_osdmap->deepish_copy_from(*osdmap);
OSDMapTestHelpers::new_osd_up(*new_osdmap, new_osd_id, pgid, 0);
update_osdmap_with_peering(new_osdmap);
```

`update_osdmap_with_peering()` calls `update_osdmap()` then `new_epoch_loop()`.

---

## Invariants and Gotchas

1. **shard == osd** — `ECPeeringTestFixture` and all tests derived from it
   (including `TestECFailoverWithPeering`) install a pg_upmap so shard N is
   always on OSD N.  `ECCrushTestFixture` overrides `use_upmap()` to return
   `false`, disabling the upmap and letting CRUSH determine placement.  When
   `use_upmap()` is false the OSD that holds shard N may be any OSD; the
   harness defers collection creation to `ensure_test_pg_exists()` (where the
   actual `pg_shard_t` is known) and routes transactions to the correct OSD
   via `pg_whoami.osd`.

2. **EC position in acting array** — for EC pools, the position of an OSD in the acting array *is*
   the shard ID. When an OSD fails, its slot is set to `CRUSH_ITEM_NONE`; it is *not* removed.
   Any code that iterates `acting_osds` must skip `CRUSH_ITEM_NONE` entries.

3. **`get_test_pg()` requires EventLoop context** — it reads `EventLoop::current_test_pg`, which
   is only set while an event callback is executing. Outside of callbacks (i.e., in test bodies),
   use `get_primary_test_pg()`, `get_test_pg_by_shard(n)`, or `get_first_test_pg_for_osd(osd)`.

4. **`get_first_test_pg_for_osd()` asserts single PG** — if the OSD has more than one PG (which
   doesn't happen in current tests, but could with future zone configurations), use a different
   accessor.

5. **Transactions are serialised via EventLoop** — `queue_transaction_helper()` calls
   `MockStore::queue_transaction()` synchronously but the callback that fires on completion is
   placed in the `EventLoop` queue. EC sub-reads and sub-writes proceed through the event loop;
   calling `run_until_idle()` is necessary to allow writes or reads to fully complete.

6. **`-EINPROGRESS` from `write()`** — when a write is sent while the primary has suspended
   delivery to a shard, `write()` returns `-EINPROGRESS`. This is normal — the op is queued inside
   the EC backend and will complete when the suspension is lifted.

7. **TearDown scrub** — for pools with `FLAG_EC_OPTIMIZATIONS`, `TearDown()` automatically scrubs
   every object in the primary's collection. Any corruption left over from the test will cause a
   TearDown failure. If a test deliberately corrupts a shard and does not repair it, it must mark
   that shard down before returning so the degraded scrub path is taken.

8. **Config restoration** — `ECPeeringTestFixture` saves `ConfigValues` at the start of `SetUp()`
   and restores them in `TearDown()`. Any `set_config()` calls are automatically undone. Tests may
   also restore values manually mid-test if needed.

9. **`ECSwitch` vs `ECBackend`** — the fixture always creates real `ECSwitch` instances backed by
   the real `ec_impl` (loaded from the plugin). No mock EC backend is used in peering tests.

10. **Pool features flag** — `PG_FEATURE_CLASSIC_ALL` is passed to `PeeringState` constructor. This
    enables all feature flags; tests that need to test feature negotiation must adjust accordingly.

---

## File Reference

| File | Role |
|---|---|
| [`EventLoop.h`](EventLoop.h) | Deterministic single-threaded event queue |
| [`OsdTestFixture.h`](OsdTestFixture.h) | `OsdTestFixture` (per-OSD) and `TestPG` (per-PG) containers |
| [`MockStore.h`](MockStore.h) / [`MockStore.cc`](MockStore.cc) | MemStore + error injection + cleanup daemon |
| [`MockMessenger.h`](MockMessenger.h) | In-process message routing between OSD stubs |
| [`MockPeeringListener.h`](MockPeeringListener.h) / [`MockPeeringListener.cc`](MockPeeringListener.cc) | `PeeringState::PeeringListener` shim |
| [`MockPGBackendListener.h`](MockPGBackendListener.h) | `PGBackend::Listener` + `ECListener` shim |
| [`MockPGBackend.h`](MockPGBackend.h) | Minimal `PGBackend` used inside `MockPeeringListener` |
| [`MockECReadPred.h`](MockECReadPred.h) / [`MockECRecPred.h`](MockECRecPred.h) | `IsPGReadablePredicate` / `IsPGRecoverablePredicate` for EC |
| [`OSDMapTestHelpers.h`](OSDMapTestHelpers.h) | OSDMap manipulation helpers (add pool, mark up/down, set pg_acting) |
| [`ObjectTracker.h`](ObjectTracker.h) / [`ObjectTracker.cc`](ObjectTracker.cc) | Tracks expected object state for read-back verification |
| [`ScrubTestFixture.h`](ScrubTestFixture.h) | Mock scrub-backend listener |
| [`PGBackendTestFixture.h`](PGBackendTestFixture.h) / [`PGBackendTestFixture.cc`](PGBackendTestFixture.cc) | Base fixture: pool setup, IO helpers, scrub |
| [`ECPeeringTestFixture.h`](ECPeeringTestFixture.h) / [`ECPeeringTestFixture.cc`](ECPeeringTestFixture.cc) | Peering fixture: epoch loop, OSD failure/recovery helpers |
| [`ECCrushTestFixture.h`](ECCrushTestFixture.h) / [`ECCrushTestFixture.cc`](ECCrushTestFixture.cc) | CRUSH fixture: extends ECPeeringTestFixture with a real EC indep CRUSH rule |
| [`TestCommon.h`](TestCommon.h) | `BackendConfig` and `WriteReadParam` shared parameter structs |
| [`TestECFailoverWithPeering.cc`](TestECFailoverWithPeering.cc) | Parameterised EC peering/failover/scrub tests |
| [`TestECWithCRUSH.cc`](TestECWithCRUSH.cc) | Parameterised EC tests exercising real CRUSH-based pool placement |
| [`TestBackendBasics.cc`](TestBackendBasics.cc) | Non-peering read/write tests using `PGBackendTestFixture` directly |
