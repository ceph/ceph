// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// Build-day integration copy of tests/test_scrubber_run_draft.cc
// (draft kept intact in the lab pack; fixes live HERE only).
//
// Fixes applied vs the draft, all verified against stock v20.2.1 sources:
//  1. mkfs(): ObjectStore::mkfs() takes no arguments. (OSD.cc:2199)
//  2. Real path: MemStore::mkfs() writes path/collections to disk, so the
//     store path must be an existing directory; the draft's bare string
//     would fail write_file. A mkdtemp() dir is used and removed in
//     TearDown.
//  3. The meta collection does not exist after mkfs() — create it first,
//     exactly like OSD::mkfs (OSD.cc:2206-2224): create_new_collection()
//     + t.create_collection(coll_t::meta(), 0) + queue_transaction().
//  4. Both meta oids are touch()ed before any omap writes; MemStore
//     _omap_setkeys returns -ENOENT for missing objects (MemStore.cc:1221).
//     Same bootstrap as OSD.cc:3848-3859.
//  5. Transaction plumbing: OSDriver has no submit(). The pattern is
//     OSDriver::get_transaction(&t) -> add_oid(..., &otxn) ->
//     store->queue_transaction(ch, std::move(t)), as production does for
//     purged_snaps (OSD::handle_get_purged_snaps_reply, OSD.cc:6933).
//  6. `SnapMapper::Transaction` does not exist; the mapper takes an
//     `OSDriver::OSTransaction` (SnapMapper.h:76).
//  7. No main() in this file: the linked-in `unit-main` object
//     (src/test/unit.cc) already defines main() with global_init() —
//     that is how ceph_test_snap_mapper gets g_ceph_context.
//  8. mk_hobj() implemented: head object, hash 0, CEPH_NOSNAP. The scrub
//     reads pool/snaps back from the mapping VALUE (Scrubber::_parse_m
//     decodes the full hobject), so hash 0 round-trips deterministically.
//  9. Per-element stray comparison added in MultiPoolMixedPoolIds —
//     PROOF-CHECKLIST: size alone is never decisive. Also scrub() returns
//     the stray vector so tests can compare elements, not just count.
// 10. ObjectStore has no is_mounted() (verified: absent from
//     ObjectStore.h); the fixture tracks mount state with a bool.
// 11. #include <algorithm> (std::sort).
// 12. FIXTURE-LIFETIME (found under RUNTIME failure, both trees, rc=134):
//     the draft built a FRESH SnapMapper (hence a fresh MapCacher) per
//     mapping entry and destroyed it right after queue_transaction(). But
//     MapCacher::set_keys/remove_keys registers a `TransHolder` on-applied
//     callback (map_cacher.hpp:195/211) holding SharedPtrRegistry VPtrs,
//     and MemStore completes on-applied contexts ASYNCHRONOUSLY in its own
//     finisher thread. SharedPtrRegistry's custom deleter locks the
//     registry's mutex (sharedptr_registry.hpp:44-53), so completing that
//     callback after the MapCacher is destroyed hits
//     std::system_error(EINVAL) — exactly the crash seen: throw inside
//     MemStore's `fn_anonymous` finisher under ~ContainerContext.
//     Production avoids this: the OSD's MapCacher lives for the OSD's
//     lifetime. Fix: ONE OSDriver + one SnapMapper PER POOL owned by the
//     fixture for its whole lifetime; TearDown umount()s (draining the
//     finisher queue) BEFORE destroying the mappers.

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <system_error>
#include <tuple>
#include <vector>
#include <filesystem>
#include <cstdlib>

#include <fmt/format.h>

#include "gtest/gtest.h"

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/buffer.h"
#include "os/ObjectStore.h"
#include "os/memstore/MemStore.h"
#include "osd/OSD.h"
#include "osd/OSDMap.h"
#include "osd/SnapMapReaderI.h"
#include "osd/SnapMapper.h"

using namespace std;

using stray_tuple_t =
  std::tuple<int64_t, snapid_t, uint32_t, shard_id_t>;

static hobject_t mk_hobj(int64_t pool, const char *name, uint32_t hash = 0)
{
  // head hobject: snap CEPH_NOSNAP; hash defaults to 0 (deterministic
  // get_hash()), an explicit hash checks the round trip through the
  // mapping VALUE (Scrubber::_parse_m decodes the full hobject).
  return hobject_t(object_t(name), "", CEPH_NOSNAP, hash, pool, "");
}

static stray_tuple_t stray_of(int64_t pool, uint64_t snap, uint32_t hash = 0,
                              shard_id_t shard = shard_id_t::NO_SHARD)
{
  return stray_tuple_t(pool, snapid_t(snap), hash, shard);
}

// ---------------------------------------------------------------------------
// Fixture: one MemStore, meta collection, the OSD's own oid handles.
// ---------------------------------------------------------------------------
class ScrubberRunTest : public ::testing::Test {
protected:
  std::unique_ptr<ObjectStore> store;
  ObjectStore::CollectionHandle ch;
  ghobject_t mapping_hoid;
  ghobject_t purged_snaps_hoid;
  // fixture-lifetime drivers/mappers: see fix 12 in the header note
  std::unique_ptr<OSDriver> purged_drv;
  std::unique_ptr<OSDriver> mapping_drv;
  // key is (pool, shard.id): mappers of different shards namespace their
  // SNA_/OBJ_ keys apart via the shard prefix, so they coexist on the one
  // mapping_drv without colliding
  std::map<std::pair<int64_t, int>, std::unique_ptr<SnapMapper>> mappers;
  std::string path;
  bool mounted = false;
  CephContext *cct = g_ceph_context;

  void SetUp() override {
    char pathbuf[] = "/tmp/scrubber-run-test-XXXXXX";
    char *d = mkdtemp(pathbuf);
    ASSERT_NE(nullptr, d);
    path = d;

    store = ObjectStore::create(cct, "memstore", path);
    ASSERT_TRUE(store);
    ASSERT_EQ(0, store->mkfs());
    ASSERT_EQ(0, store->mount());
    mounted = true;

    ch = store->open_collection(coll_t::meta());
    if (!ch) {
      // no meta collection right after mkfs — bootstrap like OSD::mkfs
      ch = store->create_new_collection(coll_t::meta());
      ceph::os::Transaction t;
      t.create_collection(coll_t::meta(), 0);
      ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
      ch->flush();
    }
    ASSERT_TRUE(ch);

    mapping_hoid = OSD::make_snapmapper_oid();
    purged_snaps_hoid = OSD::make_purged_snaps_oid();

    // snapmapper/purged_snaps objects must exist before omap writes
    // (MemStore _omap_setkeys -> -ENOENT on missing object)
    ceph::os::Transaction t;
    t.touch(coll_t::meta(), mapping_hoid);
    t.touch(coll_t::meta(), purged_snaps_hoid);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));

    // fixture-lifetime drivers (fix 12)
    purged_drv = std::make_unique<OSDriver>(store.get(), ch,
                                            purged_snaps_hoid);
    mapping_drv = std::make_unique<OSDriver>(store.get(), ch,
                                             mapping_hoid);
  }

  void TearDown() override {
    if (store && mounted) {
      // umount drains the finisher queue (wait_for_empty) BEFORE the
      // MapCacher registries below are destroyed (fix 12)
      store->umount();
      mounted = false;
    }
    mappers.clear();
    mapping_drv.reset();
    purged_drv.reset();
    store.reset();
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }

  SnapMapper &get_mapper(int64_t pool,
                         shard_id_t shard = shard_id_t::NO_SHARD) {
    auto key = std::pair(pool, static_cast<int>(shard));
    auto &m = mappers[key];
    if (!m) {
      // per-(pool, shard) mapper, fixture-lifetime (fix 12); matches
      // production, where each PG owns its shard's mapper
      m = std::make_unique<SnapMapper>(
        cct, mapping_drv.get(), 0, 0, pool, shard);
    }
    return *m;
  }

  // one record_purged_snaps() call per interval-set, fresh epoch each, so
  // rows build through the production writer.
  void make_purged(
    int64_t pool, std::vector<std::pair<snapid_t, snapid_t>> intervals) {
    std::map<epoch_t, mempool::osdmap::map<int64_t, snap_interval_set_t>> m;
    ++next_epoch;
    auto &set = m[next_epoch][pool];
    for (auto &iv : intervals) {
      set.insert(iv.first, iv.second - iv.first);
    }
    ceph::os::Transaction t;
    SnapMapper::record_purged_snaps(
      cct, *purged_drv, purged_drv->get_transaction(&t), std::move(m));
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
  }

  void make_mappings(
    std::vector<std::pair<snapid_t, hobject_t>> entries) {
    for (auto &[snap, hoid] : entries) {
      ceph::os::Transaction t;
      auto otxn = mapping_drv->get_transaction(&t);
      get_mapper(hoid.pool).add_oid(hoid, {snap}, &otxn);
      ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
    }
  }

  std::vector<stray_tuple_t> scrub() {
    SnapMapper::Scrubber s(cct, store.get(), ch, mapping_hoid,
                           purged_snaps_hoid);
    s.run();
    return s.stray;
  }

  // per-element rule (PROOF-CHECKLIST): size alone is never decisive.
  // Sorts both sides; element order is checked by the dedicated
  // StrayOrderFollowsMappingKeyDiskOrder test instead.
  void expect_strays(const std::vector<stray_tuple_t> &want,
                     std::vector<stray_tuple_t> got) {
    std::sort(got.begin(), got.end());
    ASSERT_EQ(want.size(), got.size());
    for (size_t i = 0; i < want.size(); ++i) {
      ASSERT_EQ(std::get<0>(want[i]), std::get<0>(got[i])) << "i=" << i;
      ASSERT_EQ(std::get<1>(want[i]), std::get<1>(got[i])) << "i=" << i;
      ASSERT_EQ(std::get<2>(want[i]), std::get<2>(got[i])) << "i=" << i;
      ASSERT_EQ(std::get<3>(want[i]), std::get<3>(got[i])) << "i=" << i;
    }
  }

  // one mapping through a mapper of the given shard: the SNA_ key carries
  // the shard prefix (".<hex>_" vs "" for NO_SHARD), so run()'s sscanf
  // attributes the stray to the real shard
  void make_mapping(snapid_t snap, hobject_t hoid,
                    shard_id_t shard = shard_id_t::NO_SHARD) {
    ceph::os::Transaction t;
    auto otxn = mapping_drv->get_transaction(&t);
    get_mapper(hoid.pool, shard).add_oid(hoid, {snap}, &otxn);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
  }

  // raw omap write under a meta object — bypasses the production writers,
  // for shapes record_purged_snaps()/add_oid() cannot produce
  void raw_omap_set(const ghobject_t &hoid,
                    std::map<std::string, ceph::buffer::list> kv) {
    ceph::os::Transaction t;
    t.omap_setkeys(coll_t::meta(), hoid, kv);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
  }

  // one purged row byte-identical to what make_purged_snap_key()/
  // make_purged_snap_key_value() (SnapMapper.cc) write — bypasses
  // record_purged_snaps()'s adjacent-join, which can never leave the
  // rows [a,b) and [b,c) side by side on disk
  void make_raw_purged_row(int64_t pool, snapid_t begin, snapid_t end) {
    ceph::buffer::list v;
    ceph::encode(pool, v);
    ceph::encode(begin, v);
    ceph::encode(end, v);
    std::map<std::string, ceph::buffer::list> m;
    m[fmt::format("PSN_{}_{:016x}", pool,
                  static_cast<uint64_t>(end - 1))] = v;
    raw_omap_set(purged_snaps_hoid, std::move(m));
  }

  void remove_meta_object(const ghobject_t &hoid) {
    ceph::os::Transaction t;
    t.remove(coll_t::meta(), hoid);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
  }

  epoch_t next_epoch = 0;
};

TEST_F(ScrubberRunTest, NoPurgedHistory) {
  // mapping exists, history empty -> 0 strays
  make_mappings({{snapid_t(100), mk_hobj(1, "a")}});
  ASSERT_EQ(0, scrub().size());
}

TEST_F(ScrubberRunTest, StrayInsideInterval) {
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_mappings({{snapid_t(15), mk_hobj(1, "a")}});
  ASSERT_EQ(1, scrub().size());
}

TEST_F(ScrubberRunTest, NotStrayBeforeFirstInterval) {
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_mappings({{snapid_t(5), mk_hobj(1, "a")}});
  ASSERT_EQ(0, scrub().size());
}

TEST_F(ScrubberRunTest, NotStrayAtIntervalEnd) {
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_mappings({{snapid_t(20), mk_hobj(1, "a")}});
  ASSERT_EQ(0, scrub().size());
}

TEST_F(ScrubberRunTest, OverlappingPurgedRows) {
  // record_purged_snaps() joins only ADJACENT rows: a later, wider
  // interval that strictly CONTAINS an existing row is invisible to its
  // begin-1/end probes, so both rows land on disk. Two calls — one call
  // would merge inside the interval_set and erase the corner.
  make_purged(1, {{snapid_t(4), snapid_t(5)}});   // row keyed by end-1=4
  make_purged(1, {{snapid_t(3), snapid_t(7)}});   // row keyed by end-1=6
  // purged truth = [3,7) U [4,5) = [3,7)
  make_mappings({
    {snapid_t(2), mk_hobj(1, "ok-before")},  // precedes both rows
    {snapid_t(4), mk_hobj(1, "stray-in-last")},   // in [4,5) and [3,7)
    {snapid_t(5), mk_hobj(1, "stray-wider")},     // only in [3,7)
    {snapid_t(6), mk_hobj(1, "stray-wider2")},    // only in [3,7)
    {snapid_t(7), mk_hobj(1, "ok-at-end")},  // at both row ends
    {snapid_t(8), mk_hobj(1, "ok-after")},
  });
  auto got = scrub();
  std::sort(got.begin(), got.end());
  // per-element rule (not just counts): the single-probe lower_bound
  // lookup misses strays {5, 6} whose covering row is not the last one
  // with begin <= snap — the rows-overlap case upstream flagged.
  std::vector<stray_tuple_t> want = {
    stray_tuple_t(1, snapid_t(4), 0, shard_id_t::NO_SHARD),
    stray_tuple_t(1, snapid_t(5), 0, shard_id_t::NO_SHARD),
    stray_tuple_t(1, snapid_t(6), 0, shard_id_t::NO_SHARD),
  };
  ASSERT_EQ(want.size(), got.size());
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(std::get<0>(want[i]), std::get<0>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<1>(want[i]), std::get<1>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<2>(want[i]), std::get<2>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<3>(want[i]), std::get<3>(got[i])) << "i=" << i;
  }
}

TEST_F(ScrubberRunTest, MultiPoolMixedPoolIds) {
  make_purged(1,  {{snapid_t(10),   snapid_t(20)}});
  make_purged(3,  {{snapid_t(30),   snapid_t(40)}});
  make_purged(20, {{snapid_t(2000), snapid_t(2010)}});
  make_purged(21, {{snapid_t(2100), snapid_t(2110)}});

  make_mappings({
    {snapid_t(15),    mk_hobj(1,  "a")},  // stray
    {snapid_t(35),    mk_hobj(3,  "a")},  // stray
    {snapid_t(2005),  mk_hobj(20, "a")},  // stray
    {snapid_t(5),     mk_hobj(1,  "b")},  // ok, precedes
    {snapid_t(45),    mk_hobj(3,  "b")},  // ok, at/after end
    {snapid_t(2125),  mk_hobj(21, "b")},  // ok, after last range
  });
  // per-element rule: not just the count. PSN_ rows order pools as decimal
  // strings on disk (20/21 before 3) — the ordering corner the patch fixes.
  // hobjects carry hash 0, all mappings are unsharded (NO_SHARD) here.
  auto got = scrub();
  std::sort(got.begin(), got.end());
  // evidence dump: exact strays each build found (pool,snap,hash,shard)
  cout << "GOT_STRAYS:";
  for (auto &t : got) {
    cout << " (" << std::get<0>(t) << "," << std::get<1>(t) << ","
         << std::get<2>(t) << ",shard="
         << static_cast<int>(static_cast<int8_t>(std::get<3>(t))) << ")";
  }
  cout << "\n";
  // ceph bans std::endl (global `endl` const is the compiler trap), and the
  // std:: qualifier keeps the dump explicit.
  std::vector<stray_tuple_t> want = {
    stray_tuple_t(1,  snapid_t(15),   0, shard_id_t::NO_SHARD),
    stray_tuple_t(3,  snapid_t(35),   0, shard_id_t::NO_SHARD),
    stray_tuple_t(20, snapid_t(2005), 0, shard_id_t::NO_SHARD),
  };
  ASSERT_EQ(want.size(), got.size());
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(std::get<0>(want[i]), std::get<0>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<1>(want[i]), std::get<1>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<2>(want[i]), std::get<2>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<3>(want[i]), std::get<3>(got[i])) << "i=" << i;
  }
}
// ---------------------------------------------------------------------------
// Deep functional coverage added 2026-10-09 (pre-PR). Cases marked
// "STOCK MAIN FAILS BY DESIGN" encode the pre-registered intentional
// differences (results/08, results/23): stock walks the PSN_ rows in
// on-disk order per mapping key — decimal-string pool, end-1 hex — and
// aborts the whole scan once that walk runs past the final row. The
// patch sorts numerically and carries a per-pool prefix-max end instead,
// so its stray set is exact w.r.t. the rows on disk and a superset of
// stock's. The delta is genuine strays stock silently misses.
// ---------------------------------------------------------------------------

TEST_F(ScrubberRunTest, NarrowRowSortsBeforeWideRow) {
  // PSN_ keys order rows by end-1: [10,20) sorts BEFORE [5,30) on disk.
  // STOCK MAIN FAILS BY DESIGN: the walk stops at the narrow row, so
  // stock misses snap 8 (covered only by the wide row). The broken
  // single-probe draft fails too: its probe lands on [10,20) for snap 25.
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_purged(1, {{snapid_t(5), snapid_t(30)}});
  // purged truth = [10,20) U [5,30) = [5,30)
  make_mappings({
    {snapid_t(4),  mk_hobj(1, "ok-before")},       // precedes both rows
    {snapid_t(8),  mk_hobj(1, "stray-wide-only")}, // only in [5,30)
    {snapid_t(12), mk_hobj(1, "stray-both")},      // in both rows
    {snapid_t(25), mk_hobj(1, "stray-wide-only2")},// only in [5,30)
    {snapid_t(30), mk_hobj(1, "ok-at-end")},       // at wide row end
  });
  expect_strays({
    stray_of(1, 8),
    stray_of(1, 12),
    stray_of(1, 25),
  }, scrub());
}

TEST_F(ScrubberRunTest, PurgedRowsContainmentChain) {
  // three strictly nested rows via three writer calls; truth = [2,10).
  // STOCK MAIN FAILS BY DESIGN: the [4,5) row sorts first, so stock calls
  // snap 2 ok and misses it. The broken single-probe draft finds only
  // snap 2: its probe lands on [4,5) for the snaps only [2,10) covers.
  make_purged(1, {{snapid_t(4), snapid_t(5)}});
  make_purged(1, {{snapid_t(3), snapid_t(7)}});
  make_purged(1, {{snapid_t(2), snapid_t(10)}});
  make_mappings({
    {snapid_t(1),  mk_hobj(1, "ok-before")},
    {snapid_t(2),  mk_hobj(1, "stray-lowest")},
    {snapid_t(5),  mk_hobj(1, "stray-mid")},
    {snapid_t(9),  mk_hobj(1, "stray-high")},
    {snapid_t(10), mk_hobj(1, "ok-at-end")},
  });
  expect_strays({
    stray_of(1, 2),
    stray_of(1, 5),
    stray_of(1, 9),
  }, scrub());
}

TEST_F(ScrubberRunTest, AdjacentRowsSharedBoundary) {
  // [10,20) and [20,30) side by side — unreachable through the writer,
  // which merges adjacent intervals into one row; raw rows pin the
  // half-open semantics at a shared boundary: snap 20 == second row's
  // begin is a STRAY, snap 30 == its end is not.
  make_raw_purged_row(1, snapid_t(10), snapid_t(20));
  make_raw_purged_row(1, snapid_t(20), snapid_t(30));
  make_mappings({
    {snapid_t(9),  mk_hobj(1, "ok-before")},
    {snapid_t(19), mk_hobj(1, "stray-first")},   // end-1 of row 1
    {snapid_t(20), mk_hobj(1, "stray-second")},  // == begin of row 2
    {snapid_t(29), mk_hobj(1, "stray-second2")},
    {snapid_t(30), mk_hobj(1, "ok-at-end")},
  });
  expect_strays({
    stray_of(1, 19),
    stray_of(1, 20),
    stray_of(1, 29),
  }, scrub());
}

TEST_F(ScrubberRunTest, MaxendResetsAtPoolBoundary) {
  // the false-positive trap: pool 1's prefix-max end (1000) must not
  // leak into pool 3's block — snap 500 in pool 3 is NOT a stray. Pool 4
  // has a mapping but no rows at all.
  // STOCK MAIN FAILS BY DESIGN: for pool-3 mappings the walk stops at
  // pool 20's row (decimal-string order) and stock misses (3,35).
  make_purged(1,  {{snapid_t(5),    snapid_t(1000)}});
  make_purged(3,  {{snapid_t(30),   snapid_t(40)}});
  make_purged(20, {{snapid_t(2000), snapid_t(2010)}});
  make_mappings({
    {snapid_t(500),  mk_hobj(1,  "stray-p1")},     // in [5,1000)
    {snapid_t(500),  mk_hobj(3,  "ok-not-stray")}, // trap: NOT a stray
    {snapid_t(35),   mk_hobj(3,  "stray-p3")},
    {snapid_t(40),   mk_hobj(3,  "ok-at-end")},
    {snapid_t(2005), mk_hobj(20, "stray-p20")},
    {snapid_t(100),  mk_hobj(4,  "ok-no-rows")},    // pool without rows
  });
  expect_strays({
    stray_of(1, 500),
    stray_of(3, 35),
    stray_of(20, 2005),
  }, scrub());
}

TEST_F(ScrubberRunTest, DuplicateMappingsAreNotDeduped) {
  // run() does not dedup: two distinct mapping keys with the same
  // (pool, snap, hash, shard) yield two identical stray tuples. The
  // consumer dedups one level up (OSD::scrub_purged_snaps, by (pg, snap)).
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_mapping(snapid_t(15), mk_hobj(1, "a"));
  make_mapping(snapid_t(15), mk_hobj(1, "b"));
  auto got = scrub();
  ASSERT_EQ(2u, got.size());
  expect_strays({stray_of(1, 15), stray_of(1, 15)}, std::move(got));
}

TEST_F(ScrubberRunTest, ModerateMAllStrays) {
  // every mapping is a stray; a few dozen mappings keeps it a unit test
  // (the bench binary covers the large-M shape)
  make_purged(1, {{snapid_t(10), snapid_t(1010)}});
  std::vector<std::pair<snapid_t, hobject_t>> entries;
  std::vector<stray_tuple_t> want;
  for (uint64_t s = 10; s < 60; ++s) {
    entries.emplace_back(snapid_t(s), mk_hobj(1, fmt::format("obj{}", s).c_str()));
    want.emplace_back(stray_of(1, s));
  }
  make_mappings(std::move(entries));
  expect_strays(want, scrub());
}

TEST_F(ScrubberRunTest, StrayOrderFollowsMappingKeyDiskOrder) {
  // strays are emitted in mapping-key disk order: the pool is a DECIMAL
  // STRING in the key, so SNA_20_* sorts before SNA_3_*. No sorting of
  // the result here — the exact vector order is the assertion.
  // STOCK MAIN FAILS BY DESIGN: stock misses both pool-3 strays.
  make_purged(1,  {{snapid_t(10),   snapid_t(20)}});
  make_purged(3,  {{snapid_t(30),   snapid_t(40)}});
  make_purged(20, {{snapid_t(2000), snapid_t(2010)}});
  // written in scrambled order on purpose — disk order decides
  make_mappings({
    {snapid_t(35),   mk_hobj(3,  "e")},
    {snapid_t(2007), mk_hobj(20, "d")},
    {snapid_t(12),   mk_hobj(1,  "a")},
    {snapid_t(31),   mk_hobj(3,  "f")},
    {snapid_t(2005), mk_hobj(20, "c")},
    {snapid_t(15),   mk_hobj(1,  "b")},
  });
  std::vector<stray_tuple_t> want = {
    stray_of(1, 12), stray_of(1, 15),
    stray_of(20, 2005), stray_of(20, 2007),
    stray_of(3, 31), stray_of(3, 35),
  };
  auto got = scrub();  // NO sort: order is the contract
  ASSERT_EQ(want.size(), got.size());
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(std::get<0>(want[i]), std::get<0>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<1>(want[i]), std::get<1>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<2>(want[i]), std::get<2>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<3>(want[i]), std::get<3>(got[i])) << "i=" << i;
  }
}

TEST_F(ScrubberRunTest, ShardedMappingAttributesRealShard) {
  // main's _parse_m sscanf ("SNA_%lld_%llx_.%lx", r != 3 -> NO_SHARD)
  // attributes real shards; every other test here is NO_SHARD. The
  // sharded key sorts BEFORE the unsharded one ('.' < '_'), which pins
  // the emission order too. The hash round-trips through the mapping
  // VALUE (the scrub reads pool/hash back from the decoded hobject).
  make_purged(5, {{snapid_t(10), snapid_t(20)}});
  make_mapping(snapid_t(15), mk_hobj(5, "sharded", 0x17), shard_id_t(1));
  make_mapping(snapid_t(15), mk_hobj(5, "plain", 0x17));
  std::vector<stray_tuple_t> want = {
    stray_of(5, 15, 0x17, shard_id_t(1)),
    stray_of(5, 15, 0x17),
  };
  auto got = scrub();  // NO sort: sharded key first on disk
  ASSERT_EQ(want.size(), got.size());
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(std::get<0>(want[i]), std::get<0>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<1>(want[i]), std::get<1>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<2>(want[i]), std::get<2>(got[i])) << "i=" << i;
    ASSERT_EQ(std::get<3>(want[i]), std::get<3>(got[i])) << "i=" << i;
  }
}

TEST_F(ScrubberRunTest, MissingPurgedAndMappingObjectsTolerated) {
  // -ENOENT paths: with the mapping object gone the walk sees zero
  // mappings (0 strays); with the purged object gone phase 1 bails out
  // early (0 strays). Neither may crash nor report phantom strays.
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_mapping(snapid_t(15), mk_hobj(1, "a"));
  ASSERT_EQ(1u, scrub().size());

  remove_meta_object(mapping_hoid);
  ASSERT_EQ(0u, scrub().size());

  remove_meta_object(purged_snaps_hoid);
  ASSERT_EQ(0u, scrub().size());
}

TEST_F(ScrubberRunTest, ExtremeSnapAndPoolIds) {
  // snaps up to CEPH_MAXSNAP are legal in mapping keys (get_prefix
  // asserts only NOSNAP/SNAPDIR); a 10-digit pool id round-trips through
  // the %lld key format. Stock passes this one: "1" and "9999999999"
  // keep the same relative order as decimal strings and numbers.
  constexpr uint64_t maxsnap = 0xfffffffffffffffdull;  // CEPH_MAXSNAP
  make_purged(1, {{snapid_t(maxsnap - 12), snapid_t(maxsnap)}});
  make_purged(9999999999, {{snapid_t(1), snapid_t(3)}});
  make_mappings({
    {snapid_t(maxsnap - 13), mk_hobj(1, "ok-before")},
    {snapid_t(maxsnap - 12), mk_hobj(1, "stray-at-begin")},  // == begin
    {snapid_t(maxsnap - 1),  mk_hobj(1, "stray-at-end1")},    // end-1
    {snapid_t(maxsnap),      mk_hobj(1, "ok-at-end")},        // == end
    {snapid_t(0),            mk_hobj(9999999999, "ok-snap0")},  // legal
    {snapid_t(1),            mk_hobj(9999999999, "stray-at-begin2")},
    {snapid_t(2),            mk_hobj(9999999999, "stray-at-end2")},
    {snapid_t(3),            mk_hobj(9999999999, "ok-at-end2")},
  });
  expect_strays({
    stray_of(1, maxsnap - 12),
    stray_of(1, maxsnap - 1),
    stray_of(9999999999, 1),
    stray_of(9999999999, 2),
  }, scrub());
}

TEST_F(ScrubberRunTest, ForeignKeyUnderPurgedSnapsObject) {
  // a key that is not PSN_ sorts after every PSN_ row, so run()'s
  // collector keeps the COMPLETE row set when it stops there and the
  // mapping walk still sees every stray.
  // STOCK MAIN FAILS BY DESIGN: stock walks the rows per mapping; the
  // walk for (3,35) stops at pool 20's row (decimal-string order), and
  // the walk for (21,100) runs off the end into the garbage key and
  // aborts the whole scan — stock reports only (1,15).
  make_purged(1,  {{snapid_t(10),   snapid_t(20)}});
  make_purged(3,  {{snapid_t(30),   snapid_t(40)}});
  make_purged(20, {{snapid_t(2000), snapid_t(2010)}});
  make_mappings({
    {snapid_t(15),  mk_hobj(1,  "stray-p1")},
    {snapid_t(35),  mk_hobj(3,  "stray-p3")},
    {snapid_t(100), mk_hobj(21, "ok-no-rows")},  // pool without rows
  });
  // "QSN_" > "PSN_" lexicographically: sorts after every real row
  ceph::buffer::list garbage;
  ceph::encode(std::string("garbage"), garbage);
  std::map<std::string, ceph::buffer::list> m;
  m["QSN_garbage"] = garbage;
  raw_omap_set(purged_snaps_hoid, std::move(m));
  expect_strays({stray_of(1, 15), stray_of(3, 35)}, scrub());
}
