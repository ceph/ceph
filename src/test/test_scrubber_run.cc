// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

// Unit tests for SnapMapper::Scrubber::run(): the purged_snaps audit.
// The fixture boots a MemStore the way OSD::mkfs does (meta collection,
// snapmapper and purged_snaps objects) and writes every purged row and
// mapping through the production writers (record_purged_snaps(),
// add_oid()); one raw foreign key under the purged_snaps object pins
// the collector against keys it must not read.

#include <algorithm>
#include <map>
#include <tuple>
#include <vector>

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

using stray_tuple_t =
  std::tuple<int64_t, snapid_t, uint32_t, shard_id_t>;

// one purged row as it sits on disk: the decoded (pool, begin, end)
struct purged_row_t {
  int64_t pool;
  snapid_t begin;
  snapid_t end;
};

static hobject_t mk_hobj(int64_t pool, const char *name, uint32_t hash = 0)
{
  // head hobject: snap CEPH_NOSNAP. The scrub reads the pool and the
  // hash back from the mapping VALUE (_parse_m decodes the full
  // hobject), so both round-trip deterministically.
  return hobject_t(object_t(name), "", CEPH_NOSNAP, hash, pool, "");
}

static stray_tuple_t stray_of(int64_t pool, uint64_t snap, uint32_t hash = 0,
                              shard_id_t shard = shard_id_t::NO_SHARD)
{
  return stray_tuple_t(pool, snapid_t(snap), hash, shard);
}

// ---------------------------------------------------------------------------
// Fixture: one MemStore, the meta collection, the OSD's own oid handles.
// ---------------------------------------------------------------------------
class ScrubberRunTest : public ::testing::Test {
protected:
  std::unique_ptr<ObjectStore> store;
  ObjectStore::CollectionHandle ch;
  ghobject_t mapping_hoid;
  ghobject_t purged_snaps_hoid;
  // The OSDriver and every SnapMapper live for the fixture's whole
  // lifetime: MapCacher::set_keys() registers on-applied callbacks
  // holding SharedPtrRegistry entries, and MemStore completes them
  // asynchronously, so a mapper destroyed right after queue_transaction()
  // can die before its callbacks run. TearDown() umount()s (draining the
  // finisher queue) before destroying them.
  std::unique_ptr<OSDriver> purged_drv;
  std::unique_ptr<OSDriver> mapping_drv;
  // key is (pool, shard.id): mappers of different shards namespace their
  // SNA_ keys apart via the shard prefix, so they coexist on the one
  // mapping_drv without colliding
  std::map<std::pair<int64_t, int>, std::unique_ptr<SnapMapper>> mappers;
  bool mounted = false;
  CephContext *cct = g_ceph_context;

  void SetUp() override {
    store = ObjectStore::create(cct, "memstore", "");
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

    // both meta objects must exist before omap writes: MemStore's
    // _omap_setkeys returns -ENOENT for a missing object
    ceph::os::Transaction t;
    t.touch(coll_t::meta(), mapping_hoid);
    t.touch(coll_t::meta(), purged_snaps_hoid);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));

    purged_drv = std::make_unique<OSDriver>(store.get(), ch,
                                            purged_snaps_hoid);
    mapping_drv = std::make_unique<OSDriver>(store.get(), ch,
                                             mapping_hoid);
  }

  void TearDown() override {
    if (store && mounted) {
      // umount drains the finisher queue before the MapCacher
      // registries below are destroyed
      store->umount();
      mounted = false;
    }
    mappers.clear();
    mapping_drv.reset();
    purged_drv.reset();
    store.reset();
  }

  SnapMapper &get_mapper(int64_t pool,
                         shard_id_t shard = shard_id_t::NO_SHARD) {
    auto key = std::pair(pool, static_cast<int>(shard));
    auto &m = mappers[key];
    if (!m) {
      // per-(pool, shard) mapper, like production, where each PG owns
      // its shard's mapper
      m = std::make_unique<SnapMapper>(
        cct, mapping_drv.get(), 0, 0, pool, shard);
    }
    return *m;
  }

  // one record_purged_snaps() call; every element of the outer vector
  // is one epoch's interval set (one call often carries several
  // epochs), so rows build through the production writer
  void make_purged_epochs(
    int64_t pool,
    std::vector<std::vector<std::pair<snapid_t, snapid_t>>> epochs) {
    std::map<epoch_t, mempool::osdmap::map<int64_t, snap_interval_set_t>> m;
    for (auto &intervals : epochs) {
      ++next_epoch;
      auto &set = m[next_epoch][pool];
      for (auto &iv : intervals) {
        set.insert(iv.first, iv.second - iv.first);
      }
    }
    ceph::os::Transaction t;
    SnapMapper::record_purged_snaps(
      cct, *purged_drv, purged_drv->get_transaction(&t), std::move(m));
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
  }

  // one epoch in the call: the common shape
  void make_purged(
    int64_t pool, std::vector<std::pair<snapid_t, snapid_t>> intervals) {
    make_purged_epochs(pool, {std::move(intervals)});
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

  // element-wise comparison after sorting both sides; the emission
  // order is checked by StrayOrderFollowsMappingKeyDiskOrder
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

  // raw omap write under a meta object — for shapes the production
  // writers cannot produce
  void raw_omap_set(const ghobject_t &hoid,
                    std::map<std::string, ceph::buffer::list> kv) {
    ceph::os::Transaction t;
    t.omap_setkeys(coll_t::meta(), hoid, kv);
    ASSERT_EQ(0, store->queue_transaction(ch, std::move(t)));
  }

  // the purged row key for the row ending at end: make_purged_snap_key()
  // writes the pool as a decimal string and end-1 as zero-padded hex
  std::string purged_snap_key(int64_t pool, snapid_t last) {
    return SnapMapper::make_purged_snap_key(pool, last);
  }

  // every purged row on disk right now: key -> (pool, begin, end)
  std::map<std::string, purged_row_t> get_purged_rows() {
    std::map<std::string, purged_row_t> rows;
    int r = store->omap_iterate(
      ch, purged_snaps_hoid,
      ObjectStore::omap_iter_seek_t::min_lower_bound(),
      [&rows] (std::string_view key, std::string_view value) {
        if (key.find("PSN_") != 0) {
          return ObjectStore::omap_iter_ret_t::NEXT;
        }
        ceph::buffer::list v;
        v.append(value);
        auto p = v.cbegin();
        purged_row_t row;
        ceph::decode(row.pool, p);
        ceph::decode(row.begin, p);
        ceph::decode(row.end, p);
        rows[std::string(key)] = row;
        return ObjectStore::omap_iter_ret_t::NEXT;
      });
    EXPECT_EQ(0, r);
    return rows;
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

TEST_F(ScrubberRunTest, StaggeredOverlapFromOneWriterCall) {
  // An existing [10,20) row, then ONE record_purged_snaps() call with
  // two epochs: epoch A purges [8,10), epoch B purges [20,25). Each
  // interval probes the rows ON DISK at its own edges only: [8,10)
  // joins forward onto [10,20), and [20,25) joins back onto the SAME
  // on-disk row, because epoch A's writes are not applied yet. The
  // removals land before the sets, so two staggered, overlapping
  // rows survive:
  //   [8,20)  at the key of end-1 19 (the old row's key, reused)
  //   [10,25) at the key of end-1 24
  // The row assertions are a tripwire: if the writer ever stops leaving
  // these rows behind, they fail.
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_purged_epochs(1, {{{snapid_t(8),  snapid_t(10)}},
                         {{snapid_t(20), snapid_t(25)}}});
  auto rows = get_purged_rows();
  ASSERT_EQ(2u, rows.size());
  auto k19 = purged_snap_key(1, snapid_t(19));
  auto k24 = purged_snap_key(1, snapid_t(24));
  auto it = rows.find(k19);
  ASSERT_NE(rows.end(), it) << "key " << k19;
  EXPECT_EQ(int64_t(1), it->second.pool);
  EXPECT_EQ(snapid_t(8),  it->second.begin);
  EXPECT_EQ(snapid_t(20), it->second.end);
  it = rows.find(k24);
  ASSERT_NE(rows.end(), it) << "key " << k24;
  EXPECT_EQ(int64_t(1), it->second.pool);
  EXPECT_EQ(snapid_t(10), it->second.begin);
  EXPECT_EQ(snapid_t(25), it->second.end);
  // together the rows cover [8,25): 8, 15 and 22 are strays, 7 and 25
  // are not
  make_mappings({
    {snapid_t(7),  mk_hobj(1, "ok-before")},
    {snapid_t(8),  mk_hobj(1, "stray-front")},
    {snapid_t(15), mk_hobj(1, "stray-both")},
    {snapid_t(22), mk_hobj(1, "stray-back")},
    {snapid_t(25), mk_hobj(1, "ok-at-end")},
  });
  expect_strays({
    stray_of(1, 8),
    stray_of(1, 15),
    stray_of(1, 22),
  }, scrub());
}

TEST_F(ScrubberRunTest, OverlappingPurgedRows) {
  // the writer probes only the rows at an interval's edges, so a wider
  // interval that strictly CONTAINS an existing row is invisible to the
  // probes and both rows land on disk. The scrub must still find every
  // snap the rows cover: [4,5) U [3,7) = [3,7). A later delta cannot
  // re-report already-purged snaps, so production never writes this
  // pair: it is the corrupted/unexpected-input guard. Overlap and
  // touch, by contrast, are production-real — the staggered and
  // adjacent-row tests drive those through the writer.
  make_purged(1, {{snapid_t(4), snapid_t(5)}});
  make_purged(1, {{snapid_t(3), snapid_t(7)}});
  make_mappings({
    {snapid_t(2), mk_hobj(1, "ok-before")},  // precedes both rows
    {snapid_t(4), mk_hobj(1, "stray-in-last")},   // in [4,5) and [3,7)
    {snapid_t(5), mk_hobj(1, "stray-wider")},     // only in [3,7)
    {snapid_t(6), mk_hobj(1, "stray-wider2")},    // only in [3,7)
    {snapid_t(7), mk_hobj(1, "ok-at-end")},  // at both row ends
    {snapid_t(8), mk_hobj(1, "ok-after")},
  });
  expect_strays({
    stray_of(1, 4),
    stray_of(1, 5),
    stray_of(1, 6),
  }, scrub());
}

TEST_F(ScrubberRunTest, MultiPoolMixedPoolIds) {
  // pools with different digit lengths: the PSN_ keys order pools as
  // decimal strings on disk (pool 20 before pool 3), so this only works
  // if the rows are sorted numerically before the lookup.
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
  expect_strays({
    stray_of(1, 15),
    stray_of(3, 35),
    stray_of(20, 2005),
  }, scrub());
}

TEST_F(ScrubberRunTest, NarrowRowSortsBeforeWideRow) {
  // rows order by end-1 on disk: [10,20) sorts BEFORE [5,30). The
  // lookup must not trust that order; after the numeric sort and the
  // merge the shape is just one [5,30) range, and the snaps only the
  // wide row covers (8, 25) are still strays. The nested pair is
  // corrupted/unexpected input — a wider delta would re-report
  // already-purged snaps — so the test guards the lookup.
  make_purged(1, {{snapid_t(10), snapid_t(20)}});
  make_purged(1, {{snapid_t(5), snapid_t(30)}});
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
  // three nested rows via three writer calls; together they cover
  // [2,10). The scrub must find the snaps only the outer row covers.
  // A later delta cannot re-report already-purged snaps, so
  // production never writes a nested chain: this is a corrupted/
  // unexpected-input guard.
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
  // [10,20) and [20,30) side by side, written by the writer itself:
  // the writer probes only committed rows, and both epochs ride in
  // one record_purged_snaps() call, so epoch B is blind to epoch A
  // and the rows land adjacent on disk. The test pins the half-open
  // semantics at the shared boundary — snap 20, the second row's
  // begin, is a stray; snap 30, its end, is not.
  make_purged_epochs(1, {{{snapid_t(10), snapid_t(20)}},
                         {{snapid_t(20), snapid_t(30)}}});
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

TEST_F(ScrubberRunTest, ManyDisjointPurgedRows) {
  // 100 disjoint rows in one pool, all from one record_purged_snaps()
  // call: every interval probes only rows already on disk, so each
  // lands as its own row. Drives the numeric sort over many rows, the
  // merge loop's same-pool else-branch, and the upper_bound picking
  // among many disjoint merged rows.
  std::vector<std::pair<snapid_t, snapid_t>> rows;
  for (uint64_t i = 0; i < 100; ++i) {
    rows.emplace_back(snapid_t(1000 + 100 * i),
                      snapid_t(1010 + 100 * i));
  }
  make_purged(1, std::move(rows));
  ASSERT_EQ(100u, get_purged_rows().size());
  make_mappings({
    {snapid_t(999),   mk_hobj(1, "ok-before-first")},
    {snapid_t(1000),  mk_hobj(1, "stray-first-begin")},
    {snapid_t(1009),  mk_hobj(1, "stray-first-end1")},
    {snapid_t(1010),  mk_hobj(1, "ok-gap")},
    {snapid_t(5505),  mk_hobj(1, "stray-mid")},
    {snapid_t(5610),  mk_hobj(1, "ok-mid-gap")},
    {snapid_t(10900), mk_hobj(1, "stray-last-begin")},
    {snapid_t(10909), mk_hobj(1, "stray-last-end1")},
    {snapid_t(10910), mk_hobj(1, "ok-at-last-end")},
  });
  expect_strays({
    stray_of(1, 1000),
    stray_of(1, 1009),
    stray_of(1, 5505),
    stray_of(1, 10900),
    stray_of(1, 10909),
  }, scrub());
}

TEST_F(ScrubberRunTest, MergedRangesAreIsolatedPerPool) {
  // per-pool isolation: pool 1's [5,1000) must not make pool 3's snap
  // 500 a stray, and pool 4 has a mapping but no rows. The pool ids mix
  // digit lengths (1, 3, 20) on purpose: they sort as decimal strings
  // on disk, and the sort before the merge is numeric.
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
  make_purged(1, {{snapid_t(10), snapid_t(1010)}});
  std::vector<std::pair<snapid_t, hobject_t>> entries;
  std::vector<stray_tuple_t> want;
  for (uint64_t s = 10; s < 60; ++s) {
    entries.emplace_back(
      snapid_t(s), mk_hobj(1, fmt::format("obj{}", s).c_str()));
    want.emplace_back(stray_of(1, s));
  }
  make_mappings(std::move(entries));
  expect_strays(want, scrub());
}

TEST_F(ScrubberRunTest, StrayOrderFollowsMappingKeyDiskOrder) {
  // strays are emitted in mapping-key disk order: the pool is a DECIMAL
  // STRING in the SNA_ key, so SNA_20_* sorts before SNA_3_*. The exact
  // vector order is the assertion here — no sorting of the result.
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
  // _parse_m's sscanf ("SNA_%lld_%llx_.%lx") attributes real shards;
  // every other test here is NO_SHARD. The sharded key sorts BEFORE the
  // unsharded one ('.' < '_'), which pins the emission order too. The
  // hash round-trips through the mapping VALUE (the scrub reads the
  // pool and hash back from the decoded hobject).
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
  // asserts only on NOSNAP/SNAPDIR); a 10-digit pool id round-trips
  // through the %lld key format.
  constexpr uint64_t maxsnap = CEPH_MAXSNAP;
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
  // a foreign key that sorts after every PSN_ row stops the collector
  // there; the rows it already saw must survive, so the mapping walk
  // still finds every stray.
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
