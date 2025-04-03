// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <string>

#include <boost/statechart/event_base.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/hobject.h"
#include "crimson/osd/backfill_state.h"
#include "osd/recovery_types.h"


// The sole purpose is to convert from the string representation.
// An alternative approach could use boost::range in FakeStore's
// constructor.
struct improved_hobject_t : hobject_t {
  improved_hobject_t(const char parsable_name[]) {
    this->parse(parsable_name);
  }
  improved_hobject_t(const hobject_t& obj)
    : hobject_t(obj) {
  }
  bool operator==(const improved_hobject_t& rhs) const {
    return static_cast<const hobject_t&>(*this) == \
           static_cast<const hobject_t&>(rhs);
  }
};


struct FakeStore {
  using objs_t = std::map<improved_hobject_t, eversion_t>;

  objs_t objs;

  void push(const hobject_t& obj, eversion_t version) {
    objs[obj] = version;
  }

  void drop(const hobject_t& obj, const eversion_t version) {
    auto it = objs.find(obj);
    ceph_assert(it != std::end(objs));
    ceph_assert(it->second == version);
    objs.erase(it);
  }

  template <class Func>
  hobject_t list(const hobject_t& start, Func&& per_entry) const {
    auto it = objs.lower_bound(start);
    for (auto max = std::numeric_limits<std::uint64_t>::max();
         it != std::end(objs) && max > 0;
         ++it, --max) {
      per_entry(*it);
    }
    return it != std::end(objs) ? static_cast<const hobject_t&>(it->first)
                                : hobject_t::get_max();
  }

  // Permit rhs (reference) objects to be the same version or 1 version older
  bool looks_like(const FakeStore& rhs) const {
    if (std::size(objs) != std::size(rhs.objs)) {
      return false;
    }
    for (auto &[obj, version] : objs) {
      if (!rhs.objs.contains(obj)) {
	return false;
      }
      auto version_r = rhs.objs.at(obj);
      if ((version.epoch != version_r.epoch) ||
	  ((version.version != version_r.version) &&
	   (version.version != version_r.version + 1)))
      {
	return false;
      }
    }
    return true;
  }

  bool operator==(const FakeStore& rhs) const {
    return std::size(objs) == std::size(rhs.objs) && \
           std::equal(std::begin(objs), std::end(objs), std::begin(rhs.objs));
  }

  bool operator!=(const FakeStore& rhs) const {
    return !(*this == rhs);
  }
};


struct FakeReplica {
  FakeStore store;
  hobject_t last_backfill;

  FakeReplica(FakeStore&& store)
    : store(std::move(store)) {
  }
};

struct FakePrimary {
  FakeStore store;
  eversion_t last_update;
  eversion_t projected_last_update;
  eversion_t log_tail;
  PGLog pg_log;
  pg_pool_t pool;
  PGLog::IndexedLog projected_log;

  FakePrimary(FakeStore&& store)
    : store(std::move(store)), pg_log(nullptr) {
  }
};

class BackfillFixture : public crimson::osd::BackfillState::BackfillListener {
  friend class BackfillFixtureBuilder;

  FakePrimary backfill_source;
  std::map<pg_shard_t, FakeReplica> backfill_targets;
  std::map<pg_shard_t,
           std::vector<std::pair<hobject_t, eversion_t>>> enqueued_drops;
  std::deque<
    boost::intrusive_ptr<
      const boost::statechart::event_base>> events_to_dispatch;
  crimson::osd::BackfillState backfill_state;

  BackfillFixture(FakePrimary&& backfill_source,
                  std::map<pg_shard_t, FakeReplica>&& backfill_targets);

  template <class EventT>
  void schedule_event(const EventT& event) {
    events_to_dispatch.emplace_back(event.intrusive_from_this());
  }

  template <class EventT>
  void schedule_event_immediate(const EventT& event) {
    events_to_dispatch.emplace_front(event.intrusive_from_this());
  }

  // BackfillListener {
  void request_replica_scan(
    const pg_shard_t& target,
    const hobject_t& begin,
    const hobject_t& end) override;

  void request_primary_scan(
    const hobject_t& begin) override;

  void enqueue_push(
    const hobject_t& obj,
    const eversion_t& v,
    const std::vector<pg_shard_t> &peers) override;

  void enqueue_drop(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) override;

  void maybe_flush() override;

  void update_peers_last_backfill(
    const hobject_t& new_last_backfill) override;

  bool budget_available() const override;

public:
  MOCK_METHOD(void, backfilled, (), (override));
  // }

  void next_round(std::size_t how_many=1) {
    ceph_assert(events_to_dispatch.size() >= how_many);
    while (how_many-- > 0) {
      backfill_state.process_event(std::move(events_to_dispatch.front()));
      events_to_dispatch.pop_front();
    }
  }

  template <class EventT>
  void next_round2() {
    ceph_assert(events_to_dispatch.size());
    // workaround for Clang's `-Wpotentially-evaluated-expression`. See:
    //   * https://gitlab.cern.ch/gaudi/Gaudi/-/merge_requests/970
    //   * https://stackoverflow.com/q/46494928
    const auto& front_event = *events_to_dispatch.front();
    ceph_assert(typeid(front_event) == typeid(EventT));
    backfill_state.process_event(std::move(events_to_dispatch.front()));
    events_to_dispatch.pop_front();
  }

  void next_till_done() {
    while (!events_to_dispatch.empty()) {
      next_round();
    }
  }

  bool all_stores_look_like(const FakeStore& reference) const {
    const bool all_replica_match = std::all_of(
      std::begin(backfill_targets), std::end(backfill_targets),
      [&reference] (const auto kv) {
        return kv.second.store.looks_like(reference);
      });
    return backfill_source.store == reference && all_replica_match;
  }

  struct PeeringFacade;
  struct PGFacade;

  void cancel() {
    schedule_event_immediate(crimson::osd::BackfillState::SuspendBackfill{});
  }

  void resume() {
    schedule_event_immediate(crimson::osd::BackfillState::Triggered{});
  }
};

struct BackfillFixture::PeeringFacade
  : public crimson::osd::BackfillState::PeeringFacade {
  FakePrimary& backfill_source;
  std::map<pg_shard_t, FakeReplica>& backfill_targets;
  // sorry, this is duplicative but that's the interface
  std::set<pg_shard_t> backfill_targets_as_set;

  PeeringFacade(FakePrimary& backfill_source,
                std::map<pg_shard_t, FakeReplica>& backfill_targets)
    : backfill_source(backfill_source),
      backfill_targets(backfill_targets) {
    std::transform(
      std::begin(backfill_targets), std::end(backfill_targets),
      std::inserter(backfill_targets_as_set, std::end(backfill_targets_as_set)),
      [](auto pair) {
        return pair.first;
      });
  }

  hobject_t earliest_backfill() const override {
    hobject_t e = hobject_t::get_max();
    for (const auto& kv : backfill_targets) {
      e = std::min(kv.second.last_backfill, e);
    }
    return e;
  }
  const std::set<pg_shard_t>& get_backfill_targets() const override {
    return backfill_targets_as_set;
  }
  const hobject_t& get_peer_last_backfill(pg_shard_t peer) const override {
    return backfill_targets.at(peer).last_backfill;
  }
  eversion_t get_pg_committed_to() const override {
    return backfill_source.last_update;
  }
  const eversion_t& get_log_tail() const override {
    return backfill_source.log_tail;
  }

  const PGLog& get_pg_log() const override {
    return backfill_source.pg_log;
  }

  const pg_pool_t& get_pool() const override {
    return backfill_source.pool;
  }

  void scan_log_after(eversion_t, scan_log_func_t) const override {
    /* NOP */
  }

  bool is_backfill_target(pg_shard_t peer) const override {
    return backfill_targets.count(peer) == 1;
  }
  void update_complete_backfill_object_stats(const hobject_t &hoid,
                                             const pg_stat_t &stats) override {
  }
  void prepare_backfill_for_missing(
    const hobject_t &soid,
    const eversion_t &v,
    const std::vector<pg_shard_t> &peers) override {}
  bool is_backfilling() const override {
    return true;
  }
};

struct BackfillFixture::PGFacade : public crimson::osd::BackfillState::PGFacade {
  FakePrimary& backfill_source;

  PGFacade(FakePrimary& backfill_source)
    : backfill_source(backfill_source) {
  }

  const eversion_t& get_projected_last_update() const override {
    return backfill_source.projected_last_update;
  }

  const PGLog::IndexedLog& get_projected_log() const override {
    return backfill_source.projected_log;
  }

  std::ostream &print(std::ostream &out) const override {
    return out << "FakePGFacade";
  }
};

BackfillFixture::BackfillFixture(
  FakePrimary&& backfill_source,
  std::map<pg_shard_t, FakeReplica>&& backfill_targets)
  : backfill_source(std::move(backfill_source)),
    backfill_targets(std::move(backfill_targets)),
    backfill_state(*this,
                   std::make_unique<PeeringFacade>(this->backfill_source,
                                                   this->backfill_targets),
                   std::make_unique<PGFacade>(this->backfill_source))
{
  seastar::global_logger_registry().set_all_loggers_level(
    seastar::log_level::debug
  );
  backfill_state.process_event(crimson::osd::BackfillState::Triggered{}.intrusive_from_this());
}

void BackfillFixture::request_replica_scan(
  const pg_shard_t& target,
  const hobject_t& begin,
  const hobject_t& end)
{
  ReplicaBackfillInterval bi;
  bi.end = backfill_targets.at(target).store.list(begin, [&bi](auto kv) {
    bi.objects.insert(std::move(kv));
  });
  bi.begin = begin;
  bi.version = backfill_source.last_update;

  schedule_event(crimson::osd::BackfillState::ReplicaScanned{ target, std::move(bi) });
}

void BackfillFixture::request_primary_scan(
  const hobject_t& begin)
{
  PrimaryBackfillInterval bi;
  bi.end = backfill_source.store.list(begin, [&bi](auto kv) {
    auto && [hoid,version] = kv;
    eversion_t version_zero;
    eversion_t version_next = eversion_t(version.epoch, version.version + 1);
    switch (std::rand() % 4) {
    case 0:
      // All shards at same version (Replica, EC, optimized EC after full-stripe write)
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t::NO_SHARD, version)));
      break;
    case 1:
      // Optimized EC partial write - Shard 3 at an earlier version
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t(3), version_zero)));
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t::NO_SHARD, version)));
      break;
    case 2:
      // Optimized EC partial write - Shard 1 and 2 at an earlier version
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t::NO_SHARD, version_next)));
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t(1), version)));
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t(2), version)));
      break;
    case 3:
      // Optimized EC partial write - Shard 1, 2 and 3 at different earlier versions
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t::NO_SHARD, version_next)));
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t(1), version)));
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t(2), version)));
      bi.objects.insert(std::make_pair(hoid, std::make_pair(shard_id_t(3), version_zero)));
      break;
    }
  });
  bi.begin = begin;
  bi.version = backfill_source.last_update;

  schedule_event(crimson::osd::BackfillState::PrimaryScanned{ std::move(bi) });
}

void BackfillFixture::enqueue_push(
  const hobject_t& obj,
  const eversion_t& v,
  const std::vector<pg_shard_t> &)
{
  for (auto& [ _, bt ] : backfill_targets) {
    bt.store.push(obj, v);
  }
  schedule_event(crimson::osd::BackfillState::ObjectPushed{ obj });
}

void BackfillFixture::enqueue_drop(
  const pg_shard_t& target,
  const hobject_t& obj,
  const eversion_t& v)
{
  enqueued_drops[target].emplace_back(obj, v);
}

void BackfillFixture::maybe_flush()
{
  for (const auto& [target, versioned_objs] : enqueued_drops) {
    for (const auto& [obj, v] : versioned_objs) {
      backfill_targets.at(target).store.drop(obj, v);
    }
  }
  enqueued_drops.clear();
}

void BackfillFixture::update_peers_last_backfill(
  const hobject_t& new_last_backfill)
{
  if (new_last_backfill.is_max()) {
    schedule_event(crimson::osd::BackfillState::RequestDone{});
  }
}

bool BackfillFixture::budget_available() const
{
  return true;
}

struct BackfillFixtureBuilder {
  FakeStore backfill_source;
  std::map<pg_shard_t, FakeReplica> backfill_targets;

  static BackfillFixtureBuilder add_source(FakeStore::objs_t objs) {
    BackfillFixtureBuilder bfb;
    bfb.backfill_source = FakeStore{ std::move(objs) };
    return bfb;
  }

  BackfillFixtureBuilder&& add_target(FakeStore::objs_t objs) && {
    const auto new_osd_num = std::size(backfill_targets);
    const auto new_shard_id = shard_id_t(1 + new_osd_num);
    const auto [ _, inserted ] = backfill_targets.emplace(
      pg_shard_t(new_osd_num, new_shard_id),
      FakeReplica{ FakeStore{std::move(objs)} });
    ceph_assert(inserted);
    return std::move(*this);
  }

  BackfillFixture get_result() && {
    return BackfillFixture{ std::move(backfill_source),
                            std::move(backfill_targets) };
  }
};

// The straightest case: single primary, single replica. All have the same
// content in their object stores, so the entire backfill boils into just
// `request_primary_scan()` and `request_replica_scan()`.
TEST(backfill, same_primary_same_replica)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    reference_store.objs
  ).get_result();

  cluster_fixture.next_round();
  cluster_fixture.next_round();
  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round();
  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, one_empty_replica)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing */ }
  ).get_result();

  cluster_fixture.next_round();
  cluster_fixture.next_round();
  cluster_fixture.next_round(2);
  cluster_fixture.next_round();
  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round();
  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, one_same_one_empty_replica)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    reference_store.objs
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, two_empty_replicas)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, one_behind_one_empty_replica)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {8, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 250} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 247} },
    //"1:0256710c:::rbd_data.1018ac3e755.00000000000000b1:head", deleted
  }};
  const auto behind_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {8, 234} },
    //"1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head"  missing
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {8, 165} },
    { "1:0256710c:::rbd_data.1018ac3e755.00000000000000b1:head", {8, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    behind_store.objs
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, cancel_resume_middle_of_primaryscan)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.cancel();
  cluster_fixture.next_round2<crimson::osd::BackfillState::SuspendBackfill>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::PrimaryScanned>();
  cluster_fixture.resume();
  cluster_fixture.next_round2<crimson::osd::BackfillState::Triggered>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, cancel_resume_middle_of_replicascan1)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round2<crimson::osd::BackfillState::PrimaryScanned>();
  cluster_fixture.cancel();
  cluster_fixture.next_round2<crimson::osd::BackfillState::SuspendBackfill>();
  cluster_fixture.resume();
  cluster_fixture.next_round2<crimson::osd::BackfillState::Triggered>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, cancel_resume_middle_of_replicascan2)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round2<crimson::osd::BackfillState::PrimaryScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.cancel();
  cluster_fixture.next_round2<crimson::osd::BackfillState::SuspendBackfill>();
  cluster_fixture.resume();
  cluster_fixture.next_round2<crimson::osd::BackfillState::Triggered>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, cancel_resume_middle_of_push1)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round2<crimson::osd::BackfillState::PrimaryScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.cancel();
  cluster_fixture.next_round2<crimson::osd::BackfillState::SuspendBackfill>();
  cluster_fixture.resume();
  cluster_fixture.next_round2<crimson::osd::BackfillState::Triggered>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, cancel_resume_middle_of_push2)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round2<crimson::osd::BackfillState::PrimaryScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.cancel();
  cluster_fixture.next_round2<crimson::osd::BackfillState::SuspendBackfill>();
  cluster_fixture.resume();
  cluster_fixture.next_round2<crimson::osd::BackfillState::Triggered>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, cancel_resume_middle_of_push3)
{
  const auto reference_store = FakeStore{ {
    { "1:00058bcc:::rbd_data.1018ac3e755.00000000000000d5:head", {10, 234} },
    { "1:00ed7f8e:::rbd_data.1018ac3e755.00000000000000af:head", {10, 196} },
    { "1:01483aea:::rbd_data.1018ac3e755.0000000000000095:head", {10, 169} },
  }};
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    { /* nothing 1 */ }
  ).add_target(
    { /* nothing 2 */ }
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round2<crimson::osd::BackfillState::PrimaryScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ReplicaScanned>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.cancel();
  cluster_fixture.next_round2<crimson::osd::BackfillState::SuspendBackfill>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::ObjectPushed>();
  cluster_fixture.resume();
  cluster_fixture.next_round2<crimson::osd::BackfillState::Triggered>();
  cluster_fixture.next_round2<crimson::osd::BackfillState::RequestDone>();
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

namespace StoreRandomizer {
  // FIXME: copied & pasted from test/test_snap_mapper.cc. We need to
  // find a way to avoid code duplication in test. A static library?
  std::string random_string(std::size_t size) {
    std::string name;
    for (size_t j = 0; j < size; ++j) {
      name.push_back('a' + (std::rand() % 26));
    }
    return name;
  }

  hobject_t random_hobject() {
    uint32_t mask{0};
    uint32_t bits{0};
    return hobject_t(
      random_string(1+(std::rand() % 16)),
      random_string(1+(std::rand() % 16)),
      snapid_t(std::rand() % 1000),
      (std::rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(std::rand() % 16));
  }

  eversion_t random_eversion() {
    return eversion_t{ std::rand() % 512U, std::rand() % 256UL };
  }

  FakeStore create() {
    FakeStore store;
    for (std::size_t i = std::rand() % 2048; i > 0; --i) {
      store.push(random_hobject(), random_eversion());
    }
    return store;
  }

  template <class... Args>
  void execute_random(Args&&... args) {
    std::array<std::function<void()>, sizeof...(Args)> funcs = {
      std::forward<Args>(args)...
    };
    return std::move(funcs[std::rand() % std::size(funcs)])();
  }

  FakeStore mutate(const FakeStore& source_store) {
    FakeStore mutated_store;
    source_store.list(hobject_t{}, [&] (const auto& kv) {
      const auto &oid = kv.first;
      const auto &version = kv.second;
      execute_random(
        []  { /* just drop the entry */ },
        [&] { mutated_store.push(oid, version); },
        [&] { mutated_store.push(oid, random_eversion()); },
        [&] { mutated_store.push(random_hobject(), version); },
        [&] {
          for (auto how_many = std::rand() % 8; how_many > 0; --how_many) {
            mutated_store.push(random_hobject(), random_eversion());
          }
        }
      );
    });
    return mutated_store;
  }
}

// The name might suggest randomness is involved here. Well, that's true
// but till we know the seed the test still is repeatable.
TEST(backfill, one_pseudorandomized_replica)
{
  const auto reference_store = StoreRandomizer::create();
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    StoreRandomizer::mutate(reference_store).objs
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, two_pseudorandomized_replicas)
{
  const auto reference_store = StoreRandomizer::create();
  auto cluster_fixture = BackfillFixtureBuilder::add_source(
    reference_store.objs
  ).add_target(
    StoreRandomizer::mutate(reference_store).objs
  ).add_target(
    StoreRandomizer::mutate(reference_store).objs
  ).get_result();

  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_till_done();

  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}
