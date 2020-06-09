#include <algorithm>
#include <deque>
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
#include "test/crimson/test_backfill_facades.h"


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

  FakePrimary(FakeStore&& store)
    : store(std::move(store)) {
  }
};

class BackfillFixture : public crimson::osd::BackfillState::BackfillListener {
  friend class BackfillFixtureBuilder;

  FakePrimary backfill_source;
  std::map<pg_shard_t, FakeReplica> backfill_targets;

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

  // BackfillListener {
  void request_replica_scan(
    const pg_shard_t& target,
    const hobject_t& begin,
    const hobject_t& end) override;

  void request_primary_scan(
    const hobject_t& begin) override;

  void enqueue_push(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) override;

  void enqueue_drop(
    const pg_shard_t& target,
    const hobject_t& obj,
    const eversion_t& v) override;

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

  bool all_stores_look_like(const FakeStore& reference) const {
    const bool all_replica_match = std::all_of(
      std::begin(backfill_targets), std::end(backfill_targets),
      [&reference] (const auto kv) {
        return kv.second.store == reference;
      });
    return backfill_source.store == reference && all_replica_match;
  }

  struct PeeringFacade;
  struct PGFacade;
};

struct BackfillFixture::PeeringFacade
  : public crimson::osd::BackfillState::PeeringFacade {
  FakePrimary& backfill_source;
  std::map<pg_shard_t, FakeReplica>& backfill_targets;

  PeeringFacade(FakePrimary& backfill_source,
                std::map<pg_shard_t, FakeReplica>& backfill_targets)
    : backfill_source(backfill_source),
      backfill_targets(backfill_targets) {
  }

  hobject_t earliest_backfill() const override {
    hobject_t e = hobject_t::get_max();
    for (const auto& kv : backfill_targets) {
      e = std::min(kv.second.last_backfill, e);
    }
    return e;
  }
  std::set<pg_shard_t> get_backfill_targets() const override {
    std::set<pg_shard_t> result;
    std::transform(
      std::begin(backfill_targets), std::end(backfill_targets),
      std::inserter(result, std::end(result)),
      [](auto pair) {
        return pair.first;
      });
    return result;
  }
  const hobject_t& get_peer_last_backfill(pg_shard_t peer) const override {
    return backfill_targets.at(peer).last_backfill;
  }
  const eversion_t& get_last_update() const override {
    return backfill_source.last_update;
  }
  const eversion_t& get_log_tail() const override {
    return backfill_source.log_tail;
  }
  template <class... Args>
  void scan_log_after(Args&&... args) const {
  }

  bool is_backfill_target(pg_shard_t peer) const override {
    return backfill_targets.count(peer) == 1;
  }
  void update_complete_backfill_object_stats(const hobject_t &hoid,
                                             const pg_stat_t &stats) override {
  }
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
  backfill_state.process_event(crimson::osd::BackfillState::Triggered{}.intrusive_from_this());
}

void BackfillFixture::request_replica_scan(
  const pg_shard_t& target,
  const hobject_t& begin,
  const hobject_t& end)
{
  std::cout << __func__ << std::endl;

  BackfillInterval bi;
  bi.end = backfill_targets.at(target).store.list(begin, [&bi](auto kv) {
    std::cout << kv << std::endl;
    bi.objects.insert(std::move(kv));
  });
  bi.begin = begin;
  bi.version = backfill_source.last_update;

  schedule_event(crimson::osd::BackfillState::ReplicaScanned{ target, std::move(bi) });
}

void BackfillFixture::request_primary_scan(
  const hobject_t& begin)
{
  std::cout << __func__ << std::endl;

  BackfillInterval bi;
  bi.end = backfill_source.store.list(begin, [&bi](auto kv) {
    std::cout << kv << std::endl;
    bi.objects.insert(std::move(kv));
  });
  bi.begin = begin;
  bi.version = backfill_source.last_update;

  schedule_event(crimson::osd::BackfillState::PrimaryScanned{ std::move(bi) });
}

void BackfillFixture::enqueue_push(
  const pg_shard_t& target,
  const hobject_t& obj,
  const eversion_t& v)
{
  backfill_targets.at(target).store.push(obj, v);
  schedule_event(crimson::osd::BackfillState::ObjectPushed{ obj });
}

void BackfillFixture::enqueue_drop(
  const pg_shard_t& target,
  const hobject_t& obj,
  const eversion_t& v)
{
  std::cout << __func__ << std::endl;
}

void BackfillFixture::update_peers_last_backfill(
  const hobject_t& new_last_backfill)
{
  std::cout << __func__ << std::endl;
}

bool BackfillFixture::budget_available() const
{
  std::cout << __func__ << std::endl;
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
    const auto [ _, inserted ] = backfill_targets.emplace(
      new_osd_num, FakeReplica{ FakeStore{std::move(objs)} });
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
  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round();
  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}

TEST(backfill, single_empty_replica)
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
  EXPECT_CALL(cluster_fixture, backfilled);
  cluster_fixture.next_round();
  EXPECT_TRUE(cluster_fixture.all_stores_look_like(reference_store));
}
