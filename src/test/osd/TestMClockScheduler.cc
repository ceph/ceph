// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <chrono>

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/mclock_common.h"

#include "osd/scheduler/mClockScheduler.h"
#include "osd/scheduler/OpSchedulerItem.h"

using namespace ceph::osd::scheduler;

int main(int argc, char **argv) {
  std::vector<const char*> args(argv, argv+argc);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

using namespace std::literals;

class mClockSchedulerTest : public testing::Test {
public:
  int whoami;
  uint32_t num_shards;
  int shard_id;
  bool is_rotational;
  unsigned cutoff_priority;
  bool init_perfcounter;
  mClockScheduler q;

  uint64_t client1;
  uint64_t client2;
  uint64_t client3;

  mClockSchedulerTest() :
    whoami(0),
    num_shards(1),
    shard_id(0),
    is_rotational(false),
    cutoff_priority(12),
    init_perfcounter(true),
    q(g_ceph_context, whoami, num_shards, shard_id, is_rotational,
      cutoff_priority,
      2ms, 2ms, 1ms,
      init_perfcounter),
    client1(1001),
    client2(9999),
    client3(100000001)
  {}

  struct MockDmclockItem : public PGOpQueueable {
    SchedulerClass scheduler_class;

    MockDmclockItem(SchedulerClass _scheduler_class) :
      PGOpQueueable(spg_t()),
      scheduler_class(_scheduler_class) {}

    MockDmclockItem()
      : MockDmclockItem(SchedulerClass::background_best_effort) {}

    ostream &print(ostream &rhs) const final { return rhs; }

    std::string print() const final {
      return std::string();
    }

    std::optional<OpRequestRef> maybe_get_op() const final {
      return std::nullopt;
    }

    SchedulerClass get_scheduler_class() const final {
      return scheduler_class;
    }

    void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final {}
  };
};

template <typename... Args>
OpSchedulerItem create_item(
  epoch_t e, uint64_t owner, Args&&... args)
{
  return OpSchedulerItem(
    std::make_unique<mClockSchedulerTest::MockDmclockItem>(
      std::forward<Args>(args)...),
    12, 1,
    utime_t(), owner, e);
}

template <typename... Args>
OpSchedulerItem create_high_prio_item(
  unsigned priority, epoch_t e, uint64_t owner, Args&&... args)
{
  // Create high priority item for testing high prio queue
  return OpSchedulerItem(
    std::make_unique<mClockSchedulerTest::MockDmclockItem>(
      std::forward<Args>(args)...),
    12, priority,
    utime_t(), owner, e);
}

OpSchedulerItem get_item(WorkItem item)
{
  return std::move(std::get<OpSchedulerItem>(item));
}

TEST_F(mClockSchedulerTest, TestEmpty) {
  ASSERT_TRUE(q.empty());

  for (unsigned i = 100; i < 105; i+=2) {
    q.enqueue(create_item(i, client1, SchedulerClass::client));
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  ASSERT_FALSE(q.empty());

  std::list<OpSchedulerItem> reqs;

  reqs.push_back(get_item(q.dequeue()));
  reqs.push_back(get_item(q.dequeue()));

  ASSERT_EQ(2u, reqs.size());
  ASSERT_FALSE(q.empty());

  for (auto &&i : reqs) {
    q.enqueue_front(std::move(i));
  }
  reqs.clear();

  ASSERT_FALSE(q.empty());

  for (int i = 0; i < 3; ++i) {
    ASSERT_FALSE(q.empty());
    q.dequeue();
  }

  ASSERT_TRUE(q.empty());
}

TEST_F(mClockSchedulerTest, TestSingleClientOrderedEnqueueDequeue) {
  ASSERT_TRUE(q.empty());

  for (unsigned i = 100; i < 105; ++i) {
    q.enqueue(create_item(i, client1, SchedulerClass::client));
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  auto r = get_item(q.dequeue());
  ASSERT_EQ(100u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(101u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(102u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(103u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(104u, r.get_map_epoch());
}

TEST_F(mClockSchedulerTest, TestMultiClientOrderedEnqueueDequeue) {
  const unsigned NUM = 1000;
  for (unsigned i = 0; i < NUM; ++i) {
    for (auto &&c: {client1, client2, client3}) {
      q.enqueue(create_item(i, c, SchedulerClass::client));
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }

  std::map<uint64_t, epoch_t> next;
  for (auto &&c: {client1, client2, client3}) {
    next[c] = 0;
  }
  for (unsigned i = 0; i < NUM * 3; ++i) {
    ASSERT_FALSE(q.empty());
    auto r = get_item(q.dequeue());
    auto owner = r.get_owner();
    auto niter = next.find(owner);
    ASSERT_FALSE(niter == next.end());
    ASSERT_EQ(niter->second, r.get_map_epoch());
    niter->second++;
  }
  ASSERT_TRUE(q.empty());
}

TEST_F(mClockSchedulerTest, TestHighPriorityQueueEnqueueDequeue) {
  ASSERT_TRUE(q.empty());
  for (unsigned i = 200; i < 205; ++i) {
    q.enqueue(create_high_prio_item(i, i, client1, SchedulerClass::client));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_FALSE(q.empty());
  // Higher priority ops should be dequeued first
  auto r = get_item(q.dequeue());
  ASSERT_EQ(204u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(203u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(202u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(201u, r.get_map_epoch());

  r = get_item(q.dequeue());
  ASSERT_EQ(200u, r.get_map_epoch());

  ASSERT_TRUE(q.empty());
}

TEST_F(mClockSchedulerTest, TestAllQueuesEnqueueDequeue) {
  ASSERT_TRUE(q.empty());

  // Prime last_mclock_service_time with a real, recent timestamp before
  // the priority-ordering assertions below. last_mclock_service_time
  // starts at TimeZero (treated as already-expired), so without this, the
  // very first dequeue() from a queue with both high_priority and the
  // mclock queue populated would force an mclock-queue pull ahead of
  // high_priority, which is not what this test is checking -- that
  // behavior gets its own dedicated tests below.
  q.enqueue(create_item(0, client1, SchedulerClass::client));
  get_item(q.dequeue());
  ASSERT_TRUE(q.empty());

  // Insert ops into the mClock queue
  for (unsigned i = 100; i < 102; ++i) {
    q.enqueue(create_item(i, client1, SchedulerClass::client));
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  // Insert Immediate ops
  for (unsigned i = 103; i < 105; ++i) {
    q.enqueue(create_item(i, client1, SchedulerClass::immediate));
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  // Insert ops into the high queue
  for (unsigned i = 200; i < 202; ++i) {
    q.enqueue(create_high_prio_item(i, i, client1, SchedulerClass::client));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_FALSE(q.empty());
  auto r = get_item(q.dequeue());
  // Ops classified as Immediate should be dequeued first
  ASSERT_EQ(103u, r.get_map_epoch());
  r = get_item(q.dequeue());
  ASSERT_EQ(104u, r.get_map_epoch());

  // High priority queue should be dequeued second
  // higher priority operation first
  r = get_item(q.dequeue());
  ASSERT_EQ(201u, r.get_map_epoch());
  r = get_item(q.dequeue());
  ASSERT_EQ(200u, r.get_map_epoch());

  // mClock queue will be dequeued last
  r = get_item(q.dequeue());
  ASSERT_EQ(100u, r.get_map_epoch());
  r = get_item(q.dequeue());
  ASSERT_EQ(101u, r.get_map_epoch());

  ASSERT_TRUE(q.empty());
}

const OpSchedulerItem *maybe_get_item(const WorkItem &item)
{
  return std::get_if<OpSchedulerItem>(&item);
}

TEST_F(mClockSchedulerTest, TestSlowDequeue) {
  ASSERT_TRUE(q.empty());

  // Insert ops into the mClock queue
  unsigned i = 0;
  for (; i < 100; ++i) {
    q.enqueue(create_item(i, client1, SchedulerClass::background_best_effort));
    std::this_thread::sleep_for(5ms);
  }
  for (; i < 200; ++i) {
    q.enqueue(create_item(i, client2, SchedulerClass::client));
    std::this_thread::sleep_for(5ms);
  }

  i = 0;
  for (; i < 200; ++i) {
    ASSERT_FALSE(q.empty());
    auto item = q.dequeue();
    auto *wqi = maybe_get_item(item);
    ASSERT_TRUE(wqi);
  }
  ASSERT_TRUE(q.empty());
}

// Tests for tracker 69078's starvation-bound fix: dequeue() forces one
// mclock-managed-queue pull once it has gone unserviced for at least
// mclock_conf's scheduler_max_starve_time while high_priority keeps
// refilling. The fixture is non-rotational (SSD/NVMe), so the bound is
// the fixed internal constant (50ms);
TEST_F(mClockSchedulerTest, TestNoForcedYieldBeforeThreshold) {
  ASSERT_TRUE(q.empty());

  // Set up last_mclock_service_time (see TestAllQueuesEnqueueDequeue).
  q.enqueue(create_item(0, client1, SchedulerClass::client));
  get_item(q.dequeue());
  ASSERT_TRUE(q.empty());

  q.enqueue(create_item(1, client1, SchedulerClass::background_best_effort));
  q.enqueue(create_high_prio_item(200, 200, client2, SchedulerClass::immediate));

  // Well under the 50ms SSD/NVMe threshold -- ordinary priority order
  // (high_priority before the mclock queue) is unaffected, confirming
  // the fix is a no-op in the common, not-yet-starved case.
  auto r = get_item(q.dequeue());
  ASSERT_EQ(200u, r.get_map_epoch());
}

TEST_F(mClockSchedulerTest, TestStarvationForcesYield) {
  ASSERT_TRUE(q.empty());

  // Set up last_mclock_service_time.
  q.enqueue(create_item(0, client1, SchedulerClass::client));
  get_item(q.dequeue());
  ASSERT_TRUE(q.empty());

  // One item sits pending in the mclock-managed queue throughout.
  q.enqueue(create_item(1, client1, SchedulerClass::background_best_effort));

  // Keep high_priority continuously refilled for longer than the 50ms
  // SSD/NVMe threshold, without calling dequeue() in between -- mirrors
  // a sustained immediate-class backlog (e.g. EC subop traffic under
  // heavy client I/O) that would otherwise starve the mclock queue
  // indefinitely.
  for (unsigned i = 0; i < 60; ++i) {
    q.enqueue(create_high_prio_item(100 + i, 100 + i, client2,
                                    SchedulerClass::immediate));
  }
  std::this_thread::sleep_for(60ms);

  // high_priority still has pending items at this point -- the next
  // dequeue() should force-pull the mclock-queue item instead of
  // continuing to drain high_priority.
  ASSERT_FALSE(q.empty());
  auto r = get_item(q.dequeue());
  ASSERT_EQ(1u, r.get_map_epoch());

  // high_priority items are still there afterward -- confirms this was
  // a forced yield, not high_priority having drained naturally.
  ASSERT_FALSE(q.empty());
}

TEST(mClockSchedulerHDDTest, TestStarveTimeLiveReconfig) {
  // Rotational path: osd_mclock_max_starve_time_hdd is read at
  // construction (MclockConfig::set_from_config(), called from its own
  // constructor) and re-read live via the existing handle_conf_change()
  // observer on any subsequent config change -- the same path every
  // other mclock scheduler option already uses. Uses a standalone
  // scheduler instance (not the shared fixture, which is fixed
  // non-rotational) so it can exercise the HDD-specific option.
  g_ceph_context->_conf.rm_val("osd_mclock_max_starve_time_hdd");
  g_ceph_context->_conf.apply_changes(nullptr);

  mClockScheduler q(g_ceph_context, /*whoami=*/0, /*num_shards=*/1,
                     /*shard_id=*/0, /*is_rotational=*/true,
                     /*cutoff_priority=*/12,
                     2ms, 2ms, 1ms, /*init_perfcounter=*/true);
  uint64_t client1 = 1001;
  uint64_t client2 = 9999;

  q.enqueue(create_item(0, client1, SchedulerClass::client));
  get_item(q.dequeue());
  ASSERT_TRUE(q.empty());

  q.enqueue(create_item(1, client1, SchedulerClass::background_best_effort));

  // At the 250ms default, ~150ms of continuous high_priority refill is
  // not enough to force a yield -- confirms the option's initial,
  // construction-time value.
  for (unsigned i = 0; i < 150; ++i) {
    q.enqueue(create_high_prio_item(100 + i, 100 + i, client2,
                                     SchedulerClass::immediate));
  }
  std::this_thread::sleep_for(150ms);
  auto r = get_item(q.dequeue());
  ASSERT_NE(1u, r.get_map_epoch());

  // Live-reconfigure to the 100ms floor without rebuilding the
  // scheduler.
  g_ceph_context->_conf.set_val("osd_mclock_max_starve_time_hdd", "0.1");
  g_ceph_context->_conf.apply_changes(nullptr);

  // Continue refilling high_priority under the new, shorter threshold --
  // this time the forced yield should engage well before 250ms, proving
  // the live reconfiguration actually took effect rather than the
  // unmodified default.
  for (unsigned i = 0; i < 150; ++i) {
    q.enqueue(create_high_prio_item(300 + i, 300 + i, client2,
                                     SchedulerClass::immediate));
  }
  std::this_thread::sleep_for(150ms);
  ASSERT_FALSE(q.empty());
  r = get_item(q.dequeue());
  ASSERT_EQ(1u, r.get_map_epoch());

  g_ceph_context->_conf.rm_val("osd_mclock_max_starve_time_hdd");
  g_ceph_context->_conf.apply_changes(nullptr);
}
