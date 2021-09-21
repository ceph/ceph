// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <chrono>

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/mClockCommon.h"

#include "osd/scheduler/mClockScheduler.h"
#include "osd/scheduler/OpSchedulerItem.h"
#include "messages/MOSDOp.h"

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
  MonClient *monc;
  bool init_perfcounter;
  OpTracker op_tracker;
  mClockScheduler q;

  uint64_t client1;
  uint64_t client2;
  uint64_t client3;

  hobject_t hobj;
  spg_t spgid;

  mClockSchedulerTest() :
    whoami(0),
    num_shards(1),
    shard_id(0),
    is_rotational(false),
    cutoff_priority(12),
    monc(nullptr),
    init_perfcounter(true),
    op_tracker(g_ceph_context, false, num_shards),
    q(g_ceph_context, whoami, num_shards, shard_id, is_rotational,
      cutoff_priority,
      2ms, 2ms, 1ms,
      monc, init_perfcounter),
    client1(1001),
    client2(9999),
    client3(100000001)
  {
    pg_t pgid;
    object_locator_t oloc;
    hobj = hobject_t(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
      pgid.pool(), oloc.nspace);
    spgid = spg_t(pgid);
  }

  struct MockDmclockItem : public PGOpQueueable {
    op_scheduler_class scheduler_class;
    dmc::ReqParams rp;
    OpTracker *optracker;
    OpRequestRef op;
    hobject_t hobj;
    spg_t spgid;

    MockDmclockItem(op_scheduler_class _scheduler_class, dmc::ReqParams _rp,
      OpTracker* _optrk, hobject_t _hobj, spg_t _spgid) :
        PGOpQueueable(spg_t()),
        scheduler_class(_scheduler_class),
        rp(_rp),
        optracker(_optrk),
        hobj(_hobj),
        spgid(_spgid)
    {
      if (optracker && scheduler_class == op_scheduler_class::client) {
          MOSDOp *m = new MOSDOp(0, 0, hobj, spgid, 0, 0, 0);
          m->set_qos_req_params(rp);
          op = optracker->create_request<OpRequest, MOSDOp*>(m);
        }
    }

    MockDmclockItem(op_scheduler_class _scheduler_class) :
      PGOpQueueable(spg_t()),
      scheduler_class(_scheduler_class),
      optracker(nullptr) {}

    MockDmclockItem()
      : MockDmclockItem(op_scheduler_class::background_best_effort) {}

    ostream &print(ostream &rhs) const final { return rhs; }

    std::string print() const final {
      return std::string();
    }

    std::optional<OpRequestRef> maybe_get_op() const final {
      if (optracker && scheduler_class == op_scheduler_class::client) {
        return op;
      }
      return std::nullopt;
    }

    op_scheduler_class get_scheduler_class() const final {
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
    q.enqueue(create_item(i, client1, op_scheduler_class::client));
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
    q.enqueue(create_item(i, client1, op_scheduler_class::client));
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
      q.enqueue(create_item(i, c, op_scheduler_class::client));
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
    q.enqueue(create_high_prio_item(i, i, client1, op_scheduler_class::client));
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

  // Insert ops into the mClock queue
  for (unsigned i = 100; i < 102; ++i) {
    q.enqueue(create_item(i, client1, op_scheduler_class::client));
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  // Insert Immediate ops
  for (unsigned i = 103; i < 105; ++i) {
    q.enqueue(create_item(i, client1, op_scheduler_class::immediate));
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  // Insert ops into the high queue
  for (unsigned i = 200; i < 202; ++i) {
    q.enqueue(create_high_prio_item(i, i, client1, op_scheduler_class::client));
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
    q.enqueue(create_item(i, client1, op_scheduler_class::background_best_effort));
    std::this_thread::sleep_for(5ms);
  }
  for (; i < 200; ++i) {
    q.enqueue(create_item(i, client2, op_scheduler_class::client));
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

TEST_F(mClockSchedulerTest, TestDistributedEnqueue) {
  ASSERT_TRUE(q.empty());
  dmc::ReqParams req_params;
  OpSchedulerItem r1 = create_item(100, client1, op_scheduler_class::client,
    req_params, &op_tracker, hobj, spgid);
  OpSchedulerItem r2 = create_item(101, client2, op_scheduler_class::client,
    req_params, &op_tracker, hobj, spgid);
  OpSchedulerItem r3 = create_item(102, client3, op_scheduler_class::client,
    req_params, &op_tracker, hobj, spgid);
  OpSchedulerItem r4 = create_item(103, client1, op_scheduler_class::client,
    dmc::ReqParams(100, 1), &op_tracker, hobj, spgid);
  OpSchedulerItem r5 = create_item(104, client2, op_scheduler_class::client,
    dmc::ReqParams(10, 1), &op_tracker, hobj, spgid);
  OpSchedulerItem r6 = create_item(105, client3, op_scheduler_class::client,
    dmc::ReqParams(30, 1), &op_tracker, hobj, spgid);

  q.enqueue(std::move(r1));
  q.enqueue(std::move(r2));
  q.enqueue(std::move(r3));
  q.enqueue(std::move(r4));
  q.enqueue(std::move(r5));
  q.enqueue(std::move(r6));

  std::vector<epoch_t> expected_epochs {100, 101, 102, 104, 105, 103};
  for(auto i = expected_epochs.begin(); i != expected_epochs.end(); ++i) {
    ASSERT_FALSE(q.empty());
    WorkItem work_item;
    while (!std::get_if<OpSchedulerItem>(&work_item)) {
      work_item = q.dequeue();
    }
    OpSchedulerItem r = get_item(std::move(work_item));
    ASSERT_EQ(*i, r.get_map_epoch());
  }
}
