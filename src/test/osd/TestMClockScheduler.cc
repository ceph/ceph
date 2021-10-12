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
    client_qos_params_t qrp;

    MockDmclockItem(op_scheduler_class _scheduler_class, dmc::ReqParams _rp,
      OpTracker* _optrk, hobject_t _hobj, spg_t _spgid, client_qos_params_t _qrp) :
        PGOpQueueable(spg_t()),
        scheduler_class(_scheduler_class),
        rp(_rp),
        optracker(_optrk),
        hobj(_hobj),
        spgid(_spgid),
        qrp(_qrp)
    {
      if (optracker && scheduler_class == op_scheduler_class::client) {
          MOSDOp *m = new MOSDOp(0, 0, hobj, spgid, 0, 0, 0);
          m->set_qos_req_params(rp);
          m->set_qos_profile_params(qrp);
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

  OpSchedulerItem dequeue_item() {
    WorkItem work_item;
    if (!q.empty()) {
      while (!std::get_if<OpSchedulerItem>(&work_item)) {
        work_item = q.dequeue();
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    }
    return std::move(std::get<OpSchedulerItem>(work_item));
  }
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

TEST_F(mClockSchedulerTest, TestDistributedEnqueuePullWeight) {
  ASSERT_TRUE(q.empty());

  // Client QoS profile
  client_qos_params_t c1_params = {0, 1, 0, 1};
  client_qos_params_t c2_params = {0, 3, 0, 2};
  client_qos_params_t c3_params = {0, 2, 0, 3};
  std::map<uint64_t, client_qos_params_t> client_qos_params;
  client_qos_params[client1] = c1_params;
  client_qos_params[client2] = c2_params;
  client_qos_params[client3] = c3_params;

  // Client request params
  dmc::ReqParams c1_rparams = {100, 1};
  dmc::ReqParams c2_rparams = {10, 1};
  dmc::ReqParams c3_rparams = {30, 1};
  std::map<uint64_t, dmc::ReqParams> client_req_params;
  client_req_params[client1] = c1_rparams;
  client_req_params[client2] = c2_rparams;
  client_req_params[client3] = c3_rparams;

  // Create and enqueue requests
  unsigned e = 100;
  for (unsigned i = 0; i < 2; ++i) {
    for(auto &&c: {client1, client2, client3}) {
      q.enqueue(create_item(e++, c, op_scheduler_class::client,
        client_req_params[c], &op_tracker, hobj, spgid, client_qos_params[c]));
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }

  // Expected dequeue sequence based on qos and req params
  std::vector<epoch_t> expected_epochs {100, 101, 102, 104, 105, 103};
  for(auto i = expected_epochs.begin(); i != expected_epochs.end(); ++i) {
    ASSERT_FALSE(q.empty());
    auto r = dequeue_item();
    ASSERT_EQ(*i, r.get_map_epoch());
    std::optional<OpRequestRef> _op = r.maybe_get_op();
    ASSERT_EQ((*_op)->qos_phase, dmc::PhaseType::priority);
  }

  ASSERT_TRUE(q.empty());
}

TEST_F(mClockSchedulerTest, TestDistributedEnqueuePullReservation) {
  ASSERT_TRUE(q.empty());

  // Specify client reservations in IOPS
  uint64_t c1_res = 500;
  uint64_t c2_res = 1000;
  uint64_t c3_res = 2000;
  client_qos_params_t c1_params = {c1_res, 0, 0, 1};
  client_qos_params_t c2_params = {c2_res, 0, 0, 2};
  client_qos_params_t c3_params = {c3_res, 0, 0, 3};

  // Client QoS profile
  std::map<uint64_t, client_qos_params_t> client_qos_params;
  client_qos_params[client1] = c1_params;
  client_qos_params[client2] = c2_params;
  client_qos_params[client3] = c3_params;

  // Client request params
  dmc::ReqParams c1_rparams = {20, 10};
  dmc::ReqParams c2_rparams = {4, 3};
  dmc::ReqParams c3_rparams = {2, 1};
  std::map<uint64_t, dmc::ReqParams> client_req_params;
  client_req_params[client1] = c1_rparams;
  client_req_params[client2] = c2_rparams;
  client_req_params[client3] = c3_rparams;

  // Create and enqueue requests
  unsigned e = 100;
  for (unsigned i = 0; i < 2; ++i) {
    for(auto &&c: {client1, client2, client3}) {
      q.enqueue(create_item(e++, c, op_scheduler_class::client,
        client_req_params[c], &op_tracker, hobj, spgid, client_qos_params[c]));
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  }

  // Expected dequeue sequence based on qos and req params
  std::vector<epoch_t> expected_epochs {100, 101, 102, 105, 104, 103};
  for(auto i = expected_epochs.begin(); i != expected_epochs.end(); ++i) {
    ASSERT_FALSE(q.empty());
    auto r = dequeue_item();
    ASSERT_EQ(*i, r.get_map_epoch());
    std::optional<OpRequestRef> _op = r.maybe_get_op();
    ASSERT_EQ((*_op)->qos_phase, dmc::PhaseType::reservation);
  }
}

TEST_F(mClockSchedulerTest, TestMultiDistributedEnqueuePullWeight) {
  ASSERT_TRUE(q.empty());

  // Client QoS profile
  client_qos_params_t c1_params = {0, 1, 0, 1};
  client_qos_params_t c2_params = {0, 3, 0, 2};
  client_qos_params_t c3_params = {0, 2, 0, 3};
  std::map<uint64_t, client_qos_params_t> client_qos_params;
  client_qos_params[client1] = c1_params;
  client_qos_params[client2] = c2_params;
  client_qos_params[client3] = c3_params;

  // Client request params
  dmc::ReqParams c1_rparams = {1, 1};
  dmc::ReqParams c2_rparams = {1, 1};
  dmc::ReqParams c3_rparams = {1, 1};
  std::map<uint64_t, dmc::ReqParams> client_req_params;
  client_req_params[client1] = c1_rparams;
  client_req_params[client2] = c2_rparams;
  client_req_params[client3] = c3_rparams;

  // Create and enqueue requests
  unsigned e = 100;
  for (unsigned i = 0; i < 5; ++i) {
    for(auto &&c: {client1, client2, client3}) {
      q.enqueue(create_item(e++, c, op_scheduler_class::client,
        client_req_params[c], &op_tracker, hobj, spgid, client_qos_params[c]));
    }
  }

  int c1_count = 0;
  int c2_count = 0;
  int c3_count = 0;
  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(q.empty());
    auto retn = dequeue_item();

    if (client1 == retn.get_owner()) ++c1_count;
    else if (client2 == retn.get_owner()) ++c2_count;
    else if (client3 == retn.get_owner()) ++c3_count;
    else ADD_FAILURE() << "got request from neither of two clients";

    std::optional<OpRequestRef> _op = retn.maybe_get_op();
    ASSERT_EQ((*_op)->qos_phase, dmc::PhaseType::priority);
  }

  ASSERT_EQ(c1_count, 2);
  ASSERT_EQ(c2_count, 5);
  ASSERT_EQ(c3_count, 3);

  ASSERT_FALSE(q.empty());

  while (!q.empty()) {
    auto retn = dequeue_item();
    std::optional<OpRequestRef> _op = retn.maybe_get_op();
    ASSERT_EQ((*_op)->qos_phase, dmc::PhaseType::priority);
  }

  ASSERT_TRUE(q.empty());
}

TEST_F(mClockSchedulerTest, TestMultiDistributedEnqueuePullReservation) {
  ASSERT_TRUE(q.empty());

  // Specify client reservations in IOPS
  uint64_t c1_res = 500;
  uint64_t c2_res = 2000;
  uint64_t c3_res = 1000;
  client_qos_params_t c1_params = {c1_res, 0, 0, 1};
  client_qos_params_t c2_params = {c2_res, 0, 0, 2};
  client_qos_params_t c3_params = {c3_res, 0, 0, 3};

  // Client QoS profile
  std::map<uint64_t, client_qos_params_t> client_qos_params;
  client_qos_params[client1] = c1_params;
  client_qos_params[client2] = c2_params;
  client_qos_params[client3] = c3_params;

  // Client request params
  dmc::ReqParams c1_rparams = {1, 1};
  dmc::ReqParams c2_rparams = {1, 1};
  dmc::ReqParams c3_rparams = {1, 1};
  std::map<uint64_t, dmc::ReqParams> client_req_params;
  client_req_params[client1] = c1_rparams;
  client_req_params[client2] = c2_rparams;
  client_req_params[client3] = c3_rparams;

  // Create and enqueue requests
  unsigned e = 100;
  for (unsigned i = 0; i < 5; ++i) {
    for(auto &&c: {client1, client2, client3}) {
      q.enqueue(create_item(e++, c, op_scheduler_class::client,
        client_req_params[c], &op_tracker, hobj, spgid, client_qos_params[c]));
    }
  }

  int c1_count = 0;
  int c2_count = 0;
  int c3_count = 0;
  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(q.empty());
    auto retn = dequeue_item();

    if (client1 == retn.get_owner()) ++c1_count;
    else if (client2 == retn.get_owner()) ++c2_count;
    else if (client3 == retn.get_owner()) ++c3_count;
    else ADD_FAILURE() << "got request from neither of two clients";

    std::optional<OpRequestRef> _op = retn.maybe_get_op();
    ASSERT_EQ((*_op)->qos_phase, dmc::PhaseType::reservation);
  }

  ASSERT_EQ(c1_count, 2);
  ASSERT_EQ(c2_count, 5);
  ASSERT_EQ(c3_count, 3);

  ASSERT_FALSE(q.empty());

  while (!q.empty()) {
    auto retn = dequeue_item();
    std::optional<OpRequestRef> _op = retn.maybe_get_op();
    ASSERT_EQ((*_op)->qos_phase, dmc::PhaseType::reservation);
  }

  ASSERT_TRUE(q.empty());
}
