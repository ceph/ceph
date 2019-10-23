// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

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


class mClockSchedulerTest : public testing::Test {
public:
  mClockScheduler q;

  uint64_t client1;
  uint64_t client2;
  uint64_t client3;

  mClockSchedulerTest() :
    q(g_ceph_context),
    client1(1001),
    client2(9999),
    client3(100000001)
  {}

  struct MockDmclockItem : public PGOpQueueable {
    op_scheduler_class scheduler_class;

    MockDmclockItem(op_scheduler_class _scheduler_class) :
      PGOpQueueable(spg_t()),
      scheduler_class(_scheduler_class) {}

    MockDmclockItem()
      : MockDmclockItem(op_scheduler_class::background_best_effort) {}

    op_type_t get_op_type() const final {
      return op_type_t::client_op; // not used
    }

    ostream &print(ostream &rhs) const final { return rhs; }

    std::optional<OpRequestRef> maybe_get_op() const final {
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
    12, 12,
    utime_t(), owner, e);
}

TEST_F(mClockSchedulerTest, TestEmpty) {
  ASSERT_TRUE(q.empty());

  q.enqueue(create_item(100, client1, op_scheduler_class::client));
  q.enqueue(create_item(102, client1, op_scheduler_class::client));
  q.enqueue(create_item(104, client1, op_scheduler_class::client));

  ASSERT_FALSE(q.empty());

  std::list<OpSchedulerItem> reqs;

  reqs.push_back(q.dequeue());
  reqs.push_back(q.dequeue());

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
  q.enqueue(create_item(100, client1));
  q.enqueue(create_item(101, client1));
  q.enqueue(create_item(102, client1));
  q.enqueue(create_item(103, client1));
  q.enqueue(create_item(104, client1));

  auto r = q.dequeue();
  ASSERT_EQ(100u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(101u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(102u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(103u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.get_map_epoch());
}

TEST_F(mClockSchedulerTest, TestMultiClientOrderedEnqueueDequeue) {
  const unsigned NUM = 1000;
  for (unsigned i = 0; i < NUM; ++i) {
    for (auto &&c: {client1, client2, client3}) {
      q.enqueue(create_item(i, c));
    }
  }

  std::map<uint64_t, epoch_t> next;
  for (auto &&c: {client1, client2, client3}) {
    next[c] = 0;
  }
  for (unsigned i = 0; i < NUM * 3; ++i) {
    ASSERT_FALSE(q.empty());
    auto r = q.dequeue();
    auto owner = r.get_owner();
    auto niter = next.find(owner);
    ASSERT_FALSE(niter == next.end());
    ASSERT_EQ(niter->second, r.get_map_epoch());
    niter->second++;
  }
  ASSERT_TRUE(q.empty());
}
