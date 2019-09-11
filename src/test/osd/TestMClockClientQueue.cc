// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <iostream>

#include "gtest/gtest.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include "osd/mClockClientQueue.h"

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


class MClockClientQueueTest : public testing::Test {
public:
  mClockClientQueue q;

  uint64_t client1;
  uint64_t client2;
  uint64_t client3;

  MClockClientQueueTest() :
    q(g_ceph_context),
    client1(1001),
    client2(9999),
    client3(100000001)
  {}

  struct MockDmclockItem : public PGOpQueueable {
    ceph::qos::dmclock_request_t request;
    MockDmclockItem(decltype(request) _request) :
      PGOpQueueable(spg_t()), request(_request) {}

public:
    op_type_t get_op_type() const final {
      return op_type_t::client_op;
    }

    ostream &print(ostream &rhs) const final { return rhs; }

    std::optional<OpRequestRef> maybe_get_op() const final {
      return std::nullopt;
    }

    op_scheduler_class get_scheduler_class() const final {
      return op_scheduler_class::client;
    }

    std::optional<ceph::qos::dmclock_request_t>
    get_dmclock_request_state() const final {
      return request;
    }

    void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final {}
  };

  template <typename... Args>
  Request create_dmclock(epoch_t e, uint64_t owner, Args... args) {
    return Request(
      OpSchedulerItem(
	unique_ptr<OpSchedulerItem::OpQueueable>(
	  new MockDmclockItem(
	    std::forward<Args>(args)...)),
	12, 12,
	utime_t(), owner, e));
  }

  Request create_snaptrim(epoch_t e, uint64_t owner) {
    return Request(OpSchedulerItem(unique_ptr<OpSchedulerItem::OpQueueable>(new PGSnapTrim(spg_t(), e)),
			       12, 12,
			       utime_t(), owner, e));
  }

  Request create_scrub(epoch_t e, uint64_t owner) {
    return Request(OpSchedulerItem(unique_ptr<OpSchedulerItem::OpQueueable>(new PGScrub(spg_t(), e)),
			       12, 12,
			       utime_t(), owner, e));
  }

  Request create_recovery(epoch_t e, uint64_t owner) {
    return Request(OpSchedulerItem(unique_ptr<OpSchedulerItem::OpQueueable>(new PGRecovery(spg_t(), e, 64)),
			       12, 12,
			       utime_t(), owner, e));
  }
};


TEST_F(MClockClientQueueTest, TestSize) {
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0u, q.get_size_slow());

  q.enqueue(client1, 12, 1u, create_snaptrim(100, client1));
  q.enqueue_strict(client2, 12, create_snaptrim(101, client2));
  q.enqueue(client2, 12, 1u, create_snaptrim(102, client2));
  q.enqueue_strict(client3, 12, create_snaptrim(103, client3));
  q.enqueue(client1, 12, 1u, create_snaptrim(104, client1));

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(5u, q.get_size_slow());

  std::list<Request> reqs;

  reqs.push_back(q.dequeue());
  reqs.push_back(q.dequeue());
  reqs.push_back(q.dequeue());

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(2u, q.get_size_slow());

  q.enqueue_front(client2, 12, 1u, std::move(reqs.back()));
  reqs.pop_back();

  q.enqueue_strict_front(client3, 12, std::move(reqs.back()));
  reqs.pop_back();

  q.enqueue_strict_front(client2, 12, std::move(reqs.back()));
  reqs.pop_back();

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(5u, q.get_size_slow());

  for (int i = 0; i < 5; ++i) {
    (void) q.dequeue();
  }

  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0u, q.get_size_slow());
}


TEST_F(MClockClientQueueTest, TestEnqueue) {
  q.enqueue(client1, 12, 1u, create_snaptrim(100, client1));
  q.enqueue(client2, 12, 1u, create_snaptrim(101, client2));
  q.enqueue(client2, 12, 1u, create_snaptrim(102, client2));
  q.enqueue(client3, 12, 1u, create_snaptrim(103, client3));
  q.enqueue(client1, 12, 1u, create_snaptrim(104, client1));

  Request r = q.dequeue();
  ASSERT_EQ(100u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(101u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(103u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_TRUE(r.get_map_epoch() == 102u ||
              r.get_map_epoch() == 104u);

  r = q.dequeue();
  ASSERT_TRUE(r.get_map_epoch() == 102u ||
              r.get_map_epoch() == 104u);
}


TEST_F(MClockClientQueueTest, TestDistributedEnqueue) {
  Request r1 = create_snaptrim(100, client1);
  Request r2 = create_snaptrim(101, client2);
  Request r3 = create_snaptrim(102, client3);
  Request r4 = create_dmclock(103, client1, dmc::ReqParams(50,1));
  Request r5 = create_dmclock(104, client2, dmc::ReqParams(30,1));
  Request r6 = create_dmclock(105, client3, dmc::ReqParams(10,1));

  q.enqueue(client1, 12, 0, std::move(r1));
  q.enqueue(client2, 12, 0, std::move(r2));
  q.enqueue(client3, 12, 0, std::move(r3));
  q.enqueue(client1, 12, 0, std::move(r4));
  q.enqueue(client2, 12, 0, std::move(r5));
  q.enqueue(client3, 12, 0, std::move(r6));

  Request r = q.dequeue();
  r = q.dequeue();
  r = q.dequeue();

  r = q.dequeue();
  ASSERT_EQ(105u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(103u, r.get_map_epoch());
}


TEST_F(MClockClientQueueTest, TestEnqueueStrict) {
  q.enqueue_strict(client1, 12, create_snaptrim(100, client1));
  q.enqueue_strict(client2, 13, create_snaptrim(101, client2));
  q.enqueue_strict(client2, 16, create_snaptrim(102, client2));
  q.enqueue_strict(client3, 14, create_snaptrim(103, client3));
  q.enqueue_strict(client1, 15, create_snaptrim(104, client1));

  Request r = q.dequeue();
  ASSERT_EQ(102u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(103u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(101u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(100u, r.get_map_epoch());
}


TEST_F(MClockClientQueueTest, TestRemoveByClass) {
  q.enqueue(client1, 12, 1u, create_snaptrim(100, client1));
  q.enqueue_strict(client2, 12, create_snaptrim(101, client2));
  q.enqueue(client2, 12, 1u, create_snaptrim(102, client2));
  q.enqueue_strict(client3, 12, create_snaptrim(103, client3));
  q.enqueue(client1, 12, 1u, create_snaptrim(104, client1));

  std::list<Request> filtered_out;
  q.remove_by_class(client2, &filtered_out);

  ASSERT_EQ(2u, filtered_out.size());
  while (!filtered_out.empty()) {
    auto e = filtered_out.front().get_map_epoch() ;
    ASSERT_TRUE(e == 101 || e == 102);
    filtered_out.pop_front();
  }

  ASSERT_EQ(3u, q.get_size_slow());
  Request r = q.dequeue();
  ASSERT_EQ(103u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(100u, r.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.get_map_epoch());
}
