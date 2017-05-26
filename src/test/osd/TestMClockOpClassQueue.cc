// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <iostream>

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include "osd/mClockOpClassQueue.h"


int main(int argc, char **argv) {
  std::vector<const char*> args(argv, argv+argc);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


class MClockOpClassQueueTest : public testing::Test {
public:
  mClockOpClassQueue q;

  entity_inst_t client1;
  entity_inst_t client2;
  entity_inst_t client3;

  MClockOpClassQueueTest() :
    q(g_ceph_context),
    client1(entity_name_t(CEPH_ENTITY_TYPE_OSD, 1), entity_addr_t()),
    client2(entity_name_t(CEPH_ENTITY_TYPE_OSD, 2), entity_addr_t()),
    client3(entity_name_t(CEPH_ENTITY_TYPE_CLIENT, 1), entity_addr_t())
  {}

#if 0 // more work needed here
  Request create_client_op(epoch_t e, const entity_inst_t& owner) {
    return Request(spg_t(), PGQueueable(OpRequestRef(), e));
  }
#endif

  Request create_snaptrim(epoch_t e, const entity_inst_t& owner) {
    return Request(spg_t(),
		   PGQueueable(PGSnapTrim(e),
			       12, 12,
			       utime_t(), owner, e));
  }

  Request create_scrub(epoch_t e, const entity_inst_t& owner) {
    return Request(spg_t(),
		   PGQueueable(PGScrub(e),
			       12, 12,
			       utime_t(), owner, e));
  }

  Request create_recovery(epoch_t e, const entity_inst_t& owner) {
    return Request(spg_t(),
		   PGQueueable(PGRecovery(e, 64),
			       12, 12,
			       utime_t(), owner, e));
  }
};


TEST_F(MClockOpClassQueueTest, TestSize) {
  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0u, q.length());

  q.enqueue(client1, 12, 0, create_snaptrim(100, client1));
  q.enqueue_strict(client2, 12, create_snaptrim(101, client2));
  q.enqueue(client2, 12, 0, create_snaptrim(102, client2));
  q.enqueue_strict(client3, 12, create_snaptrim(103, client3));
  q.enqueue(client1, 12, 0, create_snaptrim(104, client1));

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(5u, q.length());

  std::list<Request> reqs;

  reqs.push_back(q.dequeue());
  reqs.push_back(q.dequeue());
  reqs.push_back(q.dequeue());

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(2u, q.length());

  q.enqueue_front(client2, 12, 0, reqs.back());
  reqs.pop_back();

  q.enqueue_strict_front(client3, 12, reqs.back());
  reqs.pop_back();

  q.enqueue_strict_front(client2, 12, reqs.back());
  reqs.pop_back();

  ASSERT_FALSE(q.empty());
  ASSERT_EQ(5u, q.length());

  for (int i = 0; i < 5; ++i) {
    (void) q.dequeue();
  }

  ASSERT_TRUE(q.empty());
  ASSERT_EQ(0u, q.length());
}


TEST_F(MClockOpClassQueueTest, TestEnqueue) {
  q.enqueue(client1, 12, 0, create_snaptrim(100, client1));
  q.enqueue(client2, 12, 0, create_snaptrim(101, client2));
  q.enqueue(client2, 12, 0, create_snaptrim(102, client2));
  q.enqueue(client3, 12, 0, create_snaptrim(103, client3));
  q.enqueue(client1, 12, 0, create_snaptrim(104, client1));

  Request r = q.dequeue();
  ASSERT_EQ(100u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(101u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(102u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(103u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.second.get_map_epoch());
}


TEST_F(MClockOpClassQueueTest, TestEnqueueStrict) {
  q.enqueue_strict(client1, 12, create_snaptrim(100, client1));
  q.enqueue_strict(client2, 13, create_snaptrim(101, client2));
  q.enqueue_strict(client2, 16, create_snaptrim(102, client2));
  q.enqueue_strict(client3, 14, create_snaptrim(103, client3));
  q.enqueue_strict(client1, 15, create_snaptrim(104, client1));

  Request r = q.dequeue();
  ASSERT_EQ(102u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(103u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(101u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(100u, r.second.get_map_epoch());
}


TEST_F(MClockOpClassQueueTest, TestRemoveByClass) {
  q.enqueue(client1, 12, 0, create_snaptrim(100, client1));
  q.enqueue_strict(client2, 12, create_snaptrim(101, client2));
  q.enqueue(client2, 12, 0, create_snaptrim(102, client2));
  q.enqueue_strict(client3, 12, create_snaptrim(103, client3));
  q.enqueue(client1, 12, 0, create_snaptrim(104, client1));

  std::list<Request> filtered_out;
  q.remove_by_class(client2, &filtered_out);

  ASSERT_EQ(2u, filtered_out.size());
  while (!filtered_out.empty()) {
    auto e = filtered_out.front().second.get_map_epoch() ;
    ASSERT_TRUE(e == 101 || e == 102);
    filtered_out.pop_front();
  }

  ASSERT_EQ(3u, q.length());
  Request r = q.dequeue();
  ASSERT_EQ(103u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(100u, r.second.get_map_epoch());

  r = q.dequeue();
  ASSERT_EQ(104u, r.second.get_map_epoch());
}
