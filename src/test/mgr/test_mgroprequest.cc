// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/MgrOpRequest.h"
#include "common/TrackedOp.h"
#include "messages/MCommand.h"

TEST_F(MgrOpRequestTestHelper, BasicSetup) {
  auto msg = ceph::make_message<MCommand>();
  msg->set_tid(123);

  auto req = tracker->create_request<MgrOpRequest>(msg);
  ASSERT_TRUE(req);
  ASSERT_EQ(req->get_req(), msg);
}
