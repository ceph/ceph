// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/MgrOpRequest.h"
#include "common/TrackedOp.h"
#include "messages/MCommand.h"

// MgrOpRequest::mark_flag_point_string() and the flag_* constants are private.
// This accessor is declared a friend of MgrOpRequest so the tests can drive the
// private string-overload mark path directly (there is currently no public
// caller of it).
struct MgrOpRequestTestAccess {
  static void mark_reached_module(MgrOpRequest* op, const std::string& s) {
    op->mark_flag_point_string(MgrOpRequest::flag_reached_module, s);
  }
};

TEST_F(MgrOpRequestTestHelper, BasicSetup) {
  auto msg = ceph::make_message<MCommand>();
  msg->set_tid(123);

  auto req = tracker->create_request<MgrOpRequest>(msg);
  ASSERT_TRUE(req);
  ASSERT_EQ(req->get_req(), msg);
}

// Regression test for https://tracker.ceph.com/issues/81341
// mark_flag_point_string() must persist its detail string into
// last_event_detail so that _get_state_string() reports the current detail for
// the flag_reached_module state (mirroring mark_flag_point()).
TEST_F(MgrOpRequestTestHelper, MarkFlagPointStringSetsLastEventDetail) {
  auto msg = ceph::make_message<MCommand>();
  auto req = tracker->create_request<MgrOpRequest>(msg);
  ASSERT_TRUE(req);

  MgrOpRequestTestAccess::mark_reached_module(req.get(), "py_test_module");
  EXPECT_NE(req->_get_state_string().find("py_test_module"), std::string::npos)
      << "Current: _get_state_string() returns stale/empty detail after "
         "mark_flag_point_string(); Expected: detail string is reflected in "
         "state. Got: \""
      << req->_get_state_string() << "\"";

  // A subsequent call with a different detail string must update the result;
  // last_event_detail should always reflect the most recent detail, never a
  // stuck/stale value.
  MgrOpRequestTestAccess::mark_reached_module(req.get(), "py_other_module");
  EXPECT_NE(req->_get_state_string().find("py_other_module"), std::string::npos)
      << "Current: _get_state_string() returns stale detail after a second "
         "mark_flag_point_string(); Expected: detail string is updated. Got: \""
      << req->_get_state_string() << "\"";
  EXPECT_EQ(req->_get_state_string().find("py_test_module"), std::string::npos)
      << "Expected the stale detail \"py_test_module\" to be replaced by the "
         "latest detail; Got: \""
      << req->_get_state_string() << "\"";
}
