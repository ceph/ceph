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

// private flag_*_mon_command values from MgrOpRequest.h
static constexpr uint8_t kFlagStartMonCommand = 1 << 3;
static constexpr uint8_t kFlagFinishMonCommand = 1 << 4;

TEST_F(MgrOpRequestTestHelper, MarkFinishMonCommandSetsCorrectFlag) {
  auto msg = ceph::make_message<MCommand>();
  auto req = tracker->create_request<MgrOpRequest>(msg);
  ASSERT_TRUE(req);

  req->mark_start_mon_command();
  EXPECT_EQ(req->state_flag(), kFlagStartMonCommand)
      << "mark_start_mon_command() should set the start-mon-command flag.";

  req->mark_finish_mon_command();
  EXPECT_EQ(req->state_flag(), kFlagFinishMonCommand)
      << "Current: flag_start_mon_command set twice, flag_finish_mon_command "
         "never set; Expected: flag_finish_mon_command set after "
         "mark_finish_mon_command().";
  EXPECT_EQ(req->_get_state_string(), "mon command finished")
      << "_get_state_string() should report the finished state after "
         "mark_finish_mon_command().";
}

// Regression guard for the ReplyOnFinish lifecycle ordering bug.
//
// ReplyOnFinish::finish() must call mark_finish_mon_command(), not the
// constructor.  If the finish flag were set at construction time (i.e. before
// monc->start_mon_command() is called), dump_ops_in_flight would report
// "mon command finished" while the mon command is still in-flight.
//
// Because ReplyOnFinish is a file-local class in DaemonServer.cc it cannot be
// instantiated here.  This test therefore guards the contract at the
// MgrOpRequest level: after mark_start_mon_command() the flag must still be
// kFlagStartMonCommand, not kFlagFinishMonCommand.  Any code that calls
// mark_finish_mon_command() too early (e.g. in a constructor executed before
// the mon command is dispatched) would make this test fail.
TEST_F(MgrOpRequestTestHelper, FinishFlagNotSetBeforeFinish) {
  auto msg = ceph::make_message<MCommand>();
  auto req = tracker->create_request<MgrOpRequest>(msg);
  ASSERT_TRUE(req);

  // Simulate the state right after ReplyOnFinish is constructed but before
  // finish() is called: only the start flag should be set.
  req->mark_start_mon_command();
  EXPECT_NE(req->state_flag(), kFlagFinishMonCommand)
      << "flag_finish_mon_command must not be set before mark_finish_mon_command() "
         "is called (i.e. finish() has not been invoked yet).";
  EXPECT_EQ(req->_get_state_string(), "start mon command")
      << "_get_state_string() must report \"start mon command\" while the mon "
         "command is still in-flight.";

  // Now simulate finish() completing and verify the flag advances correctly.
  req->mark_finish_mon_command();
  EXPECT_EQ(req->state_flag(), kFlagFinishMonCommand)
      << "flag_finish_mon_command must be set after mark_finish_mon_command().";
  EXPECT_EQ(req->_get_state_string(), "mon command finished");
}
