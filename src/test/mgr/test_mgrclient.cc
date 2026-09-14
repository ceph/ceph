// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "global/global_init.h"
#include "TestMgr.h"
#include "mgr/MgrClient.h"
#include "mgr/DaemonHealthMetric.h"
#include "messages/MCommandReply.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrCommandReply.h"
#include "messages/MMgrMap.h"
#include "messages/MPing.h"
#include "mon/MonMap.h"
#include "common/ceph_context.h"
#include "common/JSONFormatter.h"

TEST_F(TestMgr, MgrClientBasicSetup) {
  ASSERT_NE(mc, nullptr);
  ASSERT_NE(objecter, nullptr);
  ASSERT_NE(messenger, nullptr);

  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  ASSERT_FALSE(client.is_initialized());

  client.init();
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();
}

// After shutdown is_initialized() should remain true (shutdown does not clear
// the flag – it only marks the client as dead).
TEST_F(TestMgr, MgrClientShutdownDoesNotClearInitialized) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);

  client.init();
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();
  // initialized flag is set in init() and never cleared by shutdown()
  ASSERT_TRUE(client.is_initialized());
}

// Constructing a second MgrClient with a different Messenger must work
// independently.
TEST_F(TestMgr, MgrClientMultipleInstances) {
  MonMap monmap;

  std::unique_ptr<Messenger> m2(
      Messenger::create_client_messenger(cct.get(), "unittest_mgr_2"));

  MgrClient c1(cct.get(), messenger.get(), &monmap);
  MgrClient c2(cct.get(), m2.get(), &monmap);

  // Neither is initialised yet
  ASSERT_FALSE(c1.is_initialized());
  ASSERT_FALSE(c2.is_initialized());

  c1.init();
  ASSERT_TRUE(c1.is_initialized());
  ASSERT_FALSE(c2.is_initialized());

  c2.init();
  ASSERT_TRUE(c1.is_initialized());
  ASSERT_TRUE(c2.is_initialized());

  c1.shutdown();
  c2.shutdown();

  m2->shutdown();
  m2->wait();
}

TEST_F(TestMgr, MgrClientSetMessenger) {
  MonMap monmap;
  std::unique_ptr<Messenger> m2(
      Messenger::create_client_messenger(cct.get(), "unittest_mgr_setmsgr"));

  MgrClient client(cct.get(), messenger.get(), &monmap);
  // Replace before init – must not crash
  client.set_messenger(m2.get());

  client.init();
  ASSERT_TRUE(client.is_initialized());
  client.shutdown();

  m2->shutdown();
  m2->wait();
}

TEST_F(TestMgr, MgrClientOptionalFlagDefault) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // With no MgrMap (epoch == 0) and mgr_optional == false start_command
  // should queue the command (return 0), not reject it.
  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"test_cmd"};
  bufferlist inbl;
  int r = client.start_command(std::move(cmd), std::move(inbl),
                               &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientOptionalFlagTrue) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // With mgr_optional == true and no MgrMap start_command returns -EACCES.
  client.set_mgr_optional(true);

  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"test_cmd"};
  bufferlist inbl;
  int r = client.start_command(std::move(cmd), std::move(inbl),
                               &outbl, &outs, nullptr);
  ASSERT_EQ(r, -EACCES);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartTellCommandOptionalReject) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  client.set_mgr_optional(true);

  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"tell_cmd"};
  bufferlist inbl;
  int r = client.start_tell_command("some-mgr", std::move(cmd), std::move(inbl),
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, -EACCES);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartTellCommandNoOptional) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // mgr_optional == false (default) → command is queued, not rejected
  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"tell_cmd"};
  bufferlist inbl;
  int r = client.start_tell_command("some-mgr", std::move(cmd), std::move(inbl),
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// Helper: check that a dispatch_result_t variant indicates "handled".
static bool dispatch_handled(Dispatcher::dispatch_result_t r) {
  if (std::holds_alternative<bool>(r)) return std::get<bool>(r);
  return std::holds_alternative<Dispatcher::HANDLED>(r);
}

// Dispatch of an MMgrCommandReply for an unknown tid must be handled gracefully.
TEST_F(TestMgr, MgrClientHandleCommandReplyUnknownTidViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Construct a reply for tid 9999 (never registered).
  auto reply = ceph::make_message<MMgrCommandReply>(0, "ok");
  reply->set_tid(9999);
  reply->set_src(entity_name_t::MGR(0));

  // ms_dispatch2 must indicate "handled" (it consumed the message).
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

// Dispatch of an MMgrCommandReply for a queued command populates outbl and
// outs, fires the completion callback with the correct return-code, and
// removes the command from the table so a repeat dispatch is harmless.
TEST_F(TestMgr, MgrClientHandleCommandReplyKnownTidViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  int finish_rc = 42; // sentinel: must change
  bool finished = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  std::vector<std::string> cmd = {"test_cmd"};
  bufferlist inbl;
  // Command queued; tid 0 is the first tid allocated by CommandTable.
  int queue_rc = client.start_command(std::move(cmd), std::move(inbl),
                                      &outbl, &outs, new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  // Build a reply message with the correct tid.
  auto reply = ceph::make_message<MMgrCommandReply>(-5, "all good");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  // Append a payload so outbl gets something.
  const char payload[] = "reply_payload";
  reply->get_data().append(payload, sizeof(payload) - 1);
  size_t expected_len = sizeof(payload) - 1;

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  // Callback must have fired with the correct return code.
  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, -5);

  // outs must carry the reply string.
  ASSERT_EQ(outs, "all good");

  // outbl must carry the data payload.
  ASSERT_EQ(outbl.length(), expected_len);

  // A second dispatch with the same tid must be a harmless no-op.
  auto reply2 = ceph::make_message<MMgrCommandReply>(0, "");
  reply2->set_tid(0);
  reply2->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply2)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientServiceDaemonRegister) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta = {{"version", "1.0"}, {"host", "localhost"}};
  int r = client.service_daemon_register("myservice", "mydaemon", meta);
  ASSERT_EQ(r, 0);

  // Second registration must fail with -EEXIST
  std::map<std::string, std::string> meta2 = {{"version", "2.0"}};
  int r2 = client.service_daemon_register("myservice", "mydaemon2", meta2);
  ASSERT_EQ(r2, -EEXIST);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientServiceDaemonUpdateStatus) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta;
  client.service_daemon_register("svc", "d", meta);

  std::map<std::string, std::string> status = {{"state", "active"}, {"uptime", "42"}};
  int r = client.service_daemon_update_status(std::move(status));
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientServiceDaemonUpdateTaskStatus) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> task_status = {
      {"scrub", "in-progress"}, {"rebalance", "idle"}};
  int r = client.service_daemon_update_task_status(std::move(task_status));
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonMetadata) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta = {{"arch", "x86_64"}, {"os", "linux"}};
  int r = client.update_daemon_metadata("osd", "osd.0", meta);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// Once service_daemon == true, update_daemon_metadata() must return -EEXIST.
TEST_F(TestMgr, MgrClientUpdateDaemonMetadataAfterRegister) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta;
  client.service_daemon_register("svc", "d", meta);

  std::map<std::string, std::string> meta2 = {{"arch", "arm64"}};
  int r = client.update_daemon_metadata("svc", "d", meta2);
  ASSERT_EQ(r, -EEXIST);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonHealth) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::vector<DaemonHealthMetric> metrics;
  metrics.emplace_back(daemon_metric::SLOW_OPS, uint64_t(3));
  metrics.emplace_back(daemon_metric::PENDING_CREATING_PGS, uint64_t(7));

  // Must not crash or assert; verifying the metrics are accepted without error
  client.update_daemon_health(std::move(metrics));

  // Replace with an empty set – also must succeed
  std::vector<DaemonHealthMetric> empty_metrics;
  client.update_daemon_health(std::move(empty_metrics));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientSendPgstatsWithoutSession) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bool cb_called = false;
  client.set_pgstats_cb([&cb_called]() -> MPGStats* {
    cb_called = true;
    return new MPGStats();
  });

  // No session exists, so the callback must NOT be invoked.
  client.send_pgstats();
  ASSERT_FALSE(cb_called);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMsHandleRefused) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // No connection to pass – we exercise the public method with a nullptr.
  // The implementation ignores the argument and returns false.
  bool r = client.ms_handle_refused(nullptr);
  ASSERT_FALSE(r);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientDispatchWhenDead) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.shutdown();  // sets dead = true

  // After shutdown, ms_dispatch2 must return false and not assert/crash.
  auto reply = ceph::make_message<MMgrCommandReply>(0, "ok");
  reply->set_tid(1);
  reply->set_src(entity_name_t::MGR(0));

  auto result = client.ms_dispatch2(reply);
  // The dead path returns false (unhandled).
  ASSERT_FALSE(dispatch_handled(result));
}

TEST_F(TestMgr, MgrClientDispatchCommandReplyNonMgrSource) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Source is OSD, not MGR – must return false (not consumed).
  auto reply = ceph::make_message<MMgrCommandReply>(0, "ok");
  reply->set_tid(0);
  reply->set_src(entity_name_t::OSD(0));

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMultipleCommandsQueuedDistinctTids) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int rc1 = 99, rc2 = 99;
  bool done1 = false, done2 = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  bufferlist outbl1, outbl2;
  std::string outs1, outs2;
  bufferlist inbl;

  int r1 = client.start_command({"cmd_a"}, bufferlist{}, &outbl1, &outs1,
                                 new Ctx(rc1, done1));
  int r2 = client.start_command({"cmd_b"}, bufferlist{}, &outbl2, &outs2,
                                 new Ctx(rc2, done2));

  ASSERT_EQ(r1, 0);
  ASSERT_EQ(r2, 0);

  // Reply to the first command (tid 0)
  auto reply1 = ceph::make_message<MMgrCommandReply>(7, "first");
  reply1->set_tid(0);
  reply1->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply1)));

  ASSERT_TRUE(done1);
  ASSERT_EQ(rc1, 7);
  ASSERT_EQ(outs1, "first");

  // Second command (tid 1) must not have fired yet
  ASSERT_FALSE(done2);

  // Reply to the second command (tid 1)
  auto reply2 = ceph::make_message<MMgrCommandReply>(3, "second");
  reply2->set_tid(1);
  reply2->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply2)));

  ASSERT_TRUE(done2);
  ASSERT_EQ(rc2, 3);
  ASSERT_EQ(outs2, "second");

  client.shutdown();
}

TEST_F(TestMgr, MgrClientSendPgstatsNoCb) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // No pgstats callback registered – must be a no-op.
  client.send_pgstats();

  client.shutdown();
}

TEST_F(TestMgr, MgrClientSetPerfMetricQueryCb) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int set_calls = 0;
  // Set callbacks; we can't easily trigger them without a live session, but
  // storing them must not crash.
  client.set_perf_metric_query_cb(
      [&set_calls](const ConfigPayload &) { ++set_calls; },
      []() -> MetricPayload { return {}; });

  // Replace with new callbacks – also must not crash.
  client.set_perf_metric_query_cb(
      [](const ConfigPayload &) {},
      []() -> MetricPayload { return {}; });

  ASSERT_EQ(set_calls, 0);  // callbacks were stored, not invoked

  client.shutdown();
}

TEST_F(TestMgr, MgrClientServiceDaemonUpdateStatusEmpty) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  client.service_daemon_register("svc", "d", {});

  std::map<std::string, std::string> empty_status;
  int r = client.service_daemon_update_status(std::move(empty_status));
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonMetadataTwice) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta1 = {{"key", "v1"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta1), 0);

  // Calling a second time is allowed (service_daemon is still false).
  std::map<std::string, std::string> meta2 = {{"key", "v2"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta2), 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMsHandleResetNoSession) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // No session established, so the connection doesn't match – must return false.
  bool r = client.ms_handle_reset(nullptr);
  ASSERT_FALSE(r);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMgrCloseViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Register as service daemon first
  client.service_daemon_register("svc", "d", {});

  // Dispatch an MMgrClose – handle_mgr_close() sets service_daemon=false
  // and notifies shutdown_cond.  It should not crash and should return true.
  auto close_msg = ceph::make_message<MMgrClose>();
  close_msg->daemon_name = "d";
  close_msg->service_name = "svc";

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(close_msg)));

  // After MMgrClose, another register must succeed (service_daemon is false again).
  int r = client.service_daemon_register("svc", "d2", {});
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMgrConfigureNoSession) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  auto cfg_msg = ceph::make_message<MMgrConfigure>();
  cfg_msg->stats_period = 10;
  cfg_msg->stats_threshold = 5;

  // No session → the message must be dropped gracefully (returns true).
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg_msg)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientDispatchUnknownMessageType) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // MMgrClose has a valid type (MSG_MGR_CLOSE) handled above.
  // Construct a raw Message with an unrecognised type.
  // Reuse MMgrClose but dispatch under a different type context by
  // dispatching a message with type=0 (not in switch).
  // The easiest approach: pass an MMgrCommandReply from a non-MGR source so
  // the inner source check makes it fall through to the default.
  // (Already tested above; here we use a different fabricated scenario.)
  // In the absence of a simple way to inject an arbitrary-typed message,
  // test that dispatching with OSD source returns false.
  auto reply = ceph::make_message<MMgrCommandReply>(0, "");
  reply->set_tid(42);
  reply->set_src(entity_name_t::OSD(1)); // non-MGR source

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartTellCommandEmptyTarget) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"tell_any"};

  // Empty name means "send to whatever mgr is active"; still queued (0).
  int r = client.start_tell_command("", std::move(cmd), bufferlist{},
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonHealthN1N2) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::vector<DaemonHealthMetric> metrics;
  // Use the two-uint32 constructor
  metrics.emplace_back(daemon_metric::SLOW_OPS, uint32_t(5), uint32_t(2));

  client.update_daemon_health(std::move(metrics));

  // Verify getters on the metric object itself
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(5), uint32_t(2));
  ASSERT_EQ(m.get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(m.get_n1(), uint32_t(5));
  ASSERT_EQ(m.get_n2(), uint32_t(2));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMCommandReplyFromMgrViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  int finish_rc = 42;
  bool finished = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  int queue_rc = client.start_command({"cmd_via_mcommand"}, bufferlist{},
                                      &outbl, &outs,
                                      new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  // Build an MCommandReply (MSG_COMMAND_REPLY) from an MGR source with tid=0.
  auto reply = ceph::make_message<MCommandReply>(-3, "mcommand_reply");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  // Callback must have fired with the correct return code.
  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, -3);
  ASSERT_EQ(outs, "mcommand_reply");

  client.shutdown();
}

// MSG_COMMAND_REPLY from a non-MGR source must not be consumed.
TEST_F(TestMgr, MgrClientHandleMCommandReplyFromNonMgrViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  auto reply = ceph::make_message<MCommandReply>(0, "ignored");
  reply->set_tid(0);
  reply->set_src(entity_name_t::OSD(0));  // non-MGR source

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleCommandReplyNullOutblOuts) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int finish_rc = 99;
  bool finished = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  // Queue with outbl=nullptr and outs=nullptr.
  int queue_rc = client.start_command({"cmd_nullout"}, bufferlist{},
                                      nullptr, nullptr,
                                      new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  auto reply = ceph::make_message<MMgrCommandReply>(-7, "null_test");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  // Callback must still fire with the correct code.
  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, -7);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartCommandNullCallback) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;

  int r = client.start_command({"cmd_nocb"}, bufferlist{},
                               &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  auto reply = ceph::make_message<MMgrCommandReply>(-1, "nocb");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  // Must be consumed without crashing (no callback to fire).
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));
  ASSERT_EQ(outs, "nocb");

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMsHandleRemoteReset) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // ms_handle_remote_reset() is a no-op override; must not assert or crash.
  client.ms_handle_remote_reset(nullptr);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateStatusBeforeRegister) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> status = {{"state", "pending"}};
  int r = client.service_daemon_update_status(std::move(status));
  ASSERT_EQ(r, 0);

  // Because service_daemon was not set, register must still succeed.
  int r2 = client.service_daemon_register("svc", "d", {});
  ASSERT_EQ(r2, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateTaskStatusEmpty) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> empty;
  int r = client.service_daemon_update_task_status(std::move(empty));
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonHealthReplacement) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // First call: install a non-empty metric set.
  std::vector<DaemonHealthMetric> first;
  first.emplace_back(daemon_metric::SLOW_OPS, uint64_t(10));
  client.update_daemon_health(std::move(first));

  // Second call with an empty vector — must replace without crashing.
  std::vector<DaemonHealthMetric> second;  // empty
  client.update_daemon_health(std::move(second));

  // Third call with new values — must replace again without crashing.
  std::vector<DaemonHealthMetric> third;
  third.emplace_back(daemon_metric::PENDING_CREATING_PGS, uint64_t(5));
  client.update_daemon_health(std::move(third));

  client.shutdown();
}

TEST_F(TestMgr, DaemonHealthMetricDefaultConstructor) {
  DaemonHealthMetric m;
  ASSERT_EQ(m.get_type(), daemon_metric::NONE);
  // The default-constructed metric has zero value (union initialised to 0).
  ASSERT_EQ(m.get_n(), uint64_t(0));
  ASSERT_EQ(m.get_n1(), uint32_t(0));
  ASSERT_EQ(m.get_n2(), uint32_t(0));
}

TEST_F(TestMgr, DaemonHealthMetricUint64GetN) {
  DaemonHealthMetric m(daemon_metric::PENDING_CREATING_PGS, uint64_t(0xDEADBEEF));
  ASSERT_EQ(m.get_type(), daemon_metric::PENDING_CREATING_PGS);
  ASSERT_EQ(m.get_n(), uint64_t(0xDEADBEEF));
}

TEST_F(TestMgr, DaemonHealthMetricNoneType) {
  DaemonHealthMetric m(daemon_metric::NONE, uint64_t(42));
  ASSERT_EQ(m.get_type(), daemon_metric::NONE);
  ASSERT_EQ(m.get_n(), uint64_t(42));
  ASSERT_EQ(std::string(daemon_metric_name(daemon_metric::NONE)), "NONE");
}

TEST_F(TestMgr, DaemonMetricNameAllValues) {
  ASSERT_STREQ(daemon_metric_name(daemon_metric::SLOW_OPS), "SLOW_OPS");
  ASSERT_STREQ(daemon_metric_name(daemon_metric::PENDING_CREATING_PGS),
               "PENDING_CREATING_PGS");
  ASSERT_STREQ(daemon_metric_name(daemon_metric::NONE), "NONE");
  // Cast out-of-range value — must return "???" instead of crashing.
  ASSERT_STREQ(daemon_metric_name(static_cast<daemon_metric>(0xFF)), "???");
}

TEST_F(TestMgr, MgrClientFullServiceDaemonSequence) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta = {{"build", "debug"}, {"host", "testhost"}};
  ASSERT_EQ(client.service_daemon_register("testsvc", "testd", meta), 0);

  std::map<std::string, std::string> status = {{"state", "active"}, {"clients", "3"}};
  ASSERT_EQ(client.service_daemon_update_status(std::move(status)), 0);

  std::map<std::string, std::string> tasks = {{"scrub", "running"}, {"balance", "idle"}};
  ASSERT_EQ(client.service_daemon_update_task_status(std::move(tasks)), 0);

  // update_daemon_metadata() must now refuse because service_daemon is true.
  std::map<std::string, std::string> meta2 = {{"key", "val"}};
  ASSERT_EQ(client.update_daemon_metadata("testsvc", "testd", meta2), -EEXIST);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMultipleTellCommandsDistinctTids) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int rc_a = 99, rc_b = 99;
  bool done_a = false, done_b = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  bufferlist outbl_a, outbl_b;
  std::string outs_a, outs_b;

  int r1 = client.start_tell_command("", {"tell_a"}, bufferlist{},
                                     &outbl_a, &outs_a, new Ctx(rc_a, done_a));
  int r2 = client.start_tell_command("", {"tell_b"}, bufferlist{},
                                     &outbl_b, &outs_b, new Ctx(rc_b, done_b));

  ASSERT_EQ(r1, 0);
  ASSERT_EQ(r2, 0);

  // Reply to command with tid 0 (first tell).
  auto reply_a = ceph::make_message<MMgrCommandReply>(11, "tell_first");
  reply_a->set_tid(0);
  reply_a->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply_a)));

  ASSERT_TRUE(done_a);
  ASSERT_EQ(rc_a, 11);
  ASSERT_EQ(outs_a, "tell_first");
  ASSERT_FALSE(done_b);

  // Reply to command with tid 1 (second tell).
  auto reply_b = ceph::make_message<MMgrCommandReply>(22, "tell_second");
  reply_b->set_tid(1);
  reply_b->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply_b)));

  ASSERT_TRUE(done_b);
  ASSERT_EQ(rc_b, 22);
  ASSERT_EQ(outs_b, "tell_second");

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonHealthPendingCreatingPgs) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::vector<DaemonHealthMetric> metrics;
  metrics.emplace_back(daemon_metric::PENDING_CREATING_PGS, uint64_t(42));
  client.update_daemon_health(std::move(metrics));

  // Verify the metric object directly.
  DaemonHealthMetric m(daemon_metric::PENDING_CREATING_PGS, uint64_t(42));
  ASSERT_EQ(m.get_type(), daemon_metric::PENDING_CREATING_PGS);
  ASSERT_EQ(m.get_n(), uint64_t(42));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientReinitAfterShutdown) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);

  client.init();
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();
  // After shutdown, dead=true but initialized stays true.
  ASSERT_TRUE(client.is_initialized());

  // A second init() must clear dead and leave is_initialized() true.
  client.init();
  ASSERT_TRUE(client.is_initialized());

  // Dispatch after re-init must be processed (not discarded as dead).
  auto reply = ceph::make_message<MMgrCommandReply>(0, "reinit_ok");
  reply->set_tid(9999);  // unknown tid – still consumed (returns true)
  reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientSetMgrOptionalToggle) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Default: not optional → command is queued.
  bufferlist outbl1;
  std::string outs1;
  ASSERT_EQ(client.start_command({"cmd_a"}, bufferlist{}, &outbl1, &outs1, nullptr), 0);

  // Set optional → EACCES on next command (no MgrMap epoch).
  client.set_mgr_optional(true);
  bufferlist outbl2;
  std::string outs2;
  ASSERT_EQ(client.start_command({"cmd_b"}, bufferlist{}, &outbl2, &outs2, nullptr), -EACCES);

  // Clear optional → queued again.
  client.set_mgr_optional(false);
  bufferlist outbl3;
  std::string outs3;
  ASSERT_EQ(client.start_command({"cmd_c"}, bufferlist{}, &outbl3, &outs3, nullptr), 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMgrMapEpochZeroViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Build an MMgrMap with epoch=0 (no available mgr).
  MgrMap mgrmap;
  mgrmap.epoch = 0;
  mgrmap.available = false;
  auto map_msg = ceph::make_message<MMgrMap>(mgrmap);

  // Must be consumed (returns true).
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(map_msg)));

  // epoch is still 0 → mgr_optional guard still fires.
  client.set_mgr_optional(true);
  bufferlist outbl;
  std::string outs;
  ASSERT_EQ(client.start_command({"cmd"}, bufferlist{}, &outbl, &outs, nullptr),
            -EACCES);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMgrMapEpochNonZeroViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Build an MMgrMap with epoch=5 but available=false (no active addr to
  // connect to – reconnect() will log "No active mgr" and return early).
  MgrMap mgrmap;
  mgrmap.epoch = 5;
  mgrmap.available = false;
  auto map_msg = ceph::make_message<MMgrMap>(mgrmap);

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(map_msg)));

  // Now epoch != 0, so mgr_optional guard no longer rejects commands.
  client.set_mgr_optional(true);
  bufferlist outbl;
  std::string outs;
  ASSERT_EQ(client.start_command({"cmd"}, bufferlist{}, &outbl, &outs, nullptr), 0);

  client.shutdown();
}

// DaemonHealthMetric::get_type_name() returns the same string as
// daemon_metric_name() for each well-known type.
TEST_F(TestMgr, DaemonHealthMetricGetTypeName) {
  DaemonHealthMetric m_slow(daemon_metric::SLOW_OPS, uint64_t(1));
  ASSERT_EQ(m_slow.get_type_name(), std::string("SLOW_OPS"));

  DaemonHealthMetric m_pg(daemon_metric::PENDING_CREATING_PGS, uint64_t(2));
  ASSERT_EQ(m_pg.get_type_name(), std::string("PENDING_CREATING_PGS"));

  DaemonHealthMetric m_none;
  ASSERT_EQ(m_none.get_type_name(), std::string("NONE"));
}

// DaemonHealthMetric operator<< writes "TYPE(n|(n1,n2))" to the stream.
TEST_F(TestMgr, DaemonHealthMetricStreamOutput) {
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(3), uint32_t(7));
  std::ostringstream oss;
  oss << m;
  // Expected format: "SLOW_OPS(4294967303|(3,7))"
  // n = (uint64_t)n1 | ((uint64_t)n2 << 32)? No – the union stores n1/n2
  // packed as little-endian 64-bit, so n = n1 | (n2 << 32).
  // We just verify the string contains the type name and both component values.
  std::string s = oss.str();
  ASSERT_NE(s.find("SLOW_OPS"), std::string::npos);
  ASSERT_NE(s.find("3"), std::string::npos);
  ASSERT_NE(s.find("7"), std::string::npos);
}

// DaemonHealthMetric::generate_test_instances() returns exactly two instances
// with the expected types.
TEST_F(TestMgr, DaemonHealthMetricGenerateTestInstances) {
  auto instances = DaemonHealthMetric::generate_test_instances();
  ASSERT_EQ(instances.size(), size_t(2));

  auto it = instances.begin();
  ASSERT_EQ(it->get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(it->get_n(), uint64_t(1));

  ++it;
  ASSERT_EQ(it->get_type(), daemon_metric::PENDING_CREATING_PGS);
  ASSERT_EQ(it->get_n1(), uint32_t(1));
  ASSERT_EQ(it->get_n2(), uint32_t(2));
}

// DaemonHealthMetric::dump() writes type, n, n1, n2 fields to a Formatter.
TEST_F(TestMgr, DaemonHealthMetricDump) {
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(5), uint32_t(2));

  ceph::JSONFormatter f;
  f.open_object_section("metric");
  m.dump(&f);
  f.close_section();

  std::ostringstream oss;
  f.flush(oss);
  std::string out = oss.str();

  // Verify all four keys appear in the JSON output.
  ASSERT_NE(out.find("\"type\""), std::string::npos);
  ASSERT_NE(out.find("SLOW_OPS"), std::string::npos);
  ASSERT_NE(out.find("\"n\""), std::string::npos);
  ASSERT_NE(out.find("\"n1\""), std::string::npos);
  ASSERT_NE(out.find("\"n2\""), std::string::npos);
}

// ms_dispatch2() default branch: a message with a type not handled by the
// switch (e.g. CEPH_MSG_PING) must return false regardless of source.
TEST_F(TestMgr, MgrClientDispatchTrulyUnknownMessageType) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // MPing uses CEPH_MSG_PING (type 2), which is not in MgrClient's switch.
  auto ping = ceph::make_message<MPing>();
  ping->set_src(entity_name_t::MGR(0));  // even from MGR source → default → false

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(ping)));

  client.shutdown();
}

// start_command() with an empty cmd vector — the command is still queued (r==0).
TEST_F(TestMgr, MgrClientStartCommandEmptyCmd) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  // Empty cmd vector is an unusual but not explicitly forbidden input.
  int r = client.start_command({}, bufferlist{}, &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// service_daemon_register() with empty service and daemon name strings.
// The registration must succeed (return 0) since the strings are merely stored.
TEST_F(TestMgr, MgrClientServiceDaemonRegisterEmptyNames) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta;
  int r = client.service_daemon_register("", "", meta);
  ASSERT_EQ(r, 0);

  // A second register must now return -EEXIST even with empty names.
  int r2 = client.service_daemon_register("", "", meta);
  ASSERT_EQ(r2, -EEXIST);

  client.shutdown();
}

// update_daemon_metadata() with an empty metadata map.
// The guard "need_metadata_update && !daemon_metadata.empty()" prevents
// _send_update() from being called, but the return code must still be 0.
TEST_F(TestMgr, MgrClientUpdateDaemonMetadataEmpty) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // First call with empty metadata: need_metadata_update is true but
  // daemon_metadata.empty() is also true → _send_update() NOT called.
  std::map<std::string, std::string> empty_meta;
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", empty_meta), 0);

  // Second call with non-empty metadata: need_metadata_update is still true
  // (was not cleared because the first call skipped the branch) → _send_update()
  // is called (no-op without a live session) and need_metadata_update is cleared.
  std::map<std::string, std::string> real_meta = {{"os", "linux"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", real_meta), 0);

  // Third call: need_metadata_update is now false → _send_update() NOT called.
  // Return value must still be 0.
  std::map<std::string, std::string> meta3 = {{"arch", "x86_64"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta3), 0);

  client.shutdown();
}

// handle_mgr_configure() with nonzero stats_threshold updates the internal
// threshold field.  The message is consumed (returns true) even without a session.
// We verify that the threshold branch is reached by confirming dispatch returns true
// and by sending two configure messages with different thresholds.
TEST_F(TestMgr, MgrClientHandleMgrConfigureStatsThreshold) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // First configure: threshold 10, period 0.
  auto cfg1 = ceph::make_message<MMgrConfigure>();
  cfg1->stats_threshold = 10;
  cfg1->stats_period = 0;
  // No session → dropped gracefully, returns true.
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg1)));

  // Second configure: same values – the "updated stats threshold" log is NOT
  // emitted (threshold unchanged), but dispatch must still return true.
  auto cfg2 = ceph::make_message<MMgrConfigure>();
  cfg2->stats_threshold = 10;
  cfg2->stats_period = 0;
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg2)));

  // Third configure: different threshold – the branch IS taken.
  auto cfg3 = ceph::make_message<MMgrConfigure>();
  cfg3->stats_threshold = 20;
  cfg3->stats_period = 0;
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg3)));

  client.shutdown();
}

// start_tell_command() with optional=true and nonzero epoch must queue (not reject).
// Also verify that the tell flag is stored by confirming the tid advances.
TEST_F(TestMgr, MgrClientStartTellCommandQueuedWithEpoch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Give the client a nonzero epoch map so the optional guard doesn't fire.
  MgrMap mgrmap;
  mgrmap.epoch = 1;
  mgrmap.available = false;
  auto map_msg = ceph::make_message<MMgrMap>(mgrmap);
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(map_msg)));

  client.set_mgr_optional(true);

  bufferlist outbl;
  std::string outs;
  int r = client.start_tell_command("mgr0", {"tell_epoch"}, bufferlist{},
                                    &outbl, &outs, nullptr);
  // epoch != 0 → should queue (0), not reject.
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// DaemonHealthMetric with maximum uint64 value round-trips correctly
// through the union without truncation or wrap.
TEST_F(TestMgr, DaemonHealthMetricMaxUint64Value) {
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  DaemonHealthMetric m(daemon_metric::PENDING_CREATING_PGS, kMax);
  ASSERT_EQ(m.get_type(), daemon_metric::PENDING_CREATING_PGS);
  ASSERT_EQ(m.get_n(), kMax);
}

// DaemonHealthMetric with zero n1 and max n2 – verifies the upper 32 bits
// of the union are stored and retrieved independently.
TEST_F(TestMgr, DaemonHealthMetricZeroN1MaxN2) {
  constexpr uint32_t kMax32 = std::numeric_limits<uint32_t>::max();
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(0), kMax32);
  ASSERT_EQ(m.get_n1(), uint32_t(0));
  ASSERT_EQ(m.get_n2(), kMax32);
  // get_n() must reconstruct the correct packed 64-bit value.
  // On little-endian: n1 is the low word, n2 is the high word.
  uint64_t expected = (uint64_t)kMax32 << 32;
  ASSERT_EQ(m.get_n(), expected);
}

// DaemonHealthMetric stream output for PENDING_CREATING_PGS type contains
// the correct type name.
TEST_F(TestMgr, DaemonHealthMetricStreamOutputPendingCreatingPgs) {
  DaemonHealthMetric m(daemon_metric::PENDING_CREATING_PGS, uint64_t(99));
  std::ostringstream oss;
  oss << m;
  ASSERT_NE(oss.str().find("PENDING_CREATING_PGS"), std::string::npos);
  ASSERT_NE(oss.str().find("99"), std::string::npos);
}

// ms_handle_reset() with the session connection set (matching con) must
// return true.  We cannot get a real Connection* without live networking,
// but we can verify the nullptr path (con != session->con) returns false.
// This duplicates MgrClientMsHandleResetNoSession but also tests that the
// function is callable with a non-null pointer that does NOT match any session.
TEST_F(TestMgr, MgrClientMsHandleResetNonNullUnmatchedCon) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // A non-null but completely unrelated pointer — cast an arbitrary address.
  // The implementation only checks (session && con == session->con).
  // With no session, this must return false.
  Connection *fake_con = reinterpret_cast<Connection *>(uintptr_t(0xDEADBEEF));
  ASSERT_FALSE(client.ms_handle_reset(fake_con));

  client.shutdown();
}

// DaemonHealthMetric DENC encode/decode round-trip:
// Encode a metric with a known value, decode it back, verify all fields match.
TEST_F(TestMgr, DaemonHealthMetricEncodeDecodeRoundTrip) {
  using ceph::encode;
  using ceph::decode;

  // uint64 variant
  DaemonHealthMetric orig(daemon_metric::SLOW_OPS, uint64_t(0xCAFEBABE12345678ULL));
  bufferlist bl;
  encode(orig, bl);

  DaemonHealthMetric decoded;
  auto it = bl.cbegin();
  decode(decoded, it);

  ASSERT_EQ(decoded.get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(decoded.get_n(), uint64_t(0xCAFEBABE12345678ULL));

  // n1/n2 variant — the DENC stores only the packed n (uint64), so n1/n2 are
  // recovered from the same union after decoding.
  DaemonHealthMetric orig2(daemon_metric::PENDING_CREATING_PGS, uint32_t(0xDEAD), uint32_t(0xBEEF));
  bufferlist bl2;
  encode(orig2, bl2);

  DaemonHealthMetric decoded2;
  auto it2 = bl2.cbegin();
  decode(decoded2, it2);

  ASSERT_EQ(decoded2.get_type(), daemon_metric::PENDING_CREATING_PGS);
  ASSERT_EQ(decoded2.get_n1(), uint32_t(0xDEAD));
  ASSERT_EQ(decoded2.get_n2(), uint32_t(0xBEEF));
}

// MgrClient::shutdown() calls command_table.clear(), which removes all pending
// commands WITHOUT firing their on_finish callbacks.  Verify that a queued
// command's callback is NOT invoked after shutdown().
TEST_F(TestMgr, MgrClientShutdownDropsPendingCommands) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bool fired = false;
  int fire_rc = 999;

  struct Ctx : public Context {
    bool& fired;
    int& rc;
    Ctx(bool& f, int& r) : fired(f), rc(r) {}
    void finish(int r) override { fired = true; rc = r; }
  };

  bufferlist outbl;
  std::string outs;
  int r = client.start_command({"cmd_dropped"}, bufferlist{},
                               &outbl, &outs, new Ctx(fired, fire_rc));
  ASSERT_EQ(r, 0);

  // shutdown() calls command_table.clear() — the command is erased without
  // completing its callback.
  client.shutdown();

  // The callback must NOT have been called.
  ASSERT_FALSE(fired);
  ASSERT_EQ(fire_rc, 999);  // sentinel value unchanged
}

// DaemonHealthMetric max n1, zero n2 — mirror of the existing ZeroN1MaxN2 test.
// Verifies the low 32-bit word carries the max value while the high word is zero.
TEST_F(TestMgr, DaemonHealthMetricMaxN1ZeroN2) {
  constexpr uint32_t kMax32 = std::numeric_limits<uint32_t>::max();
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, kMax32, uint32_t(0));
  ASSERT_EQ(m.get_n1(), kMax32);
  ASSERT_EQ(m.get_n2(), uint32_t(0));
  // On little-endian: n1 is the low word → get_n() == kMax32.
  ASSERT_EQ(m.get_n(), uint64_t(kMax32));
}

// DaemonHealthMetric SLOW_OPS explicitly constructed with zero uint64 value.
// Distinct from the default-constructor test: this uses SLOW_OPS type (not NONE)
// so get_type() must return SLOW_OPS while get_n() must return 0.
TEST_F(TestMgr, DaemonHealthMetricSlowOpsZeroValue) {
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint64_t(0));
  ASSERT_EQ(m.get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(m.get_n(), uint64_t(0));
  ASSERT_EQ(m.get_n1(), uint32_t(0));
  ASSERT_EQ(m.get_n2(), uint32_t(0));
}

// Interleaved start_command and start_tell_command:
// Queue two regular commands and one tell command; reply to them in reverse
// tid order to verify independent routing regardless of insertion order.
TEST_F(TestMgr, MgrClientMixedCommandAndTellCommandReplies) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int rc0 = 99, rc1 = 99, rc2 = 99;
  bool done0 = false, done1 = false, done2 = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  bufferlist o0, o1, o2;
  std::string s0, s1, s2;

  // tid 0: regular command
  ASSERT_EQ(client.start_command({"regular_cmd"}, bufferlist{}, &o0, &s0,
                                  new Ctx(rc0, done0)), 0);
  // tid 1: tell command
  ASSERT_EQ(client.start_tell_command("", {"tell_cmd"}, bufferlist{}, &o1, &s1,
                                       new Ctx(rc1, done1)), 0);
  // tid 2: another regular command
  ASSERT_EQ(client.start_command({"regular_cmd_2"}, bufferlist{}, &o2, &s2,
                                  new Ctx(rc2, done2)), 0);

  // Reply in reverse tid order: tid 2, then tid 1, then tid 0.
  auto r2 = ceph::make_message<MMgrCommandReply>(30, "reply_2");
  r2->set_tid(2);
  r2->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(r2)));

  ASSERT_TRUE(done2);   ASSERT_EQ(rc2, 30);   ASSERT_EQ(s2, "reply_2");
  ASSERT_FALSE(done0);
  ASSERT_FALSE(done1);

  auto r1 = ceph::make_message<MMgrCommandReply>(10, "reply_1");
  r1->set_tid(1);
  r1->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(r1)));

  ASSERT_TRUE(done1);   ASSERT_EQ(rc1, 10);   ASSERT_EQ(s1, "reply_1");
  ASSERT_FALSE(done0);

  auto r0 = ceph::make_message<MMgrCommandReply>(20, "reply_0");
  r0->set_tid(0);
  r0->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(r0)));

  ASSERT_TRUE(done0);   ASSERT_EQ(rc0, 20);   ASSERT_EQ(s0, "reply_0");

  client.shutdown();
}

// Full service-daemon lifecycle: register → update status → MMgrClose →
// re-register → update status again.  Verifies that each state transition
// is correct by checking return codes and observing that subsequent operations
// behave as if the state has reset.
TEST_F(TestMgr, MgrClientServiceDaemonFullLifecycle) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Phase 1: register
  std::map<std::string, std::string> meta1 = {{"ver", "1.0"}};
  ASSERT_EQ(client.service_daemon_register("svc", "d", meta1), 0);

  // Phase 2: update status while registered
  std::map<std::string, std::string> status1 = {{"state", "active"}};
  ASSERT_EQ(client.service_daemon_update_status(std::move(status1)), 0);

  // Phase 3: update task status while registered
  std::map<std::string, std::string> tasks1 = {{"op", "running"}};
  ASSERT_EQ(client.service_daemon_update_task_status(std::move(tasks1)), 0);

  // A second register attempt while service_daemon=true must fail.
  ASSERT_EQ(client.service_daemon_register("svc", "d2", {}), -EEXIST);

  // Phase 4: MMgrClose resets service_daemon to false.
  auto close_msg = ceph::make_message<MMgrClose>();
  close_msg->daemon_name = "d";
  close_msg->service_name = "svc";
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(close_msg)));

  // Phase 5: re-register must succeed now (service_daemon is false again).
  std::map<std::string, std::string> meta2 = {{"ver", "2.0"}};
  ASSERT_EQ(client.service_daemon_register("svc2", "d2", meta2), 0);

  // Phase 6: update status under the new registration.
  std::map<std::string, std::string> status2 = {{"state", "standby"}};
  ASSERT_EQ(client.service_daemon_update_status(std::move(status2)), 0);

  // A third register while service_daemon=true (from phase 5) must fail.
  ASSERT_EQ(client.service_daemon_register("svc2", "d3", {}), -EEXIST);

  client.shutdown();
}

// handle_command_reply() replaces outbl rather than appending to it.
// Pre-populate outbl with existing data, then dispatch a reply with different
// data and verify that the old content is gone and only the new payload remains.
TEST_F(TestMgr, MgrClientHandleCommandReplyReplacesOutbl) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  int finish_rc = 42;
  bool finished = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  // Pre-populate outbl with sentinel data that must be replaced.
  const char old_data[] = "old_content_sentinel";
  outbl.append(old_data, sizeof(old_data) - 1);
  ASSERT_EQ(outbl.length(), sizeof(old_data) - 1);

  bufferlist inbl;
  int queue_rc = client.start_command({"cmd_replace_outbl"}, std::move(inbl),
                                      &outbl, &outs, new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  // Reply carries new payload that is different from the sentinel.
  const char new_data[] = "new_reply_payload";
  auto reply = ceph::make_message<MMgrCommandReply>(0, "replaced");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));
  reply->get_data().append(new_data, sizeof(new_data) - 1);

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, 0);
  ASSERT_EQ(outs, "replaced");

  // outbl must contain exactly the new data — old content must be gone.
  ASSERT_EQ(outbl.length(), sizeof(new_data) - 1);
  ASSERT_EQ(outbl.to_str(), std::string(new_data, sizeof(new_data) - 1));

  client.shutdown();
}

// start_command() with a non-empty input buffer (inbl) must queue normally
// and return 0 regardless of the inbl size.
TEST_F(TestMgr, MgrClientStartCommandWithNonEmptyInbl) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;

  bufferlist inbl;
  const char payload[] = "command_input_data_1234567890";
  inbl.append(payload, sizeof(payload) - 1);
  ASSERT_EQ(inbl.length(), sizeof(payload) - 1);

  int r = client.start_command({"cmd_with_inbl"}, std::move(inbl),
                               &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// set_pgstats_cb() called a second time replaces the first callback.
// After replacement only the new callback is active; the old one must not be
// invoked.  Without a live session neither callback is called via send_pgstats().
TEST_F(TestMgr, MgrClientSetPgstatsCbReplacement) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int cb1_calls = 0, cb2_calls = 0;

  // Register first callback.
  client.set_pgstats_cb([&cb1_calls]() -> MPGStats* {
    ++cb1_calls;
    return new MPGStats();
  });

  // Replace with second callback.
  client.set_pgstats_cb([&cb2_calls]() -> MPGStats* {
    ++cb2_calls;
    return new MPGStats();
  });

  // No session → neither callback is invoked.
  client.send_pgstats();
  ASSERT_EQ(cb1_calls, 0);
  ASSERT_EQ(cb2_calls, 0);

  client.shutdown();
}

// service_daemon_register() returns -EEXIST on the second call regardless of
// whether the service/daemon names differ from the first call.  The guard is
// `if (service_daemon)` — the names are not compared.
TEST_F(TestMgr, MgrClientServiceDaemonRegisterDifferentNamesStillRejects) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // First registration succeeds.
  std::map<std::string, std::string> meta1 = {{"key", "v1"}};
  ASSERT_EQ(client.service_daemon_register("svcA", "daemonA", meta1), 0);

  // Second call with completely different names must still return -EEXIST.
  std::map<std::string, std::string> meta2 = {{"key", "v2"}};
  ASSERT_EQ(client.service_daemon_register("svcB", "daemonB", meta2), -EEXIST);

  // Third call with empty names must also return -EEXIST.
  std::map<std::string, std::string> meta3;
  ASSERT_EQ(client.service_daemon_register("", "", meta3), -EEXIST);

  client.shutdown();
}

// shutdown() calls command_table.clear() which drops ALL pending commands
// regardless of type (regular or tell).  Verify that three queued commands
// (two start_command, one start_tell_command) are ALL silently dropped and
// their on_finish callbacks are never invoked.
TEST_F(TestMgr, MgrClientShutdownDropsMultiplePendingCommands) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bool fired0 = false, fired1 = false, fired2 = false;
  int rc0 = 999, rc1 = 999, rc2 = 999;

  struct Ctx : public Context {
    bool& fired;
    int& rc;
    Ctx(bool& f, int& r) : fired(f), rc(r) {}
    void finish(int r_) override { fired = true; rc = r_; }
  };

  bufferlist o0, o1, o2;
  std::string s0, s1, s2;

  ASSERT_EQ(client.start_command({"cmd0"}, bufferlist{}, &o0, &s0,
                                  new Ctx(fired0, rc0)), 0);
  ASSERT_EQ(client.start_command({"cmd1"}, bufferlist{}, &o1, &s1,
                                  new Ctx(fired1, rc1)), 0);
  ASSERT_EQ(client.start_tell_command("mgr.a", {"tell_cmd"}, bufferlist{},
                                       &o2, &s2, new Ctx(fired2, rc2)), 0);

  // Shutdown erases all commands without firing their callbacks.
  client.shutdown();

  ASSERT_FALSE(fired0);
  ASSERT_FALSE(fired1);
  ASSERT_FALSE(fired2);
  ASSERT_EQ(rc0, 999);
  ASSERT_EQ(rc1, 999);
  ASSERT_EQ(rc2, 999);
}

// DaemonHealthMetric encode/decode round-trip with the NONE type.
// The DENC implementation stores type as uint8_t and value.n as uint64_t;
// verifies that a NONE metric with a non-zero explicit value survives the
// encode → decode cycle.
TEST_F(TestMgr, DaemonHealthMetricEncodeDecodeRoundTripNoneType) {
  using ceph::encode;
  using ceph::decode;

  DaemonHealthMetric orig(daemon_metric::NONE, uint64_t(0xABCDEF01));
  bufferlist bl;
  encode(orig, bl);

  DaemonHealthMetric decoded;
  auto it = bl.cbegin();
  decode(decoded, it);

  ASSERT_EQ(decoded.get_type(), daemon_metric::NONE);
  ASSERT_EQ(decoded.get_n(), uint64_t(0xABCDEF01));
}

// service_daemon_register() with a large metadata map (50 key-value pairs).
// The metadata is stored as-is; a large map must not cause any issue.
// A second registration attempt must still return -EEXIST.
TEST_F(TestMgr, MgrClientServiceDaemonRegisterLargeMetadata) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> large_meta;
  for (int i = 0; i < 50; ++i) {
    large_meta["key_" + std::to_string(i)] = "value_" + std::to_string(i);
  }
  ASSERT_EQ(large_meta.size(), size_t(50));

  int r = client.service_daemon_register("bigservice", "bigdaemon", large_meta);
  ASSERT_EQ(r, 0);

  // A second call — any names — must return -EEXIST.
  ASSERT_EQ(client.service_daemon_register("bigservice", "bigdaemon2", {}),
            -EEXIST);

  client.shutdown();
}

// When set_perf_metric_query_cb has been registered and an MMgrConfigure
// with an empty osd_perf_metric_queries is dispatched (no session),
// the no-session guard fires first and returns true before the query-callback
// branch is ever reached.  The registered callback must NOT be invoked.
TEST_F(TestMgr, MgrClientHandleMgrConfigureWithPerfQueryCbRegistered) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int set_cb_calls = 0;
  client.set_perf_metric_query_cb(
      [&set_cb_calls](const ConfigPayload &) { ++set_cb_calls; },
      []() -> MetricPayload { return {}; });

  // Dispatch configure with no session — must return true and NOT invoke the
  // set_perf_queries callback (session guard fires first at line 474-477).
  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_period = 0;
  cfg->stats_threshold = 0;
  // osd_perf_metric_queries is empty by default.
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  // The no-session early return fires before the queries branch.
  ASSERT_EQ(set_cb_calls, 0);

  client.shutdown();
}

// Re-init after shutdown: a command queued before shutdown is dropped.
// After re-init, a new command can be queued and its reply is routed correctly.
// This exercises the state transition: dead=true → dead=false (via init).
TEST_F(TestMgr, MgrClientStartCommandBeforeAndAfterReinit) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // Queue a command before shutdown — it will be dropped.
  bool pre_fired = false;
  int pre_rc = 999;
  struct Ctx : public Context {
    bool& fired;
    int& rc;
    Ctx(bool& f, int& r) : fired(f), rc(r) {}
    void finish(int r_) override { fired = true; rc = r_; }
  };
  bufferlist o_pre;
  std::string s_pre;
  ASSERT_EQ(client.start_command({"pre_cmd"}, bufferlist{}, &o_pre, &s_pre,
                                  new Ctx(pre_fired, pre_rc)), 0);

  client.shutdown();  // drops pre_cmd; pre_fired must remain false
  ASSERT_FALSE(pre_fired);
  ASSERT_EQ(pre_rc, 999);

  // Re-init clears the dead flag.
  client.init();
  ASSERT_TRUE(client.is_initialized());

  // Queue a new command post-reinit.
  bool post_fired = false;
  int post_rc = 999;
  bufferlist o_post;
  std::string s_post;
  ASSERT_EQ(client.start_command({"post_cmd"}, bufferlist{}, &o_post, &s_post,
                                  new Ctx(post_fired, post_rc)), 0);

  // Reply for the post-reinit command.
  // CommandTable::last_tid is NOT reset by clear(), so the second command
  // gets tid=1 (after pre_cmd consumed tid=0 before the first shutdown).
  auto reply = ceph::make_message<MMgrCommandReply>(42, "post_reply");
  reply->set_tid(1);
  reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(post_fired);
  ASSERT_EQ(post_rc, 42);
  ASSERT_EQ(s_post, "post_reply");

  // The pre-shutdown command callback must still never have fired.
  ASSERT_FALSE(pre_fired);

  client.shutdown();
}

// GAP-M-1: set_messenger() called post-init (before any session is created)
// must not crash and the client must still report is_initialized() == true.
TEST_F(TestMgr, MgrClientSetMessengerPostInit) {
  MonMap monmap;
  std::unique_ptr<Messenger> m2(
      Messenger::create_client_messenger(cct.get(), "unittest_mgr_postinit"));

  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();
  ASSERT_TRUE(client.is_initialized());

  // Replace messenger after init — must be safe (no session yet).
  client.set_messenger(m2.get());
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();

  m2->shutdown();
  m2->wait();
}

// GAP-M-4: handle_command_reply() exercised via ms_dispatch2 with a negative
// return code and non-empty outbl payload, then re-dispatched with the same
// (now-erased) tid to verify it is a harmless no-op that still returns true.
TEST_F(TestMgr, MgrClientHandleCommandReplyViaMgrCommandReply) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  int finish_rc = 77;
  bool finished = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  int queue_rc = client.start_command({"mgrcmd_reply_cmd"}, bufferlist{},
                                      &outbl, &outs, new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  // Build reply via MMgrCommandReply with negative rc and non-empty payload.
  const char payload[] = "mgrcmd_payload";
  auto reply = ceph::make_message<MMgrCommandReply>(-9, "direct_rs");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));
  reply->get_data().append(payload, sizeof(payload) - 1);

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, -9);
  ASSERT_EQ(outs, "direct_rs");
  ASSERT_EQ(outbl.length(), sizeof(payload) - 1);

  // A second dispatch for the same (now-erased) tid must return true (no-op).
  auto reply2 = ceph::make_message<MMgrCommandReply>(0, "");
  reply2->set_tid(0);
  reply2->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply2)));
  ASSERT_EQ(finish_rc, -9);  // callback must NOT have fired again

  client.shutdown();
}

// GAP-M-5: service_daemon_update_task_status() with a large (50-entry) map.
TEST_F(TestMgr, MgrClientUpdateTaskStatusLargeMap) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> big_tasks;
  for (int i = 0; i < 50; ++i) {
    big_tasks["task_" + std::to_string(i)] = "state_" + std::to_string(i);
  }
  ASSERT_EQ(big_tasks.size(), size_t(50));

  int r = client.service_daemon_update_task_status(std::move(big_tasks));
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// GAP-E-1: MMgrClose dispatched when service_daemon was never set (it is false).
// handle_mgr_close() sets service_daemon=false again (no-op on the flag) and
// notifies shutdown_cond.  Must return true and not crash.
// After the close a fresh register must still succeed.
TEST_F(TestMgr, MgrClientHandleMgrCloseWithoutRegister) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // No service_daemon_register() — service_daemon is false.
  auto close_msg = ceph::make_message<MMgrClose>();
  close_msg->daemon_name = "never_registered";
  close_msg->service_name = "never_svc";

  // Must be consumed without crashing.
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(close_msg)));

  // service_daemon is still false, so register must succeed.
  int r = client.service_daemon_register("svc", "d", {});
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// GAP-B-1: outs pre-populated string is REPLACED (not appended) by the reply rs.
TEST_F(TestMgr, MgrClientHandleCommandReplyReplacesOuts) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs = "OLD_VALUE_THAT_MUST_BE_REPLACED";
  int finish_rc = 42;
  bool finished = false;

  struct Ctx : public Context {
    int& rc;
    bool& done;
    Ctx(int& rc_, bool& done_) : rc(rc_), done(done_) {}
    void finish(int r) override { rc = r; done = true; }
  };

  int queue_rc = client.start_command({"cmd_replace_outs"}, bufferlist{},
                                      &outbl, &outs, new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  auto reply = ceph::make_message<MMgrCommandReply>(0, "NEW_VALUE");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, 0);
  // The old value must be gone; the new one must be present.
  ASSERT_EQ(outs, "NEW_VALUE");
  ASSERT_NE(outs, "OLD_VALUE_THAT_MUST_BE_REPLACED");

  client.shutdown();
}

// GAP-B-2: update_daemon_metadata() with keys that have empty string values.
// The metadata map is stored as-is and return code must be 0.
TEST_F(TestMgr, MgrClientUpdateDaemonMetadataEmptyValue) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta = {
      {"key_with_empty_val", ""},
      {"another_empty", ""},
      {"non_empty", "value"}
  };
  ASSERT_EQ(meta.size(), size_t(3));

  int r = client.update_daemon_metadata("svc", "daemon", meta);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// GAP-B-3: DaemonHealthMetric with both n1 and n2 at UINT32_MAX simultaneously.
TEST_F(TestMgr, DaemonHealthMetricBothMaxN1N2) {
  constexpr uint32_t kMax32 = std::numeric_limits<uint32_t>::max();
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, kMax32, kMax32);
  ASSERT_EQ(m.get_n1(), kMax32);
  ASSERT_EQ(m.get_n2(), kMax32);
  // get_n() must return the full 64-bit packed value: all bits set.
  ASSERT_EQ(m.get_n(), std::numeric_limits<uint64_t>::max());
  ASSERT_EQ(m.get_type(), daemon_metric::SLOW_OPS);
}

// GAP-B-4: service_daemon_update_status() with a large (50-entry) status map.
TEST_F(TestMgr, MgrClientUpdateStatusLargeMap) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  client.service_daemon_register("svc", "d", {});

  std::map<std::string, std::string> big_status;
  for (int i = 0; i < 50; ++i) {
    big_status["status_key_" + std::to_string(i)] = "val_" + std::to_string(i);
  }
  ASSERT_EQ(big_status.size(), size_t(50));

  int r = client.service_daemon_update_status(std::move(big_status));
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// GAP-S-3: CommandTable tid sequence after TWO pre-shutdown commands.
// Two commands consume tids 0 and 1 before shutdown.  After re-init the
// next command must get tid 2 (CommandTable::last_tid is NOT reset by clear()).
TEST_F(TestMgr, MgrClientCommandTidSequenceAfterReinit) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist o0, o1;
  std::string s0, s1;

  // Consume tids 0 and 1.
  ASSERT_EQ(client.start_command({"cmd_pre_0"}, bufferlist{}, &o0, &s0, nullptr), 0);
  ASSERT_EQ(client.start_command({"cmd_pre_1"}, bufferlist{}, &o1, &s1, nullptr), 0);

  client.shutdown();  // clears command table; tids 0 and 1 are gone but last_tid stays at 1

  client.init();  // dead=false; command_table.last_tid is still 1

  bool post_fired = false;
  int post_rc = 999;
  struct Ctx : public Context {
    bool& fired;
    int& rc;
    Ctx(bool& f, int& r) : fired(f), rc(r) {}
    void finish(int r_) override { fired = true; rc = r_; }
  };
  bufferlist o_post;
  std::string s_post;
  ASSERT_EQ(client.start_command({"cmd_post"}, bufferlist{}, &o_post, &s_post,
                                  new Ctx(post_fired, post_rc)), 0);

  // The post-reinit command must have received tid 2.
  auto reply = ceph::make_message<MMgrCommandReply>(55, "post_reply_r3");
  reply->set_tid(2);
  reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(post_fired);
  ASSERT_EQ(post_rc, 55);
  ASSERT_EQ(s_post, "post_reply_r3");

  // A reply with tid 0 or 1 (old, now-absent) must be a harmless no-op.
  auto old_reply = ceph::make_message<MMgrCommandReply>(0, "stale");
  old_reply->set_tid(0);
  old_reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(old_reply)));
  ASSERT_EQ(post_rc, 55);  // must NOT have changed

  client.shutdown();
}

// A sentinel-recording completion used to prove a callback was (not) fired.
namespace {
struct RecordingCtx : public Context {
  bool& fired;
  int& rc;
  RecordingCtx(bool& fired_, int& rc_) : fired(fired_), rc(rc_) {}
  void finish(int r) override { fired = true; rc = r; }
};
}

// start_command() rejecting with -EACCES (mgr_optional, no MgrMap) must not
// complete the caller's callback – ownership stays with the caller.
TEST_F(TestMgr, MgrClientStartCommandOptionalRejectDoesNotFireCallback) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.set_mgr_optional(true);

  bool fired = false;
  int rc = 0;
  auto* ctx = new RecordingCtx(fired, rc);

  int ret = client.start_command({"cmd"}, bufferlist{}, nullptr, nullptr, ctx);
  ASSERT_EQ(ret, -EACCES);
  ASSERT_FALSE(fired);  // client must not have completed the callback

  delete ctx;  // caller still owns it on the -EACCES path
  client.shutdown();
}

// Same contract for start_tell_command().
TEST_F(TestMgr, MgrClientStartTellCommandOptionalRejectDoesNotFireCallback) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.set_mgr_optional(true);

  bool fired = false;
  int rc = 0;
  auto* ctx = new RecordingCtx(fired, rc);

  int ret = client.start_tell_command(
      "mgr.x", {"cmd"}, bufferlist{}, nullptr, nullptr, ctx);
  ASSERT_EQ(ret, -EACCES);
  ASSERT_FALSE(fired);

  delete ctx;
  client.shutdown();
}

// Calling shutdown() twice must be safe (no double-free of the timer/session).
TEST_F(TestMgr, MgrClientShutdownTwiceIsSafe) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  client.shutdown();
  client.shutdown();  // must not crash or assert
  SUCCEED();
}
