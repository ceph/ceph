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
#include "messages/MCommand.h"
#include "messages/MMgrCommand.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrUpdate.h"
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

// shutdown() marks the client dead but never clears the initialized flag.
TEST_F(TestMgr, MgrClientShutdownDoesNotClearInitialized) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);

  client.init();
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();
  ASSERT_TRUE(client.is_initialized());
}

TEST_F(TestMgr, MgrClientMultipleInstances) {
  MonMap monmap;

  std::unique_ptr<Messenger> m2(
      Messenger::create_client_messenger(cct.get(), "unittest_mgr_2"));
  m2->start();

  MgrClient c1(cct.get(), messenger.get(), &monmap);
  MgrClient c2(cct.get(), m2.get(), &monmap);

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

// Smoke test: set_messenger() is a plain `msgr = msgr_` setter, so there is no
// observable effect to assert without driving a reconnect; this only pins that
// swapping the messenger before init() does not break initialization/teardown.
TEST_F(TestMgr, MgrClientSetMessenger) {
  MonMap monmap;
  std::unique_ptr<Messenger> m2(
      Messenger::create_client_messenger(cct.get(), "unittest_mgr_setmsgr"));
  m2->start();

  MgrClient client(cct.get(), messenger.get(), &monmap);
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

  // With no MgrMap (epoch == 0) and mgr_optional == false, start_command()
  // queues the command rather than rejecting it.
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

  // With mgr_optional == true and no MgrMap, start_command() returns -EACCES
  // instead of blocking on an absent mgr.
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

  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"tell_cmd"};
  bufferlist inbl;
  int r = client.start_tell_command("some-mgr", std::move(cmd), std::move(inbl),
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// A dispatch_result_t carries either a bool or a HANDLED tag; both mean the
// message was consumed.
static bool dispatch_handled(Dispatcher::dispatch_result_t r) {
  if (std::holds_alternative<bool>(r)) return std::get<bool>(r);
  return std::holds_alternative<Dispatcher::HANDLED>(r);
}

TEST_F(TestMgr, MgrClientHandleCommandReplyUnknownTidViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  auto reply = ceph::make_message<MMgrCommandReply>(0, "ok");
  reply->set_tid(9999);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

// A reply for a queued command fills outbl/outs, fires the callback with the
// reply's return code, and erases the command so a repeat dispatch is a no-op.
TEST_F(TestMgr, MgrClientHandleCommandReplyKnownTidViaDispatch) {
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

  std::vector<std::string> cmd = {"test_cmd"};
  bufferlist inbl;
  // tid 0 is the first tid CommandTable allocates.
  int queue_rc = client.start_command(std::move(cmd), std::move(inbl),
                                      &outbl, &outs, new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  auto reply = ceph::make_message<MMgrCommandReply>(-5, "all good");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  const char payload[] = "reply_payload";
  reply->get_data().append(payload, sizeof(payload) - 1);
  size_t expected_len = sizeof(payload) - 1;

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, -5);
  ASSERT_EQ(outs, "all good");
  ASSERT_EQ(outbl.length(), expected_len);

  // Re-dispatching the same tid must be a harmless no-op.
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

// Once service_daemon is set, update_daemon_metadata() must return -EEXIST.
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

// update_daemon_health() stages metrics that _send_report() moves into the next
// MMgrReport; drive a report over a live session and assert they reach the wire.
TEST_F(TestMgr, MgrClientUpdateDaemonHealth) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  std::vector<DaemonHealthMetric> metrics;
  metrics.emplace_back(daemon_metric::SLOW_OPS, uint64_t(3));
  metrics.emplace_back(daemon_metric::PENDING_CREATING_PGS, uint64_t(7));
  client.update_daemon_health(std::move(metrics));

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_threshold = 0;
  cfg->stats_period = 3600;  // large period: the rescheduled timer won't fire
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_MGR_REPORT);
  auto* report = static_cast<MMgrReport*>(con->sent[0].get());
  ASSERT_EQ(report->daemon_health_metrics.size(), 2u);

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

  // No session, so the pgstats callback must not be invoked.
  client.send_pgstats();
  ASSERT_FALSE(cb_called);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMsHandleRefused) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // ms_handle_refused() ignores its argument and always returns false.
  bool r = client.ms_handle_refused(nullptr);
  ASSERT_FALSE(r);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientDispatchWhenDead) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.shutdown();  // sets dead = true

  // Once dead, ms_dispatch2 returns false (unhandled) rather than asserting.
  auto reply = ceph::make_message<MMgrCommandReply>(0, "ok");
  reply->set_tid(1);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(reply)));
}

TEST_F(TestMgr, MgrClientDispatchCommandReplyNonMgrSource) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // A reply from a non-MGR source is not consumed.
  auto reply = ceph::make_message<MMgrCommandReply>(0, "ok");
  reply->set_tid(0);
  reply->set_src(entity_name_t::OSD(0));

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

// With a live session but no pgstats callback, send_pgstats() must emit nothing.
TEST_F(TestMgr, MgrClientSendPgstatsNoCb) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  client.send_pgstats();
  ASSERT_TRUE(con->sent.empty());

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateDaemonMetadataTwice) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta1 = {{"key", "v1"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta1), 0);

  // A repeat is allowed while service_daemon is still false.
  std::map<std::string, std::string> meta2 = {{"key", "v2"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta2), 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientMsHandleResetNoSession) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // With no session the connection cannot match, so ms_handle_reset() is false.
  bool r = client.ms_handle_reset(nullptr);
  ASSERT_FALSE(r);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMgrCloseViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  client.service_daemon_register("svc", "d", {});

  // handle_mgr_close() clears service_daemon and notifies shutdown_cond.
  auto close_msg = ceph::make_message<MMgrClose>();
  close_msg->daemon_name = "d";
  close_msg->service_name = "svc";

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(close_msg)));

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

  // No session, so the message is dropped but still consumed.
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg_msg)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartTellCommandEmptyTarget) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  std::vector<std::string> cmd = {"tell_any"};

  // An empty name means "whatever mgr is active", so the command still queues.
  int r = client.start_tell_command("", std::move(cmd), bufferlist{},
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// A two-value (n1, n2) metric survives staging and lands in the report with both
// sub-values intact.
TEST_F(TestMgr, MgrClientUpdateDaemonHealthN1N2) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  std::vector<DaemonHealthMetric> metrics;
  metrics.emplace_back(daemon_metric::SLOW_OPS, uint32_t(5), uint32_t(2));
  client.update_daemon_health(std::move(metrics));

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_threshold = 0;
  cfg->stats_period = 3600;
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  ASSERT_EQ(con->sent.size(), 1u);
  auto* report = static_cast<MMgrReport*>(con->sent[0].get());
  ASSERT_EQ(report->daemon_health_metrics.size(), 1u);
  ASSERT_EQ(report->daemon_health_metrics[0].get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(report->daemon_health_metrics[0].get_n1(), uint32_t(5));
  ASSERT_EQ(report->daemon_health_metrics[0].get_n2(), uint32_t(2));

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

  // An MCommandReply (MSG_COMMAND_REPLY) from an MGR source routes like an
  // MMgrCommandReply.
  auto reply = ceph::make_message<MCommandReply>(-3, "mcommand_reply");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, -3);
  ASSERT_EQ(outs, "mcommand_reply");

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMCommandReplyFromNonMgrViaDispatch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  auto reply = ceph::make_message<MCommandReply>(0, "ignored");
  reply->set_tid(0);
  reply->set_src(entity_name_t::OSD(0));

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

  // Null outbl/outs must not prevent the callback from firing.
  int queue_rc = client.start_command({"cmd_nullout"}, bufferlist{},
                                      nullptr, nullptr,
                                      new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  auto reply = ceph::make_message<MMgrCommandReply>(-7, "null_test");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

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

  // A reply with no callback registered must still be consumed.
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));
  ASSERT_EQ(outs, "nocb");

  client.shutdown();
}

// ms_handle_remote_reset() is a no-op override: unlike ms_handle_reset(), it must
// leave a live session untouched and send nothing.
TEST_F(TestMgr, MgrClientMsHandleRemoteReset) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  client.ms_handle_remote_reset(con);
  ASSERT_TRUE(client.test_has_session());
  ASSERT_TRUE(con->sent.empty());

  client.shutdown();
}

TEST_F(TestMgr, MgrClientUpdateStatusBeforeRegister) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> status = {{"state", "pending"}};
  int r = client.service_daemon_update_status(std::move(status));
  ASSERT_EQ(r, 0);

  // Updating status did not set service_daemon, so register still succeeds.
  int r2 = client.service_daemon_register("svc", "d", {});
  ASSERT_EQ(r2, 0);

  client.shutdown();
}

// Each update_daemon_health() call replaces the staged vector rather than
// appending, so only the most recent set reaches the report.
TEST_F(TestMgr, MgrClientUpdateDaemonHealthReplacement) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  std::vector<DaemonHealthMetric> first;
  first.emplace_back(daemon_metric::SLOW_OPS, uint64_t(10));
  first.emplace_back(daemon_metric::PENDING_CREATING_PGS, uint64_t(20));
  client.update_daemon_health(std::move(first));

  std::vector<DaemonHealthMetric> second;
  second.emplace_back(daemon_metric::SLOW_OPS, uint64_t(99));
  client.update_daemon_health(std::move(second));  // replaces the 2-metric set

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_threshold = 0;
  cfg->stats_period = 3600;
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  ASSERT_EQ(con->sent.size(), 1u);
  auto* report = static_cast<MMgrReport*>(con->sent[0].get());
  ASSERT_EQ(report->daemon_health_metrics.size(), 1u);

  client.shutdown();
}

TEST_F(TestMgr, DaemonHealthMetricDefaultConstructor) {
  DaemonHealthMetric m;
  ASSERT_EQ(m.get_type(), daemon_metric::NONE);
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
  // An out-of-range value returns "???" rather than crashing.
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

  // service_daemon is now true, so update_daemon_metadata() must refuse.
  std::map<std::string, std::string> meta2 = {{"key", "val"}};
  ASSERT_EQ(client.update_daemon_metadata("testsvc", "testd", meta2), -EEXIST);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientReinitAfterShutdown) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);

  client.init();
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();
  ASSERT_TRUE(client.is_initialized());

  // A second init() clears the dead flag so dispatch resumes.
  client.init();
  ASSERT_TRUE(client.is_initialized());

  auto reply = ceph::make_message<MMgrCommandReply>(0, "reinit_ok");
  reply->set_tid(9999);
  reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientSetMgrOptionalToggle) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl1;
  std::string outs1;
  ASSERT_EQ(client.start_command({"cmd_a"}, bufferlist{}, &outbl1, &outs1, nullptr), 0);

  // optional + no MgrMap epoch rejects with -EACCES...
  client.set_mgr_optional(true);
  bufferlist outbl2;
  std::string outs2;
  ASSERT_EQ(client.start_command({"cmd_b"}, bufferlist{}, &outbl2, &outs2, nullptr), -EACCES);

  // ...and clearing it queues again.
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

  MgrMap mgrmap;
  mgrmap.epoch = 0;
  mgrmap.available = false;
  auto map_msg = ceph::make_message<MMgrMap>(mgrmap);

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(map_msg)));

  // epoch is still 0, so the mgr_optional guard still rejects commands.
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

  // epoch=5 with available=false: reconnect() logs "No active mgr" and returns
  // early, but the nonzero epoch is recorded.
  MgrMap mgrmap;
  mgrmap.epoch = 5;
  mgrmap.available = false;
  auto map_msg = ceph::make_message<MMgrMap>(mgrmap);

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(map_msg)));

  // epoch != 0 now, so the mgr_optional guard no longer rejects commands.
  client.set_mgr_optional(true);
  bufferlist outbl;
  std::string outs;
  ASSERT_EQ(client.start_command({"cmd"}, bufferlist{}, &outbl, &outs, nullptr), 0);

  client.shutdown();
}

TEST_F(TestMgr, DaemonHealthMetricGetTypeName) {
  DaemonHealthMetric m_slow(daemon_metric::SLOW_OPS, uint64_t(1));
  ASSERT_EQ(m_slow.get_type_name(), std::string("SLOW_OPS"));

  DaemonHealthMetric m_pg(daemon_metric::PENDING_CREATING_PGS, uint64_t(2));
  ASSERT_EQ(m_pg.get_type_name(), std::string("PENDING_CREATING_PGS"));

  DaemonHealthMetric m_none;
  ASSERT_EQ(m_none.get_type_name(), std::string("NONE"));
}

TEST_F(TestMgr, DaemonHealthMetricStreamOutput) {
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(3), uint32_t(7));
  std::ostringstream oss;
  oss << m;
  std::string s = oss.str();
  // operator<< emits "<name>(<n>|(<n1>,<n2>))"; match the exact (n1,n2) tuple so
  // a formatting regression can't pass on a coincidental digit match.
  ASSERT_NE(s.find("SLOW_OPS"), std::string::npos);
  ASSERT_NE(s.find("(3,7)"), std::string::npos);
}

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

TEST_F(TestMgr, DaemonHealthMetricDump) {
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(5), uint32_t(2));

  ceph::JSONFormatter f;
  f.open_object_section("metric");
  m.dump(&f);
  f.close_section();

  std::ostringstream oss;
  f.flush(oss);
  std::string out = oss.str();

  ASSERT_NE(out.find("\"type\""), std::string::npos);
  ASSERT_NE(out.find("SLOW_OPS"), std::string::npos);
  ASSERT_NE(out.find("\"n\""), std::string::npos);
  ASSERT_NE(out.find("\"n1\""), std::string::npos);
  ASSERT_NE(out.find("\"n2\""), std::string::npos);
}

// A message type not in MgrClient's dispatch switch hits the default branch and
// is left unhandled regardless of source.
TEST_F(TestMgr, MgrClientDispatchTrulyUnknownMessageType) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  auto ping = ceph::make_message<MPing>();
  ping->set_src(entity_name_t::MGR(0));

  ASSERT_FALSE(dispatch_handled(client.ms_dispatch2(ping)));

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartCommandEmptyCmd) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist outbl;
  std::string outs;
  int r = client.start_command({}, bufferlist{}, &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientServiceDaemonRegisterEmptyNames) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta;
  int r = client.service_daemon_register("", "", meta);
  ASSERT_EQ(r, 0);

  int r2 = client.service_daemon_register("", "", meta);
  ASSERT_EQ(r2, -EEXIST);

  client.shutdown();
}

// The "need_metadata_update && !daemon_metadata.empty()" guard means an empty
// first call leaves need_metadata_update set; the next non-empty call sends and
// clears it, and a third call then skips the send.  All three return 0.
TEST_F(TestMgr, MgrClientUpdateDaemonMetadataEmpty) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> empty_meta;
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", empty_meta), 0);

  std::map<std::string, std::string> real_meta = {{"os", "linux"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", real_meta), 0);

  std::map<std::string, std::string> meta3 = {{"arch", "x86_64"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta3), 0);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientStartTellCommandQueuedWithEpoch) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // A nonzero epoch keeps the mgr_optional guard from firing.
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
  ASSERT_EQ(r, 0);

  client.shutdown();
}

TEST_F(TestMgr, DaemonHealthMetricMaxUint64Value) {
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  DaemonHealthMetric m(daemon_metric::PENDING_CREATING_PGS, kMax);
  ASSERT_EQ(m.get_type(), daemon_metric::PENDING_CREATING_PGS);
  ASSERT_EQ(m.get_n(), kMax);
}

TEST_F(TestMgr, DaemonHealthMetricZeroN1MaxN2) {
  constexpr uint32_t kMax32 = std::numeric_limits<uint32_t>::max();
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint32_t(0), kMax32);
  ASSERT_EQ(m.get_n1(), uint32_t(0));
  ASSERT_EQ(m.get_n2(), kMax32);
  // n1 is the low word and n2 the high word of the packed 64-bit value.
  uint64_t expected = (uint64_t)kMax32 << 32;
  ASSERT_EQ(m.get_n(), expected);
}

TEST_F(TestMgr, DaemonHealthMetricEncodeDecodeRoundTrip) {
  using ceph::encode;
  using ceph::decode;

  DaemonHealthMetric orig(daemon_metric::SLOW_OPS, uint64_t(0xCAFEBABE12345678ULL));
  bufferlist bl;
  encode(orig, bl);

  DaemonHealthMetric decoded;
  auto it = bl.cbegin();
  decode(decoded, it);

  ASSERT_EQ(decoded.get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(decoded.get_n(), uint64_t(0xCAFEBABE12345678ULL));

  // The DENC stores only the packed n, so n1/n2 are recovered from the union.
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

// shutdown() calls command_table.clear(), which drops pending commands without
// completing their on_finish callbacks (bug #83016).
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
  auto* ctx = new Ctx(fired, fire_rc);
  int r = client.start_command({"cmd_dropped"}, bufferlist{},
                               &outbl, &outs, ctx);
  ASSERT_EQ(r, 0);

  client.shutdown();

  ASSERT_FALSE(fired);
  ASSERT_EQ(fire_rc, 999);
  // shutdown drops the pending command without completing on_finish, so the
  // client never deletes it; reclaim it here to stay leak-clean.
  delete ctx;
}

TEST_F(TestMgr, DaemonHealthMetricMaxN1ZeroN2) {
  constexpr uint32_t kMax32 = std::numeric_limits<uint32_t>::max();
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, kMax32, uint32_t(0));
  ASSERT_EQ(m.get_n1(), kMax32);
  ASSERT_EQ(m.get_n2(), uint32_t(0));
  // n1 is the low word, so get_n() == kMax32.
  ASSERT_EQ(m.get_n(), uint64_t(kMax32));
}

TEST_F(TestMgr, DaemonHealthMetricSlowOpsZeroValue) {
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, uint64_t(0));
  ASSERT_EQ(m.get_type(), daemon_metric::SLOW_OPS);
  ASSERT_EQ(m.get_n(), uint64_t(0));
  ASSERT_EQ(m.get_n1(), uint32_t(0));
  ASSERT_EQ(m.get_n2(), uint32_t(0));
}

// Commands and tell-commands share one tid space; replying in reverse tid order
// routes each reply to the right callback.
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

// An MMgrClose resets service_daemon, so a register that failed with -EEXIST
// before the close succeeds afterwards.
TEST_F(TestMgr, MgrClientServiceDaemonFullLifecycle) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta1 = {{"ver", "1.0"}};
  ASSERT_EQ(client.service_daemon_register("svc", "d", meta1), 0);

  std::map<std::string, std::string> status1 = {{"state", "active"}};
  ASSERT_EQ(client.service_daemon_update_status(std::move(status1)), 0);

  std::map<std::string, std::string> tasks1 = {{"op", "running"}};
  ASSERT_EQ(client.service_daemon_update_task_status(std::move(tasks1)), 0);

  ASSERT_EQ(client.service_daemon_register("svc", "d2", {}), -EEXIST);

  auto close_msg = ceph::make_message<MMgrClose>();
  close_msg->daemon_name = "d";
  close_msg->service_name = "svc";
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(close_msg)));

  std::map<std::string, std::string> meta2 = {{"ver", "2.0"}};
  ASSERT_EQ(client.service_daemon_register("svc2", "d2", meta2), 0);

  std::map<std::string, std::string> status2 = {{"state", "standby"}};
  ASSERT_EQ(client.service_daemon_update_status(std::move(status2)), 0);

  ASSERT_EQ(client.service_daemon_register("svc2", "d3", {}), -EEXIST);

  client.shutdown();
}

// handle_command_reply() overwrites outbl rather than appending to it.
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

  // Pre-populate outbl with sentinel data that the reply must overwrite.
  const char old_data[] = "old_content_sentinel";
  outbl.append(old_data, sizeof(old_data) - 1);

  bufferlist inbl;
  int queue_rc = client.start_command({"cmd_replace_outbl"}, std::move(inbl),
                                      &outbl, &outs, new Ctx(finish_rc, finished));
  ASSERT_EQ(queue_rc, 0);

  const char new_data[] = "new_reply_payload";
  auto reply = ceph::make_message<MMgrCommandReply>(0, "replaced");
  reply->set_tid(0);
  reply->set_src(entity_name_t::MGR(0));
  reply->get_data().append(new_data, sizeof(new_data) - 1);

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(finished);
  ASSERT_EQ(finish_rc, 0);
  ASSERT_EQ(outs, "replaced");

  // Only the new data remains; the sentinel is gone.
  ASSERT_EQ(outbl.length(), sizeof(new_data) - 1);
  ASSERT_EQ(outbl.to_str(), std::string(new_data, sizeof(new_data) - 1));

  client.shutdown();
}

// With a live session the inbl is packed into the data of the emitted command
// message (CommandOp::get_message() calls set_data(inbl)).
TEST_F(TestMgr, MgrClientStartCommandWithNonEmptyInbl) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  MgrMap m;
  m.epoch = 1;
  m.active_name = "mgrA";
  m.active_mgr_features = 0;  // no SERVER_OCTOPUS -> MCommand, not MMgrCommand
  client.test_set_map(m);

  bufferlist inbl;
  const char payload[] = "command_input_data_1234567890";
  inbl.append(payload, sizeof(payload) - 1);
  const size_t inbl_len = sizeof(payload) - 1;

  bufferlist outbl;
  std::string outs;
  int r = client.start_command({"cmd_with_inbl"}, std::move(inbl),
                               &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_COMMAND);
  auto* cmd = static_cast<MCommand*>(con->sent[0].get());
  ASSERT_EQ(cmd->get_data().length(), inbl_len);

  client.shutdown();
}

// set_pgstats_cb() replaces any previous callback; without a live session
// send_pgstats() invokes neither.
TEST_F(TestMgr, MgrClientSetPgstatsCbReplacement) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int cb1_calls = 0, cb2_calls = 0;

  client.set_pgstats_cb([&cb1_calls]() -> MPGStats* {
    ++cb1_calls;
    return new MPGStats();
  });

  client.set_pgstats_cb([&cb2_calls]() -> MPGStats* {
    ++cb2_calls;
    return new MPGStats();
  });

  client.send_pgstats();
  ASSERT_EQ(cb1_calls, 0);
  ASSERT_EQ(cb2_calls, 0);

  client.shutdown();
}

// The second register fails because the guard is `if (service_daemon)`; the
// service/daemon names are never compared.
TEST_F(TestMgr, MgrClientServiceDaemonRegisterDifferentNamesStillRejects) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  std::map<std::string, std::string> meta1 = {{"key", "v1"}};
  ASSERT_EQ(client.service_daemon_register("svcA", "daemonA", meta1), 0);

  std::map<std::string, std::string> meta2 = {{"key", "v2"}};
  ASSERT_EQ(client.service_daemon_register("svcB", "daemonB", meta2), -EEXIST);

  std::map<std::string, std::string> meta3;
  ASSERT_EQ(client.service_daemon_register("", "", meta3), -EEXIST);

  client.shutdown();
}

// shutdown() drops every pending command, regular or tell, without firing
// callbacks (bug #83016).
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

  auto* c0 = new Ctx(fired0, rc0);
  auto* c1 = new Ctx(fired1, rc1);
  auto* c2 = new Ctx(fired2, rc2);
  ASSERT_EQ(client.start_command({"cmd0"}, bufferlist{}, &o0, &s0, c0), 0);
  ASSERT_EQ(client.start_command({"cmd1"}, bufferlist{}, &o1, &s1, c1), 0);
  ASSERT_EQ(client.start_tell_command("mgr.a", {"tell_cmd"}, bufferlist{},
                                       &o2, &s2, c2), 0);

  client.shutdown();

  ASSERT_FALSE(fired0);
  ASSERT_FALSE(fired1);
  ASSERT_FALSE(fired2);
  ASSERT_EQ(rc0, 999);
  ASSERT_EQ(rc1, 999);
  ASSERT_EQ(rc2, 999);
  // shutdown drops the pending commands without completing on_finish, so the
  // client never deletes them; reclaim them here to stay leak-clean.
  delete c0;
  delete c1;
  delete c2;
}

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

// With no session the configure handler returns before reaching the
// query-callback branch, so a registered set_perf_queries callback is not
// invoked.
TEST_F(TestMgr, MgrClientHandleMgrConfigureWithPerfQueryCbRegistered) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  int set_cb_calls = 0;
  client.set_perf_metric_query_cb(
      [&set_cb_calls](const ConfigPayload &) { ++set_cb_calls; },
      []() -> MetricPayload { return {}; });

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_period = 0;
  cfg->stats_threshold = 0;
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  ASSERT_EQ(set_cb_calls, 0);

  client.shutdown();
}

// A command queued before shutdown is dropped; after re-init a new command
// routes correctly.  CommandTable::last_tid is not reset by clear(), so the
// post-reinit command gets tid 1 (tid 0 was consumed before the first shutdown).
TEST_F(TestMgr, MgrClientStartCommandBeforeAndAfterReinit) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

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
  auto* pre_ctx = new Ctx(pre_fired, pre_rc);
  ASSERT_EQ(client.start_command({"pre_cmd"}, bufferlist{}, &o_pre, &s_pre,
                                  pre_ctx), 0);

  client.shutdown();
  ASSERT_FALSE(pre_fired);
  ASSERT_EQ(pre_rc, 999);
  // shutdown drops the pre-reinit command without completing on_finish, so the
  // client never deletes it; reclaim it here to stay leak-clean.
  delete pre_ctx;

  client.init();
  ASSERT_TRUE(client.is_initialized());

  bool post_fired = false;
  int post_rc = 999;
  bufferlist o_post;
  std::string s_post;
  ASSERT_EQ(client.start_command({"post_cmd"}, bufferlist{}, &o_post, &s_post,
                                  new Ctx(post_fired, post_rc)), 0);

  auto reply = ceph::make_message<MMgrCommandReply>(42, "post_reply");
  reply->set_tid(1);
  reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(post_fired);
  ASSERT_EQ(post_rc, 42);
  ASSERT_EQ(s_post, "post_reply");

  ASSERT_FALSE(pre_fired);

  client.shutdown();
}

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

  // The same tid is now erased, so a repeat dispatch does not fire again.
  auto reply2 = ceph::make_message<MMgrCommandReply>(0, "");
  reply2->set_tid(0);
  reply2->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply2)));
  ASSERT_EQ(finish_rc, -9);

  client.shutdown();
}

TEST_F(TestMgr, MgrClientHandleMgrCloseWithoutRegister) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  // service_daemon was never set; the close is still consumed.
  auto close_msg = ceph::make_message<MMgrClose>();
  close_msg->daemon_name = "never_registered";
  close_msg->service_name = "never_svc";

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(close_msg)));

  int r = client.service_daemon_register("svc", "d", {});
  ASSERT_EQ(r, 0);

  client.shutdown();
}

// A reply's rs replaces the caller's outs string rather than appending.
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
  ASSERT_EQ(outs, "NEW_VALUE");

  client.shutdown();
}

TEST_F(TestMgr, DaemonHealthMetricBothMaxN1N2) {
  constexpr uint32_t kMax32 = std::numeric_limits<uint32_t>::max();
  DaemonHealthMetric m(daemon_metric::SLOW_OPS, kMax32, kMax32);
  ASSERT_EQ(m.get_n1(), kMax32);
  ASSERT_EQ(m.get_n2(), kMax32);
  // Both words set means every bit of the packed value is set.
  ASSERT_EQ(m.get_n(), std::numeric_limits<uint64_t>::max());
  ASSERT_EQ(m.get_type(), daemon_metric::SLOW_OPS);
}

// Two commands consume tids 0 and 1 before shutdown; because last_tid survives
// clear(), the post-reinit command gets tid 2.
TEST_F(TestMgr, MgrClientCommandTidSequenceAfterReinit) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bufferlist o0, o1;
  std::string s0, s1;

  ASSERT_EQ(client.start_command({"cmd_pre_0"}, bufferlist{}, &o0, &s0, nullptr), 0);
  ASSERT_EQ(client.start_command({"cmd_pre_1"}, bufferlist{}, &o1, &s1, nullptr), 0);

  client.shutdown();
  client.init();

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

  auto reply = ceph::make_message<MMgrCommandReply>(55, "post_reply_r3");
  reply->set_tid(2);
  reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(reply)));

  ASSERT_TRUE(post_fired);
  ASSERT_EQ(post_rc, 55);
  ASSERT_EQ(s_post, "post_reply_r3");

  // The old tids are gone, so a reply for tid 0 is a harmless no-op.
  auto old_reply = ceph::make_message<MMgrCommandReply>(0, "stale");
  old_reply->set_tid(0);
  old_reply->set_src(entity_name_t::MGR(0));
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(old_reply)));
  ASSERT_EQ(post_rc, 55);

  client.shutdown();
}

namespace {
struct RecordingCtx : public Context {
  bool& fired;
  int& rc;
  RecordingCtx(bool& fired_, int& rc_) : fired(fired_), rc(rc_) {}
  void finish(int r) override { fired = true; rc = r; }
};
}

// A start_command() rejected with -EACCES must not complete the caller's
// callback; ownership of the context stays with the caller.
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
  ASSERT_FALSE(fired);

  delete ctx;  // caller still owns it on the -EACCES path
  client.shutdown();
}

// Same ownership contract for start_tell_command().
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

TEST_F(TestMgr, MgrClientShutdownTwiceIsSafe) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  client.shutdown();
  client.shutdown();
  SUCCEED();
}

// ---------------------------------------------------------------------------
// Live-session coverage.
//
// The tests above drive MgrClient through its public API with the fixture's
// real client messenger, which never reaches an available MgrMap, so a session
// is never established and the wire paths stay dark.  The tests below use
// MgrClientTestHelper (see TestMgr.h) to inject a session backed by a
// RecordingConnection, exercising the code behind the `if (session ...)` guards
// and asserting on what MgrClient actually puts on the wire.
// ---------------------------------------------------------------------------

// With a session the configure handler runs past the `!session` guard and
// applies the stats threshold.
TEST_F(TestMgr, MgrClientLiveConfigureUpdatesStatsThreshold) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.inject_session();

  ASSERT_EQ(client.test_stats_threshold(), 0u);

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_threshold = 42;
  cfg->stats_period = 0;  // keep the report timer disarmed
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  ASSERT_EQ(client.test_stats_threshold(), 42u);

  client.shutdown();
}

// With a session, start_command() takes the send branch and emits a message.
TEST_F(TestMgr, MgrClientLiveStartCommandSendsMessage) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  MgrMap m;
  m.epoch = 1;
  m.active_name = "mgrA";
  m.active_mgr_features = 0;  // no SERVER_OCTOPUS -> MCommand, not MMgrCommand
  client.test_set_map(m);

  bufferlist outbl;
  std::string outs;
  int r = client.start_command({"df"}, bufferlist{}, &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  // The command is sent and stays tracked until a reply arrives.
  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_COMMAND);
  ASSERT_EQ(client.test_command_table_size(), 1u);

  client.shutdown();
}

// A tell addressed to the active mgr is sent immediately.
TEST_F(TestMgr, MgrClientLiveTellActiveMgrSends) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  MgrMap m;
  m.epoch = 1;
  m.active_name = "mgrA";
  client.test_set_map(m);

  bufferlist outbl;
  std::string outs;
  int r = client.start_tell_command("mgrA", {"status"}, bufferlist{},
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_COMMAND);

  client.shutdown();
}

// A stats_period 0->nonzero transition drives _send_stats() -> _send_report(),
// emitting one MMgrReport.  The large period keeps the rescheduled report timer
// from firing during the test; shutdown() cancels the pending event.
TEST_F(TestMgr, MgrClientLiveConfigureStartsStatsSendsReport) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_threshold = 0;
  cfg->stats_period = 3600;
  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));

  ASSERT_EQ(client.test_stats_period(), 3600u);

  // Exactly one report; _send_pgstats() sent nothing with no pgstats_cb set.
  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_MGR_REPORT);

  client.shutdown();
}

// ms_handle_reset() for the session's own connection tears the session down via
// reconnect(), which, with no available MgrMap, stops at "no active mgr".
TEST_F(TestMgr, MgrClientMsHandleResetMatchedConnection) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();
  ASSERT_TRUE(client.test_has_session());

  ASSERT_TRUE(client.ms_handle_reset(con));
  ASSERT_FALSE(client.test_has_session());

  client.shutdown();
}

// With a live session, update_daemon_metadata() sends an MMgrUpdate carrying the
// new metadata (need_metadata_update defaults true, so the first update ships it).
TEST_F(TestMgr, MgrClientLiveUpdateDaemonMetadataSendsUpdate) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  std::map<std::string, std::string> meta = {{"arch", "x86_64"}};
  ASSERT_EQ(client.update_daemon_metadata("osd", "osd.0", meta), 0);

  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_MGR_UPDATE);
  auto* upd = static_cast<MMgrUpdate*>(con->sent[0].get());
  ASSERT_EQ(upd->service_name, "osd");
  ASSERT_EQ(upd->daemon_name, "osd.0");
  ASSERT_TRUE(upd->need_metadata_update);
  ASSERT_EQ(upd->daemon_metadata.size(), 1u);

  client.shutdown();
}

// With a live session and a pgstats callback, send_pgstats() puts the callback's
// MPGStats on the wire.
TEST_F(TestMgr, MgrClientLiveSendPgstatsSendsMessage) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  int cb_calls = 0;
  client.set_pgstats_cb([&cb_calls]() -> MPGStats* {
    ++cb_calls;
    return new MPGStats();
  });

  client.send_pgstats();
  ASSERT_EQ(cb_calls, 1);
  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_PGSTATS);

  client.shutdown();
}

// service_daemon_register() over a live client session "late-registers" by
// sending an MMgrOpen that advertises the service daemon and its metadata.  The
// unregister wait is disabled so the trailing shutdown() does not block.
TEST_F(TestMgr, MgrClientLiveServiceDaemonRegisterSendsOpen) {
  cct->_conf.set_val("mgr_client_service_daemon_unregister_timeout", "0");
  cct->_conf.apply_changes(nullptr);

  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  std::map<std::string, std::string> meta = {{"ceph_version", "test"}};
  ASSERT_EQ(client.service_daemon_register("rgw", "rgw.a", meta), 0);

  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_MGR_OPEN);
  auto* open = static_cast<MMgrOpen*>(con->sent[0].get());
  ASSERT_EQ(open->service_name, "rgw");
  ASSERT_EQ(open->daemon_name, "rgw.a");
  ASSERT_TRUE(open->service_daemon);
  ASSERT_EQ(open->daemon_metadata.size(), 1u);

  client.shutdown();
}

// A registered service daemon with a live session sends an MMgrClose on shutdown.
// The unregister wait is disabled so shutdown() does not block on shutdown_cond.
TEST_F(TestMgr, MgrClientLiveShutdownSendsClose) {
  cct->_conf.set_val("mgr_client_service_daemon_unregister_timeout", "0");
  cct->_conf.apply_changes(nullptr);

  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();
  // shutdown() resets the session's own reference to con; keep it alive so we can
  // inspect what was sent after shutdown returns.
  ceph::ref_t<Connection> con_ref(con);

  std::map<std::string, std::string> meta;
  ASSERT_EQ(client.service_daemon_register("mds", "mds.a", meta), 0);
  con->sent.clear();  // drop the late-register open so only the close remains

  client.shutdown();

  ASSERT_EQ(con->sent.size(), 1u);
  ASSERT_EQ(con->sent[0]->get_type(), MSG_MGR_CLOSE);
  auto* close = static_cast<MMgrClose*>(con->sent[0].get());
  ASSERT_EQ(close->daemon_name, "mds.a");
  ASSERT_EQ(close->service_name, "mds");
}

// A configure carrying osd_perf_metric_queries routes them to the registered
// perf-query callback (the branch handle_mgr_configure() takes when the query map
// is non-empty), which the no-session guard test at
// MgrClientHandleMgrConfigureWithPerfQueryCbRegistered cannot reach.
TEST_F(TestMgr, MgrClientLiveConfigurePerfMetricQueryInvokesCb) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.inject_session();

  int set_calls = 0;
  client.set_perf_metric_query_cb(
      [&set_calls](const ConfigPayload &) { ++set_calls; },
      []() -> MetricPayload { return {}; });

  auto cfg = ceph::make_message<MMgrConfigure>();
  cfg->stats_period = 0;
  cfg->stats_threshold = 0;
  cfg->osd_perf_metric_queries[OSDPerfMetricQuery()];  // one entry -> non-empty

  ASSERT_TRUE(dispatch_handled(client.ms_dispatch2(cfg)));
  ASSERT_EQ(set_calls, 1);

  client.shutdown();
}

// Regression for https://tracker.ceph.com/issues/83015.
//
// A tell addressed to a mgr that is NOT the active one must be queued, not sent
// (production resends it on reconnect, or completes it -ENXIO if the target is
// gone).  start_tell_command() checks `name` *after* std::move(name), so the
// moved-from (empty) name makes `name.size() == 0` true and the message is sent
// to the wrong target.  Re-enable once the guard is fixed to test the target
// before the move.
TEST_F(TestMgr, DISABLED_MgrClientTellWrongTargetMustNotSend) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  auto* con = client.inject_session();

  MgrMap m;
  m.epoch = 1;
  m.active_name = "mgrA";
  client.test_set_map(m);

  bufferlist outbl;
  std::string outs;
  int r = client.start_tell_command("mgrB", {"status"}, bufferlist{},
                                    &outbl, &outs, nullptr);
  ASSERT_EQ(r, 0);

  // Non-fatal so the test still reaches shutdown() (which clears command_table)
  // when it fails pre-fix, instead of aborting in ~CommandTable.
  EXPECT_TRUE(con->sent.empty());
  EXPECT_EQ(client.test_command_table_size(), 1u);

  client.shutdown();
}

// Regression for https://tracker.ceph.com/issues/83016.
//
// shutdown() calls command_table.clear(), which drops pending commands without
// completing their on_finish callbacks (the active
// MgrClientShutdownDropsPendingCommands tests pin that current behavior). A fix
// should complete each pending command on shutdown; re-enable once it does.
TEST_F(TestMgr, DISABLED_MgrClientShutdownCompletesPendingCommands) {
  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  client.init();

  bool fired = false;
  int fire_rc = 0;
  struct Ctx : public Context {
    bool& fired;
    int& rc;
    Ctx(bool& f, int& r) : fired(f), rc(r) {}
    void finish(int r) override { fired = true; rc = r; }
  };

  bufferlist outbl;
  std::string outs;
  auto* ctx = new Ctx(fired, fire_rc);
  ASSERT_EQ(client.start_command({"cmd_dropped"}, bufferlist{},
                                 &outbl, &outs, ctx), 0);

  client.shutdown();

  // Fixed behavior: shutdown completes the pending command's callback.
  EXPECT_TRUE(fired);
  // Pre-fix the client drops it without completing or deleting it; reclaim it so
  // the test stays leak-clean if it is force-run with --gtest_also_run_disabled_tests.
  if (!fired) {
    delete ctx;
  }
}

// Regression for https://tracker.ceph.com/issues/83017.
//
// _send_report() clears report_callback without cancelling its scheduled timer
// event. A stats_period 0->nonzero->0->nonzero sequence therefore runs
// _send_stats() while the first report event is still pending, orphaning it in
// the timer instead of cancelling it. Re-enable once _send_stats() cancels the
// previous report event before scheduling the next.
TEST_F(TestMgr, DISABLED_MgrClientSendStatsCancelsPreviousReportEvent) {
  MonMap monmap;
  MgrClientTestHelper client(cct.get(), messenger.get(), &monmap);
  client.init();
  client.inject_session();

  // 0->3600 schedules the first report event (E1). The large period keeps it
  // from firing during the test.
  auto cfg_on = ceph::make_message<MMgrConfigure>();
  cfg_on->stats_threshold = 0;
  cfg_on->stats_period = 3600;
  EXPECT_TRUE(dispatch_handled(client.ms_dispatch2(cfg_on)));
  Context* e1 = client.test_report_callback();
  EXPECT_NE(e1, nullptr);

  // Drop to 0 (E1 stays pending), then 0->3600 again runs _send_stats() with E1
  // still scheduled.
  auto cfg_off = ceph::make_message<MMgrConfigure>();
  cfg_off->stats_threshold = 0;
  cfg_off->stats_period = 0;
  EXPECT_TRUE(dispatch_handled(client.ms_dispatch2(cfg_off)));

  auto cfg_on2 = ceph::make_message<MMgrConfigure>();
  cfg_on2->stats_threshold = 0;
  cfg_on2->stats_period = 3600;
  EXPECT_TRUE(dispatch_handled(client.ms_dispatch2(cfg_on2)));
  Context* e2 = client.test_report_callback();
  EXPECT_NE(e2, nullptr);

  // E1 must no longer be scheduled. If the fix cancelled (and freed) E1, the
  // allocator may reuse its address for E2; that reuse only happens when E1 was
  // truly cancelled, so treat e1 == e2 as cancelled. Otherwise probe the timer:
  // cancel_event() returns false when E1 is already gone.
  bool e1_still_scheduled = (e1 != e2) && client.test_timer_cancel_event(e1);
  EXPECT_FALSE(e1_still_scheduled);

  client.shutdown();
}
