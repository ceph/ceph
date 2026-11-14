// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 sts=2 smarttab

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "global/global_context.h"
#include "gtest/gtest.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrMap.h"
#include "messages/MPGStats.h"
#include "mgr/MgrClient.h"
#include "mon/MgrMap.h"
#include "mon/MonMap.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

using namespace std::chrono_literals;

// Starts the messenger's dispatch thread, which drains the connection
// resets queued when the connect to the fake mgr address fails. The
// MgrClient is not registered, so the resets don't end its session.
class NullDispatcher : public Dispatcher {
public:
  explicit NullDispatcher(CephContext *cct) : Dispatcher(cct) {}
  dispatch_result_t ms_dispatch2(const MessageRef &m) override {
    return false;
  }
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
};

// Drives a MgrClient with MMgrMap/MMgrConfigure messages and counts the
// stats reports it sends (each one calls the pgstats callback). The active
// mgr address is never reached: the lossy session just drops the reports.
class MgrClientStats : public ::testing::Test {
protected:
  MonMap monmap;
  NullDispatcher dispatcher{g_ceph_context};
  std::unique_ptr<Messenger> msgr;
  std::unique_ptr<MgrClient> mgrc;
  std::atomic<int> reports{0};

  void SetUp() override {
    msgr.reset(Messenger::create(g_ceph_context, "async+posix",
				 entity_name_t::OSD(0), "test",
				 Messenger::get_random_nonce()));
    msgr->set_default_policy(Messenger::Policy::lossy_client(0));
    msgr->add_dispatcher_tail(&dispatcher);
    ASSERT_EQ(0, msgr->start());

    mgrc = std::make_unique<MgrClient>(g_ceph_context, msgr.get(), &monmap);
    mgrc->set_pgstats_cb([this]() {
      ++reports;
      return new MPGStats();
    });
    mgrc->init();

    MgrMap map;
    map.epoch = 1;
    map.active_gid = 4100;
    map.active_name = "x";
    map.available = true;
    entity_addr_t addr;
    addr.parse("v2:127.0.0.1:1/0");
    map.active_addrs = entity_addrvec_t(addr);
    mgrc->ms_dispatch2(make_message<MMgrMap>(map));
  }

  void TearDown() override {
    mgrc->shutdown();
    msgr->shutdown();
    msgr->wait();
  }

  void configure(uint32_t stats_period) {
    auto m = make_message<MMgrConfigure>();
    m->stats_period = stats_period;
    mgrc->ms_dispatch2(m);
  }
};

TEST_F(MgrClientStats, StopCancelsPendingReport)
{
  configure(1);
  EXPECT_EQ(1, reports.load());
  configure(0);
  // the report scheduled one period after the first one must not be sent
  std::this_thread::sleep_for(1500ms);
  EXPECT_EQ(1, reports.load());
}

TEST_F(MgrClientStats, RestartBeforeNextReportKeepsOneLoop)
{
  configure(1);
  configure(0);
  configure(1);
  EXPECT_EQ(2, reports.load());
  // Wait for the first tick instead of sleeping a fixed time, so a slow
  // test or timer thread can't make the count drift either way.
  auto deadline = std::chrono::steady_clock::now() + 10s;
  while (reports.load() < 3 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  ASSERT_EQ(3, reports.load());
  // A duplicate loop is due at the same time as the first one, so its
  // report follows right away; a single loop is not due for another 1s.
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(3, reports.load());
}
