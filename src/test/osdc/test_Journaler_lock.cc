// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Regression for MDS stall: submit thread held Journaler::lock across
 * Objecter::_throttle_op (prezero/flush) while the timer thread held
 * mds_lock and blocked in Journaler::get_layout_period().
 *
 * get_layout_period() must remain usable while Journaler::lock is held
 * (lock-free cached layout period). Journaler::lock is non-recursive, so
 * taking it again from the same thread would deadlock — that is the
 * pre-fix failure mode for this test.
 */

#include <atomic>
#include <chrono>
#include <thread>

#include <boost/asio/io_context.hpp>

#include "gtest/gtest.h"

#include "common/Finisher.h"
#include "global/global_context.h"
#include "msg/Messenger.h"
#include "mon/MonClient.h"
#include "osdc/Journaler.h"
#include "osdc/Objecter.h"
#include "include/fs_types.h"

using namespace std::chrono_literals;

namespace {

class JournalerLockFixture : public ::testing::Test {
protected:
  void SetUp() override {
    ioctx = std::make_unique<boost::asio::io_context>();
    msgr.reset(Messenger::create(g_ceph_context, "async",
				 entity_name_t::CLIENT(-1),
				 "jrnl_lock", getpid()));
    ASSERT_TRUE(msgr);
    msgr->set_default_policy(Messenger::Policy::lossy_client(0));
    msgr->start();

    monc = std::make_unique<MonClient>(g_ceph_context, *ioctx);
    objecter = std::make_unique<Objecter>(g_ceph_context, msgr.get(),
					  monc.get(), *ioctx);

    finisher = std::make_unique<Finisher>(g_ceph_context, "jrnl_lk",
					  "fn_jrnl_lk");
    finisher->start();

    const int64_t pool = 1;
    journaler = std::make_unique<Journaler>(
	"test", inodeno_t(0x1000), pool, "journaler_lock_magic",
	objecter.get(), nullptr, 0, finisher.get());

    journaler->set_writeable();
    file_layout_t layout = file_layout_t::get_default();
    layout.pool_id = pool;
    journaler->create(&layout, JOURNAL_FORMAT_RESILIENT);
    expected_period = layout.get_period();
  }

  void TearDown() override {
    journaler.reset();
    if (finisher) {
      finisher->stop();
    }
    finisher.reset();
    objecter.reset();
    monc.reset();
    if (msgr) {
      msgr->shutdown();
      msgr->wait();
    }
    msgr.reset();
    ioctx.reset();
  }

  std::unique_ptr<boost::asio::io_context> ioctx;
  std::unique_ptr<Messenger> msgr;
  std::unique_ptr<MonClient> monc;
  std::unique_ptr<Objecter> objecter;
  std::unique_ptr<Finisher> finisher;
  std::unique_ptr<Journaler> journaler;
  uint64_t expected_period = 0;
};

} // namespace

TEST_F(JournalerLockFixture, GetLayoutPeriodWhileLockHeld)
{
  ASSERT_EQ(expected_period, journaler->get_layout_period());
  ASSERT_GT(expected_period, 0u);

  // Same-thread: Journaler::lock is non-recursive. Pre-fix get_layout_period
  // took the lock and would deadlock here. Post-fix uses layout_period.
  std::lock_guard l(JournalerTestAccess::lock(*journaler));
  EXPECT_EQ(expected_period, journaler->get_layout_period());
}

TEST_F(JournalerLockFixture, GetLayoutPeriodConcurrentWithLockHeld)
{
  // Mirrors the MDS stall: one thread holds Journaler::lock (submit / throttle
  // wait), another needs get_layout_period (scatter_tick under mds_lock).
  std::atomic<bool> holder_ready{false};
  std::atomic<bool> reader_done{false};
  std::atomic<uint64_t> got{0};

  std::thread holder([&] {
    std::lock_guard l(JournalerTestAccess::lock(*journaler));
    holder_ready = true;
    // Stay in the critical section until the reader finishes (or times out).
    auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!reader_done.load() &&
	   std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(10ms);
    }
  });

  while (!holder_ready.load()) {
    std::this_thread::sleep_for(1ms);
  }

  std::thread reader([&] {
    got = journaler->get_layout_period();
    reader_done = true;
  });

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!reader_done.load() &&
	 std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }

  EXPECT_TRUE(reader_done.load())
      << "get_layout_period blocked while Journaler::lock was held "
	 "(throttle-under-lock deadlock regression)";
  if (reader_done.load()) {
    EXPECT_EQ(expected_period, got.load());
    reader.join();
  } else {
    // Avoid hanging forever if the regression returns.
    reader.detach();
  }

  holder_ready = true; // ensure holder can observe reader_done
  reader_done = true;
  holder.join();
}
