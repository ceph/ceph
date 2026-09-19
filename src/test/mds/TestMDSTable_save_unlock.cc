// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Regression for MDS stall: MDLog::log_trim_upkeep held mds_lock across
 * Objecter::_throttle_op in MDSTable::save (via try_to_expire). Dispatch and
 * asok blocked until the throttle drained.
 *
 * MDSTable::save must drop mds_lock around the Objecter submit, and must not
 * start a second RADOS write while one is already in flight.
 */

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "common/Finisher.h"
#include "common/fair_mutex.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "mds/MDSTable.h"

using namespace std::chrono_literals;

namespace {

class TestTable : public MDSTable {
public:
  TestTable() :
    MDSTable(nullptr, "testtable", true)
  {}

  void
  reset_state() override
  {}

  void
  encode_state(bufferlist& bl) const override
  {
    encode((__u8)0, bl);
  }

  void
  decode_state(bufferlist::const_iterator& p) override
  {
    __u8 v;
    decode(v, p);
  }
};

class MDSTableSaveUnlockFixture : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    lock = std::make_unique<ceph::fair_mutex>("tbl_save_unlock");
    finisher =
        std::make_unique<Finisher>(g_ceph_context, "tbl_save", "fn_tbl_sv");
    finisher->start();

    test_io.lock = lock.get();
    test_io.finisher = finisher.get();
    test_io.pool = 1;
    MDSTableTestAccess::set_test_io(&test_io);

    table = std::make_unique<TestTable>();
    table->set_rank(mds_rank_t(0));
    MDSTableTestAccess::set_active(*table);
    MDSTableTestAccess::set_versions(*table, 1, 0, 0);
  }

  void
  TearDown() override
  {
    MDSTableTestAccess::clear_write_hook();
    MDSTableTestAccess::clear_test_io();
    table.reset();
    if (finisher) {
      finisher->stop();
    }
    finisher.reset();
    lock.reset();
  }

  std::unique_ptr<ceph::fair_mutex> lock;
  std::unique_ptr<Finisher> finisher;
  MDSTableTestAccess::TestIO test_io;
  std::unique_ptr<TestTable> table;
};

} // namespace

TEST_F(MDSTableSaveUnlockFixture, SaveDropsLockDuringSubmit)
{
  // Mirrors the stall: save blocks in Objecter submit while another thread
  // needs mds_lock (dispatch / asok / scatter_tick).
  std::atomic<bool> submit_entered{false};
  std::atomic<bool> release_submit{false};
  std::atomic<bool> lock_acquired{false};
  Context* pending_fin = nullptr;

  MDSTableTestAccess::set_write_hook([&](Context* fin) {
    pending_fin = fin;
    submit_entered = true;
    auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!release_submit.load() &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(10ms);
    }
  });

  std::thread saver([&] {
    std::lock_guard l(*lock);
    table->save(nullptr);
  });

  while (!submit_entered.load()) {
    std::this_thread::sleep_for(1ms);
  }

  std::thread dispatcher([&] {
    std::lock_guard l(*lock);
    lock_acquired = true;
  });

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!lock_acquired.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }

  EXPECT_TRUE(lock_acquired.load())
      << "mds_lock blocked while MDSTable::save was in Objecter submit "
         "(throttle-under-lock deadlock regression)";

  release_submit = true;
  saver.join();
  if (lock_acquired.load()) {
    dispatcher.join();
  } else {
    dispatcher.detach();
  }

  delete pending_fin;
  MDSTableTestAccess::clear_write_hook();
}

TEST_F(MDSTableSaveUnlockFixture, SaveDefersWhileWriteInFlight)
{
  std::atomic<int> write_calls{0};
  MDSTableTestAccess::set_write_hook([&](Context* fin) {
    write_calls++;
    delete fin;
  });

  // Prior write still "in flight": committing > committed.
  MDSTableTestAccess::set_versions(*table, 5, 4, 3);

  {
    std::lock_guard l(*lock);
    table->save(nullptr);
  }

  EXPECT_EQ(0, write_calls.load())
      << "overlapping save must defer until in-flight write completes";
  EXPECT_EQ(4u, table->get_committing_version());
  EXPECT_EQ(3u, table->get_committed_version());

  MDSTableTestAccess::clear_write_hook();
}

TEST_F(MDSTableSaveUnlockFixture, SaveSubmitsWhenIdle)
{
  std::atomic<int> write_calls{0};
  MDSTableTestAccess::set_write_hook([&](Context* fin) {
    write_calls++;
    delete fin;
  });

  MDSTableTestAccess::set_versions(*table, 2, 0, 0);

  {
    std::lock_guard l(*lock);
    table->save(nullptr);
  }

  EXPECT_EQ(1, write_calls.load());
  EXPECT_EQ(2u, table->get_committing_version());

  MDSTableTestAccess::clear_write_hook();
}
