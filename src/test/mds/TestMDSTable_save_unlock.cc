// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Regression for MDS stall under Objecter balanced-budget throttle.
 *
 * MDSTable::save used to call Objecter::write_full on the caller (trim /
 * ms_dispatch) while holding mds_lock. When _throttle_op blocked, dispatch
 * could not free budget → deadlock.
 *
 * Saves are now queued via MDSRank::queue_objecter (or a test write hook).
 * The caller must not block in Objecter submit, and must not start a second
 * RADOS write while one is already in flight.
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

TEST_F(MDSTableSaveUnlockFixture, SaveReturnsWithoutBlockingOnSubmit)
{
  // queue_objecter / write-hook path must not leave the caller stuck in
  // Objecter::_throttle_op under mds_lock. The hook stands in for the queued
  // submit and must return promptly.
  std::atomic<int> write_calls{0};
  std::atomic<bool> held_lock_in_hook{false};

  MDSTableTestAccess::set_write_hook([&](Context* fin) {
    held_lock_in_hook = ceph_mutex_is_locked_by_me(*lock);
    write_calls++;
    delete fin;
  });

  auto start = std::chrono::steady_clock::now();
  {
    std::lock_guard l(*lock);
    table->save(nullptr);
  }
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_EQ(1, write_calls.load());
  EXPECT_TRUE(held_lock_in_hook.load())
      << "write path is invoked under mds_lock; it must only enqueue work";
  EXPECT_LT(elapsed, 1s)
      << "save must return without blocking on Objecter throttle";
  EXPECT_EQ(1u, table->get_committing_version());

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
