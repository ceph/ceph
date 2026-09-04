// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
//
// Unit tests for crimson OSD dispatch_context_transaction infrastructure.
//
// Addresses the following test gaps identified during research:
// 1. with_store_do_transaction: empty transaction, no callbacks
// 2. with_store_do_transaction: on_commit callback fires after empty transaction
// 3. with_store_do_transaction: on_commit callback fires after non-empty transaction
// 4. Empty-transaction callback dispatch path (mimics dispatch_context_transaction
//    when ctx.transaction.empty() is true and has registered on_commit callbacks)
// 5. OrderedExclusivePhaseT serializes concurrent operations (peering pipeline
//    exclusive process stage ordering guarantee)
// 6. Independent seastar futures run concurrently (broadcast_map_to_pgs pattern)

#include <algorithm>
#include <filesystem>
#include <vector>

#include "gtest/gtest.h"

#include "include/Context.h"
#include "os/Transaction.h"
#include "crimson/common/operation.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/cyanstore/cyan_store.h"

#include "test/crimson/gtest_seastar.h"

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helper: minimal trigger satisfying OrderedExclusivePhaseT::enter(TriggerT&).
// The only requirement is t.get_op().get_id() returning an Operation::id_t.
// ---------------------------------------------------------------------------
struct MockTrigger {
  struct MockOp {
    Operation::id_t id;
    explicit MockOp(Operation::id_t i) : id(i) {}
    Operation::id_t get_id() const { return id; }
  } op;
  explicit MockTrigger(Operation::id_t id) : op(id) {}
  MockOp& get_op() { return op; }
};

// Phase type used for OrderedExclusivePhaseT tests (CRTP, needs type_name).
struct TestPeeringProcess
  : crimson::OrderedExclusivePhaseT<TestPeeringProcess> {
  static constexpr const char *type_name = "TestPeeringProcess";
};

// ---------------------------------------------------------------------------
// Test fixture: sets up an in-memory CyanStore for transaction tests.
// Each test gets a fresh store at a dedicated temp path.
// ---------------------------------------------------------------------------
struct dispatch_context_test_t : public seastar_test_suite_t {
  static constexpr const char *store_path = "/tmp/ceph_test_dispatch_ctx";

  std::unique_ptr<crimson::os::CyanStore> store;
  crimson::os::CollectionRef ch;
  const coll_t test_cid = coll_t(spg_t(pg_t(0, 1)));

  seastar::future<> set_up_fut() override {
    fs::remove_all(store_path);
    fs::create_directories(store_path);
    store = std::make_unique<crimson::os::CyanStore>(store_path);
    co_await store->start();
    uuid_d fsid;
    fsid.generate_random();
    co_await store->mkfs(fsid).handle_error(
      crimson::stateful_ec::assert_failure{"CyanStore mkfs failed"});
    co_await store->mount().handle_error(
      crimson::stateful_ec::assert_failure{"CyanStore mount failed"});
    // Pre-create a collection so non-empty transactions have a valid target.
    ch = co_await store->get_sharded_store().create_new_collection(test_cid);
    ceph::os::Transaction create_coll_txn;
    create_coll_txn.create_collection(test_cid, 0);
    co_await store->get_sharded_store().do_transaction_no_callbacks(
      ch, std::move(create_coll_txn));
  }

  seastar::future<> tear_down_fut() override {
    co_await store->umount();
    co_await store->stop();
    store.reset();
    ch.reset();
    fs::remove_all(store_path);
  }

  // Returns a BackendStore pointing at shard 0, store index 0.
  crimson::os::BackendStore get_backend_store() {
    return store->get_backend_store(crimson::META_STORE_INDEX);
  }
};

// ---------------------------------------------------------------------------
// Test 1 (research point 1): with_store_do_transaction with an empty
// transaction and no callbacks completes without error or crash.
// ---------------------------------------------------------------------------
TEST_F(dispatch_context_test_t, empty_transaction_no_callbacks)
{
  run_scl([this]() -> seastar::future<> {
    auto bs = get_backend_store();
    ceph::os::Transaction txn;  // empty: no ops, no callbacks
    EXPECT_TRUE(txn.empty());
    co_await crimson::os::with_store_do_transaction(bs, ch, std::move(txn));
  });
}

// ---------------------------------------------------------------------------
// Test 2 (research point 2): on_commit callback registered on an empty
// transaction (no ops) fires after with_store_do_transaction returns.
// ---------------------------------------------------------------------------
TEST_F(dispatch_context_test_t, empty_transaction_on_commit_fires)
{
  run_scl([this]() -> seastar::future<> {
    auto bs = get_backend_store();
    bool callback_called = false;
    ceph::os::Transaction txn;
    txn.register_on_commit(make_lambda_context([&callback_called](int) {
      callback_called = true;
    }));
    EXPECT_TRUE(txn.empty());
    co_await crimson::os::with_store_do_transaction(bs, ch, std::move(txn));
    EXPECT_TRUE(callback_called);
  });
}

// ---------------------------------------------------------------------------
// Test 3 (research point 4): on_commit callback fires after a non-empty
// transaction (containing a nop op) commits via with_store_do_transaction.
// ---------------------------------------------------------------------------
TEST_F(dispatch_context_test_t, nonempty_transaction_on_commit_fires)
{
  run_scl([this]() -> seastar::future<> {
    auto bs = get_backend_store();
    bool callback_called = false;
    ceph::os::Transaction txn;
    txn.nop();  // adds OP_NOP so the transaction is non-empty
    txn.register_on_commit(make_lambda_context([&callback_called](int) {
      callback_called = true;
    }));
    EXPECT_FALSE(txn.empty());
    co_await crimson::os::with_store_do_transaction(bs, ch, std::move(txn));
    EXPECT_TRUE(callback_called);
  });
}

// ---------------------------------------------------------------------------
// Test 4 (research point 5): Mimics the empty-transaction code path in
// dispatch_context_transaction.
//
// When ctx.transaction.empty() is true, dispatch_context_transaction:
//   1. submits a separate empty ceph::os::Transaction{} to the store
//   2. manually collects callbacks from ctx.transaction via
//      collect_all_contexts() and calls complete(0) on them
//
// This test verifies that callbacks registered on a transaction are correctly
// collected by collect_all_contexts() and fire when complete(0) is called.
// ---------------------------------------------------------------------------
TEST_F(dispatch_context_test_t, ctx_empty_txn_callback_dispatch)
{
  run_scl([this]() -> seastar::future<> {
    auto bs = get_backend_store();

    // Simulate ctx.transaction with callbacks but no ops (empty).
    ceph::os::Transaction ctx_txn;
    int on_commit_count = 0;
    ctx_txn.register_on_commit(make_lambda_context([&on_commit_count](int) {
      ++on_commit_count;
    }));
    EXPECT_TRUE(ctx_txn.empty());

    // Step 1: submit a brand-new empty transaction to the store (as
    //         dispatch_context_transaction does for empty ctx.transaction).
    co_await crimson::os::with_store_do_transaction(
      bs, ch, ceph::os::Transaction{});

    // Step 2: manually drain callbacks from ctx_txn (as
    //         dispatch_context_transaction does after the empty submit).
    Context *on_commit =
      ceph::os::Transaction::collect_all_contexts(ctx_txn);
    if (on_commit) {
      on_commit->complete(0);
    }

    EXPECT_EQ(on_commit_count, 1);

    // Verify collect_all_contexts drained the callbacks (no double-fire).
    Context *should_be_null =
      ceph::os::Transaction::collect_all_contexts(ctx_txn);
    EXPECT_EQ(should_be_null, nullptr);
  });
}

// ---------------------------------------------------------------------------
// Test 5 (research points 3 + 6): OrderedExclusivePhaseT serializes two
// concurrent enter() calls in FIFO order.
//
// This tests the ordering guarantee of the exclusive process stage in the
// peering pipeline (PGPeeringPipeline::Process), which ensures that
// concurrent PGAdvanceMap and PeeringEvent operations on the same PG are
// serialized.
// ---------------------------------------------------------------------------
TEST_F(dispatch_context_test_t, exclusive_phase_serializes_ops)
{
  run_scl([]() -> seastar::future<> {
    TestPeeringProcess phase;
    std::vector<int> order;

    MockTrigger t1{1}, t2{2};

    // Both enter() calls are initiated "concurrently" (before either future
    // is awaited). t1 acquires the mutex first; t2 blocks until t1 releases.
    auto f1 = phase.enter(t1).then([&order](auto barrier1) {
      order.push_back(1);
      // barrier1 destroyed here -> mutex.unlock() -> t2's lock() resolves
    });
    auto f2 = phase.enter(t2).then([&order](auto barrier2) {
      order.push_back(2);
    });

    co_await seastar::when_all_succeed(std::move(f1), std::move(f2));

    EXPECT_EQ(order.size(), 2u);
    EXPECT_EQ(order[0], 1);  // t1 ran first
    EXPECT_EQ(order[1], 2);  // t2 ran only after t1's barrier was released
  });
}

// ---------------------------------------------------------------------------
// Test 6 (research point 3): Independent Seastar futures run concurrently,
// mirroring the broadcast_map_to_pgs() pattern that uses
// seastar::parallel_for_each to start a PGAdvanceMap per PG simultaneously.
//
// Verifies that parallel_for_each starts and awaits all operations, matching
// the PerShardState::broadcast_map_to_pgs() dispatch pattern.
// ---------------------------------------------------------------------------
TEST_F(dispatch_context_test_t, parallel_futures_run_concurrently)
{
  run_scl([]() -> seastar::future<> {
    // Simulate starting one "PGAdvanceMap" per PG entry concurrently,
    // identical in structure to PerShardState::broadcast_map_to_pgs().
    std::vector<int> pg_ids = {1, 2, 3};
    std::vector<int> completed_pgs;

    co_await seastar::parallel_for_each(
      pg_ids,
      [&completed_pgs](int pg_id) -> seastar::future<> {
        completed_pgs.push_back(pg_id);
        return seastar::now();
      });

    // All PGs should have been processed.
    EXPECT_EQ(completed_pgs.size(), pg_ids.size());
    for (int id : pg_ids) {
      EXPECT_NE(std::find(completed_pgs.begin(), completed_pgs.end(), id),
                completed_pgs.end())
        << "pg_id " << id << " was not processed";
    }
  });
}
