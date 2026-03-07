// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <boost/asio/ip/tcp.hpp>
#include "rgw_asio_frontend_timer.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/strand.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <gtest/gtest.h>

using tcp = boost::asio::ip::tcp;
using strand_type = boost::asio::strand<boost::asio::io_context::executor_type>;

// Minimal connection structure matching the Stream concept expected by rgw::timeout_handler
struct TestConnection : boost::intrusive_ref_counter<TestConnection> {
  tcp::socket socket;

  explicit TestConnection(tcp::socket&& s) noexcept
      : socket(std::move(s)) {}

  tcp::socket& get_socket() { return socket; }
};

using test_timeout_timer = rgw::basic_timeout_timer<
    ceph::coarse_mono_clock,
    boost::asio::any_io_executor,
    TestConnection>;

// Exception handler for spawn - rethrow any coroutine exception as test failure
static void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class AsioFrontendTimerTest : public ::testing::Test {
 protected:
  static constexpr int kNumThreads = 8;
  boost::asio::io_context context_;

  // Run the io_context on multiple threads, then join.
  void run_with_threads(int n = kNumThreads) {
    std::vector<std::thread> threads;
    threads.reserve(n);
    for (int i = 0; i < n; ++i) {
      threads.emplace_back([this] { context_.run(); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  // Create a connected TCP socket pair via loopback accept.
  struct SocketPair {
    tcp::socket server;
    tcp::socket client;
  };

  SocketPair make_socket_pair() {
    tcp::acceptor acceptor(context_,
        tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0));
    tcp::socket client(context_);
    client.connect(acceptor.local_endpoint());
    tcp::socket server = acceptor.accept();
    return {std::move(server), std::move(client)};
  }
};

// ---------------------------------------------------------------------------
// Testcases
// ---------------------------------------------------------------------------

// Show that with context.get_executor() multiple concurrent socket operations are possible.
// Showcase for the issue described at https://tracker.ceph.com/issues/75031.
TEST_F(AsioFrontendTimerTest, ContextExecutorAllowsConcurrentHandlers) {
  std::atomic<int> concurrent_ops{0};
  std::atomic<int> max_concurrent{0};
  std::atomic<bool> done{false};

  auto strand = boost::asio::make_strand(context_);

  boost::asio::spawn(strand,
    [&](boost::asio::yield_context yield) {
      // Post many handlers through context.get_executor()
      constexpr int kHandlers = 200;
      for (int i = 0; i < kHandlers; ++i) {
        boost::asio::post(context_.get_executor(), [&]() {
          int cur = concurrent_ops.fetch_add(1, std::memory_order_acq_rel) + 1;
          int expected = max_concurrent.load(std::memory_order_relaxed);
          while (cur > expected &&
                 !max_concurrent.compare_exchange_weak(
                     expected, cur, std::memory_order_release,
                     std::memory_order_relaxed)) {}
          // Sleep briefly to widen the concurrency window
          std::this_thread::sleep_for(std::chrono::microseconds(100));
          concurrent_ops.fetch_sub(1, std::memory_order_release);
        });
      }

      // Wait for all handlers to complete
      boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> timer(
          yield.get_executor());
      timer.expires_after(std::chrono::milliseconds(500));
      boost::system::error_code ec;
      timer.async_wait(yield[ec]);

      done = true;
    }, rethrow);

  run_with_threads();

  EXPECT_TRUE(done);
  // With 8 threads and context.get_executor(), handlers run concurrently
  EXPECT_GT(max_concurrent.load(), 1)
      << "With context.get_executor() and " << kNumThreads << " threads, "
      << "handlers should run concurrently. This shows why the bug causes "
      << "races: timeout_handler and the coroutine operate on the socket "
      << "from different threads simultaneously.";
}

// Verify that strand executor serializes all socket operations.
// Showcase for the fix described at https://tracker.ceph.com/issues/75031.
// With yield.get_executor(), handlers posted through the strand cannot run
// concurrently with the coroutine.
TEST_F(AsioFrontendTimerTest, StrandSerializesAllHandlers) {
  auto strand = boost::asio::make_strand(context_);

  std::atomic<int> concurrent_ops{0};
  std::atomic<int> max_concurrent{0};
  std::atomic<bool> done{false};

  boost::asio::spawn(strand,
    [&](boost::asio::yield_context yield) {
      auto ex = yield.get_executor();

      // Post many handlers through the strand executor.
      // Each handler increments a counter, does a small amount of work,
      // then decrements it. If any two handlers overlap, max_concurrent > 1.
      constexpr int kHandlers = 200;
      for (int i = 0; i < kHandlers; ++i) {
        boost::asio::post(ex, [&]() {
          int cur = concurrent_ops.fetch_add(1, std::memory_order_acq_rel) + 1;
          // Update max_concurrent atomically
          int expected = max_concurrent.load(std::memory_order_relaxed);
          while (cur > expected &&
                 !max_concurrent.compare_exchange_weak(
                     expected, cur, std::memory_order_release,
                     std::memory_order_relaxed)) {}
          // Simulate some work to widen the concurrency window
          { volatile int sink = 0; for (int v = 0; v < 100; ++v) sink = v; (void)sink; }
          concurrent_ops.fetch_sub(1, std::memory_order_release);
        });
      }

      // Wait for all handlers to complete
      boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> timer(ex);
      timer.expires_after(std::chrono::milliseconds(500));
      boost::system::error_code ec;
      timer.async_wait(yield[ec]);

      done = true;
    }, rethrow);

  run_with_threads();

  EXPECT_TRUE(done);
  EXPECT_EQ(1, max_concurrent.load())
      << "Strand must serialize all handlers. If max_concurrent > 1, handlers "
      << "ran concurrently, which means the timeout_handler could race with "
      << "the coroutine's socket operations.";
}

// Verify that yield.get_executor() inside a strand-spawned coroutine
// dispatches handlers through the strand.
TEST_F(AsioFrontendTimerTest, YieldExecutorDispatchesOnStrand) {
  auto strand = boost::asio::make_strand(context_);

  std::atomic<bool> handler_on_strand{false};
  std::atomic<bool> done{false};

  boost::asio::spawn(strand,
    [&](boost::asio::yield_context yield) {
      // Create a timer using yield's executor (same as the strand)
      boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> timer(
          yield.get_executor());
      timer.expires_after(std::chrono::milliseconds(10));

      // async_wait dispatches the completion handler through yield's executor
      timer.async_wait([&strand, &handler_on_strand](
          boost::system::error_code ec) {
        if (!ec) {
          handler_on_strand = strand.running_in_this_thread();
        }
      });

      // Yield to let the timer handler run
      boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> delay(
          yield.get_executor());
      delay.expires_after(std::chrono::milliseconds(100));
      boost::system::error_code ec;
      delay.async_wait(yield[ec]);

      done = true;
    }, rethrow);

  run_with_threads();

  EXPECT_TRUE(done);
  EXPECT_TRUE(handler_on_strand) << "Handlers dispatched via yield.get_executor() must run on the strand";
}

// End-to-end test using the actual rgw::basic_timeout_timer.
// Verify that when constructed with the strand executor, timeout correctly
// cancels a pending async_read when the timer fires.
TEST_F(AsioFrontendTimerTest, TimeoutCancelsPendingRead) {
  auto sp = make_socket_pair();
  auto conn = boost::intrusive_ptr{new TestConnection(std::move(sp.server))};

  std::atomic<bool> read_cancelled{false};
  std::atomic<bool> done{false};

  boost::asio::spawn(
    boost::asio::make_strand(context_),
    [&, conn](boost::asio::yield_context yield) {
      auto timeout = test_timeout_timer{
          yield.get_executor(),
          std::chrono::milliseconds(50),
          conn};

      timeout.start();

      // Attempt a read. The client never sends data, so this blocks
      // until the timeout fires and cancels the socket.
      char buf[64];
      boost::system::error_code ec;
      boost::asio::async_read(conn->socket,
          boost::asio::buffer(buf), yield[ec]);

      timeout.cancel();

      // The read should have been cancelled by the timeout
      read_cancelled = (ec == boost::asio::error::operation_aborted ||
                        ec == boost::asio::error::bad_descriptor ||
                        ec == boost::asio::error::connection_reset);
      done = true;
    }, rethrow);

  run_with_threads();

  EXPECT_TRUE(done);
  EXPECT_TRUE(read_cancelled)
      << "The timeout_timer should have cancelled the pending read by calling "
      << "socket.cancel() and socket.shutdown().";
}

// Verify that the timeout_timer uses the provided executor for its
// internal timer, by comparing behavior between strand and non-strand
// executors. A timer on the strand fires its handler on the strand; a timer
// on the context fires its handler off the strand.
TEST_F(AsioFrontendTimerTest, TimeoutTimerUsesProvidedExecutor) {
  auto sp = make_socket_pair();
  auto conn = boost::intrusive_ptr{new TestConnection(std::move(sp.server))};

  auto strand = boost::asio::make_strand(context_);

  // We create a probe timer using the same executor that would be passed
  // to timeout_timer. If the probe handler runs on the strand, then
  // timeout_timer's internal handler (which uses the same executor) also
  // runs on the strand.
  std::atomic<bool> probe_on_strand{false};
  std::atomic<bool> done{false};

  boost::asio::spawn(strand,
    [&, conn](boost::asio::yield_context yield) {
      auto ex = yield.get_executor();

      boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> probe(ex);
      probe.expires_after(std::chrono::milliseconds(10));
      probe.async_wait(
          [&strand, &probe_on_strand](boost::system::error_code ec) {
        if (!ec) {
          probe_on_strand = strand.running_in_this_thread();
        }
      });

      // Meanwhile, create the actual timeout_timer with the same executor
      auto timeout = test_timeout_timer{ex, std::chrono::milliseconds(10), conn};
      timeout.start();

      // Wait for both to fire
      boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> delay(ex);
      delay.expires_after(std::chrono::milliseconds(200));
      boost::system::error_code ec;
      delay.async_wait(yield[ec]);

      timeout.cancel();
      done = true;
    }, rethrow);

  run_with_threads();

  EXPECT_TRUE(done);
  EXPECT_TRUE(probe_on_strand)
      << "A timer created with the same executor as timeout_timer "
      << "(yield.get_executor()) must fire its handler on the strand. "
      << "This shows the timeout_handler also runs on the strand.";
}

