// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw/rgw_asio_frontend_connection.h"

#include <memory>
#include <thread>

#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/write.hpp>
#include <boost/intrusive_ptr.hpp>

#include "common/async/shared_mutex.h"

#include <gtest/gtest.h>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using rgw::asio::Connection;
using rgw::asio::ConnectionList;

// Testcases for ConnectionList::close() shutdown behavior.
//
// Verifies that close() uses socket.cancel() + ::shutdown() rather than
// socket.close(). Calling socket.close() from outside the socket's strand
// is a boost::asio thread-safety violation that can SIGSEGV or orphaned
// pending operations.

// Verify that ConnectionList::close() does NOT close the sockets (fd stays
// valid, is_open() remains true) but does shut down the transport (writes
// fail). This ensures we never call socket.close() from outside the strand.
TEST(BeastFrontendShutdown, CloseShutdownsButDoesNotCloseSocket)
{
  asio::io_context ioctx;
  asio::spawn(
      ioctx.get_executor(),
      [&](asio::yield_context yield) -> void {
        ConnectionList connections;

        tcp::acceptor acceptor(ioctx, tcp::endpoint(tcp::v4(), 0));
        acceptor.listen(3);

        tcp::socket c0(ioctx), c1(ioctx), c2(ioctx);
        c0.async_connect(acceptor.local_endpoint(), yield);
        auto s0 = acceptor.async_accept(yield);
        c1.async_connect(acceptor.local_endpoint(), yield);
        auto s1 = acceptor.async_accept(yield);
        c2.async_connect(acceptor.local_endpoint(), yield);
        auto s2 = acceptor.async_accept(yield);

        boost::intrusive_ptr<Connection> conns[] = {
            new Connection(
                std::move(s0), asio::make_strand(yield.get_executor())),
            new Connection(
                std::move(s1), asio::make_strand(yield.get_executor())),
            new Connection(
                std::move(s2), asio::make_strand(yield.get_executor())),
        };

        {
          auto g0 = connections.add(*conns[0]);
          auto g1 = connections.add(*conns[1]);
          auto g2 = connections.add(*conns[2]);

          ASSERT_EQ(3u, connections.size());

          connections.close();

          // After close(): sockets must still be "open" from boost::asio's
          // perspective (fd not released, descriptor_data not zeroed).
          // socket.close() would have set is_open()=false and fd=-1, which is the
          // thread-safety violation that causes SIGSEGV.
          for (auto& conn : conns) {
            EXPECT_TRUE(conn->socket.is_open());
            EXPECT_GE(conn->socket.native_handle(), 0);
          }

          // But the transport is shut down — writes must fail
          for (auto& conn : conns) {
            boost::system::error_code ec;
            char buf[] = "test";
            asio::async_write(conn->socket, asio::buffer(buf), yield[ec]);
            EXPECT_TRUE(ec.failed()) << "Write should fail after shutdown";
          }
        } // guards destroyed here — connections removed from list

        EXPECT_EQ(0u, connections.size());
      },
      [](std::exception_ptr eptr) {
        if (eptr) {
          std::rethrow_exception(eptr);
        }
      });
  ioctx.run();
}

// Testcases for lock_or_close_on_timeout(), used by the frontend to bound a
// graceful pause during realm reload.

using SharedMutex = ceph::async::SharedMutex<asio::any_io_executor>;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

class BeastFrontendPause : public ::testing::Test {
 protected:
  asio::io_context ioctx;
  asio::executor_work_guard<asio::io_context::executor_type> work =
      asio::make_work_guard(ioctx);
  std::thread worker;
  SharedMutex mutex{ioctx.get_executor()};
  ConnectionList connections;
  tcp::acceptor acceptor{ioctx, tcp::endpoint(tcp::v4(), 0)};
  tcp::socket client{ioctx};
  boost::intrusive_ptr<Connection> conn;
  std::unique_ptr<ConnectionList::Guard> guard;

  void SetUp() override {
    acceptor.listen(1);
    client.connect(acceptor.local_endpoint());
    conn = new Connection(acceptor.accept(), asio::make_strand(ioctx));
    // Guard removes the connection on destruction, so it must not be copied
    guard.reset(new ConnectionList::Guard{connections.add(*conn)});
    ASSERT_EQ(1u, connections.size());
    // hold a shared lock like an outstanding request does
    ASSERT_TRUE(mutex.try_lock_shared());
  }

  void TearDown() override {
    guard.reset();
    work.reset();
    ioctx.stop();
    if (worker.joinable()) {
      worker.join();
    }
  }

  void run() {
    worker = std::thread([this] { ioctx.run(); });
  }

  // the outstanding request reads from its connection and releases the shared
  // lock once the read fails, which happens when the connection is closed
  void spawn_reader() {
    asio::spawn(conn->strand, [this] (asio::yield_context yield) {
        char buf[16];
        boost::system::error_code ec;
        conn->socket.async_read_some(asio::buffer(buf), yield[ec]);
        mutex.unlock_shared();
      }, asio::detached);
  }

  // the outstanding request finishes on its own after the given delay
  void spawn_finisher(Clock::duration delay) {
    asio::spawn(ioctx, [this, delay] (asio::yield_context yield) {
        asio::steady_timer timer(ioctx, delay);
        timer.async_wait(yield);
        mutex.unlock_shared();
      }, asio::detached);
  }

  bool connection_writable() {
    boost::system::error_code ec;
    char buf[] = "test";
    asio::write(conn->socket, asio::buffer(buf), ec);
    return !ec;
  }
};

TEST_F(BeastFrontendPause, TimeoutClosesConnections)
{
  spawn_reader();
  run();

  boost::system::error_code ec;
  const auto start = Clock::now();
  EXPECT_TRUE(rgw::asio::lock_or_close_on_timeout(mutex, connections,
                                                  200ms, ec));
  const auto elapsed = Clock::now() - start;
  EXPECT_FALSE(ec);
  EXPECT_GE(elapsed, 200ms);
  EXPECT_LT(elapsed, 10s);
  mutex.unlock();

  EXPECT_FALSE(connection_writable());
}

TEST_F(BeastFrontendPause, DrainBeforeTimeoutKeepsConnections)
{
  spawn_finisher(50ms);
  run();

  boost::system::error_code ec;
  const auto start = Clock::now();
  EXPECT_FALSE(rgw::asio::lock_or_close_on_timeout(mutex, connections,
                                                   30s, ec));
  const auto elapsed = Clock::now() - start;
  EXPECT_FALSE(ec);
  EXPECT_LT(elapsed, 30s);
  mutex.unlock();

  EXPECT_TRUE(connection_writable());
}

TEST_F(BeastFrontendPause, ZeroTimeoutWaitsWithoutClosing)
{
  spawn_finisher(300ms);
  run();

  boost::system::error_code ec;
  const auto start = Clock::now();
  EXPECT_FALSE(rgw::asio::lock_or_close_on_timeout(mutex, connections,
                                                   0s, ec));
  EXPECT_FALSE(ec);
  EXPECT_GE(Clock::now() - start, 300ms);
  mutex.unlock();

  EXPECT_TRUE(connection_writable());
}
