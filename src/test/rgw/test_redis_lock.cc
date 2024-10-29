#include <gtest/gtest.h>
#include <rgw_redis_lock.h>

#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "common/async/blocked_completion.h"

using boost::redis::config;
using boost::redis::connection;

static constexpr auto dout_subsys = ceph_subsys_rgw;

class TestDPP : public DoutPrefixProvider {
  CephContext* get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out; }
};

class RGWRedisLockTest : public ::testing::Test {
 protected:
  boost::asio::io_context io;
  connection* conn;
  config* cfg;
  DoutPrefixProvider* dpp;

  void SetUp() {
    cfg = new config();
    conn = new connection(io);
    dpp = new TestDPP();

    boost::asio::spawn(
        io,
        [this](boost::asio::yield_context yield) {
          int res = rgw::redis::load_lua_rgwlib(io, conn, cfg, yield);
          ASSERT_EQ(res, 0);
        },
        [this](std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
          io.stop();
        });
    io.run();
  }

  void TearDown() {
    delete conn;
    delete cfg;
    delete dpp;
  }
};

TEST_F(RGWRedisLockTest, Lock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int duration = 1000;
        const std::string name = "lock:lock";
        const std::string cookie = "mycookie";

        int return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code =
            rgw::redislock::assert_locked(dpp, conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, Unlock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:unlock";
        const std::string cookie = "mycookie";
        int duration = 1000;

        int return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code = rgw::redislock::unlock(dpp, conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);

        return_code =
            rgw::redislock::assert_locked(dpp, conn, name, cookie, yield);
        ASSERT_EQ(return_code, -ENOENT);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, RenewBeforeLeaseExpiry) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:renew";
        const std::string cookie = "mycookie";
        int duration = 1000;

        int return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // wait for 500ms
        boost::asio::steady_timer timer(io, std::chrono::milliseconds(500));
        timer.async_wait(yield);

        return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // wait for 600ms - the initial lock timeout has expired by now but the
        // renewel process has kept it valid
        return_code =
            rgw::redislock::assert_locked(dpp, conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
}

TEST_F(RGWRedisLockTest, AcquireAfterRenew) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:renewacquire";
        const std::string cookie1 = "mycookie1";
        const std::string cookie2 = "mycookie2";
        int duration = 1000;

        int return_code =
            rgw::redislock::lock(dpp, conn, name, cookie1, duration, yield);
        ASSERT_EQ(return_code, 0);

        boost::asio::steady_timer timer(io, std::chrono::milliseconds(500));
        timer.async_wait(yield);

        return_code =
            rgw::redislock::lock(dpp, conn, name, cookie1, duration, yield);
        ASSERT_EQ(return_code, 0);

        // Try to acquire the lock with another client after the initial client
        // has renewed it
        return_code =
            rgw::redislock::lock(dpp, conn, name, cookie2, duration, yield);
        ASSERT_EQ(return_code, -EBUSY);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

// Lock is expired and then taken over by another client
// A renew attempt shall fail with EBUSY
TEST_F(RGWRedisLockTest, RenewAfterReacquisition) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        const std::string name = "lock:renewreacquire";
        const std::string cookie = "mycookie";
        int duration = 500;

        int return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code =
            rgw::redislock::assert_locked(dpp, conn, name, cookie, yield);
        ASSERT_EQ(return_code, 0);

        // wait for the lock to expire
        boost::asio::steady_timer timer(io, std::chrono::milliseconds(1000));
        timer.async_wait(yield);

        const std::string new_cookie = "differentcookie";
        return_code =
            rgw::redislock::lock(dpp, conn, name, new_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        // attempt to lock with the initial client
        return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, -EBUSY);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, MultiLock) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int duration = 12000;
        const std::string name = "lock:multi";
        const std::string clientCookie1 = "mycookie1";
        const std::string clientCookie2 = "mycookie2";

        int return_code = rgw::redislock::lock(dpp, conn, name, clientCookie1,
                                               duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code = rgw::redislock::lock(dpp, conn, name, clientCookie2,
                                           duration, yield);
        ASSERT_EQ(return_code, -EBUSY);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisLockTest, Timeout) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int duration = 500;
        const std::string name = "lock:timeout";
        const std::string cookie = "mycookie";

        int return_code =
            rgw::redislock::lock(dpp, conn, name, cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        boost::asio::steady_timer timer(io, std::chrono::milliseconds(1000));
        timer.async_wait(yield);

        return_code =
            rgw::redislock::assert_locked(dpp, conn, name, cookie, yield);
        ASSERT_EQ(return_code, -ENOENT);

        const std::string new_cookie = "newcookie";
        return_code =
            rgw::redislock::lock(dpp, conn, name, new_cookie, duration, yield);
        ASSERT_EQ(return_code, 0);

        return_code =
            rgw::redislock::assert_locked(dpp, conn, name, new_cookie, yield);
        ASSERT_EQ(return_code, 0);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}
