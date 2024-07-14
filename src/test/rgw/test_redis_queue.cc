#include <gtest/gtest.h>
#include <rgw_redis_queue.h>

#include <boost/asio/io_context.hpp>

using boost::redis::config;
using boost::redis::connection;

class RGWRedisQueueTest : public ::testing::Test {
 protected:
  boost::asio::io_context io;
  connection* conn;
  config* cfg;

  void SetUp() {
    // Creating the default config
    cfg = new config;
    conn = new connection(io);

    boost::asio::spawn(
        io,
        [this](boost::asio::yield_context yield) {
          int res = rgw::redisqueue::initQueue(io, conn, cfg, yield);
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
  }
};

TEST_F(RGWRedisQueueTest, Reserve) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<int, int> status;

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, Commit) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<int, int> status;

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);

        res = rgw::redisqueue::commit(conn, "test_queue", "test_data", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue + 1);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}

TEST_F(RGWRedisQueueTest, Abort) {
  io.restart();
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res;
        std::tuple<int, int> status;

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);

        int initial_reserve = std::get<0>(status);
        int initial_queue = std::get<1>(status);

        res = rgw::redisqueue::reserve(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve + 1);
        ASSERT_EQ(std::get<1>(status), initial_queue);

        res = rgw::redisqueue::abort(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);

        res = rgw::redisqueue::queueStatus(conn, "test_queue", status, yield);
        ASSERT_EQ(res, 0);
        ASSERT_EQ(std::get<0>(status), initial_reserve);
        ASSERT_EQ(std::get<1>(status), initial_queue);
      },
      [this](std::exception_ptr eptr) {
        conn->cancel();
        if (eptr) std::rethrow_exception(eptr);
      });
  io.run();
}
