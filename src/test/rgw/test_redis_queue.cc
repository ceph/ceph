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
    // Remove the two queues
    // boost::asio::spawn(
    //     io,
    //     [this](boost::asio::yield_context yield) {
    //       boost::redis::request req;
    //       boost::redis::response<int> resp;

    //       std::string name = "test_queue";
    //       req.push("DEL", "reserve:" + name);
    //       rgw::redis::doRedisFunc(conn, req, resp, yield);

    //       req.clear();
    //       //   resp.clear();

    //       req.push("DEL", "reserve:" + name);
    //       rgw::redis::doRedisFunc(conn, req, resp, yield);
    //     },
    //     [this](std::exception_ptr eptr) {
    //       if (eptr) std::rethrow_exception(eptr);
    //       io.stop();
    //     });
    delete conn;
    delete cfg;
  }
};

TEST_F(RGWRedisQueueTest, Reserve) {
  boost::asio::spawn(
      io,
      [this](boost::asio::yield_context yield) {
        int res = rgw::redisqueue::reserve(conn, "test_queue", yield);
        ASSERT_EQ(res, 0);
      },
      [this](std::exception_ptr eptr) {
        if (eptr) std::rethrow_exception(eptr);
        io.stop();
      });
  io.run();
}
