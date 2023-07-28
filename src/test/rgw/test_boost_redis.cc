#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <gtest/gtest.h>

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class ConnectionFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      config cfg;
      cfg.addr.host = "127.0.0.1";
      cfg.addr.port = "6379";

      conn = new connection{io};

      conn->async_run(cfg, {}, net::detached);
    }

    virtual void TearDown() {
      delete conn;
    }

    net::io_context io;
    connection* conn;
};

TEST_F(ConnectionFixture, Ping) {
  ASSERT_NE(conn, nullptr);

  request req;
  req.push("PING", "Hello world");

  response<std::string> resp;

  conn->async_exec(req, resp, [&](auto ec, auto) {
    if (!ec)
      std::cout << "PING: " << std::get<0>(resp).value() << std::endl;
    conn->cancel();
  });

  io.run();

  ASSERT_EQ(std::get<0>(resp).value(), "Hello world");
}

TEST_F(ConnectionFixture, HMSet) {
  request req;
  response<std::string> resp;

  req.push("HMSET", "key", "data", "value");

  conn->async_exec(req, resp, [&](auto ec, auto) {
    if (!ec)
      std::cout << "Result: " << std::get<0>(resp).value() << std::endl;

    conn->cancel();
  });

  io.run();

  ASSERT_EQ(std::get<0>(resp).value(), "OK");
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
