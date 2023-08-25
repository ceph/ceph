#include <thread>
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class ConnectionFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      conn = new connection{io};

      /* Run context */
      using Executor = net::io_context::executor_type;
      using Work = net::executor_work_guard<Executor>;
      work = new std::optional<Work>(io.get_executor());
      worker = new std::thread([&] { io.run(); });
      
      config cfg;
      cfg.addr.host = "127.0.0.1";
      cfg.addr.port = "6379";

      conn->async_run(cfg, {}, net::detached);
    }

    virtual void TearDown() {
      io.stop();

      delete conn;
      delete work;
      delete worker;
    }

    net::io_context io;
    connection* conn;

    using Executor = net::io_context::executor_type;
    using Work = net::executor_work_guard<Executor>;
    std::optional<Work>* work;
    std::thread* worker;
};

TEST_F(ConnectionFixture, Ping) {
  ASSERT_NE(conn, nullptr);

  request req;
  req.push("PING", "Hello world");

  response<std::string> resp;

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, 0);
    EXPECT_EQ(std::get<0>(resp).value(), "Hello world");

    conn->cancel();
  });

  *work = std::nullopt;
  worker->join();
}

TEST_F(ConnectionFixture, HMSet) {
  request req;
  response<std::string> resp;

  req.push("HMSET", "key", "data", "value");

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, 0);
    EXPECT_EQ(std::get<0>(resp).value(), "OK");

    conn->cancel();
  });

  *work = std::nullopt;
  worker->join();
}

TEST_F(ConnectionFixture, HGETALL) {
  request req;
  response<std::string,
           std::map<std::string, std::string> > resp;

  req.push("HMSET", "key", "data", "value");
  req.push("HGETALL", "key");

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, 0);
    EXPECT_EQ(std::get<0>(resp).value(), "OK");
    EXPECT_EQ(std::get<1>(resp).value().size(), 1);

    conn->cancel();
  });

  *work = std::nullopt;
  worker->join();
}

TEST_F(ConnectionFixture, HMGET) {
  request req;
  response<std::string,
           std::vector<std::string> > resp;

  req.push("HMSET", "key", "data", "value");
  req.push("HMGET", "key", "data");

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, 0);
    EXPECT_EQ(std::get<0>(resp).value(), "OK");
    EXPECT_EQ(std::get<1>(resp).value()[0], "value");

    conn->cancel();
  });

  *work = std::nullopt;
  worker->join();
}

TEST_F(ConnectionFixture, HSET) {
  request req;
  response<int> resp;

  req.push_range("HSET", "key", std::map<std::string, std::string>{{"field", "value"}});

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, 0);
    EXPECT_EQ(std::get<0>(resp).value(), 1);

    conn->cancel();
  });

  *work = std::nullopt;
  worker->join();
}

TEST_F(ConnectionFixture, FLUSHALL) {
  request req;
  response<std::string, boost::redis::ignore_t> resp;

  req.push("HMSET", "key", "data", "value");
  req.push("FLUSHALL");

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, 0);
    EXPECT_EQ(std::get<0>(resp).value(), "OK");

    conn->cancel();
  });

  *work = std::nullopt;
  worker->join();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
