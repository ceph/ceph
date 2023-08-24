#include <iostream>
#include <spawn/spawn.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "../rgw/driver/d4n/d4n_directory.h" // Fix -Sam

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

std::string portStr;
std::string hostStr;
std::string redisHost = "";

class DirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      dir = new rgw::d4n::BlockDirectory{io, hostStr, stoi(portStr)};
      block = new rgw::d4n::CacheBlock{
        .cacheObj = {
	  .objName = "testName",
	  .bucketName = "testBucket",
	  .creationTime = 0,
	  .dirty = false,
	  .hostsList = {redisHost}
	},
	.version = 0,
	.size = 0,
	.hostsList = {redisHost}
      };

      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(block, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      dir->init();

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = hostStr;
      cfg.addr.port = portStr;

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete conn;
      delete block;
      delete dir;
    }

    rgw::d4n::CacheBlock* block;
    rgw::d4n::BlockDirectory* dir;

    net::io_context io;
    connection* conn;

    std::vector<std::string> vals{"0", "0", "0", redisHost, 
                                   "testName", "testBucket", "0", "0", redisHost};
    std::vector<std::string> fields{"version", "size", "globalWeight", "blockHosts", 
                                   "objName", "bucketName", "creationTime", "dirty", "objHosts"};
};

TEST_F(DirectoryFixture, SetValueYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    ASSERT_EQ(0, dir->set_value(block, optional_yield{io, yield}));
    dir->shutdown();
  });

    request req;
    req.push_range("HMGET", "testBucket_testName_0", fields);
    req.push("FLUSHALL");

    response< std::vector<std::string>,
	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, [&](auto ec, auto) {
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), vals);

      conn->cancel();
    });

  io.run();
}

#if 0
/* Successful set_value Call and Redis Check */
TEST_F(DirectoryFixture, SetValueTest) {
  blockDir->init();
  int setReturn = blockDir->set_value(block, null_yield);

  ASSERT_EQ(setReturn, 0);

  vector<std::string> results;
  request req;
  req.push_range("HMGET", "testBucket_testName_0", fields);
  req.push("FLUSHALL");

  response< std::vector<std::string>,
            boost::redis::ignore_t > resp;

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(results, vals);*/

    conn->cancel();
  });
}

/* Successful get_value Calls and Redis Check */
TEST_F(DirectoryFixture, GetValueTest) {
  blockDir->init();
  int setReturn = blockDir->set_value(block, null_yield);

  ASSERT_EQ(setReturn, 0);

  /* Check if object name in directory instance matches redis update */
  request req;
  req.push("HSET", "testBucket_testName_0");
  req.push("FLUSHALL");

  response<std::string, boost::redis::ignore_t> resp;

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "OK");

    conn->cancel();
  });

  int getReturn = blockDir->get_value(block, null_yield);

  ASSERT_EQ(getReturn, 0);

  *work = std::nullopt;
  worker->join();

  EXPECT_EQ(block->cacheObj.objName, "newoid");
}

/* Successful del_value Call and Redis Check */
TEST_F(DirectoryFixture, DelValueTest) {
  cpp_redis::client client;
  vector<string> keys;
  int setReturn = blockDir->set_value(block, null_yield);

  ASSERT_EQ(setReturn, 0);

  /* Ensure entry exists in directory before deletion */
  keys.push_back("rgw-object:" + oid + ":block-directory");

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 1);
    }
  });

  int delReturn = blockDir->del_value(block, null_yield);

  ASSERT_EQ(delReturn, 0);

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 0);  /* Zero keys exist */
    }
  });

  client.flushall();

  *work = std::nullopt;
  worker->join();
}
#endif

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  /* Other ports can be passed to the program */
  if (argc == 1) {
    portStr = "6379";
    hostStr = "127.0.0.1";
  } else if (argc == 3) {
    hostStr = argv[1];
    portStr = argv[2];
  } else {
    std::cout << "Incorrect number of arguments." << std::endl;
    return -1;
  }

  redisHost = hostStr + ":" + portStr;

  return RUN_ALL_TESTS();
}
