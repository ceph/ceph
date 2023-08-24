#include <thread>
#include "../rgw/driver/d4n/d4n_directory.h" // Fix -Sam
#include "rgw_process_env.h"
#include <cpp_redis/cpp_redis>
#include <iostream>
#include <string>
#include "gtest/gtest.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

using namespace std;

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

string portStr;
string hostStr;
string redisHost = "";

class DirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      conn = new connection{io};

      /* Run context */
      using Executor = net::io_context::executor_type;
      using Work = net::executor_work_guard<Executor>;
      work = new std::optional<Work>(io.get_executor());
      worker = new std::thread([&] { io.run(); });

      /* Create block directory and cache block */
      blockDir = new rgw::d4n::BlockDirectory(io, hostStr, stoi(portStr));
      block = new rgw::d4n::CacheBlock();

      block->version = 0;
      block->size = 0; 
      block->globalWeight = 0; 
      block->hostsList.push_back(redisHost);

      block->cacheObj.objName = "testName";
      block->cacheObj.bucketName = "testBucket";
      block->cacheObj.creationTime = 0;
      block->cacheObj.dirty = false;
      block->cacheObj.hostsList.push_back(redisHost);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = hostStr;
      cfg.addr.port = portStr;

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete blockDir;
      delete block;

      io.stop();

      delete conn;
      delete work;
      delete worker;
    }

    rgw::d4n::BlockDirectory* blockDir;
    rgw::d4n::CacheBlock* block;

    net::io_context io;
    connection* conn;

    using Executor = net::io_context::executor_type;
    using Work = net::executor_work_guard<Executor>;
    std::optional<Work>* work;
    std::thread* worker;

    std::vector<std::string> vals{"0", "0", "0", redisHost, 
                                   "testName", "testBucket", "0", "0", redisHost};
    std::vector<std::string> fields{"version", "size", "globalWeight", "blockHosts", 
                                   "objName", "bucketName", "creationTime", "dirty", "objHosts"};
};

/* Successful initialization */
TEST_F(DirectoryFixture, DirectoryInit) {
  ASSERT_NE(blockDir, nullptr);
  ASSERT_NE(block, nullptr);
  ASSERT_NE(redisHost.length(), (long unsigned int)0);

  conn->cancel();
  *work = std::nullopt;
  worker->join();
}

/* Successful set_value Call and Redis Check */
TEST_F(DirectoryFixture, SetValueTest) {
  blockDir->init();
  int setReturn = blockDir->set_value(block, null_yield);

  ASSERT_EQ(setReturn, 0);

  #if 0
  vector<std::string> results;
  request req;
  req.push_range("HGETALL", "testBucket_testName_0");
  //req.push_range("HMGET", "testBucket_testName_0", fields);
  //req.push("FLUSHALL");

  response< std::map<std::string, std::string>,
            boost::redis::ignore_t > resp;

  conn->async_exec(req, resp, [&](auto ec, auto) {
    ASSERT_EQ((bool)ec, false);

    /*for (auto const& pair : std::get<0>(resp).value()) {
      results.push_back(pair.second);
    }
    EXPECT_EQ(results, vals);*/


    for (auto const& pair : std::get<0>(resp).value()) {
      if (pair.first == "version")
	EXPECT_EQ(pair.second, vals[0]);
      else if (pair.first == "size")
	EXPECT_EQ(pair.second, vals[1]);
      else if (pair.first == "globalWeight")
	EXPECT_EQ(pair.second, vals[2]);
      else if (pair.first == "blockHosts")
	EXPECT_EQ(pair.second, vals[3]);
      else if (pair.first == "objName")
	EXPECT_EQ(pair.second, vals[4]);
      else if (pair.first == "bucketName")
	EXPECT_EQ(pair.second, vals[5]);
      else if (pair.first == "creationTime")
	EXPECT_EQ(pair.second, vals[6]);
      else if (pair.first == "dirty")
	EXPECT_EQ(pair.second, vals[7]);
      else if (pair.first == "objHosts")
	EXPECT_EQ(pair.second, vals[8]);
      else
        EXPECT_EQ(false, true); /* Redundant fail; this statement should not be entered */
    }

    conn->cancel();
  });
  #endif

  *work = std::nullopt;
  worker->join();
}

#if 0
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
    cout << "Incorrect number of arguments." << std::endl;
    return -1;
  }

  redisHost = hostStr + ":" + portStr;

  return RUN_ALL_TESTS();
}
