#include <iostream>
#include <spawn/spawn.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "driver/d4n/d4n_directory.h"

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
      std::vector<const char*> args;
      std::string conf_file_list;
      std::string cluster = "";
      CephInitParameters iparams = ceph_argparse_early_args(
	args, CEPH_ENTITY_TYPE_CLIENT,
	&cluster, &conf_file_list);

      auto cct = common_preinit(iparams, CODE_ENVIRONMENT_UTILITY, {}); 
      auto dpp = new DoutPrefix(cct->get(), dout_subsys, "D4N Directory Test: ");

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

      dir->init(cct, dpp);

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

    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", "testBucket_testName_0", fields);
    req.push("FLUSHALL");

    response< std::vector<std::string>,
	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), vals);
    conn->cancel();
  });

  io.run();
}

TEST_F(DirectoryFixture, GetValueYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    ASSERT_EQ(0, dir->set_value(block, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket_testName_0", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ(0, dir->get_value(block, optional_yield{io, yield}));
    EXPECT_EQ(block->cacheObj.objName, "newoid");
    dir->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    conn->cancel();
  });

  io.run();
}

TEST_F(DirectoryFixture, DelValueYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    ASSERT_EQ(0, dir->set_value(block, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del_value(block, optional_yield{io, yield}));
    dir->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  });

  io.run();
}

TEST_F(DirectoryFixture, UpdateValueYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    ASSERT_EQ(0, dir->set_value(block, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del_value(block, optional_yield{io, yield}));
    dir->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  });

  io.run();
}

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
