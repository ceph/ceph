#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "driver/d4n/d4n_directory.h"

#define dout_subsys ceph_subsys_rgw

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class Environment* env;

class Environment : public ::testing::Environment {
  public:
    Environment() {}

    virtual ~Environment() {}

    void SetUp() override {
      std::vector<const char*> args;
      std::string conf_file_list;
      std::string cluster = "";
      CephInitParameters iparams = ceph_argparse_early_args(
	args, CEPH_ENTITY_TYPE_CLIENT,
	&cluster, &conf_file_list);

      cct = common_preinit(iparams, CODE_ENVIRONMENT_UTILITY, {}); 
      dpp = new DoutPrefix(cct->get(), dout_subsys, "D4N Object Directory Test: ");
      
      redisHost = cct->_conf->rgw_d4n_host + ":" + std::to_string(cct->_conf->rgw_d4n_port); 
    }
    
    void TearDown() override {
      delete dpp;
    }

    std::string redisHost;
    CephContext* cct;
    DoutPrefixProvider* dpp;
};

class ObjectDirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      dir = new rgw::d4n::ObjectDirectory{io};
      obj = new rgw::d4n::CacheObj{
	.objName = "testName",
	.bucketName = "testBucket",
	.creationTime = "",
	.dirty = false,
	.hostsList = { env->redisHost }
      };

      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(obj, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      dir->init(env->cct, env->dpp);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->cct->_conf->rgw_d4n_host;
      cfg.addr.port = std::to_string(env->cct->_conf->rgw_d4n_port);

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete conn;
      delete obj;
      delete dir;
    }

    rgw::d4n::CacheObj* obj;
    rgw::d4n::ObjectDirectory* dir;

    net::io_context io;
    connection* conn;

    std::vector<std::string> vals{"testName", "testBucket", "", "0", env->redisHost};
    std::vector<std::string> fields{"objName", "bucketName", "creationTime", "dirty", "objHosts"};
};

class BlockDirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      dir = new rgw::d4n::BlockDirectory{io};
      block = new rgw::d4n::CacheBlock{
        .cacheObj = {
	  .objName = "testName",
	  .bucketName = "testBucket",
	  .creationTime = "",
	  .dirty = false,
	  .hostsList = { env->redisHost }
	},
        .blockID = 0,
	.version = "",
	.size = 0,
	.hostsList = { env->redisHost }
      };

      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(block, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      dir->init(env->cct, env->dpp);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->cct->_conf->rgw_d4n_host;
      cfg.addr.port = std::to_string(env->cct->_conf->rgw_d4n_port);

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

    std::vector<std::string> vals{"0", "", "0", "0", env->redisHost, 
                                   "testName", "testBucket", "", "0", env->redisHost};
    std::vector<std::string> fields{"blockID", "version", "size", "globalWeight", "blockHosts", 
				     "objName", "bucketName", "creationTime", "dirty", "objHosts"};
};

TEST_F(ObjectDirectoryFixture, SetYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(obj, optional_yield{io, yield}));
    dir->shutdown();

    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", "testBucket_testName", fields);
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

TEST_F(ObjectDirectoryFixture, GetYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(obj, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket_testName", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ(0, dir->get(obj, optional_yield{io, yield}));
    EXPECT_EQ(obj->objName, "newoid");
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

TEST_F(ObjectDirectoryFixture, CopyYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(obj, optional_yield{io, yield}));
    ASSERT_EQ(0, dir->copy(obj, "copyTestName", "copyBucketName", optional_yield{io, yield}));
    dir->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "copyBucketName_copyTestName");
    req.push_range("HMGET", "copyBucketName_copyTestName", fields);
    req.push("FLUSHALL");

    response<int, std::vector<std::string>, 
	     boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);

    auto copyVals = vals;
    copyVals[0] = "copyTestName";
    copyVals[1] = "copyBucketName";
    EXPECT_EQ(std::get<1>(resp).value(), copyVals);

    conn->cancel();
  });

  io.run();
}

TEST_F(ObjectDirectoryFixture, DelYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(obj, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del(obj, optional_yield{io, yield}));
    dir->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName");
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

TEST_F(ObjectDirectoryFixture, UpdateFieldYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(obj, optional_yield{io, yield}));
    ASSERT_EQ(0, dir->update_field(obj, "objName", "newTestName", optional_yield{io, yield}));
    ASSERT_EQ(0, dir->update_field(obj, "objHosts", "127.0.0.1:5000", optional_yield{io, yield}));
    dir->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "testBucket_testName", "objName", "objHosts");
    req.push("FLUSHALL");
    response< std::vector<std::string>, 
	      boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "newTestName");
    EXPECT_EQ(std::get<0>(resp).value()[1], "127.0.0.1:6379_127.0.0.1:5000");

    conn->cancel();
  });

  io.run();
}


TEST_F(BlockDirectoryFixture, SetYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));
    dir->shutdown();

    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", "testBucket_testName_0_0", fields);
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

TEST_F(BlockDirectoryFixture, GetYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket_testName_0_0", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ(0, dir->get(block, optional_yield{io, yield}));
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

TEST_F(BlockDirectoryFixture, CopyYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));
    ASSERT_EQ(0, dir->copy(block, "copyTestName", "copyBucketName", optional_yield{io, yield}));
    dir->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "copyBucketName_copyTestName_0_0");
    req.push_range("HMGET", "copyBucketName_copyTestName_0_0", fields);
    req.push("FLUSHALL");

    response<int, std::vector<std::string>, 
	     boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);

    auto copyVals = vals;
    copyVals[5] = "copyTestName";
    copyVals[6] = "copyBucketName";
    EXPECT_EQ(std::get<1>(resp).value(), copyVals);

    conn->cancel();
  });

  io.run();
}

TEST_F(BlockDirectoryFixture, DelYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0_0");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del(block, optional_yield{io, yield}));
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

TEST_F(BlockDirectoryFixture, UpdateFieldYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));
    ASSERT_EQ(0, dir->update_field(block, "objName", "newTestName", optional_yield{io, yield}));
    ASSERT_EQ(0, dir->update_field(block, "blockHosts", "127.0.0.1:5000", optional_yield{io, yield}));
    dir->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "testBucket_testName_0_0", "objName", "blockHosts");
    req.push("FLUSHALL");
    response< std::vector<std::string>, 
	      boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "newTestName");
    EXPECT_EQ(std::get<0>(resp).value()[1], "127.0.0.1:6379_127.0.0.1:5000");

    conn->cancel();
  });

  io.run();
}

TEST_F(BlockDirectoryFixture, RemoveHostYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    block->hostsList.push_back("127.0.0.1:6000");
    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));
    ASSERT_EQ(0, dir->remove_host(block, "127.0.0.1:6379", optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "testBucket_testName_0_0", "blockHosts");
      req.push("HGET", "testBucket_testName_0_0", "blockHosts");
      response<int, std::string> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value(), "127.0.0.1:6000");
    }

    ASSERT_EQ(0, dir->remove_host(block, "127.0.0.1:6000", optional_yield{io, yield}));
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

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
