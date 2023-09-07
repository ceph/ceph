#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "driver/d4n/d4n_policy.h"

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

    std::string redisHost;
    CephContext* cct;
    DoutPrefixProvider* dpp;
};

class LFUDAPolicyFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      rgw::cache::Partition partition_info{ .location = "RedisCache" };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};
      policyDriver = new rgw::d4n::PolicyDriver(io, "lfuda");
      dir = new rgw::d4n::BlockDirectory{io};
      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(dir, nullptr);
      ASSERT_NE(cacheDriver, nullptr);
      ASSERT_NE(policyDriver, nullptr);
      ASSERT_NE(conn, nullptr);

      dir->init(env->cct, env->dpp);
      cacheDriver->initialize(env->cct, env->dpp);
      policyDriver->init();
      policyDriver->get_cache_policy()->init(env->cct, env->dpp);

      bl.append("test data");
      bufferlist attrVal;
      attrVal.append("attrVal");
      attrs.insert({"attr", attrVal});

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->cct->_conf->rgw_d4n_host;
      cfg.addr.port = std::to_string(env->cct->_conf->rgw_d4n_port);

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete conn;
      delete dir;
      delete cacheDriver;
      delete policyDriver;
    }

    rgw::d4n::BlockDirectory* dir;
    rgw::d4n::PolicyDriver* policyDriver;
    rgw::cache::RedisDriver* cacheDriver;

    net::io_context io;
    connection* conn;

    bufferlist bl;
    rgw::sal::Attrs attrs;
    std::string key = "testName";
};

TEST_F(LFUDAPolicyFixture, LocalGetBlockYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
      .cacheObj = {
	.objName = "testName",
	.bucketName = "testBucket",
	.creationTime = 0,
	.dirty = false,
	.hostsList = { env->redisHost }
      },
      .version = 0,
      .size = 0,
      .hostsList = { env->redisHost }
    };

    ASSERT_EQ(0, cacheDriver->put(env->dpp, key, bl, bl.length(), attrs, optional_yield{io, yield}));
    policyDriver->get_cache_policy()->insert(env->dpp, key, 0, bl.length(), cacheDriver, optional_yield{io, yield});

    /* Change cache age for testing purposes */
    { 
      boost::system::error_code ec;
      request req;
      req.push("HSET", "lfuda", "age", "5");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_GE(policyDriver->get_cache_policy()->get_block(env->dpp, &block, cacheDriver, optional_yield{io, yield}), 0);

    dir->shutdown();
    cacheDriver->shutdown();
    policyDriver->get_cache_policy()->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testName", "localWeight");
    req.push("FLUSHALL");

    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "5");
    conn->cancel();
  });

  io.run();
}

TEST_F(LFUDAPolicyFixture, RemoteGetBlockYield) // not completely supported yet
{
  spawn::spawn(io, [this] (yield_context yield) {
    rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
      .cacheObj = {
	.objName = "testName",
	.bucketName = "testBucket",
	.creationTime = 0,
	.dirty = false,
	.hostsList = { "127.0.0.1:6000" }
      },
      .version = 0,
      .size = 0,
      .hostsList = { "127.0.0.1:6000" }
    };

    ASSERT_EQ(0, dir->set(&block, optional_yield{io, yield}));
    ASSERT_GE(policyDriver->get_cache_policy()->get_block(env->dpp, &block, cacheDriver, optional_yield{io, yield}), 0);

    dir->shutdown();
    cacheDriver->shutdown();
    policyDriver->get_cache_policy()->shutdown();

    boost::system::error_code ec;
    request req;
//    req.push_range("HMGET", "testBucket_testName", fields);
    req.push("FLUSHALL");

    response<boost::redis::ignore_t> resp;
  //  response< std::vector<std::string>,
//	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

  /*  ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), vals);*/
    conn->cancel();
  });

  io.run();
}

TEST_F(LFUDAPolicyFixture, BackendGetBlockYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    rgw::d4n::CacheBlock block = rgw::d4n::CacheBlock{
      .cacheObj = {
	.objName = "testName",
	.bucketName = "testBucket",
	.creationTime = 0,
	.dirty = false,
	.hostsList = { "127.0.0.1:6000" }
      },
      .version = 0,
      .size = 0,
      .hostsList = { "127.0.0.1:6000" }
    };

    ASSERT_GE(policyDriver->get_cache_policy()->get_block(env->dpp, &block, cacheDriver, optional_yield{io, yield}), 0);

    dir->shutdown();
    cacheDriver->shutdown();
    policyDriver->get_cache_policy()->shutdown();

    boost::system::error_code ec;
    request req;
//    req.push_range("HMGET", "testBucket_testName", fields);
    req.push("FLUSHALL");

    response<boost::redis::ignore_t> resp;
  //  response< std::vector<std::string>,
//	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

  /*  ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), vals);*/
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
