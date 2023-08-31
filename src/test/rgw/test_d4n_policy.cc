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

class PolicyFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      rgw::cache::Partition partition_info{ .location = "RedisCache" };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};
      policyDriver = new rgw::d4n::PolicyDriver(io, "lfuda");
      dir = new rgw::d4n::BlockDirectory{io};
      block = new rgw::d4n::CacheBlock{
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

      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(block, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(cacheDriver, nullptr);
      ASSERT_NE(policyDriver, nullptr);
      ASSERT_NE(conn, nullptr);

      dir->init(env->cct, env->dpp);
      cacheDriver->initialize(env->cct, env->dpp);

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
      delete block;
      delete dir;
      delete cacheDriver;
      delete policyDriver;
    }

    rgw::d4n::CacheBlock* block;
    rgw::d4n::BlockDirectory* dir;
    rgw::d4n::PolicyDriver* policyDriver;
    rgw::cache::RedisDriver* cacheDriver;

    net::io_context io;
    connection* conn;

    bufferlist bl;
    rgw::sal::Attrs attrs;
};

TEST_F(PolicyFixture, GetBlockYield)
{
  spawn::spawn(io, [this] (yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));
    cacheDriver->shutdown();

    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));
    dir->shutdown();

    ASSERT_EQ(0, policyDriver->get_cache_policy()->get_block(env->dpp, cacheDriver, optional_yield{io, yield}));
    policyDriver->get_cache_policy()->shutdown();

/*    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", "testBucket_testName", fields);
    req.push("FLUSHALL");

    response< std::vector<std::string>,
	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
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
