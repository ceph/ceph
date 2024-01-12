#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "gtest/gtest_prod.h"
#include "common/ceph_argparse.h"
#include "common/async/blocked_completion.h"
#include "rgw_auth_registry.h"
#include "driver/d4n/d4n_policy.h"

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

    std::string redisHost;
    CephContext* cct;
    DoutPrefixProvider* dpp;
};

class LFUDAPolicyFixture : public ::testing::Test {
  protected:
    virtual void SetUp() {
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
	.size = bl.length(),
	.hostsList = { env->redisHost }
      };

      conn = std::make_shared<connection>(net::make_strand(io));
      rgw::cache::Partition partition_info{ .location = "RedisCache", .size = 1000 };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};
      policyDriver = new rgw::d4n::PolicyDriver(conn, cacheDriver, "lfuda");
      dir = new rgw::d4n::BlockDirectory{conn};

      ASSERT_NE(dir, nullptr);
      ASSERT_NE(cacheDriver, nullptr);
      ASSERT_NE(policyDriver, nullptr);
      ASSERT_NE(conn, nullptr);

      dir->init(env->cct);
      cacheDriver->initialize(env->dpp);

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
      delete block;
      delete dir;
      delete cacheDriver;
      delete policyDriver;
    }

    std::string build_index(std::string bucketName, std::string oid, uint64_t offset, uint64_t size) {
      return bucketName + "_" + oid + "_" + std::to_string(offset) + "_" + std::to_string(size);
    }

    int lfuda(const DoutPrefixProvider* dpp, rgw::d4n::CacheBlock* block, rgw::cache::CacheDriver* cacheDriver, optional_yield y) {
      int age = 1;  
      std::string oid = build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size);

      if (this->policyDriver->get_cache_policy()->exist_key(build_index(block->cacheObj.bucketName, block->cacheObj.objName, block->blockID, block->size))) { /* Local copy */
	policyDriver->get_cache_policy()->update(env->dpp, oid, 0, bl.length(), "", y);
        return 0;
      } else {
	if (this->policyDriver->get_cache_policy()->eviction(dpp, block->size, y) < 0)
	  return -1;

	int exists = dir->exist_key(block, y);
	if (exists > 0) { /* Remote copy */
	  if (dir->get(block, y) < 0) {
	    return -1;
	  } else {
	    if (!block->hostsList.empty()) { 
	      block->globalWeight += age;
	      
	      if (dir->update_field(block, "globalWeight", std::to_string(block->globalWeight), y) < 0) {
		return -1;
	      } else {
		return 0;
	      }
	    } else {
	      return -1;
	    }
	  }
	} else if (!exists) { /* No remote copy */
	  block->hostsList.push_back(dir->cct->_conf->rgw_local_cache_address);
	  if (dir->set(block, y) < 0)
	    return -1;

	  this->policyDriver->get_cache_policy()->update(dpp, oid, 0, bl.length(), "", y);
	  if (cacheDriver->put(dpp, oid, bl, bl.length(), attrs, y) < 0)
            return -1;
	  return cacheDriver->set_attr(dpp, oid, "localWeight", std::to_string(age), y);
	} else {
	  return -1;
	}
      }
    }

    rgw::d4n::CacheBlock* block;
    rgw::d4n::BlockDirectory* dir;
    rgw::d4n::PolicyDriver* policyDriver;
    rgw::cache::RedisDriver* cacheDriver;

    net::io_context io;
    std::shared_ptr<connection> conn;

    bufferlist bl;
    rgw::sal::Attrs attrs;
};

TEST_F(LFUDAPolicyFixture, LocalGetBlockYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    std::string key = block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
    ASSERT_EQ(0, cacheDriver->put(env->dpp, key, bl, bl.length(), attrs, optional_yield{io, yield}));
    policyDriver->get_cache_policy()->update(env->dpp, key, 0, bl.length(), "", optional_yield{io, yield});

    ASSERT_EQ(lfuda(env->dpp, block, cacheDriver, optional_yield{io, yield}), 0);

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testBucket_testName_0_0", "user.rgw.localWeight");
    req.push("FLUSHALL");

    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "2");
    conn->cancel();
  });

  io.run();
}

TEST_F(LFUDAPolicyFixture, RemoteGetBlockYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    /* Set victim block for eviction */
    rgw::d4n::CacheBlock victim = rgw::d4n::CacheBlock{
      .cacheObj = {
	.objName = "victimName",
	.bucketName = "testBucket",
	.creationTime = "",
	.dirty = false,
	.hostsList = { env->redisHost }
      },
      .blockID = 0,
      .version = "",
      .size = bl.length(),
      .globalWeight = 5,
      .hostsList = { env->redisHost }
    };

    bufferlist attrVal;
    attrVal.append(std::to_string(bl.length()));
    attrs.insert({"accounted_size", attrVal});
    attrVal.clear();
    attrVal.append("testBucket");
    attrs.insert({"bucket_name", attrVal});

    ASSERT_EQ(0, dir->set(&victim, optional_yield{io, yield}));
    std::string victimKey = victim.cacheObj.bucketName + "_" + victim.cacheObj.objName + "_" + std::to_string(victim.blockID) + "_" + std::to_string(victim.size);
    ASSERT_EQ(0, cacheDriver->put(env->dpp, victimKey, bl, bl.length(), attrs, optional_yield{io, yield}));
    policyDriver->get_cache_policy()->update(env->dpp, victimKey, 0, bl.length(), "", optional_yield{io, yield});

    /* Remote block */
    block->size = cacheDriver->get_free_space(env->dpp) + 1; /* To trigger eviction */
    block->hostsList.clear();  
    block->cacheObj.hostsList.clear();
    block->hostsList.push_back("127.0.0.1:6000");
    block->cacheObj.hostsList.push_back("127.0.0.1:6000");

    ASSERT_EQ(0, dir->set(block, optional_yield{io, yield}));

    ASSERT_GE(lfuda(env->dpp, block, cacheDriver, optional_yield{io, yield}), 0);

    cacheDriver->shutdown();

    std::string key = block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "RedisCache/" + victimKey);
    req.push("EXISTS", victimKey, "globalWeight");
    req.push("HGET", key, "globalWeight");
    req.push("FLUSHALL");

    response<int, int, std::string, std::string, 
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 0);
    EXPECT_EQ(std::get<1>(resp).value(), 0);
    EXPECT_EQ(std::get<2>(resp).value(), "1");
    conn->cancel();
  });

  io.run();
}

TEST_F(LFUDAPolicyFixture, BackendGetBlockYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_GE(lfuda(env->dpp, block, cacheDriver, optional_yield{io, yield}), 0);

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("FLUSHALL");

    response<boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

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
