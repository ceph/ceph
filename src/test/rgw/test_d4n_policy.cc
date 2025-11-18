#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "gtest/gtest_prod.h"
#include "common/ceph_argparse.h"
#include "common/async/blocked_completion.h"
#include "rgw_auth_registry.h"
#include "rgw_cache_driver.h"
#include "driver/d4n/d4n_policy.h"

#define dout_subsys ceph_subsys_rgw

#define TEST_DATA_LENGTH 9

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
      auto _cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			      CODE_ENVIRONMENT_UTILITY,
			      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

      cct = _cct.get();
      dpp = new DoutPrefix(cct->get(), dout_subsys, "D4N Policy Test: ");
      common_init_finish(g_ceph_context);
      
      redisHost = cct->_conf->rgw_d4n_address; 
    }

    std::string redisHost;
    CephContext* cct;
    DoutPrefixProvider* dpp;
};

static inline std::string get_prefix(const std::string& bucketName, const std::string& oid, std::string& version) {
  if (version.empty()) {
    return fmt::format("{}{}{}", bucketName, CACHE_DELIM, oid);
  } else {
    return fmt::format("{}{}{}{}{}", bucketName, CACHE_DELIM, version, CACHE_DELIM, oid);
  }
}

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
	.version = "version",
	.deleteMarker = false,
	.size = TEST_DATA_LENGTH,
	.globalWeight = 0
      };

      conn = std::make_shared<connection>(net::make_strand(io));
      rgw::cache::Partition partition_info{ .location = "RedisCache", .reserve_size = 1073741824 };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};
      policyDriver = new rgw::d4n::PolicyDriver(conn, cacheDriver, "lfuda", null_yield);
      dir = new rgw::d4n::BlockDirectory{conn};

      ASSERT_NE(dir, nullptr);
      ASSERT_NE(cacheDriver, nullptr);
      ASSERT_NE(policyDriver, nullptr);
      ASSERT_NE(conn, nullptr);

      env->cct->_conf->rgw_d4n_l1_datacache_address = "127.0.0.1:6379";
      cacheDriver->initialize(env->dpp);

      bl.append("test data");
      bufferlist attrVal;
      attrVal.append("attrVal");
      attrs.insert({"attr", attrVal});

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      cfg.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete block;
      delete dir;
      delete cacheDriver;
      
      if (policyDriver)
	delete policyDriver;
    }

    int lfuda(const DoutPrefixProvider* dpp, rgw::d4n::CacheBlock* block, rgw::cache::CacheDriver* cacheDriver, optional_yield y) {
      int age = 1;  
      std::string version;
      std::string oid = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));

      if (this->policyDriver->get_cache_policy()->exist_key(oid)) { /* Local copy */
	policyDriver->get_cache_policy()->update(env->dpp, oid, 0, TEST_DATA_LENGTH, "", false, rgw::d4n::RefCount::NOOP, y);
        return 0;
      } else {
	if (this->policyDriver->get_cache_policy()->eviction(dpp, block->size, y) < 0)
	  return -1;

	int exists = dir->exist_key(env->dpp, block, y);
	if (exists > 0) { /* Remote copy */
	  if (dir->get(env->dpp, block, y) < 0) {
	    return -1;
	  } else {
	    if (!block->cacheObj.hostsList.empty()) { 
	      block->globalWeight += age;
	      auto globalWeight = std::to_string(block->globalWeight);
	      if (dir->update_field(env->dpp, block, "globalWeight", globalWeight, y) < 0) {
		return -1;
	      } else {
		return 0;
	      }
	    } else {
	      return -1;
	    }
	  }
	} else if (!exists) { /* No remote copy */
	  block->cacheObj.hostsList.insert(env->dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address);
	  if (dir->set(env->dpp, block, y) < 0)
	    return -1;

	  this->policyDriver->get_cache_policy()->update(dpp, oid, 0, TEST_DATA_LENGTH, "", false, rgw::d4n::RefCount::NOOP, y);
	  if (cacheDriver->put(dpp, oid, bl, TEST_DATA_LENGTH, attrs, y) < 0)
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
    rgw::sal::D4NFilterDriver* driver = nullptr;

    net::io_context io;
    std::shared_ptr<connection> conn;

    bufferlist bl;
    rgw::sal::Attrs attrs;
};

class TestRedisDriver : public rgw::cache::RedisDriver {
  public:
    TestRedisDriver(net::io_context& io_context, rgw::cache::Partition& _partition_info) : RedisDriver(io_context, _partition_info), 
                                                                                           partition_info(_partition_info),
									                   free_space(-1)
    {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
    }
    virtual rgw::cache::Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp, optional_yield y) override { return free_space + TEST_DATA_LENGTH; }

    virtual int initialize(const DoutPrefixProvider* dpp) override 
    {  
      RedisDriver::initialize(dpp); 
      if (partition_info.location.back() != '/') {
	partition_info.location += "/";
      }
	
      std::string address = dpp->get_cct()->_conf->rgw_d4n_l1_datacache_address;
	      
      config cfg;
      cfg.addr.host = address.substr(0, address.find(":"));
      cfg.addr.port = address.substr(address.find(":") + 1, address.length());
      cfg.clientname = "TestRedisDriver";

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 0) << "RedisDriver::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
	return -EDESTADDRREQ; 
      }
      
      conn->async_run(cfg, {}, net::consign(net::detached, conn));

      return 0;
    }
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) override
    {
      return RedisDriver::put(dpp, key, bl, len, attrs, y);
    }
    virtual rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len,
					  const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) override 
    { 
      return RedisDriver::put_async(dpp, y, aio, key, bl, len, attrs, cost, id);
    }
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override 
    {
      return RedisDriver::get(dpp, key, offset, len, bl, attrs, y);
    }
    virtual rgw::AioResultList get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override 
    {
      return RedisDriver::get_async(dpp, y, aio, key, ofs, len, cost, id);
    }
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) override { return RedisDriver::append_data(dpp, key, bl_data, y); }
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override { return RedisDriver::delete_data(dpp, key, y); }
    virtual int rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y) override { return RedisDriver::rename(dpp, oldKey, newKey, y); }
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override { return RedisDriver::set_attrs(dpp, key, attrs, y); }
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override { return RedisDriver::get_attrs(dpp, key, attrs, y); }
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override { return RedisDriver::update_attrs(dpp, key, attrs, y); }
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override { return RedisDriver::delete_attrs(dpp, key, del_attrs, y); }
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override 
    { 
      return RedisDriver::set_attr(dpp, key, attr_name, attr_val, y); 
    }
    virtual int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override 
    { 
      return RedisDriver::get_attr(dpp, key, attr_name, attr_val, y); 
    }
    void shutdown() 
    { 
      RedisDriver::shutdown();
      boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); }); 
    }

    virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, rgw::cache::ObjectDataCallback obj_func, rgw::cache::BlockDataCallback block_func) override { return 0; }

  private:
    std::shared_ptr<connection> conn;
    rgw::cache::Partition partition_info;
    uint64_t free_space;
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

#if 0
TEST_F(LFUDAPolicyFixture, LocalGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    env->cct->_conf->rgw_lfuda_sync_frequency = 1;
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    std::string version;
    std::string key = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));
    ASSERT_EQ(0, cacheDriver->put(env->dpp, key, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));
    policyDriver->get_cache_policy()->update(env->dpp, key, 0, TEST_DATA_LENGTH, "", false, rgw::d4n::RefCount::NOOP, optional_yield{yield});

    ASSERT_EQ(lfuda(env->dpp, block, cacheDriver, yield), 0);

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testBucket#testName#0#0", RGW_CACHE_ATTR_LOCAL_WEIGHT);
    req.push("FLUSHALL");

    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "2");
    conn->cancel();
    
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  io.run();
}
#endif

TEST_F(LFUDAPolicyFixture, EvictionYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    rgw::cache::Partition partition_info{ .location = "RedisCache", .reserve_size = 1073741824 };
    TestRedisDriver testDriver(io, partition_info);
    rgw::d4n::PolicyDriver policyDriver(conn, &testDriver, "lfuda", optional_yield{yield});

    env->cct->_conf->rgw_d4n_l1_datacache_address = "127.0.0.1:6379";
    testDriver.initialize(env->dpp);
    policyDriver.get_cache_policy()->init(env->cct, env->dpp, io, driver);

    /* Set victim block for eviction */
    rgw::d4n::CacheBlock victim = rgw::d4n::CacheBlock{
      .cacheObj = {
	.objName = "victimName",
	.bucketName = "testBucket",
	.creationTime = "",
	.dirty = false,
	.hostsList = { env->cct->_conf->rgw_d4n_local_rgw_address }
      },
      .blockID = 0,
      .version = "version",
      .deleteMarker = false,
      .size = TEST_DATA_LENGTH,
      .globalWeight = 5,
    };

    buffer::list attrVal;
    auto length_str = std::to_string(TEST_DATA_LENGTH);
    attrVal.append(length_str.c_str() + '\0', length_str.length() + 1);
    attrs.insert({"accounted_size", std::move(attrVal)});
    attrVal.clear();
    attrVal.append("testBucket\0", 10);
    attrs.insert({"bucket_name", std::move(attrVal)});

    /* Set head blocks */
    std::string victimHeadObj = get_prefix(victim.cacheObj.bucketName, victim.cacheObj.objName, victim.version);
    ASSERT_EQ(0, testDriver.put(env->dpp, victimHeadObj, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));

    std::string victimKeyInCache = rgw::sal::get_key_in_cache(get_prefix(victim.cacheObj.bucketName, victim.cacheObj.objName, victim.version), 
							       std::to_string(victim.blockID), std::to_string(TEST_DATA_LENGTH));
    ASSERT_EQ(0, testDriver.put(env->dpp, victimKeyInCache, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));
    policyDriver.get_cache_policy()->update(env->dpp, victimKeyInCache, 0, TEST_DATA_LENGTH, victim.version, false, rgw::d4n::RefCount::NOOP, optional_yield{yield});

    ASSERT_EQ(0, dir->set(env->dpp, &victim, optional_yield{yield}));

    /* Remote block */
    block->cacheObj.hostsList.clear();
    block->cacheObj.hostsList.insert("127.0.0.1:6000");

    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    { /* Avoid sending victim block to remote cache since no network is available */
      boost::system::error_code ec;
      request req;
      req.push("HSET", "lfuda", "minLocalWeights_sum", "10", "minLocalWeights_size", "1");

      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    EXPECT_EQ(policyDriver.get_cache_policy()->eviction(env->dpp, block->size, optional_yield{yield}), 0);

    std::string victimKey = victim.cacheObj.bucketName + "_version_" + victim.cacheObj.objName + "_" + std::to_string(victim.blockID) + "_" + std::to_string(victim.size);
    std::string key = block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "RedisCache/" + victimKeyInCache);
    req.push("EXISTS", victimKey, "globalWeight");
    //req.push("HGET", key, "globalWeight"); // TODO: global weight logic is currently commented out
    req.push("FLUSHALL");

    response<int, int, /*std::string,*/ 
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 0);
    EXPECT_EQ(std::get<1>(resp).value(), 0);
    //EXPECT_EQ(std::get<2>(resp).value(), "1");
    conn->cancel();

    testDriver.shutdown();
    cacheDriver->shutdown();
    conn->cancel();
  }, rethrow);

  io.run(); 
}

#if 0
TEST_F(LFUDAPolicyFixture, BackendGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    env->cct->_conf->rgw_lfuda_sync_frequency = 1;
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    ASSERT_GE(lfuda(env->dpp, block, cacheDriver, optional_yield{yield}), 0);

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("FLUSHALL");

    response<boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    conn->cancel();

    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  io.run();
}

TEST_F(LFUDAPolicyFixture, RedisSyncTest)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    env->cct->_conf->rgw_lfuda_sync_frequency = 1;
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);
  
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "lfuda", "age");
    req.push("HGET", "lfuda", "minLocalWeights_sum");
    req.push("HGET", "lfuda", "minLocalWeights_size");
    req.push("HGET", "lfuda", "minLocalWeights_address");
    req.push("HGET", "127.0.0.1:8000", "avgLocalWeight_sum");
    req.push("HGET", "127.0.0.1:8000", "avgLocalWeight_size");
    req.push("FLUSHALL");

    response<std::string, std::string, std::string,
             std::string, std::string, std::string,
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "1");
    EXPECT_EQ(std::get<1>(resp).value(), "0");
    EXPECT_EQ(std::get<2>(resp).value(), "0");
    EXPECT_EQ(std::get<3>(resp).value(), "127.0.0.1:8000");
    EXPECT_EQ(std::get<4>(resp).value(), "0");
    EXPECT_EQ(std::get<4>(resp).value(), "0");
    conn->cancel();
    
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  io.run();
}
#endif

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
