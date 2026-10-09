#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "gtest/gtest_prod.h"
#include "common/ceph_argparse.h"
#include "common/ceph_argparse.h"
#include "common/async/blocked_completion.h"
#include "rgw_auth_registry.h"
#include "rgw_cache_driver.h"
#include "driver/d4n/d4n_policy.h"
#include "driver/d4n/d4n_directory_redis.h"
#include "driver/d4n/d4n_connection.h"

#define dout_subsys ceph_subsys_rgw

constexpr unsigned int TEST_DATA_LENGTH = 9;

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

inline std::string to_legacy_index(const std::string& key) {
  auto parts = split(key, "_");
  std::vector<std::string> block_info;
  block_info.assign(parts.begin(), parts.end());

  return fmt::format("{}{}{}{}{}{}{}", block_info[0], CACHE_DELIM, block_info[1], "/block/", block_info[2], "/", block_info[3]);
}

inline std::string to_legacy_versioned_index(const std::string& key) {
  auto parts = split(key, "_");
  std::vector<std::string> block_info;
  block_info.assign(parts.begin(), parts.end());

  return fmt::format("{}{}{}{}{}{}{}{}{}", block_info[0], CACHE_DELIM, url_encode("_" + block_info[1], true), "_", block_info[2], "/block/", block_info[3], "/", block_info[4]);
}

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
      
      redisHost = cct->_conf->rgw_d4n_l1_datacache_address; 
	  env->cct->_conf->rgw_d4n_directory_type = "redis";
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

      conn = std::make_shared<connection>(boost::asio::make_strand(io));
	  auto redis_native = std::make_shared<rgw::d4n::RedisConnection>(conn);
      redis_conn = std::dynamic_pointer_cast<rgw::d4n::RedisConnection>(redis_native);
      rgw::cache::Partition partition_info{ .location = "RedisCache", .reserve_size = 1073741824 };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};

	  dir = std::make_unique<rgw::d4n::RedisDirectory>(redis_conn);
      blockDir = new rgw::d4n::RedisBlockDirectory{redis_conn};
      objDir = new rgw::d4n::RedisObjectDirectory{redis_conn};
      bucketDir = new rgw::d4n::RedisBucketDirectory{redis_conn};

      policyDriver = new rgw::d4n::PolicyDriver(*dir, *blockDir, *objDir, *bucketDir, "redis", cacheDriver, "lfuda", null_yield);

      ASSERT_NE(dir, nullptr);
      ASSERT_NE(blockDir, nullptr);
      ASSERT_NE(objDir, nullptr);
      ASSERT_NE(bucketDir, nullptr);
      ASSERT_NE(cacheDriver, nullptr);
      ASSERT_NE(policyDriver, nullptr);
      ASSERT_NE(conn, nullptr);

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
      delete blockDir;
      delete objDir;
      delete bucketDir;
      
      if (policyDriver)
        delete policyDriver;
    }

    /* This method demonstrates the generally flow of the policy-specific logic that dictates LFUDA. It directs where the block to be retrieved
     * should come from and how its LFUDA-related metadata must be changed (as well as local cache handling). It handles the following scenarios:
     * 1. Block is available in local cache
     * 2. Block is available in remote cache
     * 3. Block is not available in any cache but is in the backend
     * The last scenario is that the block does not exist, but this is not tested because the backend is simulated to carry the block, which is more useful
     * for testing purposes. */
    int lfuda(const DoutPrefixProvider* dpp, rgw::d4n::CacheBlock* block, optional_yield y) {
      int age = 1, ret;  
      std::string version;
      std::string oid = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));

      if (this->policyDriver->get_cache_policy()->exist_key(oid)) { /* Local copy */
		policyDriver->get_cache_policy()->update(env->dpp, oid, 0, TEST_DATA_LENGTH, "", std::nullopt, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, y, nullptr);
        return 0;
      } else {
		if ((ret = blockDir->get(env->dpp, block, y)) < 0 && ret != -ENOENT) {
		  std::cout << "ERROR: Directory get failed, ret=" << ret << std::endl;
		  return ret;
		} else if (ret == 0) { 
		  /* Since the block is not in the local cache, we will be either retrieving the block from a remote source or the backend and then
		   * writing it locally. As a result, we must ensure the local cache has enough space to accomodate the block by evicting if necessary. */
		  if ((ret = this->policyDriver->get_cache_policy()->eviction(dpp, block->size, y)) < 0) {
			std::cout << "ERROR: Eviction failed, ret=" << ret << std::endl;
			return ret;
		  }
		  if (block->cacheObj.hostsList.size() > 0) { /* Remote copy */
			block->globalWeight += age;
			auto globalWeight = std::to_string(block->globalWeight);
			if ((ret = blockDir->update_field(env->dpp, block, "globalWeight", globalWeight, y)) < 0) {
			  std::cout << "ERROR: update_field failed, ret=" << ret << std::endl;
			  return ret;
			} 
			// Write block to local cache
			if ((ret = cacheDriver->put(dpp, oid, bl, TEST_DATA_LENGTH, attrs, y)) < 0) {
			  std::cout << "ERROR: Cache put failed, ret=" << ret << std::endl;
			  return ret;
			}
			this->policyDriver->get_cache_policy()->update(dpp, oid, 0, TEST_DATA_LENGTH, "", false, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, y, nullptr);
			// Add local cache address to block's directory entry
            std::string host = "127.0.0.1:6379";
			if ((ret = blockDir->update_field(env->dpp, block, "hosts", host, y)) < 0) {
			  std::cout << "ERROR: update_field failed, ret=" << ret << std::endl;
			  return ret;
			}
			return 0;
		  }
		  return -ENOENT;
		} else { /* No remote copy; retrieve from backend */
		  // Write block to local cache
		  if ((ret = cacheDriver->put(dpp, oid, bl, TEST_DATA_LENGTH, attrs, y)) < 0) {
			std::cout << "ERROR: Cache put failed, ret=" << ret << std::endl;
			return ret;
		  }
		  this->policyDriver->get_cache_policy()->update(dpp, oid, 0, TEST_DATA_LENGTH, "", false, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, y, nullptr);
		  // Add local cache address to block's directory entry
		  if ((ret = blockDir->set(env->dpp, block, y)) < 0) {
			std::cout << "ERROR: Directory set failed, ret=" << ret << std::endl;
			return ret;
		  }
		  return 0;
		}
	  }
    }

    rgw::d4n::CacheBlock* block;
    std::unique_ptr<rgw::d4n::RedisDirectory> dir;
    rgw::d4n::RedisBlockDirectory* blockDir;
    rgw::d4n::RedisObjectDirectory* objDir;
    rgw::d4n::RedisBucketDirectory* bucketDir;
    rgw::d4n::PolicyDriver* policyDriver;
    rgw::cache::RedisDriver* cacheDriver;
    rgw::sal::D4NFilterDriver* driver = nullptr;
	rgw_user uid{"test_tenant", "test"};

    net::io_context io;
    std::shared_ptr<connection> conn;
    std::shared_ptr<rgw::d4n::RedisConnection> redis_conn;

    bufferlist bl;
    rgw::sal::Attrs attrs;
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

TEST_F(LFUDAPolicyFixture, LocalGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    env->cct->_conf->rgw_lfuda_sync_frequency = 6;
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    std::string version;
    std::string key = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));
    ASSERT_EQ(0, cacheDriver->put(env->dpp, key, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));
	policyDriver->get_cache_policy()->update(env->dpp, key, 0, TEST_DATA_LENGTH, "", false, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, optional_yield{yield}, nullptr);

    // Should retrieve block locally (from the cache backend) and increment its local weight
    ASSERT_EQ(lfuda(env->dpp, block, yield), 0);

    boost::asio::steady_timer timer(io);
    timer.expires_after(std::chrono::seconds(5));
    boost::system::error_code timer_ec;
    timer.async_wait(yield[timer_ec]);

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("FLUSHALL");

    response<boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    conn->cancel();
    
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] { io.run(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(LFUDAPolicyFixture, RemoteGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    // Set victim block for eviction
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
    attrVal.append(length_str.c_str(), length_str.length() + 1);
    attrs.insert({"accounted_size", std::move(attrVal)});
    attrVal.clear();
    attrVal.append("testBucket\0", 10);
    attrs.insert({"bucket_name", std::move(attrVal)});

    std::string victimKeyInCache = rgw::sal::get_key_in_cache(get_prefix(victim.cacheObj.bucketName, victim.cacheObj.objName, victim.version), 
                                                               std::to_string(victim.blockID), std::to_string(TEST_DATA_LENGTH));
    ASSERT_EQ(0, cacheDriver->put(env->dpp, victimKeyInCache, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));
	policyDriver->get_cache_policy()->update(env->dpp, victimKeyInCache, 0, TEST_DATA_LENGTH, victim.version, false, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, optional_yield{yield}, nullptr);

    ASSERT_EQ(0, blockDir->set(env->dpp, &victim, optional_yield{yield}));

    // Remote block
    block->cacheObj.hostsList.clear();
    block->cacheObj.hostsList.insert("127.0.0.1:6000");

    ASSERT_EQ(0, blockDir->set(env->dpp, block, optional_yield{yield}));

    { // Avoid sending victim block to remote cache since no network is available
      boost::system::error_code ec;
      request req;
      req.push("HSET", "lfuda", "minLocalWeights_sum", "10", "minLocalWeights_size", "1");

      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    ASSERT_EQ(lfuda(env->dpp, block, optional_yield{yield}), 0);

    std::string version;
	std::string oid = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "RedisCache/" + oid); // Remote block cache entry (now in local cache)
    req.push("EXISTS", "RedisCache/" + victimKeyInCache); // Victim cache entry
    req.push("EXISTS", to_legacy_index(victim.cacheObj.bucketName + "_" + victim.cacheObj.objName + "_" + std::to_string(victim.blockID) + "_" + std::to_string(TEST_DATA_LENGTH))); // Directory entry
    req.push("HGET", to_legacy_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "globalWeight");
    req.push("HGET", to_legacy_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "hosts");
    req.push("FLUSHALL");

    response<int, int, int, std::string, std::string,
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);
    EXPECT_EQ(std::get<1>(resp).value(), 0);
    EXPECT_EQ(std::get<2>(resp).value(), 0);
    EXPECT_EQ(std::get<3>(resp).value(), "1");
    EXPECT_EQ(std::get<4>(resp).value(), "127.0.0.1:6000_127.0.0.1:6379");
    conn->cancel();

    std::string victimKeyInPolicy = victim.cacheObj.bucketName + "#version#" + victim.cacheObj.objName + "#" + std::to_string(victim.blockID) + "#" + std::to_string(victim.size);
    EXPECT_EQ(policyDriver->get_cache_policy()->exist_key(victimKeyInPolicy), 0);

    cacheDriver->shutdown();
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  io.run(); 
}

TEST_F(LFUDAPolicyFixture, RemoteVersionEnabledGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    // Set victim block for eviction
    rgw::d4n::CacheBlock victim = rgw::d4n::CacheBlock{
      .cacheObj = {
        .objName = "_:version_victimName",
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
    attrVal.append(length_str.c_str(), length_str.length() + 1);
    attrs.insert({"accounted_size", std::move(attrVal)});
    attrVal.clear();
    attrVal.append("testBucket\0", 10);
    attrs.insert({"bucket_name", std::move(attrVal)});
    attrVal.clear();
    attrVal.append("version\0");
    attrs.insert({RGW_CACHE_ATTR_VERSION_ID, std::move(attrVal)});

    std::string victimKeyInCache = rgw::sal::get_key_in_cache(get_prefix(victim.cacheObj.bucketName, "victimName", victim.version), 
                                                               std::to_string(victim.blockID), std::to_string(TEST_DATA_LENGTH));
    ASSERT_EQ(0, cacheDriver->put(env->dpp, victimKeyInCache, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));
	policyDriver->get_cache_policy()->update(env->dpp, victimKeyInCache, 0, TEST_DATA_LENGTH, victim.version, false, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, optional_yield{yield}, nullptr);

    ASSERT_EQ(0, blockDir->set(env->dpp, &victim, optional_yield{yield}));

    // Remote block
    block->cacheObj.hostsList.clear();
    block->cacheObj.hostsList.insert("127.0.0.1:6000");

    block->cacheObj.objName = "_:version_testName";
    ASSERT_EQ(0, blockDir->set(env->dpp, block, optional_yield{yield}));

    { // Avoid sending victim block to remote cache since no network is available
      boost::system::error_code ec;
      request req;
      req.push("HSET", "lfuda", "minLocalWeights_sum", "10", "minLocalWeights_size", "1");

      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    ASSERT_EQ(lfuda(env->dpp, block, optional_yield{yield}), 0);

    std::string version;
    std::string oid = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "RedisCache/" + oid); // Remote block cache entry (now in local cache)
    req.push("EXISTS", "RedisCache/" + victimKeyInCache); // Victim cache entry
    req.push("EXISTS", to_legacy_versioned_index(victim.cacheObj.bucketName + "_" + victim.cacheObj.objName + "_" + std::to_string(victim.blockID) + "_" + std::to_string(TEST_DATA_LENGTH))); // Directory entry
    req.push("HGET", to_legacy_versioned_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "globalWeight");
    req.push("HGET", to_legacy_versioned_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "hosts");
    req.push("FLUSHALL");

    response<int, int, int, std::string, std::string,
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);
    EXPECT_EQ(std::get<1>(resp).value(), 0);
    EXPECT_EQ(std::get<2>(resp).value(), 0);
    EXPECT_EQ(std::get<3>(resp).value(), "1");
    EXPECT_EQ(std::get<4>(resp).value(), "127.0.0.1:6000_127.0.0.1:6379");
    conn->cancel();

    std::string victimKeyInPolicy = victim.cacheObj.bucketName + "#version#" + victim.cacheObj.objName + "#" + std::to_string(victim.blockID) + "#" + std::to_string(victim.size);
    EXPECT_EQ(policyDriver->get_cache_policy()->exist_key(victimKeyInPolicy), 0);

    cacheDriver->shutdown();
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  io.run(); 
}

TEST_F(LFUDAPolicyFixture, RemoteVersionSuspendedGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    // Set victim block for eviction
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
    attrVal.append(length_str.c_str(), length_str.length() + 1);
    attrs.insert({"accounted_size", std::move(attrVal)});
    attrVal.clear();
    attrVal.append("testBucket\0", 10);
    attrs.insert({"bucket_name", std::move(attrVal)});
    attrVal.clear();
    attrVal.append("null");
    attrs.insert({RGW_CACHE_ATTR_VERSION_ID, std::move(attrVal)});

    std::string victimKeyInCache = rgw::sal::get_key_in_cache(get_prefix(victim.cacheObj.bucketName, "victimName", victim.version), 
                                                               std::to_string(victim.blockID), std::to_string(TEST_DATA_LENGTH));
    ASSERT_EQ(0, cacheDriver->put(env->dpp, victimKeyInCache, bl, TEST_DATA_LENGTH, attrs, optional_yield{yield}));
	policyDriver->get_cache_policy()->update(env->dpp, victimKeyInCache, 0, TEST_DATA_LENGTH, victim.version, false, uid, block->cacheObj.bucketName, rgw::d4n::RefCount::NOOP, optional_yield{yield}, nullptr);

    ASSERT_EQ(0, blockDir->set(env->dpp, &victim, optional_yield{yield}));

    // Remote block
    block->cacheObj.hostsList.clear();
    block->cacheObj.hostsList.insert("127.0.0.1:6000");

    block->cacheObj.objName = "_:version_testName";
    ASSERT_EQ(0, blockDir->set(env->dpp, block, optional_yield{yield}));

    { // Avoid sending victim block to remote cache since no network is available
      boost::system::error_code ec;
      request req;
      req.push("HSET", "lfuda", "minLocalWeights_sum", "10", "minLocalWeights_size", "1");

      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    ASSERT_EQ(lfuda(env->dpp, block, optional_yield{yield}), 0);

    std::string version;
    std::string oid = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "RedisCache/" + oid); // Remote block cache entry (now in local cache)
    req.push("EXISTS", "RedisCache/" + victimKeyInCache); // Victim cache entry
    req.push("EXISTS", to_legacy_index(victim.cacheObj.bucketName + "_" + victim.cacheObj.objName + "_" + std::to_string(victim.blockID) + "_" + std::to_string(TEST_DATA_LENGTH))); // Directory entry
    req.push("HGET", to_legacy_versioned_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "globalWeight");
    req.push("HGET", to_legacy_versioned_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "hosts");
    req.push("FLUSHALL");

    response<int, int, int, std::string, std::string,
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);
    EXPECT_EQ(std::get<1>(resp).value(), 0);
    EXPECT_EQ(std::get<2>(resp).value(), 0);
    EXPECT_EQ(std::get<3>(resp).value(), "1");
    EXPECT_EQ(std::get<4>(resp).value(), "127.0.0.1:6000_127.0.0.1:6379");
    conn->cancel();

    std::string victimKeyInPolicy = victim.cacheObj.bucketName + "#version#" + victim.cacheObj.objName + "#" + std::to_string(victim.blockID) + "#" + std::to_string(victim.size);
    EXPECT_EQ(policyDriver->get_cache_policy()->exist_key(victimKeyInPolicy), 0);

    cacheDriver->shutdown();
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] { io.run(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(LFUDAPolicyFixture, BackendGetBlockYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(policyDriver->get_cache_policy())->save_y(optional_yield{yield});
    policyDriver->get_cache_policy()->init(env->cct, env->dpp, io, driver);

    ASSERT_EQ(lfuda(env->dpp, block, optional_yield{yield}), 0);

	std::string version;
	std::string oid = rgw::sal::get_key_in_cache(get_prefix(block->cacheObj.bucketName, block->cacheObj.objName, version), std::to_string(block->blockID), std::to_string(block->size));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "RedisCache/" + oid); // Remote block cache entry (now in local cache)
    req.push("HGET", to_legacy_index(block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(TEST_DATA_LENGTH)), "hosts");
    req.push("FLUSHALL");

    response<int, std::string,
             boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);
    EXPECT_EQ(std::get<1>(resp).value(), "127.0.0.1:6379");
    conn->cancel();

    cacheDriver->shutdown();
    delete policyDriver; 
    policyDriver = nullptr;
  }, rethrow);

  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] { io.run(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
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

  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] { io.run(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
