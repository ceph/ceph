#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/split.h"
#include "rgw_auth_registry.h"
#include "rgw_cache_driver.h"
#include "driver/d4n/d4n_directory.h"
#include "driver/d4n/d4n_directory_redis.h"
#include "driver/d4n/d4n_connection.h"

#define dout_subsys ceph_subsys_rgw

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

inline std::string to_legacy_object_index(const std::string& key) {
  auto parts = split(key, "_");
  std::vector<std::string> block_info;
  block_info.assign(parts.begin(), parts.end());

  return fmt::format("{}{}{}", block_info[0], CACHE_DELIM, block_info[1]);
}

inline std::string to_legacy_block_index(const std::string& key) {
  auto parts = split(key, "_");
  std::vector<std::string> block_info;
  block_info.assign(parts.begin(), parts.end());

  return fmt::format("{}{}{}{}{}{}{}", block_info[0], CACHE_DELIM, block_info[1], "/block/", block_info[2], "/", block_info[3]);
}

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
      
      redisHost = cct->_conf->rgw_d4n_l1_datacache_address; 
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
      conn = std::make_shared<connection>(boost::asio::make_strand(io));
	  auto redis_native = std::make_shared<rgw::d4n::RedisConnection>(conn);
      redis_conn = std::dynamic_pointer_cast<rgw::d4n::RedisConnection>(redis_native);
      dir = new rgw::d4n::RedisObjectDirectory{redis_conn};
      obj = new rgw::d4n::CacheObj{
		.objName = "testName",
		.bucketName = "testBucket",
		.creationTime = "",
		.dirty = false,
		.hostsList = { env->redisHost }
      };

      ASSERT_NE(obj, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      cfg.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

	  redis_pool = redis_pool = std::make_shared<rgw::d4n::RedisPool>(&io, cfg, 8);
      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete obj;
      delete dir;
    }

    rgw::d4n::CacheObj* obj;
    rgw::d4n::RedisObjectDirectory* dir;

    net::io_context io;
    std::shared_ptr<connection> conn;
    std::shared_ptr<rgw::d4n::RedisConnection> redis_conn;
	std::shared_ptr<rgw::d4n::RedisPool> redis_pool; 

	ceph::real_time time = real_clock::now();
    std::string version = "test_version";
};

class BlockDirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      conn = std::make_shared<connection>(boost::asio::make_strand(io));
	  auto redis_native = std::make_shared<rgw::d4n::RedisConnection>(conn);
      redis_conn = std::dynamic_pointer_cast<rgw::d4n::RedisConnection>(redis_native);
      dir = new rgw::d4n::RedisBlockDirectory{redis_conn};
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
		.deleteMarker = false,
		.size = 0
      };

      ASSERT_NE(block, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      cfg.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

	  redis_pool = redis_pool = std::make_shared<rgw::d4n::RedisPool>(&io, cfg, 8);
      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete block;
      delete dir;
    }

    rgw::d4n::CacheBlock* block;
    rgw::d4n::RedisBlockDirectory* dir;

    net::io_context io;
    std::shared_ptr<connection> conn;
    std::shared_ptr<rgw::d4n::RedisConnection> redis_conn;
	std::shared_ptr<rgw::d4n::RedisPool> redis_pool; 

    std::vector<std::string> vals{"0", "", "0", "0", "0", 
                                   "testName", "testBucket", "", "0", env->redisHost};
    std::vector<std::string> fields{"blockID", "version", "deleteMarker", "size", "globalWeight", 
				     "objName", "bucketName", "creationTime", "dirty", "hosts"};
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

TEST_F(ObjectDirectoryFixture, AddVersion)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
	auto r_conn = redis_conn->get_redis_conn();
	rgw::d4n::Pipeline p = rgw::d4n::Pipeline(r_conn, redis_pool);
    p.start();
    ASSERT_EQ(0, dir->add_version(env->dpp, obj->bucketName, obj->objName, version, time, std::nullopt, yield, &p));
	p.execute(env->dpp, optional_yield{yield});

    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", to_legacy_object_index("testBucket_testName"), "0", "-1");
    req.push("FLUSHALL");

    response< std::vector<std::string>,
	          boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], version);
    redis_pool->cancel_all();
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, RemoveVersion)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
	auto r_conn = redis_conn->get_redis_conn();
	rgw::d4n::Pipeline p = rgw::d4n::Pipeline(r_conn, redis_pool);
    p.start();
    ASSERT_EQ(0, dir->add_version(env->dpp, obj->bucketName, obj->objName, version, time, std::nullopt, yield, &p));
	p.execute(env->dpp, optional_yield{yield});

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", to_legacy_object_index("testBucket_testName"));
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->remove_version(env->dpp, obj->bucketName, obj->objName, version, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", to_legacy_object_index("testBucket_testName"));
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    redis_pool->cancel_all();
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, RemoveVersionCreationTime)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
	auto r_conn = redis_conn->get_redis_conn();
	rgw::d4n::Pipeline p = rgw::d4n::Pipeline(r_conn, redis_pool);
    p.start();
    ASSERT_EQ(0, dir->add_version(env->dpp, obj->bucketName, obj->objName, version, time, std::nullopt, yield, &p));
	p.execute(env->dpp, optional_yield{yield});

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", to_legacy_object_index("testBucket_testName"));
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->remove_version_by_creation_time(env->dpp, obj->bucketName, obj->objName, time, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", to_legacy_object_index("testBucket_testName"));
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    redis_pool->cancel_all();
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, ListVersions)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
	auto r_conn = redis_conn->get_redis_conn();
	rgw::d4n::Pipeline p = rgw::d4n::Pipeline(r_conn, redis_pool);
	ceph::real_time time_next = real_clock::now();
    std::string version_next = "test_version_next";
    p.start();
    ASSERT_EQ(0, dir->add_version(env->dpp, obj->bucketName, obj->objName, version, time, std::nullopt, yield, &p));
    ASSERT_EQ(0, dir->add_version(env->dpp, obj->bucketName, obj->objName, version_next, time_next, std::nullopt, yield, &p));
	p.execute(env->dpp, optional_yield{yield});

	std::vector<rgw::d4n::CacheObjectVersion> obj_versions;
	std::string continuation_token;
    ASSERT_EQ(0, dir->list_versions(env->dpp, obj->bucketName, obj->objName, "", 2, obj_versions, continuation_token, yield));
	auto out = rgw::d4n::CacheObjectVersion{
      .objName = obj->objName,
      .bucketId = obj->bucketName,
      .version = version_next,
      .user_id = "",
      .display_name = ""};
	EXPECT_EQ(obj_versions[0], out); 
    out.version = version;
    EXPECT_EQ(obj_versions[1], out);

    redis_pool->cancel_all();
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, Delete)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
	auto r_conn = redis_conn->get_redis_conn();
	rgw::d4n::Pipeline p = rgw::d4n::Pipeline(r_conn, redis_pool);
    p.start();
    ASSERT_EQ(0, dir->add_version(env->dpp, obj->bucketName, obj->objName, version, time, std::nullopt, yield, &p));
	p.execute(env->dpp, optional_yield{yield});

    EXPECT_EQ(0, dir->del(env->dpp, obj, yield));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", to_legacy_object_index("testBucket_testName"));
    req.push("FLUSHALL");

    response<int, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 0);
    redis_pool->cancel_all();
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, SetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", to_legacy_block_index("testBucket_testName_0_0"), fields);
    req.push("FLUSHALL");

    response< std::vector<std::string>,
			  boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), vals);
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, GetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET",  to_legacy_block_index("testBucket_testName_0_0"), "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ(0, dir->get(env->dpp, block, optional_yield{yield}));
    EXPECT_EQ(block->cacheObj.objName, "newoid");

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

/* Does not currently pass on Ubuntu due to incompatible Redis version.
TEST_F(BlockDirectoryFixture, CopyYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));
    ASSERT_EQ(0, dir->copy(env->dpp, block, "copyTestName", "copyBucketName", optional_yield{yield}));

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
    copyVals[6] = "copyTestName";
    copyVals[7] = "copyBucketName";
    EXPECT_EQ(std::get<1>(resp).value(), copyVals);

    conn->cancel();
  }, rethrow);

  io.run();
}
*/

TEST_F(BlockDirectoryFixture, DelYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", to_legacy_block_index("testBucket_testName_0_0"));
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del(env->dpp, block, optional_yield{yield}));

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
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, UpdateFieldYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));
    std::string oid = "newTestName";
    std::string host = "127.0.0.1:5000";
    ASSERT_EQ(0, dir->update_field(env->dpp, block, "objName", oid, optional_yield{yield}));
    ASSERT_EQ(0, dir->update_field(env->dpp, block, "hosts", host, optional_yield{yield}));

    boost::system::error_code ec;
    request req;
    req.push("HMGET", to_legacy_block_index("testBucket_testName_0_0"), "objName", "hosts");
    req.push("FLUSHALL");
    response< std::vector<std::string>, 
	      boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], oid);
    EXPECT_EQ(std::get<0>(resp).value()[1], "127.0.0.1:6379_127.0.0.1:5000");

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, RemoveHostYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    block->cacheObj.hostsList.insert("127.0.0.1:6000");
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));
    {
      std::string host = "127.0.0.1:6379";
      ASSERT_EQ(0, dir->remove_host(env->dpp, block, host, optional_yield{yield}));
    }

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", to_legacy_block_index("testBucket_testName_0_0"), "hosts");
      req.push("HGET", to_legacy_block_index("testBucket_testName_0_0"), "hosts");
      response<int, std::string> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value(), "127.0.0.1:6000");
    }

    {
      std::string host = "127.0.0.1:6000";
      ASSERT_EQ(0, dir->remove_host(env->dpp, block, host, optional_yield{yield}));
    }

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", to_legacy_block_index("testBucket_testName_0_0"));
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, WatchExecuteYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
  {
    boost::system::error_code ec;
    request req;
    req.push("WATCH", "testBucket");
    response<std::string> resp;

    conn->async_exec(req, resp, yield[ec]);
    ASSERT_EQ((bool)ec, false);

    // The number of members added
    EXPECT_EQ(std::get<0>(resp).value(), "OK");
  }

  {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
  }

  {
      boost::system::error_code ec;
      request req;
      req.push("EXEC");
      response<std::vector<std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
  }

  {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
  }

  conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, IncrYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    for (int i = 0; i < 10; i++) {
      {
        boost::system::error_code ec;
        request req;
        req.push("INCR", "testObject");
        response<std::string> resp;

        conn->async_exec(req, resp, yield[ec]);
        ASSERT_EQ((bool)ec, false);
        std::cout << "thread id: " << std::this_thread::get_id() << std::endl;
        std::cout << "INCR value: " << std::get<0>(resp).value() << std::endl;
      }
    }
    boost::asio::post(conn->get_executor(), [c = conn] { c->cancel(); });
  }, rethrow);

  std::vector<std::thread> threads;

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&] { io.run(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(BlockDirectoryFixture, ZScan)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    boost::system::error_code ec;
    {
      request req;
      response<std::string> resp;
      req.push("ZADD", "myzset", "0", "v1");
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      std::cout << "ZADD value: " << std::get<0>(resp).value() << std::endl;
    }
    {
      request req;
      response<std::string> resp;
      req.push("ZADD", "myzset", "0", "v2");
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      std::cout << "ZADD value: " << std::get<0>(resp).value() << std::endl;
    }
    {
      request req;
      req.push("ZSCAN", "myzset", 0, "MATCH", "v*", "COUNT", 2);

      boost::redis::generic_response resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);

      std::vector<boost::redis::resp3::basic_node<std::__cxx11::basic_string<char> > > root_array;
      if (resp.has_value()) {
        root_array = resp.value();
        std::cout << "ZADD aggregate size is: " << root_array.size() << std::endl;
        auto size = root_array.size();
        if (size >= 2) {
          //Nothing of interest at index 0, index 1 has the next cursor value
          std::string new_cursor = root_array[1].value;

          //skip the first 3 values to get the actual member, score
          for (uint64_t i = 3; i < size; i = i+2) {
            std::string member = root_array[i].value;
            std::cout << "ZADD member: " << member << std::endl;
          }
        }
      }
    }
    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

template<typename T, typename Seq>
struct expander;

template<typename T, std::size_t... Is>
struct expander<T, std::index_sequence<Is...>> {
    template<typename E, std::size_t>
    using elem = E;

    using type = boost::redis::response<elem<T, Is>...>;
};

template <size_t N, class Type>
struct my_tuple
{
   using type = typename expander<Type, std::make_index_sequence<N>>::type;
};

template <typename Integer, Integer ...I, typename F>
constexpr void constexpr_for_each(std::integer_sequence<Integer, I...>, F &&func)
{
    (func(std::integral_constant<Integer, I>{}) , ...);
}

template <auto N, typename F>
constexpr void constexpr_for(F &&func)
{
    if constexpr (N > 0)
    {
        constexpr_for_each(std::make_integer_sequence<decltype(N), N>{}, std::forward<F>(func));
    }
}

template <typename T>
void foo(T t, std::vector<std::vector<std::string>>& responses)
{
    constexpr_for<std::tuple_size_v<T>>([&](auto index)
    {
        constexpr auto i = index.value;
        std::vector<std::string> empty_vector;
        if (std::get<i>(t).value().has_value()) {
          if (std::get<i>(t).value().value().empty()) {
            responses.emplace_back(empty_vector);
            std::cout << "Empty value for i: " << i << std::endl;
          } else {
            responses.emplace_back(std::get<i>(t).value().value());
          }
        } else {
          std::cout << "No value for i: " << i << std::endl;
          responses.emplace_back(empty_vector);
        }
    });
}

TEST_F(BlockDirectoryFixture, Pipeline)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    boost::system::error_code ec;
    {
      request req;
      response<boost::redis::ignore_t> resp;
      req.push("HSET", "testkey1", "name", "abc");
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
    }
    {
      request req;
      response<boost::redis::ignore_t> resp;
      req.push("HSET", "testkey2", "name", "def");
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
    }
    {
      //using boost::redis::response
      std::vector<std::string> fields;
      fields.push_back("name");
      request req;
      req.push_range("HMGET", "testkey1", fields);
      req.push_range("HMGET", "abc", fields);

      ASSERT_EQ(req.get_commands(), 2);
      //using template parameterization in case we need to read responses for large numebr of elements (1000 elements)
      my_tuple<5, std::optional<std::vector<std::string>>>::type resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      std::vector<std::vector<std::string>> responses;
      foo<decltype(resp)>(resp, responses);
      for (auto vec : responses) {
        if (!vec.empty()) {
          std::cout << "HMGET: " << vec[0] << std::endl;
        }
      }
    }
    {
      //using boost::redis::generic_response
      std::vector<std::string> fields;
      fields.push_back("name");
      request req;
      req.push("HGETALL", "testkey1");
      req.push("HGETALL", "testkey2");

      ASSERT_EQ(req.get_commands(), 2);
      boost::redis::generic_response resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);

      //1st node gives data type and number of elements of that type
      //if data type is aggrgate, like array, map, then next n elements will be values of the aggregate type
      std::unordered_map<std::string, std::unordered_map<std::string,std::string> > key_val_map;
      auto i = 0, j = 0;
      std::string key, fieldkey, fieldval;
      int num_elements = 0;
      for (auto& element : resp.value()) {
        if (element.data_type == boost::redis::resp3::type::array || element.data_type == boost::redis::resp3::type::map) {
          num_elements = element.aggregate_size;
          if (j == 0) {
            key = "testkey1";
            j++;
          } else {
            key = "testkey2";
          }
          continue;
        } else {
          if (i < num_elements) {
            fieldkey = element.value;
            i++;
          } else {
            fieldval = element.value;
            key_val_map.emplace(key, std::unordered_map<std::string,std::string>{{fieldkey,fieldval}});
            key.clear();
            fieldkey.clear();
            fieldval.clear();
            i = 0;
          }
        }
      }
      std::cout << "HGETALL response size is: " << key_val_map.size() << std::endl;
      for (auto& it : key_val_map) {
        std::cout << "key: " << it.first << std::endl;
        std::unordered_map<std::string,std::string> field_key_val_map = it.second;
        for (auto& inner_it : field_key_val_map) {
          std::cout << "fieldkey: " << inner_it.first << std::endl;
          std::cout << "fieldval: " << inner_it.second << std::endl;
        }
      }
    }
    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
