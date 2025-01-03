#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_aio_throttle.h"
#include "rgw_redis_driver.h"

#define dout_subsys ceph_subsys_rgw

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class Environment* env;
rgw::AioResultList completed;
uint64_t offset = 0;

int flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  while (!completed.empty() && completed.front().id == offset) {
    auto ret = std::move(completed.front().result);

    EXPECT_EQ(0, ret);
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }
  return 0;
}

void cancel(rgw::Aio* aio) {
  aio->drain();
}

int drain(const DoutPrefixProvider* dpp, rgw::Aio* aio) {
  auto c = aio->wait(); 
  while (!c.empty()) {
    int r = flush(dpp, std::move(c));
    if (r < 0) {
      cancel(aio);
      return r;
    }
    c = aio->wait();
  }
  return flush(dpp, std::move(c));
}

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

      redisHost = cct->_conf->rgw_d4n_address; 
    }

    std::string redisHost;
    CephContext* cct;
    DoutPrefixProvider* dpp;
};

class RedisDriverFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      rgw::cache::Partition partition_info{ .location = "RedisCache" };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};

      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(cacheDriver, nullptr);
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
      delete conn;
      delete cacheDriver;
    }

    rgw::cache::RedisDriver* cacheDriver;

    net::io_context io;
    connection* conn; 

    bufferlist bl;
    rgw::sal::Attrs attrs;
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

TEST_F(RedisDriverFixture, PutYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testName", "attr");
    req.push("FLUSHALL");

    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "attrVal");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, GetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "data", "new data", "attr", "newVal");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    bufferlist ret;
    rgw::sal::Attrs retAttrs;

    ASSERT_EQ(0, cacheDriver->get(env->dpp, "testName", 0, bl.length(), ret, retAttrs, yield));
    EXPECT_EQ(ret.to_str(), "new data");
    EXPECT_EQ(retAttrs.begin()->second.to_str(), "newVal");
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, PutAsyncYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(env->cct->_conf->rgw_get_obj_window_size, yield);
    auto completed = cacheDriver->put_async(env->dpp, yield, aio.get(), "testName", bl, bl.length(), attrs, 0, 0);
    drain(env->dpp, aio.get());

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "RedisCache/testName", "attr", "data");
    req.push("FLUSHALL");
    response<std::vector<std::string>, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "attrVal");
    EXPECT_EQ(std::get<0>(resp).value()[1], "test data");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, GetAsyncYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(env->cct->_conf->rgw_get_obj_window_size, yield);
    auto completed = cacheDriver->get_async(env->dpp, yield, aio.get(), "testName", 0, bl.length(), 0, 0);
    drain(env->dpp, aio.get());

    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "RedisCache/testName", "attr", "data");
    req.push("FLUSHALL");
    response<std::vector<std::string>, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "attrVal");
    EXPECT_EQ(std::get<0>(resp).value()[1], "test data");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, DelYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "RedisCache/testName");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, cacheDriver->del(env->dpp, "testName", yield));
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "RedisCache/testName");
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

TEST_F(RedisDriverFixture, AppendDataYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HGET", "RedisCache/testName", "data");
      response<std::string> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), "test data");
    }

    bufferlist val;
    val.append(" has been written");

    ASSERT_EQ(0, cacheDriver->append_data(env->dpp, "testName", val, yield));
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("HGET", "RedisCache/testName", "data");
      req.push("FLUSHALL");
      response<std::string, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), "test data has been written");
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, DeleteDataYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "data");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, cacheDriver->delete_data(env->dpp, "testName", yield));
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "data");
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

TEST_F(RedisDriverFixture, SetAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    rgw::sal::Attrs newAttrs;
    bufferlist newVal;
    newVal.append("newVal");
    newAttrs.insert({"newAttr", newVal});
     
    newVal.clear();
    newVal.append("nextVal");
    newAttrs.insert({"nextAttr", newVal});

    ASSERT_EQ(0, cacheDriver->set_attrs(env->dpp, "testName", newAttrs, yield));
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "RedisCache/testName", "newAttr", "nextAttr");
    req.push("FLUSHALL");

    response< std::vector<std::string>,
              boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "newVal");
    EXPECT_EQ(std::get<0>(resp).value()[1], "nextVal");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, GetAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    rgw::sal::Attrs nextAttrs = attrs;
    bufferlist nextVal;
    nextVal.append("nextVal");
    nextAttrs.insert({"nextAttr", nextVal});

    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), nextAttrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "attr", "newVal1", "nextAttr", "newVal2");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    rgw::sal::Attrs retAttrs;

    ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testName", retAttrs, yield));
   
    auto it = retAttrs.begin();
    EXPECT_EQ(it->second.to_str(), "newVal1");
    ++it;
    EXPECT_EQ(it->second.to_str(), "newVal2");
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, UpdateAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    rgw::sal::Attrs newAttrs;
    bufferlist newVal;
    newVal.append("newVal");
    newAttrs.insert({"attr", newVal});

    ASSERT_EQ(0, cacheDriver->update_attrs(env->dpp, "testName", newAttrs, yield));
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testName", "attr");
    req.push("FLUSHALL");
    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "newVal");

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, DeleteAttrsYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "attr");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    rgw::sal::Attrs delAttrs;
    bufferlist delVal;
    delAttrs.insert({"attr", delVal});

    ASSERT_GE(cacheDriver->delete_attrs(env->dpp, "testName", delAttrs, yield), 0);
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "attr");
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

TEST_F(RedisDriverFixture, SetAttrYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));
    ASSERT_GE(cacheDriver->set_attr(env->dpp, "testName", "newAttr", "newVal", yield), 0);
    cacheDriver->shutdown();

    boost::system::error_code ec;
    request req;
    req.push("HGET", "RedisCache/testName", "newAttr");
    req.push("FLUSHALL");

    response<std::string, boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), "newVal");
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(RedisDriverFixture, GetAttrYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "attr", "newVal");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }
    std::string attr_val;
    ASSERT_EQ(0, cacheDriver->get_attr(env->dpp, "testName", "attr", attr_val, yield));
    ASSERT_EQ("newVal", attr_val);
    cacheDriver->shutdown();

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
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
