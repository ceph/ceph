#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_redis_driver.h"

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

class RedisDriverFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      rgw::cache::Partition partition_info{ .location = "RedisCache" };
      cacheDriver = new rgw::cache::RedisDriver{io, partition_info};

      conn = new connection{boost::asio::make_strand(io)};

      ASSERT_NE(cacheDriver, nullptr);
      ASSERT_NE(conn, nullptr);

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
      delete cacheDriver;
    }

    rgw::cache::RedisDriver* cacheDriver;

    net::io_context io;
    connection* conn; 

    bufferlist bl;
    rgw::sal::Attrs attrs;
};

TEST_F(RedisDriverFixture, PutYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, GetYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

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

    ASSERT_EQ(0, cacheDriver->get(env->dpp, "testName", 0, bl.length(), ret, retAttrs, optional_yield{io, yield}));
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, DelYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "RedisCache/testName");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, cacheDriver->del(env->dpp, "testName", optional_yield{io, yield}));
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
  });

  io.run();
}

// How can I test get_async? -Sam

TEST_F(RedisDriverFixture, AppendDataYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

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

    ASSERT_EQ(0, cacheDriver->append_data(env->dpp, "testName", val, optional_yield{io, yield}));
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, DeleteDataYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "RedisCache/testName", "data");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, cacheDriver->delete_data(env->dpp, "testName", optional_yield{io, yield}));
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, SetAttrsYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

    rgw::sal::Attrs newAttrs;
    bufferlist newVal;
    newVal.append("newVal");
    newAttrs.insert({"newAttr", newVal});
     
    newVal.clear();
    newVal.append("nextVal");
    newAttrs.insert({"nextAttr", newVal});

    ASSERT_EQ(0, cacheDriver->set_attrs(env->dpp, "testName", newAttrs, optional_yield{io, yield}));
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, GetAttrsYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    rgw::sal::Attrs nextAttrs = attrs;
    bufferlist nextVal;
    nextVal.append("nextVal");
    nextAttrs.insert({"nextAttr", nextVal});

    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), nextAttrs, optional_yield{io, yield}));

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

    ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testName", retAttrs, optional_yield{io, yield}));
   
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, UpdateAttrsYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

    rgw::sal::Attrs newAttrs;
    bufferlist newVal;
    newVal.append("newVal");
    newAttrs.insert({"attr", newVal});

    ASSERT_EQ(0, cacheDriver->update_attrs(env->dpp, "testName", newAttrs, optional_yield{io, yield}));
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, DeleteAttrsYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

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

    ASSERT_GE(cacheDriver->delete_attrs(env->dpp, "testName", delAttrs, optional_yield{io, yield}), 0);
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, SetAttrYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));
    ASSERT_GE(cacheDriver->set_attr(env->dpp, "testName", "newAttr", "newVal", optional_yield{io, yield}), 0);
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
  });

  io.run();
}

TEST_F(RedisDriverFixture, GetAttrYield)
{
  spawn::spawn(io, [this] (spawn::yield_context yield) {
    ASSERT_EQ(0, cacheDriver->put(env->dpp, "testName", bl, bl.length(), attrs, optional_yield{io, yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "RedisCache/testName", "attr", "newVal");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ("newVal", cacheDriver->get_attr(env->dpp, "testName", "attr", optional_yield{io, yield}));
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
  });

  io.run();
}


int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
