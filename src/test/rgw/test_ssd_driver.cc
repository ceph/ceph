#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_aio_throttle.h"
#include "rgw_ssd_driver.h"

#define dout_subsys ceph_subsys_rgw

namespace net = boost::asio;

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
      dpp = new DoutPrefix(cct->get(), dout_subsys, "SSD backed Cache backend Test: ");
    }

    CephContext* cct;
    DoutPrefixProvider* dpp;
};

class SSDDriverFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
        rgw::cache::Partition partition_info{.name = "d4n", .type = "read-cache", .location = "rgw_d4n_datacache", .size = 5368709120};
        cacheDriver = new rgw::cache::SSDDriver{partition_info};

        ASSERT_NE(cacheDriver, nullptr);

        cacheDriver->initialize(env->dpp);

        bl.append("This is testdata");
        attrVal.append("attrVal");
        attrs.insert({"user.rgw.attrName", attrVal});

        updateAttrVal1.append("newAttrVal1");
        updateAttrVal2.append("newAttrVal2");
        update_attrs.insert({"user.rgw.attrName", updateAttrVal1});
        update_attrs.insert({"user.rgw.testAttr", updateAttrVal2});

        del_attrs = attrs;
    } 

    virtual void TearDown() {
      delete cacheDriver;
    }

    rgw::cache::SSDDriver* cacheDriver;

    net::io_context io;

    bufferlist bl;
    bufferlist attrVal, updateAttrVal1, updateAttrVal2;
    rgw::sal::Attrs attrs;
    rgw::sal::Attrs update_attrs;
    rgw::sal::Attrs del_attrs;
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

TEST_F(SSDDriverFixture, PutAndGet)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testPutGet", bl, bl.length(), attrs, yield));
        bufferlist ret;
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testPutGet", 0, bl.length(), ret, get_attrs, yield));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(get_attrs.size(), 0);
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, AppendData)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testAppend", bl, bl.length(), attrs, yield));
    
        bufferlist bl_append;
        bl_append.append(" xyz");
        ASSERT_EQ(0, cacheDriver->append_data(env->dpp, "testAppend", bl_append, yield));
    
        bufferlist ret;
        bl.append(bl_append);
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testAppend", 0, bl.length(), ret, get_attrs, yield));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(get_attrs.size(), 0);
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, SetGetAttrs)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testSetGetAttrs", bl, bl.length(), attrs, yield));
        bufferlist ret;
        rgw::sal::Attrs ret_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testSetGetAttrs", 0, bl.length(), ret, ret_attrs, yield));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(ret_attrs.size(), 1);
        for (auto& it : ret_attrs) {
          EXPECT_EQ(it.first, "user.rgw.attrName");
          EXPECT_EQ(it.second, attrVal);
        }
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, UpdateAttrs)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testUpdateAttrs", bl, bl.length(), attrs, yield));
        ASSERT_EQ(0, cacheDriver->update_attrs(env->dpp, "testUpdateAttrs", update_attrs, yield));
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testUpdateAttrs", get_attrs, yield));
        EXPECT_EQ(get_attrs.size(), 2);
        EXPECT_EQ(get_attrs["user.rgw.attrName"], updateAttrVal1);
        EXPECT_EQ(get_attrs["user.rgw.testAttr"], updateAttrVal2);
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, SetGetAttr)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
      rgw::sal::Attrs attrs = {};
      ASSERT_EQ(0, cacheDriver->put(env->dpp, "testSetGetAttr", bl, bl.length(), attrs, yield));
      std::string attr_name = "user.ssd.testattr";
      std::string attr_val = "testattrVal";
      ASSERT_EQ(0, cacheDriver->set_attr(env->dpp, "testSetGetAttr", attr_name, attr_val, yield));
      std::string attr_val_ret;
      ASSERT_EQ(0, cacheDriver->get_attr(env->dpp, "testSetGetAttr", attr_name, attr_val_ret, yield));
      ASSERT_EQ(attr_val, attr_val_ret);
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, DeleteAttr)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
      rgw::sal::Attrs attrs = {};
      ASSERT_EQ(0, cacheDriver->put(env->dpp, "testDeleteAttr", bl, bl.length(), attrs, yield));
      std::string attr_name = "user.ssd.testattr";
      std::string attr_val = "testattrVal";
      ASSERT_EQ(0, cacheDriver->set_attr(env->dpp, "testDeleteAttr", attr_name, attr_val, yield));
      std::string attr_val_ret;
      ASSERT_EQ(0, cacheDriver->get_attr(env->dpp, "testDeleteAttr", attr_name, attr_val_ret, yield));
      ASSERT_EQ(attr_val, attr_val_ret);

      attr_val_ret.clear();
      ASSERT_EQ(0, cacheDriver->delete_attr(env->dpp, "testDeleteAttr", attr_name));
      ASSERT_EQ(ENODATA, cacheDriver->get_attr(env->dpp, "testDeleteAttr", attr_name, attr_val_ret, yield));
      ASSERT_EQ("", attr_val_ret);
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, DeleteAttrs)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
      ASSERT_EQ(0, cacheDriver->put(env->dpp, "testDeleteAttr", bl, bl.length(), attrs, yield));
      rgw::sal::Attrs ret_attrs;
      ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testDeleteAttr", ret_attrs, yield));
      EXPECT_EQ(ret_attrs.size(), 1);
      for (auto& it : ret_attrs) {
        EXPECT_EQ(it.first, "user.rgw.attrName");
        EXPECT_EQ(it.second, attrVal);
      }

      ASSERT_EQ(0, cacheDriver->delete_attrs(env->dpp, "testDeleteAttr", del_attrs, yield));
      ret_attrs.clear();
      ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testDeleteAttr", del_attrs, yield));
      EXPECT_EQ(ret_attrs.size(), 0);
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, DeleteData)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testDeleteData", bl, bl.length(), attrs, yield));
        bufferlist ret;
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testDeleteData", 0, bl.length(), ret, get_attrs, yield));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(get_attrs.size(), 0);
        ASSERT_EQ(0, cacheDriver->delete_data(env->dpp, "testDeleteData", yield));
        ASSERT_EQ(-ENOENT, cacheDriver->get(env->dpp, "testDeleteData", 0, bl.length(), ret, get_attrs, yield));
    }, rethrow);

    io.run();
}

TEST_F(SSDDriverFixture, PutAsync)
{
    boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        const uint64_t window_size = env->cct->_conf->rgw_put_obj_min_window_size;
        std::unique_ptr<rgw::Aio> aio = rgw::make_throttle(window_size, yield);
        auto results = cacheDriver->put_async(env->dpp, yield, aio.get(), "testPutAsync", bl, bl.length(), attrs, bl.length(), 0);
        drain(env->dpp, aio.get());
    }, rethrow);

    io.run();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}