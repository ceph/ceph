#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_aio_throttle.h"
#include "rgw_ssd_driver.h"

#define dout_subsys ceph_subsys_rgw

namespace net = boost::asio;

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

        cacheDriver->initialize(env->cct, env->dpp);

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

TEST_F(SSDDriverFixture, PutAndGet)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testPutGet", bl, bl.length(), attrs, optional_yield{io, yield}));
        bufferlist ret;
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testPutGet", 0, bl.length(), ret, get_attrs, optional_yield{io, yield}));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(get_attrs.size(), 0);
    });

    io.run();
}

TEST_F(SSDDriverFixture, AppendData)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testAppend", bl, bl.length(), attrs, optional_yield{io, yield}));
    
        bufferlist bl_append;
        bl_append.append(" xyz");
        ASSERT_EQ(0, cacheDriver->append_data(env->dpp, "testAppend", bl_append, optional_yield{io, yield}));
    
        bufferlist ret;
        bl.append(bl_append);
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testAppend", 0, bl.length(), ret, get_attrs, optional_yield{io, yield}));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(get_attrs.size(), 0);
    });

    io.run();
}

TEST_F(SSDDriverFixture, SetGetAttrs)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testSetGetAttrs", bl, bl.length(), attrs, optional_yield{io, yield}));
        bufferlist ret;
        rgw::sal::Attrs ret_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testSetGetAttrs", 0, bl.length(), ret, ret_attrs, optional_yield{io, yield}));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(ret_attrs.size(), 1);
        for (auto& it : ret_attrs) {
          EXPECT_EQ(it.first, "user.rgw.attrName");
          EXPECT_EQ(it.second, attrVal);
        }
    });

    io.run();
}

TEST_F(SSDDriverFixture, UpdateAttrs)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testUpdateAttrs", bl, bl.length(), attrs, optional_yield{io, yield}));
        ASSERT_EQ(0, cacheDriver->update_attrs(env->dpp, "testUpdateAttrs", update_attrs, optional_yield{io, yield}));
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testUpdateAttrs", get_attrs, optional_yield{io, yield}));
        EXPECT_EQ(get_attrs.size(), 2);
        EXPECT_EQ(get_attrs["user.rgw.attrName"], updateAttrVal1);
        EXPECT_EQ(get_attrs["user.rgw.testAttr"], updateAttrVal2);
    });

    io.run();
}

TEST_F(SSDDriverFixture, SetGetAttr)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
      rgw::sal::Attrs attrs = {};
      ASSERT_EQ(0, cacheDriver->put(env->dpp, "testSetGetAttr", bl, bl.length(), attrs, optional_yield{io, yield}));
      std::string attr_name = "user.ssd.testattr";
      std::string attr_val = "testattrVal";
      ASSERT_EQ(0, cacheDriver->set_attr(env->dpp, "testSetGetAttr", attr_name, attr_val, optional_yield{io, yield}));
      std::string attr_val_ret;
      ASSERT_EQ(0, cacheDriver->get_attr(env->dpp, "testSetGetAttr", attr_name, attr_val_ret, optional_yield{io, yield}));
      ASSERT_EQ(attr_val, attr_val_ret);
    });

    io.run();
}

TEST_F(SSDDriverFixture, DeleteAttr)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
      rgw::sal::Attrs attrs = {};
      ASSERT_EQ(0, cacheDriver->put(env->dpp, "testDeleteAttr", bl, bl.length(), attrs, optional_yield{io, yield}));
      std::string attr_name = "user.ssd.testattr";
      std::string attr_val = "testattrVal";
      ASSERT_EQ(0, cacheDriver->set_attr(env->dpp, "testDeleteAttr", attr_name, attr_val, optional_yield{io, yield}));
      std::string attr_val_ret;
      ASSERT_EQ(0, cacheDriver->get_attr(env->dpp, "testDeleteAttr", attr_name, attr_val_ret, optional_yield{io, yield}));
      ASSERT_EQ(attr_val, attr_val_ret);

      attr_val_ret.clear();
      ASSERT_EQ(0, cacheDriver->delete_attr(env->dpp, "testDeleteAttr", attr_name));
      ASSERT_EQ(ENODATA, cacheDriver->get_attr(env->dpp, "testDeleteAttr", attr_name, attr_val_ret, optional_yield{io, yield}));
      ASSERT_EQ("", attr_val_ret);
    });

    io.run();
}

TEST_F(SSDDriverFixture, DeleteAttrs)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
      ASSERT_EQ(0, cacheDriver->put(env->dpp, "testDeleteAttr", bl, bl.length(), attrs, optional_yield{io, yield}));
      rgw::sal::Attrs ret_attrs;
      ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testDeleteAttr", ret_attrs, optional_yield{io, yield}));
      EXPECT_EQ(ret_attrs.size(), 1);
      for (auto& it : ret_attrs) {
        EXPECT_EQ(it.first, "user.rgw.attrName");
        EXPECT_EQ(it.second, attrVal);
      }

      ASSERT_EQ(0, cacheDriver->delete_attrs(env->dpp, "testDeleteAttr", del_attrs, optional_yield{io, yield}));
      ret_attrs.clear();
      ASSERT_EQ(0, cacheDriver->get_attrs(env->dpp, "testDeleteAttr", del_attrs, optional_yield{io, yield}));
      EXPECT_EQ(ret_attrs.size(), 0);
    });

    io.run();
}

TEST_F(SSDDriverFixture, DeleteData)
{
    spawn::spawn(io, [this] (spawn::yield_context yield) {
        rgw::sal::Attrs attrs = {};
        ASSERT_EQ(0, cacheDriver->put(env->dpp, "testDeleteData", bl, bl.length(), attrs, optional_yield{io, yield}));
        bufferlist ret;
        rgw::sal::Attrs get_attrs;
        ASSERT_EQ(0, cacheDriver->get(env->dpp, "testDeleteData", 0, bl.length(), ret, get_attrs, optional_yield{io, yield}));
        EXPECT_EQ(ret, bl);
        EXPECT_EQ(get_attrs.size(), 0);
        ASSERT_EQ(0, cacheDriver->delete_data(env->dpp, "testDeleteData", optional_yield{io, yield}));
        ASSERT_EQ(-ENOENT, cacheDriver->get(env->dpp, "testDeleteData", 0, bl.length(), ret, get_attrs, optional_yield{io, yield}));
    });

    io.run();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}