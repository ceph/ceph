// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <iostream>
#include <sstream>
#include <string>

#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "common/async/context_pool.h"

#include "common/common_init.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "driver/rados/rgw_zone.h"
#include "rgw_sal_config.h"

#include "gtest/gtest.h"

using namespace std::literals;

static constexpr auto dout_subsys = ceph_subsys_rgw;

static rgw::sal::RadosStore* store = nullptr;

static const DoutPrefixProvider* dpp() {
  struct GlobalPrefix : public DoutPrefixProvider {
    CephContext *get_cct() const override { return g_ceph_context; }
    unsigned get_subsys() const override { return dout_subsys; }
    std::ostream& gen_prefix(std::ostream& out) const override { return out; }
  };
  static GlobalPrefix global_dpp;
  return &global_dpp;
}

class StoreDestructor {
  rgw::sal::Driver* driver;
public:
  explicit StoreDestructor(rgw::sal::RadosStore* _s) : driver(_s) {}
  ~StoreDestructor() {
    DriverManager::close_storage(store);
  }
};

struct TempPool {
  inline static uint64_t num = 0;
  std::string name =
    fmt::format("{}-{}-{}", ::time(nullptr), ::getpid(),num++);

  TempPool() {
    [[maybe_unused]] auto r =
        store->getRados()->get_rados_handle()->pool_create(name.c_str());
    assert(r == 0);
  }

  ~TempPool() {
    [[maybe_unused]] auto r =
        store->getRados()->get_rados_handle()->pool_delete(name.c_str());
    assert(r == 0);
  }

  operator rgw_pool() {
    return { name };
  }

  operator librados::IoCtx() {
    librados::IoCtx ioctx;
    [[maybe_unused]] auto r =
        store->getRados()->get_rados_handle()->ioctx_create(name.c_str(),
                                                            ioctx);
    assert(r == 0);
    return ioctx;
  }
};

int run(RGWCoroutine* cr) {
  RGWCoroutinesManager cr_mgr{store->ctx(),
                              store->getRados()->get_cr_registry()};
  std::list<RGWCoroutinesStack *> stacks;
  auto stack = new RGWCoroutinesStack(store->ctx(), &cr_mgr);
  stack->call(cr);
  stacks.push_back(stack);
  return cr_mgr.run(dpp(), stacks);
}

TEST(ReadAttrs, Unfiltered) {
  TempPool pool;
  ceph::bufferlist bl;
  auto dummy = "Dummy attribute value"s;
  encode(dummy, bl);
  const std::map<std::string, ceph::bufferlist> ref_attrs{
    { "foo"s, bl }, { "bar"s, bl }, { "baz"s, bl }
  };
  auto oid = "object"s;
  {
    librados::IoCtx ioctx(pool);
    librados::ObjectWriteOperation op;
    op.setxattr("foo", bl);
    op.setxattr("bar", bl);
    op.setxattr("baz", bl);
    auto r = ioctx.operate(oid, &op);
    ASSERT_EQ(0, r);
  }
  std::map<std::string, ceph::bufferlist> attrs;
  auto r = run(new RGWSimpleRadosReadAttrsCR(dpp(), store, {pool, oid}, &attrs,
					     true));
  ASSERT_EQ(0, r);
  ASSERT_EQ(ref_attrs, attrs);
}

TEST(ReadAttrs, Filtered) {
  TempPool pool;
  ceph::bufferlist bl;
  auto dummy = "Dummy attribute value"s;
  encode(dummy, bl);
  const std::map<std::string, ceph::bufferlist> ref_attrs{
    { RGW_ATTR_PREFIX "foo"s, bl },
    { RGW_ATTR_PREFIX "bar"s, bl },
    { RGW_ATTR_PREFIX "baz"s, bl }
  };
  auto oid = "object"s;
  {
    librados::IoCtx ioctx(pool);
    librados::ObjectWriteOperation op;
    op.setxattr(RGW_ATTR_PREFIX "foo", bl);
    op.setxattr(RGW_ATTR_PREFIX "bar", bl);
    op.setxattr(RGW_ATTR_PREFIX "baz", bl);
    op.setxattr("oneOfTheseThingsIsNotLikeTheOthers", bl);
    auto r = ioctx.operate(oid, &op);
    ASSERT_EQ(0, r);
  }
  std::map<std::string, ceph::bufferlist> attrs;
  auto r = run(new RGWSimpleRadosReadAttrsCR(dpp(), store, {pool, oid}, &attrs,
					     false));
  ASSERT_EQ(0, r);
  ASSERT_EQ(ref_attrs, attrs);
}

TEST(Read, Dne) {
  TempPool pool;
  std::string result;
  auto r = run(new RGWSimpleRadosReadCR(dpp(), store, {pool, "doesnotexist"},
					&result, false));
  ASSERT_EQ(-ENOENT, r);
}

TEST(Read, Read) {
  TempPool pool;
  auto data = "I am test data!"sv;
  auto oid = "object"s;
  {
    bufferlist bl;
    encode(data, bl);
    librados::IoCtx ioctx(pool);
    auto r = ioctx.write_full(oid, bl);
    ASSERT_EQ(0, r);
  }
  std::string result;
  auto r = run(new RGWSimpleRadosReadCR(dpp(), store, {pool, oid}, &result,
					false));
  ASSERT_EQ(0, r);
  ASSERT_EQ(result, data);
}

TEST(Read, ReadVersion) {
  TempPool pool;
  auto data = "I am test data!"sv;
  auto oid = "object"s;
  RGWObjVersionTracker wobjv;
  {
    bufferlist bl;
    encode(data, bl);
    librados::IoCtx ioctx(pool);
    librados::ObjectWriteOperation op;
    wobjv.generate_new_write_ver(store->ctx());
    wobjv.prepare_op_for_write(&op);
    op.write_full(bl);
    auto r = ioctx.operate(oid, &op);
    EXPECT_EQ(0, r);
    wobjv.apply_write();
  }
  RGWObjVersionTracker robjv;
  std::string result;
  auto r = run(new RGWSimpleRadosReadCR(dpp(), store, {pool, oid}, &result,
					false, &robjv));
  ASSERT_EQ(0, r);
  ASSERT_EQ(result, data);
  data = "I am NEW test data!";
  {
    bufferlist bl;
    encode(data, bl);
    librados::IoCtx ioctx(pool);
    librados::ObjectWriteOperation op;
    wobjv.generate_new_write_ver(store->ctx());
    wobjv.prepare_op_for_write(&op);
    op.write_full(bl);
    r = ioctx.operate(oid, &op);
    EXPECT_EQ(0, r);
    wobjv.apply_write();
  }
  result.clear();
  r = run(new RGWSimpleRadosReadCR(dpp(), store, {pool, oid}, &result, false,
				   &robjv));
  ASSERT_EQ(-ECANCELED, r);
  ASSERT_TRUE(result.empty());

  robjv.clear();
  r = run(new RGWSimpleRadosReadCR(dpp(), store, {pool, oid}, &result, false,
				   &robjv));
  ASSERT_EQ(0, r);
  ASSERT_EQ(result, data);
  ASSERT_EQ(wobjv.read_version, robjv.read_version);
}

TEST(Write, Exclusive) {
  TempPool pool;
  auto oid = "object"s;
  {
    bufferlist bl;
    bl.append("I'm some data!"s);
    librados::IoCtx ioctx(pool);
    auto r = ioctx.write_full(oid, bl);
    ASSERT_EQ(0, r);
  }
  auto r = run(new RGWSimpleRadosWriteCR(dpp(), store, {pool, oid},
					 "I am some DIFFERENT data!"s, nullptr,
					 true));
  ASSERT_EQ(-EEXIST, r);
}

TEST(Write, Write) {
  TempPool pool;
  auto oid = "object"s;
  auto data = "I'm some data!"s;
  auto r = run(new RGWSimpleRadosWriteCR(dpp(), store, {pool, oid},
					 data, nullptr, true));
  ASSERT_EQ(0, r);
  bufferlist bl;
  librados::IoCtx ioctx(pool);
  ioctx.read(oid, bl, 0, 0);
  ASSERT_EQ(0, r);
  std::string result;
  decode(result, bl);
  ASSERT_EQ(data, result);
}

TEST(Write, ObjV) {
  TempPool pool;
  auto oid = "object"s;
  RGWObjVersionTracker objv;
  objv.generate_new_write_ver(store->ctx());
  auto r = run(new RGWSimpleRadosWriteCR(dpp(), store, {pool, oid},
					 "I'm some data!"s, &objv,
					 true));
  RGWObjVersionTracker interfering_objv(objv);
  r = run(new RGWSimpleRadosWriteCR(dpp(), store, {pool, oid},
				    "I'm some newer, better data!"s,
				    &interfering_objv, false));
  ASSERT_EQ(0, r);
  r = run(new RGWSimpleRadosWriteCR(dpp(), store, {pool, oid},
				    "I'm some treacherous, obsolete data!"s,
				    &objv, false));
  ASSERT_EQ(-ECANCELED, r);
}

TEST(WriteAttrs, Attrs) {
  TempPool pool;
  auto oid = "object"s;
  bufferlist bl;
  bl.append("I'm some data.");
  std::map<std::string, bufferlist> wrattrs {
    { "foo", bl }, { "bar", bl }, { "baz", bl }
  };
  auto r = run(new RGWSimpleRadosWriteAttrsCR(dpp(), store, {pool, oid},
					      wrattrs, nullptr, true));
  ASSERT_EQ(0, r);
  std::map<std::string, bufferlist> rdattrs;
  librados::IoCtx ioctx(pool);
  r = ioctx.getxattrs(oid, rdattrs);
  ASSERT_EQ(0, r);
  ASSERT_EQ(wrattrs, rdattrs);
}

TEST(WriteAttrs, Empty) {
  TempPool pool;
  auto oid = "object"s;
  bufferlist bl;
  std::map<std::string, bufferlist> wrattrs {
    { "foo", bl }, { "bar", bl }, { "baz", bl }
  };
  // With an empty bufferlist all attributes should be skipped.
  auto r = run(new RGWSimpleRadosWriteAttrsCR(dpp(), store, {pool, oid},
					      wrattrs, nullptr, true));
  ASSERT_EQ(0, r);
  std::map<std::string, bufferlist> rdattrs;
  librados::IoCtx ioctx(pool);
  r = ioctx.getxattrs(oid, rdattrs);
  ASSERT_EQ(0, r);
  ASSERT_TRUE(rdattrs.empty());
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = rgw_global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_UTILITY, 0);

  // for region -> zonegroup conversion (must happen before common_init_finish())
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  /* common_init_finish needs to be called after g_conf().set_val() */
  common_init_finish(g_ceph_context);


  ceph::async::io_context_pool context_pool{cct->_conf->rgw_thread_pool_size};
  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  std::unique_ptr<rgw::sal::ConfigStore> cfgstore
    = DriverManager::create_config_store(dpp(), config_store_type);
  if (!cfgstore) {
    std::cerr << "Unable to initialize config store." << std::endl;
    exit(1);
  }
  rgw::SiteConfig site;
  auto r = site.load(dpp(), null_yield, cfgstore.get());
  if (r < 0) {
    std::cerr << "Unable to initialize config store." << std::endl;
    exit(1);
  }

  store = static_cast<rgw::sal::RadosStore*>(
    DriverManager::get_storage(dpp(),
			      g_ceph_context,
			      cfg,
			      context_pool,
			      site,
			      false,
			      false,
			      false,
			      false,
			      false,
			      true, null_yield, 
			      false));
  if (!store) {
    std::cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }
  StoreDestructor store_destructor(static_cast<rgw::sal::RadosStore*>(store));

  std::string pool{"rgw_cr_test"};
  store->getRados()->create_pool(dpp(), pool);

  testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}
