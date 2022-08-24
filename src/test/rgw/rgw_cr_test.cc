// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <iostream>
#include <sstream>
#include <string>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "common/common_init.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"

#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

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
  rgw::sal::Store* store;
public:
  explicit StoreDestructor(rgw::sal::RadosStore* _s) : store(_s) {}
  ~StoreDestructor() {
    StoreManager::close_storage(store);
  }
};

struct TempPool {
  inline static uint64_t num = 0;
  std::string name =
    fmt::format("{}-{}-{}", ::time(nullptr), ::getpid(),num++);

  TempPool() {
    auto r = store->getRados()->get_rados_handle()->pool_create(name.c_str());
    assert(r == 0);
  }

  ~TempPool() {
    auto r = store->getRados()->get_rados_handle()->pool_delete(name.c_str());
    assert(r == 0);
  }

  operator rgw_pool() {
    return { name };
  }

  operator librados::IoCtx() {
    librados::IoCtx ioctx;
    auto r = store->getRados()->get_rados_handle()->ioctx_create(name.c_str(),
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


  StoreManager::Config cfg = StoreManager::get_config(true, g_ceph_context);

  store = static_cast<rgw::sal::RadosStore*>(
    StoreManager::get_storage(dpp(),
			      g_ceph_context,
			      cfg,
			      false,
			      false,
			      false,
			      false,
			      false,
			      true,
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
