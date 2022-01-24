// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_gc_log.h"

#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

// creates a rados client and temporary pool
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
 public:
  static std::optional<librados::Rados> rados;

  void SetUp() override {
    rados.emplace();
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, *rados));
    pool_name = name;
  }
  void TearDown() override {
    if (pool_name) {
      ASSERT_EQ(0, destroy_one_pool_pp(*pool_name, *rados));
    }
    rados.reset();
  }

  static int ioctx_create(librados::IoCtx& ioctx) {
    return rados->ioctx_create(pool_name->c_str(), ioctx);
  }
};
std::optional<std::string> RadosEnv::pool_name;
std::optional<librados::Rados> RadosEnv::rados;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

class rgw_gc_log : public ::testing::Test {
 protected:
  static librados::IoCtx ioctx;

  static void SetUpTestSuite() {
    ASSERT_EQ(0, RadosEnv::ioctx_create(ioctx));
  }
  static void TearDownTestSuite() {
    ioctx.close();
  }

  // use the test's name as the oid so different tests don't conflict
  std::string get_test_oid() const {
    return ::testing::UnitTest::GetInstance()->current_test_info()->name();
  }
};
librados::IoCtx rgw_gc_log::ioctx;


TEST_F(rgw_gc_log, init_existing_queue)
{
  const std::string oid = get_test_oid();
  {
    // successfully inits new object
    librados::ObjectWriteOperation op;
    gc_log_init2(op, 1, 1);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }
  {
    // version check fails on second init
    librados::ObjectWriteOperation op;
    gc_log_init2(op, 1, 1);
    ASSERT_EQ(-ECANCELED, ioctx.operate(oid, &op));
  }
}

TEST_F(rgw_gc_log, init_existing_omap)
{
  const std::string oid = get_test_oid();
  {
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;
    gc_log_enqueue1(op, 5, info);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }
  {
    // init succeeds with existing omap entries
    librados::ObjectWriteOperation op;
    gc_log_init2(op, 1, 1);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }
}

TEST_F(rgw_gc_log, enqueue1_after_init)
{
  const std::string oid = get_test_oid();
  {
    librados::ObjectWriteOperation op;
    gc_log_init2(op, 1, 1);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }
  {
    // version check fails on omap enqueue
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;
    gc_log_enqueue1(op, 5, info);
    ASSERT_EQ(-ECANCELED, ioctx.operate(oid, &op));
  }
}

TEST_F(rgw_gc_log, enqueue2_before_init)
{
  const std::string oid = get_test_oid();
  {
    // version check fails on cls_rgw_gc enqueue
    librados::ObjectWriteOperation op;
    gc_log_enqueue2(op, 5, {});
    ASSERT_EQ(-ECANCELED, ioctx.operate(oid, &op));
  }
}

TEST_F(rgw_gc_log, defer1_after_init)
{
  const std::string oid = get_test_oid();
  {
    librados::ObjectWriteOperation op;
    gc_log_init2(op, 1, 1);
    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }
  {
    // version check fails on omap defer
    librados::ObjectWriteOperation op;
    gc_log_defer1(op, 5, {});
    ASSERT_EQ(-ECANCELED, ioctx.operate(oid, &op));
  }
}

TEST_F(rgw_gc_log, defer2_before_init)
{
  const std::string oid = get_test_oid();
  {
    // version check fails on cls_rgw_gc defer
    librados::ObjectWriteOperation op;
    gc_log_defer2(op, 5, {});
    ASSERT_EQ(-ECANCELED, ioctx.operate(oid, &op));
  }
}
