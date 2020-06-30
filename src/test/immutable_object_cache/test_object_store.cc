// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <unistd.h>

#include <experimental/filesystem>

#include "gtest/gtest.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "test/librados/test.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "test/librados/test_cxx.h"

#include "tools/immutable_object_cache/ObjectCacheStore.h"

namespace efs = std::experimental::filesystem;
using namespace ceph::immutable_obj_cache;

std::string test_cache_path("/tmp/test_ceph_immutable_shared_cache");

class TestObjectStore : public ::testing::Test {
public:
  ObjectCacheStore* m_object_cache_store;
  librados::Rados* m_test_rados;
  CephContext* m_ceph_context;
  librados::IoCtx m_local_io_ctx;
  std::string m_temp_pool_name;
  std::string m_temp_volume_name;

  TestObjectStore(): m_object_cache_store(nullptr), m_test_rados(nullptr), m_ceph_context(nullptr){}

  ~TestObjectStore(){}

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  void SetUp() override {
    m_test_rados = new librados::Rados();
    ASSERT_EQ("", connect_cluster_pp(*m_test_rados));
    ASSERT_EQ(0, m_test_rados->conf_set("rbd_cache", "false"));
    ASSERT_EQ(0, m_test_rados->conf_set("immutable_object_cache_max_size", "1024"));
    ASSERT_EQ(0, m_test_rados->conf_set("immutable_object_cache_path", test_cache_path.c_str()));

  }

  void create_object_cache_store(uint64_t entry_num) {
    m_temp_pool_name = get_temp_pool_name("test_pool_");
    ASSERT_EQ(0, m_test_rados->pool_create(m_temp_pool_name.c_str()));
    ASSERT_EQ(0, m_test_rados->ioctx_create(m_temp_pool_name.c_str(), m_local_io_ctx));
    m_temp_volume_name = "test_volume";
    m_ceph_context = reinterpret_cast<CephContext*>(m_test_rados->cct());
    m_object_cache_store = new ObjectCacheStore(m_ceph_context);
  }

  void init_object_cache_store(std::string pool_name, std::string vol_name, uint64_t vol_size, bool reset) {
    ASSERT_EQ(0, m_object_cache_store->init(reset));
    ASSERT_EQ(0, m_object_cache_store->init_cache());
  }

  void shutdown_object_cache_store() {
    ASSERT_EQ(0, m_object_cache_store->shutdown());
  }

  void lookup_object_cache_store(std::string pool_name, std::string vol_name, std::string obj_name, int& ret) {
    std::string cache_path;
    ret = m_object_cache_store->lookup_object(pool_name, 1, 2, obj_name, cache_path);
  }

  void TearDown() override {
    if(m_test_rados)
      delete m_test_rados;
    if(m_object_cache_store)
      delete m_object_cache_store;
  }
};

TEST_F(TestObjectStore, test_1) {
  create_object_cache_store(1000);

  std::string cache_path(test_cache_path);

  efs::remove_all(test_cache_path);

  init_object_cache_store(m_temp_pool_name, m_temp_volume_name, 1000, true);


  // TODO add lookup interface testing

  shutdown_object_cache_store();
}
