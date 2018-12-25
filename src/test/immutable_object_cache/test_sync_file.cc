// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "include/Context.h"
#include "include/buffer_fwd.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include <experimental/filesystem>

#include "tools/immutable_object_cache/ObjectCacheFile.h"

using namespace ceph::immutable_obj_cache;
namespace efs = std::experimental::filesystem;

class TestObjectCacheFile :public ::testing::Test {
public:
  std::string m_cache_root_dir;

  TestObjectCacheFile(){}
  ~TestObjectCacheFile(){}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  void SetUp() override {
    m_cache_root_dir = g_ceph_context->_conf.get_val<std::string>("immutable_object_cache_path")
      + "/ceph_immutable_obj_cache/";

    if (efs::exists(m_cache_root_dir)) {
      efs::remove_all(m_cache_root_dir);
    }
    efs::create_directories(m_cache_root_dir);
  }

  void TearDown() override {
    efs::remove_all(m_cache_root_dir);
  }

};

TEST_F(TestObjectCacheFile, test_write_object_to_file) {
  ObjectCacheFile* m_sync_file_1 = new ObjectCacheFile(g_ceph_context, "test_sync_file_1");
  ObjectCacheFile* m_sync_file_2 = new ObjectCacheFile(g_ceph_context, "test_sync_file_2");
  ASSERT_TRUE(m_sync_file_1->get_file_size() == -1);
  ASSERT_TRUE(m_sync_file_2->get_file_size() == -1);
  bufferlist* buf_1 = new ceph::bufferlist();
  bufferlist* buf_2 = new ceph::bufferlist();
  buf_1->append(std::string(1024, '0'));
  buf_2->append(std::string(4096, '0'));
  ASSERT_TRUE(m_sync_file_1->write_object_to_file(*buf_1, 1024) == 1024);
  ASSERT_TRUE(m_sync_file_2->write_object_to_file(*buf_2, 4096) == 4096);
  ASSERT_TRUE(m_sync_file_1->get_file_size() == 1024);
  ASSERT_TRUE(m_sync_file_2->get_file_size() == 4096);
  delete m_sync_file_1;
  delete m_sync_file_2;
  delete buf_1;
  delete buf_2;
}

TEST_F(TestObjectCacheFile, test_read_object_from_file) {
  ObjectCacheFile* m_sync_file_1 = new ObjectCacheFile(g_ceph_context, "test_sync_file_1");
  ObjectCacheFile* m_sync_file_2 = new ObjectCacheFile(g_ceph_context, "test_sync_file_2");
  bufferlist* buf_1 = new ceph::bufferlist();
  bufferlist* buf_2 = new ceph::bufferlist();
  ASSERT_EQ(m_sync_file_1->read_object_from_file(buf_1, 0, 1024), -1);
  ASSERT_EQ(m_sync_file_2->read_object_from_file(buf_2, 0, 1024), -1);
  ASSERT_TRUE(m_sync_file_1->get_file_size() == -1);
  ASSERT_TRUE(m_sync_file_2->get_file_size() == -1);
  ASSERT_EQ(m_sync_file_1->read_object_from_file(buf_1, 0, 1024), -1);
  ASSERT_EQ(m_sync_file_2->read_object_from_file(buf_2, 0, 1024), -1);
  buf_1->append(std::string(1024, '0'));
  buf_2->append(std::string(4096, '2'));
  ASSERT_TRUE(m_sync_file_1->write_object_to_file(*buf_1, 1024) == 1024);
  ASSERT_TRUE(m_sync_file_2->write_object_to_file(*buf_2, 4096) == 4096);
  ASSERT_TRUE(m_sync_file_1->get_file_size() == 1024);
  ASSERT_TRUE(m_sync_file_2->get_file_size() == 4096);
  ASSERT_EQ(m_sync_file_1->read_object_from_file(buf_1, 0, 1024), 1024);
  ASSERT_EQ(m_sync_file_2->read_object_from_file(buf_2, 0, 4096), 4096);
  delete m_sync_file_1;
  delete m_sync_file_2;
  delete buf_1;
  delete buf_2;
}
