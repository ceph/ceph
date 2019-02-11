#include "gtest/gtest.h"
#include "tools/immutable_object_cache/Types.h"

using namespace ceph::immutable_obj_cache;

TEST(test_for_message, test_1) 
{
  std::string pool_nspace("this is a pool namespace");
  std::string oid_name("this is a oid name");
  std::string cache_file_path("/temp/ceph_immutable_object_cache");

  ObjectCacheRequest req;

  req.m_data.seq = 1;
  req.m_data.type = 2;
  req.m_data.m_read_offset = 222222;
  req.m_data.m_read_len = 333333;
  req.m_data.m_pool_id = 444444;
  req.m_data.m_snap_id = 555555;
  req.m_data.m_oid = oid_name;
  req.m_data.m_pool_namespace = pool_nspace;
  req.m_data.m_cache_path = cache_file_path;

  // ObjectRequest --> bufferlist
  req.encode();


  // bufferlist --> ObjectCacheRequest
  auto data_bl = req.get_data_buffer();
  uint32_t data_len = get_data_len(data_bl.c_str());
  ASSERT_EQ(data_bl.length(), data_len + get_header_size());

  ObjectCacheRequest* req_decode = decode_object_cache_request(data_bl);

  ASSERT_EQ(req_decode->m_data.seq, 1);
  ASSERT_EQ(req_decode->m_data.m_read_offset, 222222);
  ASSERT_EQ(req_decode->m_data.m_read_len, 333333);
  ASSERT_EQ(req_decode->m_data.m_pool_namespace, pool_nspace);
  ASSERT_EQ(req_decode->m_data.m_cache_path, cache_file_path);
  ASSERT_EQ(req_decode->m_data.m_oid, oid_name);

  delete req_decode;
}
