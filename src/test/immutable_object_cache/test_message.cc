#include "gtest/gtest.h"
#include "tools/immutable_object_cache/Types.h"

using namespace ceph::immutable_obj_cache;

TEST(test_for_message, test_1) 
{
  std::string pool_nspace("this is a pool namespace");
  std::string oid_name("this is a oid name");
  std::string cache_file_path("/temp/ceph_immutable_object_cache");

  ObjectCacheMsgHeader header;
  header.seq = 1;
  header.type = 2;
  header.version =3;
  header.data_len = 0;
  header.reserved = 5;

  ObjectCacheRequest req;

  ASSERT_EQ(req.m_head_buffer.length(), 0);
  ASSERT_EQ(req.m_data_buffer.length(), 0);

  req.m_head = header;

  req.m_data.m_read_offset = 222222;
  req.m_data.m_read_len = 333333;
  req.m_data.m_pool_id = 444444;
  req.m_data.m_snap_id = 555555;
  req.m_data.m_oid = oid_name;
  req.m_data.m_pool_namespace = pool_nspace;
  req.m_data.m_cache_path = cache_file_path;

  // ObjectRequest --> bufferlist
  req.encode();

  ASSERT_EQ(req.m_head_buffer.length(), sizeof(req.m_head));

  ObjectCacheRequest* req_decode;

  auto head_bl = req.get_head_buffer();
  auto data_bl = req.get_data_buffer();

  // bufferlist --> ObjectCacheRequest
  req_decode = decode_object_cache_request(head_bl, data_bl);

  ASSERT_EQ(req_decode->m_head.seq, header.seq);
  ASSERT_EQ(req_decode->m_head.seq, 1);
  ASSERT_EQ(req_decode->m_head.type, header.type);
  ASSERT_EQ(req_decode->m_head.type, 2);
  ASSERT_EQ(req_decode->m_head.version, header.version);
  ASSERT_EQ(req_decode->m_head.version, 3);
  ASSERT_EQ(req_decode->m_head.data_len, req.m_data_buffer.length());
  ASSERT_EQ(req_decode->m_head.data_len, data_bl.length());
  ASSERT_EQ(req_decode->m_head.reserved, header.reserved);
  ASSERT_EQ(req_decode->m_head.reserved, 5);

  ASSERT_EQ(req_decode->m_data.m_read_offset, req.m_data.m_read_offset);
  ASSERT_EQ(req_decode->m_data.m_read_offset, 222222);
  ASSERT_EQ(req_decode->m_data.m_read_len, req.m_data.m_read_len);
  ASSERT_EQ(req_decode->m_data.m_read_len, 333333);
  ASSERT_EQ(req_decode->m_data.m_pool_namespace, req.m_data.m_pool_namespace);
  ASSERT_EQ(req_decode->m_data.m_pool_namespace, pool_nspace);
  ASSERT_EQ(req_decode->m_data.m_cache_path, req.m_data.m_cache_path);
  ASSERT_EQ(req_decode->m_data.m_cache_path, cache_file_path);
  ASSERT_EQ(req_decode->m_data.m_oid, req.m_data.m_oid);
  ASSERT_EQ(req_decode->m_data.m_oid, oid_name);

  delete req_decode;
}
