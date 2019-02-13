#include "gtest/gtest.h"
#include "tools/immutable_object_cache/Types.h"
#include "tools/immutable_object_cache/SocketCommon.h"

using namespace ceph::immutable_obj_cache;

TEST(test_for_message, test_1) 
{
  std::string pool_nspace("this is a pool namespace");
  std::string oid_name("this is a oid name");
  std::string cache_file_path("/temp/ceph_immutable_object_cache");

  ObjectCacheReadData data;

  data.seq = 1UL;
  data.type = RBDSC_READ;
  data.m_read_offset = 222222;
  data.m_read_len = 333333;
  data.m_pool_id = 444444;
  data.m_snap_id = 555555;
  data.m_oid = oid_name;
  data.m_pool_namespace = pool_nspace;

  // ObjectRequest --> bufferlist
  ObjectCacheRequest* req = encode_object_cache_request(&data, RBDSC_READ);

  auto data_bl = req->get_data_buffer();
  uint32_t data_len = get_data_len(data_bl.c_str());
  ASSERT_EQ(data_bl.length(), data_len + get_header_size());
  ASSERT_TRUE(data_bl.c_str() != nullptr);

  // bufferlist --> ObjectCacheRequest
  ObjectCacheRequest* req_decode = decode_object_cache_request(data_bl);
  ObjectCacheReadData* reply_data = (ObjectCacheReadData*)(req_decode->m_data);

  ASSERT_EQ(req_decode->type, RBDSC_READ);

  ASSERT_EQ(reply_data->seq, 1UL);
  ASSERT_EQ(reply_data->type, RBDSC_READ);
  ASSERT_EQ(reply_data->m_read_offset, 222222UL);
  ASSERT_EQ(reply_data->m_read_len, 333333UL);
  ASSERT_EQ(reply_data->m_pool_id, 444444UL);
  ASSERT_EQ(reply_data->m_snap_id, 555555UL);
  ASSERT_EQ(reply_data->m_pool_namespace, pool_nspace);
  ASSERT_EQ(reply_data->m_oid, oid_name);

  delete req;
  delete req_decode;
}
