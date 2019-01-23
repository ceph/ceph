#include "gtest/gtest.h"
#include "tools/immutable_object_cache/Types.h"

using namespace ceph::immutable_obj_cache;

TEST(test_for_message, test_1) 
{
  std::string oid_name("this is a oid name");

  ObjectCacheMsgHeader header;
  header.seq = 1;
  header.type = 2;
  header.version =3;
  header.data_len = 5;
  header.reserved = 6;

  ObjectCacheRequest req;

  ASSERT_EQ(req.m_head_buffer.length(), 0);
  ASSERT_EQ(req.m_data_buffer.length(), 0);

  req.m_head = header;

  req.m_data.m_read_offset = 222222;
  req.m_data.m_read_len = 333333;
  req.m_data.m_oid = oid_name;

  req.encode();

  ASSERT_EQ(req.m_head_buffer.length(), sizeof(req.m_head));


  ObjectCacheRequest* req_decode;

  auto x = req.get_head_buffer();
  auto z = req.get_data_buffer();

  req_decode = decode_object_cache_request(x, z);

  ASSERT_EQ(req_decode->m_head.seq, header.seq);
  ASSERT_EQ(req_decode->m_head.type, header.type);
  ASSERT_EQ(req_decode->m_head.version, header.version);
  ASSERT_EQ(req_decode->m_head.data_len, req.m_data_buffer.length());
  ASSERT_EQ(req_decode->m_head.reserved, header.reserved);


  ASSERT_EQ(req_decode->m_data.m_read_offset, 222222);
  ASSERT_EQ(req_decode->m_data.m_read_len, 333333);
  ASSERT_EQ(req_decode->m_data.m_oid, oid_name);
}
