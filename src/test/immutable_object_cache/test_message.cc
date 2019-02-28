#include "gtest/gtest.h"
#include "tools/immutable_object_cache/Types.h"
#include "tools/immutable_object_cache/SocketCommon.h"

using namespace ceph::immutable_obj_cache;

TEST(test_for_message, test_1) 
{
  std::string pool_nspace("this is a pool namespace");
  std::string oid_name("this is a oid name");
  std::string cache_file_path("/temp/ceph_immutable_object_cache");

  uint16_t type = RBDSC_READ;
  uint64_t seq = 123456UL;
  uint64_t read_offset = 222222UL;
  uint64_t read_len = 333333UL;
  uint64_t pool_id = 444444UL;
  uint64_t snap_id = 555555UL;

  // ObjectRequest --> bufferlist
  ObjectCacheRequest* req = new ObjectCacheReadData(type, seq, read_offset, read_len,
                                    pool_id, snap_id, oid_name, pool_nspace);
  req->encode();
  auto payload_bl = req->get_payload_bufferlist();

  uint32_t data_len = get_data_len(payload_bl.c_str());
  ASSERT_EQ(payload_bl.length(), data_len + get_header_size());
  ASSERT_TRUE(payload_bl.c_str() != nullptr);

  // bufferlist --> ObjectCacheRequest
  ObjectCacheRequest* req_decode = decode_object_cache_request(payload_bl);

  ASSERT_EQ(req_decode->get_request_type(), RBDSC_READ);

  ASSERT_EQ(req_decode->type, RBDSC_READ);
  ASSERT_EQ(req_decode->seq, 123456UL);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->type, RBDSC_READ);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->seq, 123456UL);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->read_offset, 222222UL);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->read_len, 333333UL);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->pool_id, 444444UL);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->snap_id, 555555UL);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->pool_namespace, pool_nspace);
  ASSERT_EQ(((ObjectCacheReadData*)req_decode)->oid, oid_name);

  delete req;
  delete req_decode;
}
