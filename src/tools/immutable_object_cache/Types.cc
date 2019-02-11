// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

void ObjectCacheMsgData::encode(bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  ceph::encode(seq, bl);
  ceph::encode(type, bl);
  ceph::encode(m_read_offset, bl);
  ceph::encode(m_read_len, bl);
  ceph::encode(m_pool_id, bl);
  ceph::encode(m_snap_id, bl);
  ceph::encode(m_oid, bl);
  ceph::encode(m_pool_namespace, bl);
  ceph::encode(m_cache_path, bl);
  ENCODE_FINISH(bl);
}

void ObjectCacheMsgData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(seq, i);
  ceph::decode(type, i);
  ceph::decode(m_read_offset, i);
  ceph::decode(m_read_len, i);
  ceph::decode(m_pool_id, i);
  ceph::decode(m_snap_id, i);
  ceph::decode(m_oid, i);
  ceph::decode(m_pool_namespace, i);
  ceph::decode(m_cache_path, i);
  DECODE_FINISH(i);
}

void ObjectCacheRequest::encode() {
  m_data.encode(m_data_buffer);
}

uint8_t get_header_size() {
  //return sizeof(ObjectCacheMsgHeader);
  return 6;
}

struct encode_header{
  uint8_t v;
  uint8_t c_v;
  uint32_t len;
}__attribute__((packed));

uint32_t get_data_len(char* buf) {
  encode_header* header = (encode_header*)buf;
  return header->len;
}

bufferlist ObjectCacheRequest::get_data_buffer() {
  return m_data_buffer;
}

ObjectCacheRequest* decode_object_cache_request(bufferlist data_buffer) {
  ObjectCacheRequest* req = new ObjectCacheRequest();
  req->m_data.decode(data_buffer);
  return req;
}

} // namespace immutable_obj_cache
} // namespace ceph
