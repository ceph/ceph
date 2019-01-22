// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

void ObjectCacheMsgHeader::encode(bufferlist& bl) const {
  ceph::encode(seq, bl);
  ceph::encode(type, bl);
  ceph::encode(version, bl);
  ceph::encode(padding, bl);
  ceph::encode(data_len, bl);
  ceph::encode(reserved, bl);
}

void ObjectCacheMsgHeader::decode(bufferlist::const_iterator& it) {
  ceph::decode(seq, it);
  ceph::decode(type, it);
  ceph::decode(version, it);
  ceph::decode(padding, it);
  ceph::decode(data_len, it);
  ceph::decode(reserved, it);
}

void ObjectCacheMsgData::encode(bufferlist& bl) {
  ceph::encode(m_image_size, bl);
  ceph::encode(m_read_offset, bl);
  ceph::encode(m_read_len, bl);
  ceph::encode(m_pool_id, bl);
  ceph::encode(m_snap_id, bl);
  ceph::encode(m_pool_name, bl);
  ceph::encode(m_image_name, bl);
  ceph::encode(m_oid, bl);
  ceph::encode(m_pool_namespace, bl);
  ceph::encode(m_cache_path, bl);
}

void ObjectCacheMsgData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  ceph::decode(m_image_size, i);
  ceph::decode(m_read_offset, i);
  ceph::decode(m_read_len, i);
  ceph::decode(m_pool_id, i);
  ceph::decode(m_snap_id, i);
  ceph::decode(m_pool_name, i);
  ceph::decode(m_image_name, i);
  ceph::decode(m_oid, i);
  ceph::decode(m_pool_namespace, i);
  ceph::decode(m_cache_path, i);
}

void ObjectCacheRequest::encode() {
  m_data.encode(m_data_buffer);
  m_head.data_len = m_data_buffer.length();
  m_head.data_len = m_data_buffer.length();
  ceph_assert(m_head_buffer.length() == 0);
  m_head.encode(m_head_buffer);
  ceph_assert(sizeof(ObjectCacheMsgHeader) == m_head_buffer.length());
}

bufferlist ObjectCacheRequest::get_head_buffer() {
  return m_head_buffer;
}
bufferlist ObjectCacheRequest::get_data_buffer() {
  return m_data_buffer;
}

ObjectCacheRequest* decode_object_cache_request(
            ObjectCacheMsgHeader* head, bufferlist data_buffer) {
  ObjectCacheRequest* req = new ObjectCacheRequest();
  req->m_head = *head;
  ceph_assert(req->m_head.data_len == data_buffer.length());
  req->m_data.decode(data_buffer);
  return req;
}

ObjectCacheRequest* decode_object_cache_request(
             bufferlist head_buffer, bufferlist data_buffer) {
  return decode_object_cache_request((ObjectCacheMsgHeader*)(head_buffer.c_str()), data_buffer);
}

} // namespace immutable_obj_cache
} // namespace ceph
