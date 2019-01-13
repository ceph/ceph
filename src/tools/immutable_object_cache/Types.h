// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_TYPES_H
#define CEPH_CACHE_TYPES_H

#include "include/encoding.h"
#include "include/Context.h"

namespace ceph {
namespace immutable_obj_cache {

struct ObjectCacheMsgHeader {
    uint64_t seq;                         /* sequence id */
    uint16_t type;                        /* msg type */
    uint16_t version;                     /* object cache version */
    uint32_t padding;
    uint32_t data_len;
    uint32_t reserved;

    void encode(bufferlist& bl) const {
      ceph::encode(seq, bl);
      ceph::encode(type, bl);
      ceph::encode(version, bl);
      ceph::encode(padding, bl);
      ceph::encode(data_len, bl);
      ceph::encode(reserved, bl);
    }

    void decode(bufferlist::const_iterator& it) {
      ceph::decode(seq, it);
      ceph::decode(type, it);
      ceph::decode(version, it);
      ceph::decode(padding, it);
      ceph::decode(data_len, it);
      ceph::decode(reserved, it);
    }
};

class ObjectCacheMsgData {
public:
  uint64_t m_image_size;
  uint64_t m_read_offset;
  uint64_t m_read_len;
  std::string m_pool_name;
  std::string m_image_name;
  std::string m_oid;

   ObjectCacheMsgData(){}
   ~ObjectCacheMsgData(){}

   void encode(bufferlist& bl) {
     ceph::encode(m_image_size, bl);
     ceph::encode(m_read_offset, bl);
     ceph::encode(m_read_len, bl);
     ceph::encode(m_pool_name, bl);
     ceph::encode(m_image_name, bl);
     ceph::encode(m_oid, bl);
   }

   void decode(bufferlist& bl) {
     auto i = bl.cbegin();
     ceph::decode(m_image_size, i);
     ceph::decode(m_read_offset, i);
     ceph::decode(m_read_len, i);
     ceph::decode(m_pool_name, i);
     ceph::decode(m_image_name, i);
     ceph::decode(m_oid, i);
   }
};

class ObjectCacheRequest {
public:
    ObjectCacheMsgHeader m_head;
    ObjectCacheMsgData m_data;
    bufferlist m_head_buffer;
    bufferlist m_data_buffer;
    Context* m_on_finish;
    GenContext<ObjectCacheRequest*>* m_process_msg;

    ObjectCacheRequest() {}
    ~ObjectCacheRequest() {}
    void encode() {
      m_data.encode(m_data_buffer);
      m_head.data_len = m_data_buffer.length();
      m_head.data_len = m_data_buffer.length();
      assert(m_head_buffer.length() == 0);
      m_head.encode(m_head_buffer);
      assert(sizeof(ObjectCacheMsgHeader) == m_head_buffer.length());
    }
    bufferlist get_head_buffer() {
      return m_head_buffer;
    }
    bufferlist get_data_buffer() {
      return m_data_buffer;
    }
};

inline ObjectCacheRequest* decode_object_cache_request(
            ObjectCacheMsgHeader* head, bufferlist data_buffer) {
  ObjectCacheRequest* req = new ObjectCacheRequest();
  req->m_head = *head;
  assert(req->m_head.data_len == data_buffer.length());
  req->m_data.decode(data_buffer);
  return req;
}

inline ObjectCacheRequest* decode_object_cache_request(
             bufferlist head_buffer, bufferlist data_buffer) {
  return decode_object_cache_request((ObjectCacheMsgHeader*)(head_buffer.c_str()), data_buffer);
}

} // namespace immutable_obj_cache
} // namespace ceph
#endif
