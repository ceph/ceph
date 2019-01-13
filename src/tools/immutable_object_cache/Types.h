// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_TYPES_H
#define CEPH_CACHE_TYPES_H

#include "include/encoding.h"

namespace ceph {
namespace immutable_obj_cache {

struct ObjectCacheMsgHeader {
    uint64_t seq;                         /* sequence id */
    uint16_t type;                        /* msg type */
    uint16_t version;                     /* object cache version */
    uint32_t padding;
    uint64_t mid_len;
    uint32_t data_len;
    uint32_t reserved;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& it);
};

class ObjectCacheMsgMiddle {
public:
  uint64_t m_image_size;
  uint64_t m_read_offset;
  uint64_t m_read_len;
  std::string m_pool_name;
  std::string m_image_name;
  std::string m_oid;

   ObjectCacheMsgMiddle(){}
   ~ObjectCacheMsgMiddle(){}

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
    ObjectCacheMsgMiddle m_mid;

    bufferlist m_head_buffer;
    bufferlist m_mid_buffer;
    bufferlist m_data_buffer;

    ObjectCacheRequest() {}
    ~ObjectCacheRequest() {}

    void encode() {
      m_mid.encode(m_mid_buffer);

      m_head.mid_len = m_mid_buffer.length();
      m_head.data_len = m_data_buffer.length();

      assert(m_head_buffer.length() == 0);
      m_head.encode(m_head_buffer);
      assert(sizeof(ObjectCacheMsgHeader) == m_head_buffer.length());
    }

    bufferlist get_head_buffer() {
      return m_head_buffer;
    }

    bufferlist get_mid_buffer() {
      return m_mid_buffer;
    }

    bufferlist get_data_buffer() {
      return m_data_buffer;
    }
};

// currently, just use this interface.
inline ObjectCacheRequest* decode_object_cache_request(
            ObjectCacheMsgHeader* head, bufferlist mid_buffer)
{
  ObjectCacheRequest* req = new ObjectCacheRequest();

  // head
  req->m_head = *head;
  assert(req->m_head.mid_len == mid_buffer.length());

  // mid
  req->m_mid.decode(mid_buffer);

  return req;
}

inline ObjectCacheRequest* decode_object_cache_request(
             ObjectCacheMsgHeader* head, bufferlist& mid_buffer,
             bufferlist& data_buffer)
{
  ObjectCacheRequest* req = decode_object_cache_request(head, mid_buffer);

  // data
  if(data_buffer.length() != 0) {
    req->m_data_buffer = data_buffer;
  }

  return req;
}

inline ObjectCacheRequest* decode_object_cache_request(bufferlist& head,
                bufferlist& mid_buffer, bufferlist& data_buffer)
{
  assert(sizeof(ObjectCacheMsgHeader) == head.length());
  return decode_object_cache_request((ObjectCacheMsgHeader*)(head.c_str()), mid_buffer, data_buffer);
}


} // namespace immutable_obj_cache
} // namespace ceph
#endif
