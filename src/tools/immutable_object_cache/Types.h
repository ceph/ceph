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

    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& it);
};

class ObjectCacheMsgData {
public:
  uint64_t m_read_offset;
  uint64_t m_read_len;
  uint64_t m_pool_id;
  uint64_t m_snap_id;
  std::string m_oid;
  std::string m_pool_namespace;
  std::string m_cache_path;

  void encode(bufferlist& bl);
  void decode(bufferlist& bl);
};

class ObjectCacheRequest {
public:
    ObjectCacheMsgHeader m_head;
    ObjectCacheMsgData m_data;
    bufferlist m_head_buffer;
    bufferlist m_data_buffer;
    GenContext<ObjectCacheRequest*>* m_process_msg;

    void encode();
    bufferlist get_head_buffer();
    bufferlist get_data_buffer();

};

ObjectCacheRequest* decode_object_cache_request(
            ObjectCacheMsgHeader* head, bufferlist data_buffer);

ObjectCacheRequest* decode_object_cache_request(
             bufferlist head_buffer, bufferlist data_buffer);

} // namespace immutable_obj_cache
} // namespace ceph
#endif
