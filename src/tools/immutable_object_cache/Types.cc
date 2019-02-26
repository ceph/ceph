// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "SocketCommon.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

ObjectCacheRequest::ObjectCacheRequest(){}
ObjectCacheRequest::ObjectCacheRequest(uint16_t t, uint64_t s)
  : type(t), seq(s) {}
ObjectCacheRequest::~ObjectCacheRequest(){}

void ObjectCacheRequest::encode() {
  ENCODE_START(1, 1, m_payload);
  ceph::encode(type, m_payload);
  ceph::encode(seq, m_payload);
  if (!payload_empty()) {
    encode_payload();
  }
  ENCODE_FINISH(m_payload);
}

void ObjectCacheRequest::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  if (!payload_empty()) {
    decode_payload(i);
  }
  DECODE_FINISH(i);
}

ObjectCacheRegData::ObjectCacheRegData() {}
ObjectCacheRegData::ObjectCacheRegData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheRegData::~ObjectCacheRegData() {}

void ObjectCacheRegData::encode_payload() {}

void ObjectCacheRegData::decode_payload(bufferlist::const_iterator i) {}

ObjectCacheRegReplyData::ObjectCacheRegReplyData() {}
ObjectCacheRegReplyData::ObjectCacheRegReplyData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheRegReplyData::~ObjectCacheRegReplyData() {}

void ObjectCacheRegReplyData::encode_payload() {}

void ObjectCacheRegReplyData::decode_payload(bufferlist::const_iterator bl) {}

ObjectCacheReadData::ObjectCacheReadData(uint16_t t, uint64_t s,
                                         uint64_t read_offset, uint64_t read_len,
                                         uint64_t pool_id, uint64_t snap_id,
                                         std::string oid, std::string pool_namespace)
  : ObjectCacheRequest(t, s), m_read_offset(read_offset),
    m_read_len(read_len), m_pool_id(pool_id), m_snap_id(snap_id),
    m_oid(oid), m_pool_namespace(pool_namespace)
{}

ObjectCacheReadData::ObjectCacheReadData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheReadData::~ObjectCacheReadData() {}

void ObjectCacheReadData::encode_payload() {
  ceph::encode(m_read_offset, m_payload);
  ceph::encode(m_read_len, m_payload);
  ceph::encode(m_pool_id, m_payload);
  ceph::encode(m_snap_id, m_payload);
  ceph::encode(m_oid, m_payload);
  ceph::encode(m_pool_namespace, m_payload);
}

void ObjectCacheReadData::decode_payload(bufferlist::const_iterator i) {
  ceph::decode(m_read_offset, i);
  ceph::decode(m_read_len, i);
  ceph::decode(m_pool_id, i);
  ceph::decode(m_snap_id, i);
  ceph::decode(m_oid, i);
  ceph::decode(m_pool_namespace, i);
}

ObjectCacheReadReplyData::ObjectCacheReadReplyData(uint16_t t, uint64_t s, string cache_path)
  : ObjectCacheRequest(t, s), m_cache_path(cache_path) {}
ObjectCacheReadReplyData::ObjectCacheReadReplyData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheReadReplyData::~ObjectCacheReadReplyData() {}

void ObjectCacheReadReplyData::encode_payload() {
  ceph::encode(m_cache_path, m_payload);
}

void ObjectCacheReadReplyData::decode_payload(bufferlist::const_iterator i) {
  ceph::decode(m_cache_path, i);
}

ObjectCacheReadRadosData::ObjectCacheReadRadosData() {}
ObjectCacheReadRadosData::ObjectCacheReadRadosData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheReadRadosData::~ObjectCacheReadRadosData() {}

void ObjectCacheReadRadosData::encode_payload() {}

void ObjectCacheReadRadosData::decode_payload(bufferlist::const_iterator i) {}

ObjectCacheRequest* decode_object_cache_request(bufferlist payload_buffer) 
{
  ObjectCacheRequest* req = nullptr;

  uint16_t type;
  uint64_t seq;
  auto i = payload_buffer.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  DECODE_FINISH(i);

  switch(type) {
    case RBDSC_REGISTER: {
      req = new ObjectCacheRegData(type, seq);
      break;
    }
    case RBDSC_READ: {
      req = new ObjectCacheReadData(type, seq);
      break;
    }
    case RBDSC_REGISTER_REPLY: {
      req = new ObjectCacheRegReplyData(type, seq);
      break;
    }
    case RBDSC_READ_REPLY: {
      req = new ObjectCacheReadReplyData(type, seq);
      break;
    }
    case RBDSC_READ_RADOS: {
      req = new ObjectCacheReadRadosData(type, seq);
      break;
    }
    default:
      ceph_assert(0);
  }

  req->decode(payload_buffer);

  return req;
}

} // namespace immutable_obj_cache
} // namespace ceph
