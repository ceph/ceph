// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "SocketCommon.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

ObjectCacheRequest::ObjectCacheRequest() {}
ObjectCacheRequest::ObjectCacheRequest(uint16_t t, uint64_t s)
  : type(t), seq(s) {}
ObjectCacheRequest::~ObjectCacheRequest() {}

void ObjectCacheRequest::encode() {
  ENCODE_START(1, 1, payload);
  ceph::encode(type, payload);
  ceph::encode(seq, payload);
  if (!payload_empty()) {
    encode_payload();
  }
  ENCODE_FINISH(payload);
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
ObjectCacheRegData::ObjectCacheRegData(uint16_t t, uint64_t s,
                                       const std::string &version)
  : ObjectCacheRequest(t, s),
    version(version) {
}

ObjectCacheRegData::~ObjectCacheRegData() {}

void ObjectCacheRegData::encode_payload() {
  ceph::encode(version, payload);
}

void ObjectCacheRegData::decode_payload(bufferlist::const_iterator i) {
  if (i.end()) {
    return;
  }
  ceph::decode(version, i);
}

ObjectCacheRegReplyData::ObjectCacheRegReplyData() {}
ObjectCacheRegReplyData::ObjectCacheRegReplyData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheRegReplyData::~ObjectCacheRegReplyData() {}

void ObjectCacheRegReplyData::encode_payload() {}

void ObjectCacheRegReplyData::decode_payload(bufferlist::const_iterator bl) {}

ObjectCacheReadData::ObjectCacheReadData(uint16_t t, uint64_t s,
                                         uint64_t read_offset,
                                         uint64_t read_len,
                                         uint64_t pool_id, uint64_t snap_id,
                                         std::string oid,
                                         std::string pool_namespace)
  : ObjectCacheRequest(t, s), read_offset(read_offset),
    read_len(read_len), pool_id(pool_id), snap_id(snap_id),
    oid(oid), pool_namespace(pool_namespace)
{}

ObjectCacheReadData::ObjectCacheReadData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheReadData::~ObjectCacheReadData() {}

void ObjectCacheReadData::encode_payload() {
  ceph::encode(read_offset, payload);
  ceph::encode(read_len, payload);
  ceph::encode(pool_id, payload);
  ceph::encode(snap_id, payload);
  ceph::encode(oid, payload);
  ceph::encode(pool_namespace, payload);
}

void ObjectCacheReadData::decode_payload(bufferlist::const_iterator i) {
  ceph::decode(read_offset, i);
  ceph::decode(read_len, i);
  ceph::decode(pool_id, i);
  ceph::decode(snap_id, i);
  ceph::decode(oid, i);
  ceph::decode(pool_namespace, i);
}

ObjectCacheReadReplyData::ObjectCacheReadReplyData(uint16_t t, uint64_t s,
                                                   string cache_path)
  : ObjectCacheRequest(t, s), cache_path(cache_path) {}
ObjectCacheReadReplyData::ObjectCacheReadReplyData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheReadReplyData::~ObjectCacheReadReplyData() {}

void ObjectCacheReadReplyData::encode_payload() {
  ceph::encode(cache_path, payload);
}

void ObjectCacheReadReplyData::decode_payload(bufferlist::const_iterator i) {
  ceph::decode(cache_path, i);
}

ObjectCacheReadRadosData::ObjectCacheReadRadosData() {}
ObjectCacheReadRadosData::ObjectCacheReadRadosData(uint16_t t, uint64_t s)
  : ObjectCacheRequest(t, s) {}

ObjectCacheReadRadosData::~ObjectCacheReadRadosData() {}

void ObjectCacheReadRadosData::encode_payload() {}

void ObjectCacheReadRadosData::decode_payload(bufferlist::const_iterator i) {}

ObjectCacheRequest* decode_object_cache_request(bufferlist payload_buffer) {
  ObjectCacheRequest* req = nullptr;

  uint16_t type;
  uint64_t seq;
  auto i = payload_buffer.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  DECODE_FINISH(i);

  switch (type) {
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

}  // namespace immutable_obj_cache
}  // namespace ceph
