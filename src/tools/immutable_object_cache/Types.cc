// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "SocketCommon.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

void ObjectCacheRegData::encode(bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  ceph::encode(type, bl);
  ceph::encode(seq, bl);
  ENCODE_FINISH(bl);
}

void ObjectCacheRegData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  DECODE_FINISH(i);
}

void ObjectCacheRegReplyData::encode(bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  ceph::encode(type, bl);
  ceph::encode(seq, bl);
  ENCODE_FINISH(bl);
}

void ObjectCacheRegReplyData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  DECODE_FINISH(i);
}

void ObjectCacheReadData::encode(bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  ceph::encode(type, bl);
  ceph::encode(seq, bl);
  ceph::encode(m_read_offset, bl);
  ceph::encode(m_read_len, bl);
  ceph::encode(m_pool_id, bl);
  ceph::encode(m_snap_id, bl);
  ceph::encode(m_oid, bl);
  ceph::encode(m_pool_namespace, bl);
  ENCODE_FINISH(bl);
}

void ObjectCacheReadData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  ceph::decode(m_read_offset, i);
  ceph::decode(m_read_len, i);
  ceph::decode(m_pool_id, i);
  ceph::decode(m_snap_id, i);
  ceph::decode(m_oid, i);
  ceph::decode(m_pool_namespace, i);
  DECODE_FINISH(i);
}

void ObjectCacheReadReplyData::encode(bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  ceph::encode(type, bl);
  ceph::encode(seq, bl);
  ceph::encode(m_cache_path, bl);
  ENCODE_FINISH(bl);
}

void ObjectCacheReadReplyData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  ceph::decode(m_cache_path, i);
  DECODE_FINISH(i);
}

void ObjectCacheReadRadosData::encode(bufferlist& bl) {
  ENCODE_START(1, 1, bl);
  ceph::encode(type, bl);
  ceph::encode(seq, bl);
  ENCODE_FINISH(bl);
}

void ObjectCacheReadRadosData::decode(bufferlist& bl) {
  auto i = bl.cbegin();
  DECODE_START(1, i);
  ceph::decode(type, i);
  ceph::decode(seq, i);
  DECODE_FINISH(i);
}

uint8_t get_header_size() {
  return 6; //uint8_t + uint8_t + uint32_t
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

uint16_t get_data_type(bufferlist buf) {
  uint16_t type;
  auto i = buf.cbegin();
  DECODE_START(1, i);
  decode(type, i);
  DECODE_FINISH(i);
  return type;
}

bufferlist ObjectCacheRequest::get_data_buffer() {
  return m_data_buffer;
}

ObjectCacheRequest* encode_object_cache_request(void* m_data, uint16_t type) {
  ObjectCacheRequest* req = new ObjectCacheRequest();

  switch(type) {
    case RBDSC_REGISTER: {
      ObjectCacheRegData* data = (ObjectCacheRegData*)m_data;
      data->encode(req->m_data_buffer);
      break;
    }
    case RBDSC_REGISTER_REPLY: {
      ObjectCacheRegReplyData* data = (ObjectCacheRegReplyData*)m_data;
      data->encode(req->m_data_buffer);
      break;
    }
    case RBDSC_READ: {
      ObjectCacheReadData* data = (ObjectCacheReadData*)m_data;
      data->encode(req->m_data_buffer);
      break;
    }
    case RBDSC_READ_RADOS: {
      ObjectCacheReadRadosData* data = (ObjectCacheReadRadosData*)m_data;
      data->encode(req->m_data_buffer);
      break;
    }
    case RBDSC_READ_REPLY: {
      ObjectCacheReadReplyData* data = (ObjectCacheReadReplyData*)m_data;
      data->encode(req->m_data_buffer);
      break;
    }
    default:
      ceph_assert(0);
  }

  req->type = type;
  return req;
}

ObjectCacheRequest* decode_object_cache_request(bufferlist data_buffer) {
  ObjectCacheRequest* req = new ObjectCacheRequest();
  uint16_t type = get_data_type(data_buffer);
  uint64_t seq;

  switch(type) {
    case RBDSC_REGISTER: {
      ObjectCacheRegData* data = new ObjectCacheRegData();
      data->decode(data_buffer);
      seq = data->seq;
      req->m_data = data;
      break;
    }
    case RBDSC_READ: {
      ObjectCacheReadData* data = new ObjectCacheReadData();
      data->decode(data_buffer);
      seq = data->seq;
      req->m_data = data;
      break;
    }
    case RBDSC_REGISTER_REPLY: {
      ObjectCacheRegReplyData* data = new ObjectCacheRegReplyData();
      data->decode(data_buffer);
      seq = data->seq;
      req->m_data = data;
      break;
    }
    case RBDSC_READ_REPLY: {
      ObjectCacheReadReplyData* data = new ObjectCacheReadReplyData();
      data->decode(data_buffer);
      seq = data->seq;
      req->m_data = data;
      break;
    }
    case RBDSC_READ_RADOS: {
      ObjectCacheReadRadosData* data = new ObjectCacheReadRadosData();
      data->decode(data_buffer);
      seq = data->seq;
      req->m_data = data;
      break;
    }
    default:
      ceph_assert(0);
  }

  req->type = type;
  req->seq = seq;
  return req;
}

} // namespace immutable_obj_cache
} // namespace ceph
