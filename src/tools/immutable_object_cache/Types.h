// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_TYPES_H
#define CEPH_CACHE_TYPES_H

#include "include/encoding.h"
#include "include/Context.h"
#include "SocketCommon.h"

namespace ceph {
namespace immutable_obj_cache {

namespace {
struct HeaderHelper {
  uint8_t v;
  uint8_t c_v;
  uint32_t len;
}__attribute__((packed));

inline uint8_t get_header_size() {
  return sizeof(HeaderHelper);
}

inline uint32_t get_data_len(char* buf) {
  HeaderHelper* header = reinterpret_cast<HeaderHelper*>(buf);
  return header->len;
}
}  //  namespace

class ObjectCacheRequest {
 public:
  uint16_t type;
  uint64_t seq;

  bufferlist payload;

  GenContext<ObjectCacheRequest*>* process_msg;

  ObjectCacheRequest();
  ObjectCacheRequest(uint16_t type, uint64_t seq);
  virtual ~ObjectCacheRequest();

  // encode consists of two steps
  // step 1 : directly encode common bits using encode method of base classs.
  // step 2 : according to payload_empty, determine whether addtional bits
  //          need to be encoded which be implements by child class.
  void encode();
  void decode(bufferlist& bl);
  bufferlist get_payload_bufferlist() { return payload; }

  virtual void encode_payload() = 0;
  virtual void decode_payload(bufferlist::const_iterator bl_it) = 0;
  virtual uint16_t get_request_type() = 0;
  virtual bool payload_empty() = 0;
};

class ObjectCacheRegData : public ObjectCacheRequest {
 public:
  ObjectCacheRegData();
  ObjectCacheRegData(uint16_t t, uint64_t s);
  ~ObjectCacheRegData() override;
  void encode_payload() override;
  void decode_payload(bufferlist::const_iterator bl) override;
  uint16_t get_request_type() override { return RBDSC_REGISTER; }
  bool payload_empty() override { return true; }
};

class ObjectCacheRegReplyData : public ObjectCacheRequest {
 public:
  ObjectCacheRegReplyData();
  ObjectCacheRegReplyData(uint16_t t, uint64_t s);
  ~ObjectCacheRegReplyData() override;
  void encode_payload() override;
  void decode_payload(bufferlist::const_iterator iter) override;
  uint16_t get_request_type() override { return RBDSC_REGISTER_REPLY; }
  bool payload_empty() override { return true; }
};

class ObjectCacheReadData : public ObjectCacheRequest {
 public:
  uint64_t read_offset;
  uint64_t read_len;
  uint64_t pool_id;
  uint64_t snap_id;
  std::string oid;
  std::string pool_namespace;
  ObjectCacheReadData(uint16_t t, uint64_t s, uint64_t read_offset,
                      uint64_t read_len, uint64_t pool_id,
                      uint64_t snap_id, std::string oid,
                      std::string pool_namespace);
  ObjectCacheReadData(uint16_t t, uint64_t s);
  ~ObjectCacheReadData() override;
  void encode_payload() override;
  void decode_payload(bufferlist::const_iterator bl) override;
  uint16_t get_request_type() override { return RBDSC_READ; }
  bool payload_empty() override { return false; }
};

class ObjectCacheReadReplyData : public ObjectCacheRequest {
 public:
  std::string cache_path;
  ObjectCacheReadReplyData(uint16_t t, uint64_t s, std::string cache_path);
  ObjectCacheReadReplyData(uint16_t t, uint64_t s);
  ~ObjectCacheReadReplyData() override;
  void encode_payload() override;
  void decode_payload(bufferlist::const_iterator bl) override;
  uint16_t get_request_type() override { return RBDSC_READ_REPLY; }
  bool payload_empty() override { return false; }
};

class ObjectCacheReadRadosData : public ObjectCacheRequest {
 public:
  ObjectCacheReadRadosData();
  ObjectCacheReadRadosData(uint16_t t, uint64_t s);
  ~ObjectCacheReadRadosData() override;
  void encode_payload() override;
  void decode_payload(bufferlist::const_iterator bl) override;
  uint16_t get_request_type() override { return RBDSC_READ_RADOS; }
  bool payload_empty() override { return true; }
};

ObjectCacheRequest* decode_object_cache_request(bufferlist payload_buffer);

}  // namespace immutable_obj_cache
}  // namespace ceph
#endif  // CEPH_CACHE_TYPES_H
