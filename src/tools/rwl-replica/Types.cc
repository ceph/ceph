#include "Types.h"
#include "include/rados/librados.hpp"

namespace librbd::cache::pwl::rwl::replica {

void RwlCacheInfo::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(cache_id, bl);
  encode(cache_size, bl);
  encode(pool_name, bl);
  encode(image_name, bl);
  ENCODE_FINISH(bl);
}

void RwlCacheInfo::decode(bufferlist::const_iterator &it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(cache_id, it);
  decode(cache_size, it);
  decode(pool_name, it);
  decode(image_name, it);
  DECODE_FINISH(it);
}

void RpmaConfigDescriptor::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(mr_desc_size, bl);
  encode(pcfg_desc_size, bl);
  encode(descriptors, bl);
  ENCODE_FINISH(bl);
}

void RpmaConfigDescriptor::decode(bufferlist::const_iterator &it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(mr_desc_size, it);
  decode(pcfg_desc_size, it);
  decode(descriptors, it);
  DECODE_FINISH(it);
}

void RwlReplicaRequest::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(type, bl);
  ENCODE_FINISH(bl);
}

void RwlReplicaRequest::decode(bufferlist::const_iterator &it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(type, it);
  DECODE_FINISH(it);
}

void RwlReplicaInitRequest::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(type, bl);
  encode(info, bl);
  ENCODE_FINISH(bl);
}

void RwlReplicaInitRequest::decode(bufferlist::const_iterator &it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(type, it);
  decode(info, it);
  DECODE_FINISH(it);
}

void RwlReplicaInitRequestReply::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(type, bl);
  encode(desc, bl);
  ENCODE_FINISH(bl);
}

void RwlReplicaInitRequestReply::decode(bufferlist::const_iterator &it) {
  using ceph::decode;
  DECODE_START(1, it);
  decode(type, it);
  decode(desc, it);
  DECODE_FINISH(it);
}

} //namespace ceph::librbd::cache::pwl::rwl::replica