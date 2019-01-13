// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

// TODO : fix compile issue
/*
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
*/

} // namespace immutable_obj_cache
} // namespace ceph
