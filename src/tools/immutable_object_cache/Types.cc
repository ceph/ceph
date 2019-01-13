// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::Types: " << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

void ObjectCacheMsgHeader::encode(bufferlist& bl) const {
  using ceph::encode;
  ::encode(seq, bl);
  ::encode(type, bl);
  ::encode(version, bl);
  ::encode(padding, bl);
  ::encode(mid_len, bl);
  ::encode(data_len, bl);
  ::encode(reserved, bl);
}

void ObjectCacheMsgHeader::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  ::decode(seq, it);
  ::decode(type, it);
  ::decode(version, it);
  ::decode(padding, it);
  ::decode(mid_len, it);
  ::decode(data_len, it);
  ::decode(reserved, it);
}

} // namespace immutable_obj_cache
} // namespace ceph
