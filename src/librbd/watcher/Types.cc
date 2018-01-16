// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/Types.h"
#include "common/Formatter.h"

namespace librbd {
namespace watcher {

void ClientId::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(gid, bl);
  encode(handle, bl);
}

void ClientId::decode(bufferlist::iterator &iter) {
  using ceph::decode;
  decode(gid, iter);
  decode(handle, iter);
}

void ClientId::dump(Formatter *f) const {
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("handle", handle);
}

void NotifyResponse::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(acks, bl);
  encode(timeouts, bl);
}

void NotifyResponse::decode(bufferlist::iterator& iter) {
  using ceph::decode;
  decode(acks, iter);
  decode(timeouts, iter);
}
std::ostream &operator<<(std::ostream &out,
                         const ClientId &client_id) {
  out << "[" << client_id.gid << "," << client_id.handle << "]";
  return out;
}

} // namespace watcher
} // namespace librbd
