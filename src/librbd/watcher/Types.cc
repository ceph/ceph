// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/Types.h"
#include "common/Formatter.h"

namespace librbd {
namespace watcher {

void ClientId::encode(bufferlist &bl) const {
  ::encode(gid, bl);
  ::encode(handle, bl);
}

void ClientId::decode(bufferlist::iterator &iter) {
  ::decode(gid, iter);
  ::decode(handle, iter);
}

void ClientId::dump(Formatter *f) const {
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("handle", handle);
}

WRITE_CLASS_ENCODER(ClientId);

void NotifyResponse::encode(bufferlist& bl) const {
  ::encode(acks, bl);
  ::encode(timeouts, bl);
}

void NotifyResponse::decode(bufferlist::iterator& iter) {
  ::decode(acks, iter);
  ::decode(timeouts, iter);
}

} // namespace watcher
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::watcher::ClientId &client_id) {
  out << "[" << client_id.gid << "," << client_id.handle << "]";
  return out;
}
