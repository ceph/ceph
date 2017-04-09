// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "common/Formatter.h"

namespace rbd {
namespace mirror {
namespace instance_watcher {

void PeerImageId::encode(bufferlist &bl) const {
  ::encode(mirror_uuid, bl);
  ::encode(image_id, bl);
}

void PeerImageId::decode(bufferlist::iterator &iter) {
  ::decode(mirror_uuid, iter);
  ::decode(image_id, iter);
}

void PeerImageId::dump(Formatter *f) const {
  f->dump_string("mirror_uuid", mirror_uuid);
  f->dump_string("image_id", image_id);
}

} // namespace instance_watcher
} // namespace mirror
} // namespace rbd
