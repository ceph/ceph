// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "librbd/mirror/snapshot/Types.h"

namespace librbd {
namespace mirror {
namespace snapshot {

void ImageStateHeader::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(object_count, bl);
  ENCODE_FINISH(bl);
}

void ImageStateHeader::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode(object_count, bl);
  DECODE_FINISH(bl);
}

void SnapState::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(id, bl);
  encode(snap_namespace, bl);
  encode(name, bl);
  encode(protection_status, bl);
  ENCODE_FINISH(bl);
}

void SnapState::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode(id, bl);
  decode(snap_namespace, bl);
  decode(name, bl);
  DECODE_FINISH(bl);
}

void SnapState::dump(Formatter *f) const {
  f->dump_unsigned("id", id);
  f->open_object_section("namespace");
  snap_namespace.dump(f);
  f->close_section();
  f->dump_string("name", name);
  f->dump_unsigned("protection_status", protection_status);
}

std::ostream& operator<<(std::ostream& os, const SnapState& snap_state) {
  os << "[" << snap_state.id << " " << snap_state.snap_namespace << " "
     << snap_state.name << " " << snap_state.protection_status << "]";
  return os;
}

void ImageState::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(name, bl);
  encode(features, bl);
  encode(snap_limit, bl);
  encode(snapshots, bl);
  encode(metadata, bl);
  ENCODE_FINISH(bl);
}

void ImageState::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode(name, bl);
  decode(features, bl);
  decode(snap_limit, bl);
  decode(snapshots, bl);
  decode(metadata, bl);
  DECODE_FINISH(bl);
}

void ImageState::dump(Formatter *f) const {
  f->dump_string("name", name);
  f->dump_unsigned("features", features);
  f->dump_unsigned("snap_limit", snap_limit);
  f->open_array_section("snapshots");
  for (auto &[id, snap_state] : snapshots) {
    f->open_object_section(stringify(id).c_str());
    snap_state.dump(f);
    f->close_section(); // snap_state
  }
  f->close_section(); // snapshots
  f->open_object_section("metadata");
  for (auto &it : metadata) {
    f->dump_stream(it.first.c_str()) << it.second;
  }
  f->close_section(); // metadata
}

std::ostream& operator<<(std::ostream& os, const ImageState& image_state) {
  os << "[" << image_state.name << " " << image_state.features << " "
     << image_state.snap_limit << " " << image_state.snapshots.size()
     << " " << image_state.metadata.size() << "]";
  return os;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd
