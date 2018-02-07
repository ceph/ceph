// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/Types.h"
#include "include/buffer_fwd.h"

namespace librbd {
namespace cache {
namespace file {

namespace meta_store {

void Header::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ceph::encode(journal_sequence, bl);
  ENCODE_FINISH(bl);
}

void Header::decode(bufferlist::iterator& it) {
  DECODE_START(1, it);
  ceph::decode(journal_sequence, it);
  DECODE_FINISH(it);
}

void Header::dump(Formatter *f) const {
  // TODO
}

void Header::generate_test_instances(std::list<Header *> &o) {
  // TODO
}

} // namespace meta_store

namespace journal_store {

void Event::encode_fields(bufferlist& bl) const {
  uint8_t *fields = reinterpret_cast<uint8_t*>(&fields);
  ceph::encode(*fields, bl);
}

void Event::encode(bufferlist& bl) const {
  size_t start_offset = bl.end().get_off();
  ENCODE_START(1, 1, bl);
  ceph::encode(tid, bl);
  ceph::encode(block, bl);
  ceph::encode(crc, bl);
  assert(bl.length() - start_offset == ENCODED_FIELDS_OFFSET);
  encode_fields(bl);
  ENCODE_FINISH(bl);
  assert(bl.length() - start_offset <= ENCODED_SIZE);
}

void Event::decode(bufferlist::iterator& it) {
  DECODE_START(1, it);
  ceph::decode(tid, it);
  ceph::decode(block, it);
  ceph::decode(crc, it);
  uint8_t byte_fields;
  ceph::decode(byte_fields, it);
  reinterpret_cast<uint8_t&>(fields) = byte_fields;
  DECODE_FINISH(it);
}

void Event::dump(Formatter *f) const {
  // TODO
}

void Event::generate_test_instances(std::list<Event *> &o) {
  // TODO
}

void EventBlock::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ceph::encode(sequence, bl);

  // TODO
  ENCODE_FINISH(bl);
}

void EventBlock::decode(bufferlist::iterator& it) {
  DECODE_START(1, it);
  ceph::decode(sequence, it);

  // TODO
  DECODE_FINISH(it);
}

void EventBlock::dump(Formatter *f) const {
  // TODO
}

void EventBlock::generate_test_instances(std::list<EventBlock *> &o) {
  // TODO
}

} // namespace journal_store

} // namespace file
} // namespace cache
} // namespace librbd
