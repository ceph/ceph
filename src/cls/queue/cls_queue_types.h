// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_QUEUE_TYPES_H
#define CEPH_CLS_QUEUE_TYPES_H

#include <errno.h>
#include "include/types.h"

//Size of head leaving out urgent data
#define QUEUE_HEAD_SIZE_1K 1024

#define QUEUE_START_OFFSET_1K QUEUE_HEAD_SIZE_1K

constexpr unsigned int QUEUE_HEAD_START = 0xDEAD;
constexpr unsigned int QUEUE_ENTRY_START = 0xBEEF;
constexpr unsigned int QUEUE_ENTRY_OVERHEAD = sizeof(uint16_t) + sizeof(uint64_t);

struct cls_queue_entry
{
  ceph::buffer::list data;
  std::string marker;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(data, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(data, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("marker", marker);
    f->dump_unsigned("data_len", data.length());
  }
  static void generate_test_instances(std::list<cls_queue_entry*>& o) {
    o.push_back(new cls_queue_entry);
    o.push_back(new cls_queue_entry);
    o.back()->data.append(std::string_view("data"));
    o.back()->marker = "marker";
  }
};
WRITE_CLASS_ENCODER(cls_queue_entry)

struct cls_queue_marker
{
  uint64_t offset{0};
  uint64_t gen{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(gen, bl);
    encode(offset, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(gen, bl);
    decode(offset, bl);
    DECODE_FINISH(bl);
  }

  std::string to_str() {
    return std::to_string(gen) + '/' + std::to_string(offset);
  }

  int from_str(const char* str) {
    errno = 0;
    char* end = nullptr;
    gen = ::strtoull(str, &end, 10);
    if (errno) {
      return errno;
    }
    if (str == end || *end != '/') { // expects delimiter
      return -EINVAL;
    }
    str = end + 1;
    offset = ::strtoull(str, &end, 10);
    if (errno) {
      return errno;
    }
    if (str == end || *end != 0) { // expects null terminator
      return -EINVAL;
    }
    return 0;
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("offset", offset);
    f->dump_unsigned("gen", gen);
  }
  static void generate_test_instances(std::list<cls_queue_marker*>& o) {
    o.push_back(new cls_queue_marker);
    o.push_back(new cls_queue_marker);
    o.back()->offset = 1024;
    o.back()->gen = 0;
  }
};
WRITE_CLASS_ENCODER(cls_queue_marker)

struct cls_queue_head
{
  uint64_t max_head_size = QUEUE_HEAD_SIZE_1K;
  cls_queue_marker front{QUEUE_START_OFFSET_1K, 0};
  cls_queue_marker tail{QUEUE_START_OFFSET_1K, 0};
  uint64_t queue_size{0}; // size of queue requested by user, with head size added to it
  uint64_t max_urgent_data_size{0};
  ceph::buffer::list bl_urgent_data;  // special data known to application using queue

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_head_size, bl);
    encode(front, bl);
    encode(tail, bl);
    encode(queue_size, bl);
    encode(max_urgent_data_size, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_head_size, bl);
    decode(front, bl);
    decode(tail, bl);
    decode(queue_size, bl);
    decode(max_urgent_data_size, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("max_head_size", max_head_size);
    f->dump_unsigned("queue_size", queue_size);
    f->dump_unsigned("max_urgent_data_size", max_urgent_data_size);
    f->dump_unsigned("front_offset", front.offset);
    f->dump_unsigned("front_gen", front.gen);
    f->dump_unsigned("tail_offset", tail.offset);
    f->dump_unsigned("tail_gen", tail.gen);
  }
  static void generate_test_instances(std::list<cls_queue_head*>& o) {
    o.push_back(new cls_queue_head);
    o.push_back(new cls_queue_head);
    o.back()->max_head_size = 1024;
    o.back()->front.offset = 1024;
    o.back()->front.gen = 0;
    o.back()->tail.offset = 1024;
    o.back()->tail.gen = 0;
    o.back()->queue_size = 1024;
    o.back()->max_urgent_data_size = 0;
  }
};
WRITE_CLASS_ENCODER(cls_queue_head)

#endif
