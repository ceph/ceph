// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_QUEUE_OPS_H
#define CEPH_CLS_QUEUE_OPS_H

#include "cls/queue/cls_queue_types.h"

struct cls_queue_init_op {
  uint64_t queue_size{0};
  uint64_t max_urgent_data_size{0};
  ceph::buffer::list bl_urgent_data;

  cls_queue_init_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(queue_size, bl);
    encode(max_urgent_data_size, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(queue_size, bl);
    decode(max_urgent_data_size, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_queue_init_op)

struct cls_queue_enqueue_op {
  std::vector<ceph::buffer::list> bl_data_vec;

  cls_queue_enqueue_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data_vec, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data_vec, bl);
    DECODE_FINISH(bl);
  } 
};
WRITE_CLASS_ENCODER(cls_queue_enqueue_op)

struct cls_queue_list_op {
  uint64_t max;
  std::string start_marker;
  std::string end_marker;

  cls_queue_list_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(max, bl);
    encode(start_marker, bl);
    encode(end_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(max, bl);
    decode(start_marker, bl);
    if (struct_v > 1) {
      decode(end_marker, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_op)

struct cls_queue_list_ret {
  bool is_truncated;
  std::string next_marker;
  std::vector<cls_queue_entry> entries;

  cls_queue_list_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(is_truncated, bl);
    encode(next_marker, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(is_truncated, bl);
    decode(next_marker, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_ret)

struct cls_queue_remove_op {
  std::string end_marker;

  cls_queue_remove_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(end_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(end_marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_remove_op)

struct cls_queue_get_capacity_ret {
  uint64_t queue_capacity;

  cls_queue_get_capacity_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(queue_capacity, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(queue_capacity, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_get_capacity_ret)

struct cls_queue_get_stats_ret {
  uint64_t queue_size;
  uint32_t queue_entries;

  cls_queue_get_stats_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(queue_size, bl);
    encode(queue_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(queue_size, bl);
    decode(queue_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_get_stats_ret)

#endif /* CEPH_CLS_QUEUE_OPS_H */
