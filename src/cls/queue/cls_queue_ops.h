// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_QUEUE_OPS_H
#define CEPH_CLS_QUEUE_OPS_H

#include "common/ceph_json.h"
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

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("queue_size", queue_size);
    f->dump_unsigned("max_urgent_data_size", max_urgent_data_size);
    f->dump_unsigned("urgent_data_len", bl_urgent_data.length());
  }

  static void generate_test_instances(std::list<cls_queue_init_op*>& o) {
    o.push_back(new cls_queue_init_op);
    o.push_back(new cls_queue_init_op);
    o.back()->queue_size = 1024;
    o.back()->max_urgent_data_size = 1024;
    o.back()->bl_urgent_data.append(std::string_view("data"));
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

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("data_vec_len", bl_data_vec.size());
  }

  static void generate_test_instances(std::list<cls_queue_enqueue_op*>& o) {
    o.push_back(new cls_queue_enqueue_op);
    o.push_back(new cls_queue_enqueue_op);
    o.back()->bl_data_vec.push_back(ceph::buffer::list());
    o.back()->bl_data_vec.back().append(std::string_view("data"));
  }
};
WRITE_CLASS_ENCODER(cls_queue_enqueue_op)

struct cls_queue_list_op {
  uint64_t max{0};
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

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("max", max);
    f->dump_string("start_marker", start_marker);
  }

  static void generate_test_instances(std::list<cls_queue_list_op*>& o) {
    o.push_back(new cls_queue_list_op);
    o.push_back(new cls_queue_list_op);
    o.back()->max = 123;
    o.back()->start_marker = "foo";
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

  void dump(ceph::Formatter *f) const {
    f->dump_bool("is_truncated", is_truncated);
    f->dump_string("next_marker", next_marker);
    encode_json("entries", entries, f);
  }

  static void generate_test_instances(std::list<cls_queue_list_ret*>& o) {
    o.push_back(new cls_queue_list_ret);
    o.back()->is_truncated = true;
    o.back()->next_marker = "foo";
    o.back()->entries.push_back(cls_queue_entry());
    o.back()->entries.push_back(cls_queue_entry());
    o.back()->entries.back().marker = "id";
    o.back()->entries.back().data.append(std::string_view("data"));
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

  void dump(ceph::Formatter *f) const {
    f->dump_string("end_marker", end_marker);
  }
  static void generate_test_instances(std::list<cls_queue_remove_op*>& o) {
    o.push_back(new cls_queue_remove_op);
    o.push_back(new cls_queue_remove_op);
    o.back()->end_marker = "foo";
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

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("queue_capacity", queue_capacity);
  }
  static void generate_test_instances(std::list<cls_queue_get_capacity_ret*>& o) {
    o.push_back(new cls_queue_get_capacity_ret);
    o.back()->queue_capacity = 123;
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
