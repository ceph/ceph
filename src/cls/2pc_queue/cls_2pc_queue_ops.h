// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/ceph_json.h"
#include "include/types.h"
#include "cls_2pc_queue_types.h"

struct cls_2pc_queue_reserve_op {
  uint64_t size;
  uint32_t entries{0};

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("size", size);
    f->dump_unsigned("entries", entries);
  }

  static void generate_test_instances(std::list<cls_2pc_queue_reserve_op*>& ls) {
    ls.push_back(new cls_2pc_queue_reserve_op);
    ls.back()->size = 0;
    ls.push_back(new cls_2pc_queue_reserve_op);
    ls.back()->size = 123;
    ls.back()->entries = 456;
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reserve_op)

struct cls_2pc_queue_reserve_ret {
  cls_2pc_reservation::id_t id; // allocated reservation id

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("id", id);
  }

  static void generate_test_instances(std::list<cls_2pc_queue_reserve_ret*>& ls) {
    ls.push_back(new cls_2pc_queue_reserve_ret);
    ls.back()->id = 123;
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reserve_ret)

struct cls_2pc_queue_commit_op {
  cls_2pc_reservation::id_t id; // reservation to commit
  std::vector<ceph::buffer::list> bl_data_vec; // the data to enqueue

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(bl_data_vec, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(bl_data_vec, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("id", id);
    encode_json("bl_data_vec", bl_data_vec, f);
  }

  static void generate_test_instances(std::list<cls_2pc_queue_commit_op*>& ls) {
    ls.push_back(new cls_2pc_queue_commit_op);
    ls.back()->id = 123;
    ls.back()->bl_data_vec.push_back(ceph::buffer::list());
    ls.back()->bl_data_vec.back().append("foo");
    ls.back()->bl_data_vec.push_back(ceph::buffer::list());
    ls.back()->bl_data_vec.back().append("bar");
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_commit_op)

struct cls_2pc_queue_abort_op {
  cls_2pc_reservation::id_t id; // reservation to abort

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("id", id);
  }
  static void generate_test_instances(std::list<cls_2pc_queue_abort_op*>& ls) {
    ls.push_back(new cls_2pc_queue_abort_op);
    ls.back()->id = 1;
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_abort_op)

struct cls_2pc_queue_expire_op {
  // any reservation older than this time should expire
  ceph::coarse_real_time stale_time;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(stale_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(stale_time, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("stale_time") << stale_time;
  }
  static void generate_test_instances(std::list<cls_2pc_queue_expire_op*>& ls) {
    ls.push_back(new cls_2pc_queue_expire_op);
    ls.push_back(new cls_2pc_queue_expire_op);
    ls.back()->stale_time = ceph::coarse_real_time::min();
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_expire_op)

struct cls_2pc_queue_reservations_ret {
  cls_2pc_reservations reservations; // reservation list (keyed by id)

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(reservations, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(reservations, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("reservations");
    for (const auto& i : reservations) {
      f->open_object_section("reservation");
      f->dump_unsigned("id", i.first);
      i.second.dump(f);
      f->close_section();
    }
    f->close_section();
  }

  static void generate_test_instances(std::list<cls_2pc_queue_reservations_ret*>& ls) {
    ls.push_back(new cls_2pc_queue_reservations_ret);
    ls.push_back(new cls_2pc_queue_reservations_ret);
    ls.back()->reservations[1] = cls_2pc_reservation();
    ls.back()->reservations[2] = cls_2pc_reservation();
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reservations_ret)

struct cls_2pc_queue_remove_op {
  std::string end_marker;
  uint32_t entries_to_remove = 0;

  cls_2pc_queue_remove_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(end_marker, bl);
    encode(entries_to_remove, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(end_marker, bl);
    if (struct_v > 1) {
      decode(entries_to_remove, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_remove_op)