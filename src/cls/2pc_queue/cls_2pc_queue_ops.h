// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include "cls_2pc_queue_types.h"

struct cls_2pc_queue_reserve_op {
  uint64_t size;
  uint32_t entries;

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
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reservations_ret)

struct cls_2pc_queue_remove_op {
  std::string end_marker;
  uint32_t entries_to_remove;

  cls_2pc_queue_remove_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(end_marker, bl);
    encode(entries_to_remove, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(end_marker, bl);
    decode(entries_to_remove, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_remove_op)