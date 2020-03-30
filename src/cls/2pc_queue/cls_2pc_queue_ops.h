#pragma once

#include "include/types.h"
#include "cls_2pc_queue_types.h"

struct cls_2pc_queue_reserve_op {
  uint64_t size;
  uint32_t entries;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reserve_op)

struct cls_2pc_queue_reserve_ret {
  cls_2pc_reservation::id_t id; // allocated reservation id

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reserve_ret)

struct cls_2pc_queue_commit_op {
  cls_2pc_reservation::id_t id; // reservation to commit
  std::vector<bufferlist> bl_data_vec; // the data to enqueue

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(bl_data_vec, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(bl_data_vec, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_2pc_queue_commit_op)

struct cls_2pc_queue_abort_op {
  cls_2pc_reservation::id_t id; // reservation to abort

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_abort_op)

struct cls_2pc_queue_reservations_ret {
  cls_2pc_reservations reservations; // reservation list (keyed by id)

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(reservations, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(reservations, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_queue_reservations_ret)

