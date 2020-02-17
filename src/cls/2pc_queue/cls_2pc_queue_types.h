#pragma once

#include "include/types.h"

struct cls_2pc_reservation
{
  using id_t = uint32_t;
  inline static const id_t NO_ID{0};
  uint64_t size;        // how many entries are reserved
  ceph::real_time timestamp;  // when the reservation was done (used for cleaning stale reservations)

  cls_2pc_reservation(uint64_t _size, ceph::real_time _timestamp) :
      size(_size), timestamp(_timestamp) {}

  cls_2pc_reservation() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(timestamp, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_reservation)

using cls_2pc_reservations = ceph::unordered_map<cls_2pc_reservation::id_t, cls_2pc_reservation>;

struct cls_2pc_urgent_data
{
  uint64_t reserved_size{0};   // pending reservations size in bytes
  cls_2pc_reservation::id_t last_id{cls_2pc_reservation::NO_ID}; // last allocated id
  cls_2pc_reservations reservations; // reservation list (keyed by id)

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(reserved_size, bl);
    encode(last_id, bl);
    encode(reservations, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(reserved_size, bl);
    decode(last_id, bl);
    decode(reservations, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_2pc_urgent_data)

