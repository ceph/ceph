// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "include/types.h"

struct cls_2pc_reservation
{
  using id_t = uint32_t;
  inline static const id_t NO_ID{0};
  uint64_t size = 0;                 // how much size to reserve (bytes)
  ceph::coarse_real_time timestamp;  // when the reservation was done (used for cleaning stale reservations)
  uint32_t entries = 0;              // how many entries are reserved

  cls_2pc_reservation(uint64_t _size, ceph::coarse_real_time _timestamp, uint32_t _entries) :
      size(_size), timestamp(_timestamp), entries(_entries) {}

  cls_2pc_reservation() = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(size, bl);
    encode(timestamp, bl);
    encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(size, bl);
    decode(timestamp, bl);
    if (struct_v >= 2) {
      decode(entries, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("size", size);
    f->dump_stream("timestamp") << timestamp;
  }

  static void generate_test_instances(std::list<cls_2pc_reservation*>& ls) {
    ls.push_back(new cls_2pc_reservation);
    ls.back()->size = 0;
    ls.push_back(new cls_2pc_reservation);
    ls.back()->size = 123;
    ls.back()->timestamp = ceph::coarse_real_clock::zero();
  }
};
WRITE_CLASS_ENCODER(cls_2pc_reservation)

using cls_2pc_reservations = ceph::unordered_map<cls_2pc_reservation::id_t, cls_2pc_reservation>;

struct cls_2pc_urgent_data
{
  uint64_t reserved_size{0};   // pending reservations size in bytes
  cls_2pc_reservation::id_t last_id{cls_2pc_reservation::NO_ID}; // last allocated id
  cls_2pc_reservations reservations; // reservation list (keyed by id)
  bool has_xattrs{false};
  uint32_t committed_entries{0}; // how many entries have been committed so far

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(reserved_size, bl);
    encode(last_id, bl);
    encode(reservations, bl);
    encode(has_xattrs, bl);
    encode(committed_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(reserved_size, bl);
    decode(last_id, bl);
    decode(reservations, bl);
    decode(has_xattrs, bl);
    if (struct_v >= 2) {
      decode(committed_entries, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("reserved_size", reserved_size);
    f->dump_unsigned("last_id", last_id);
    f->open_array_section("reservations");
    for (const auto& [id, res] : reservations) {
      f->open_object_section("reservation");
      f->dump_unsigned("id", id);
      res.dump(f);
      f->close_section();
    }
    f->close_section();
    f->dump_bool("has_xattrs", has_xattrs);
  }

  static void generate_test_instances(std::list<cls_2pc_urgent_data*>& ls) {
    ls.push_back(new cls_2pc_urgent_data);
    ls.push_back(new cls_2pc_urgent_data);
    ls.back()->reserved_size = 123;
    ls.back()->last_id = 456;
    ls.back()->reservations.emplace(789, cls_2pc_reservation(1, ceph::coarse_real_clock::zero(), 2));
    ls.back()->has_xattrs = true;
  }
};
WRITE_CLASS_ENCODER(cls_2pc_urgent_data)
