// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "cls/rgw/cls_rgw_types.h"

struct cls_rgw_gc_queue_init_op {
  uint64_t size;
  uint64_t num_deferred_entries{0};

  cls_rgw_gc_queue_init_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(num_deferred_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(num_deferred_entries, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("size", size);
    f->dump_unsigned("num_deferred_entries", num_deferred_entries);
  }

  static void generate_test_instances(std::list<cls_rgw_gc_queue_init_op*>& o) {
    o.push_back(new cls_rgw_gc_queue_init_op);
    o.back()->size = 1024;
    o.back()->num_deferred_entries = 512;
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_init_op)

struct cls_rgw_gc_queue_remove_entries_op {
  uint64_t num_entries;

  cls_rgw_gc_queue_remove_entries_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(num_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(num_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_remove_entries_op)

struct cls_rgw_gc_queue_defer_entry_op {
  uint32_t expiration_secs;
  cls_rgw_gc_obj_info info;
  cls_rgw_gc_queue_defer_entry_op() : expiration_secs(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(expiration_secs, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(expiration_secs, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_defer_entry_op)
