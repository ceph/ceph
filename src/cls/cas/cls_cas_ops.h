// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_CAS_OPS_H
#define CEPH_CLS_CAS_OPS_H

#include "include/types.h"
#include "common/hobject.h"

#define CHUNK_REFCOUNT_ATTR "chunk_refcount"

struct cls_chunk_refcount_get_op {
  hobject_t source;

  cls_chunk_refcount_get_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_chunk_refcount_get_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_chunk_refcount_get_op)

struct cls_chunk_refcount_put_op {
  hobject_t source;

  cls_chunk_refcount_put_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_chunk_refcount_put_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_chunk_refcount_put_op)

struct cls_chunk_refcount_set_op {
  std::set<hobject_t> refs;

  cls_chunk_refcount_set_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(refs, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_chunk_refcount_set_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_chunk_refcount_set_op)

struct cls_chunk_refcount_read_ret {
  std::set<hobject_t> refs;

  cls_chunk_refcount_read_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(refs, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<cls_chunk_refcount_read_ret*>& ls);
};
WRITE_CLASS_ENCODER(cls_chunk_refcount_read_ret)

struct chunk_obj_refcount {
  std::set<hobject_t> refs;

  chunk_obj_refcount() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(refs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(chunk_obj_refcount)

#endif
