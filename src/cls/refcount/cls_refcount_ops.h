// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLS_REFCOUNT_OPS_H
#define CEPH_CLS_REFCOUNT_OPS_H

#include "include/types.h"
#include "common/hobject.h"

struct cls_refcount_get_op {
  std::string tag;
  std::string src_tag;

  cls_refcount_get_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(src_tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(src_tag, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static std::list<cls_refcount_get_op> generate_test_instances();
};
WRITE_CLASS_ENCODER(cls_refcount_get_op)

struct cls_refcount_put_op {
  std::string tag;

  cls_refcount_put_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static std::list<cls_refcount_put_op> generate_test_instances();
};
WRITE_CLASS_ENCODER(cls_refcount_put_op)

struct cls_refcount_set_op {
  std::list<std::string> refs;

  cls_refcount_set_op() {}

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
  static std::list<cls_refcount_set_op> generate_test_instances();
};
WRITE_CLASS_ENCODER(cls_refcount_set_op)

struct cls_refcount_read_op {
  cls_refcount_read_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static std::list<cls_refcount_read_op> generate_test_instances();
};
WRITE_CLASS_ENCODER(cls_refcount_read_op)

struct cls_refcount_read_ret {
  std::list<std::string> refs;

  cls_refcount_read_ret() {}

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
  static std::list<cls_refcount_read_ret> generate_test_instances();
};
WRITE_CLASS_ENCODER(cls_refcount_read_ret)

struct obj_refcount {
  std::map<std::string, bool> refs;
  std::set<std::string> retired_refs;

  obj_refcount() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(refs, bl);
    encode(retired_refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(refs, bl);
    if (struct_v >= 2) {
      decode(retired_refs, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static std::list<obj_refcount> generate_test_instances();
};
WRITE_CLASS_ENCODER(obj_refcount)

#endif
