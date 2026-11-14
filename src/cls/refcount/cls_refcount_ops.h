// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLS_REFCOUNT_OPS_H
#define CEPH_CLS_REFCOUNT_OPS_H

#include "include/types.h"
#include "common/hobject.h"
#include "include/rados/cls_traits.hpp"

struct cls_refcount_get_op {
  std::string tag;
  bool implicit_ref;
  std::string src_tag; // tag the source object's delete will put

  cls_refcount_get_op() : implicit_ref(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(tag, bl);
    encode(implicit_ref, bl);
    encode(src_tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(tag, bl);
    decode(implicit_ref, bl);
    if (struct_v >= 2) {
      decode(src_tag, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const;
  static std::list<cls_refcount_get_op> generate_test_instances();
};
WRITE_CLASS_ENCODER(cls_refcount_get_op)

struct cls_refcount_put_op {
  std::string tag;
  bool implicit_ref; // assume wildcard reference for
                          // objects without a std::set ref

  cls_refcount_put_op() : implicit_ref(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(tag, bl);
    encode(implicit_ref, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(tag, bl);
    decode(implicit_ref, bl);
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
  bool implicit_ref; // assume wildcard reference for
                          // objects without a std::set ref

  cls_refcount_read_op() : implicit_ref(false) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(implicit_ref, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(implicit_ref, bl);
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
  std::string wildcard_owner; // if set, the only tag whose put may drop the wildcard

  obj_refcount() {}

  void encode(ceph::buffer::list& bl) const {
    // require decoders to recognize v3 when the owner is set
    const uint8_t compat = wildcard_owner.empty() ? 1 : 3;
    ENCODE_START(3, compat, bl);
    encode(refs, bl);
    encode(retired_refs, bl);
    encode(wildcard_owner, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(refs, bl);
    if (struct_v >= 2) {
      decode(retired_refs, bl);
    }
    if (struct_v >= 3) {
      decode(wildcard_owner, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
  static std::list<obj_refcount> generate_test_instances();
};
WRITE_CLASS_ENCODER(obj_refcount)

namespace cls::refcount {
struct ClassId {
  static constexpr auto name = "refcount";
};
namespace method {
constexpr auto get = ClsMethod<RdWrTag, ClassId>("get");
constexpr auto put = ClsMethod<RdWrTag, ClassId>("put");
constexpr auto set = ClsMethod<RdWrTag, ClassId>("set");
constexpr auto read = ClsMethod<RdTag, ClassId>("read");
}
}
#endif
