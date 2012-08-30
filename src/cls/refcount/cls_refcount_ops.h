#ifndef CEPH_CLS_REFCOUNT_OPS_H
#define CEPH_CLS_REFCOUNT_OPS_H

#include <map>

#include "include/types.h"

struct cls_refcount_get_op {
  string tag;
  bool implicit_ref;

  cls_refcount_get_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(tag, bl);
    ::encode(implicit_ref, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(tag, bl);
    ::decode(implicit_ref, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_refcount_get_op)

struct cls_refcount_put_op {
  string tag;
  bool implicit_ref; // assume wildcard reference for
                          // objects without a set ref

  cls_refcount_put_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(tag, bl);
    ::encode(implicit_ref, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(tag, bl);
    ::decode(implicit_ref, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_refcount_put_op)

struct cls_refcount_set_op {
  list<string> refs;

  cls_refcount_set_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(refs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_refcount_set_op)

struct cls_refcount_read_op {
  bool implicit_ref; // assume wildcard reference for
                          // objects without a set ref

  cls_refcount_read_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(implicit_ref, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(implicit_ref, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_refcount_read_op)

struct cls_refcount_read_ret {
  list<string> refs;

  cls_refcount_read_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(refs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(refs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_refcount_read_ret)


#endif
