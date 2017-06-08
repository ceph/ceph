// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_VERSION_OPS_H
#define CEPH_CLS_VERSION_OPS_H

#include <map>

#include "include/types.h"
#include "cls_version_types.h"

struct cls_version_set_op {
  obj_version objv;

  cls_version_set_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(objv, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(objv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_version_set_op)

struct cls_version_inc_op {
  obj_version objv;
  list<obj_version_cond> conds;

  cls_version_inc_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(objv, bl);
    ::encode(conds, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(objv, bl);
    ::decode(conds, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_version_inc_op)

struct cls_version_check_op {
  obj_version objv;
  list<obj_version_cond> conds;

  cls_version_check_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(objv, bl);
    ::encode(conds, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(objv, bl);
    ::decode(conds, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_version_check_op)

struct cls_version_read_ret {
  obj_version objv;

  cls_version_read_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(objv, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(objv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_version_read_ret)


#endif
