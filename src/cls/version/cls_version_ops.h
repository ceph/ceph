// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_VERSION_OPS_H
#define CEPH_CLS_VERSION_OPS_H

#include "cls_version_types.h"
#include "common/ceph_json.h"

struct cls_version_set_op {
  obj_version objv;

  cls_version_set_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_object("objv", objv);
  }

  static void generate_test_instances(std::list<cls_version_set_op*>& o) {
    o.push_back(new cls_version_set_op);
    o.push_back(new cls_version_set_op);
    o.back()->objv.ver = 123;
    o.back()->objv.tag = "foo";
  }
};
WRITE_CLASS_ENCODER(cls_version_set_op)

struct cls_version_inc_op {
  obj_version objv;
  std::list<obj_version_cond> conds;

  cls_version_inc_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    encode(conds, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    decode(conds, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_object("objv", objv);
    encode_json("conds", conds, f);
  }

  static void generate_test_instances(std::list<cls_version_inc_op*>& o) {
    o.push_back(new cls_version_inc_op);
    o.push_back(new cls_version_inc_op);
    o.back()->objv.ver = 123;
    o.back()->objv.tag = "foo";
    o.back()->conds.push_back(obj_version_cond());
    o.back()->conds.back().ver.ver = 123;
    o.back()->conds.back().ver.tag = "foo";
    o.back()->conds.back().cond = VER_COND_GE;
  }
};
WRITE_CLASS_ENCODER(cls_version_inc_op)

struct cls_version_check_op {
  obj_version objv;
  std::list<obj_version_cond> conds;

  cls_version_check_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    encode(conds, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    decode(conds, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_object("objv", objv);
    encode_json("conds", conds, f);
  }

  static void generate_test_instances(std::list<cls_version_check_op*>& o) {
    o.push_back(new cls_version_check_op);
    o.push_back(new cls_version_check_op);
    o.back()->objv.ver = 123;
    o.back()->objv.tag = "foo";
    o.back()->conds.push_back(obj_version_cond());
    o.back()->conds.back().ver.ver = 123;
    o.back()->conds.back().ver.tag = "foo";
    o.back()->conds.back().cond = VER_COND_GE;
  }
};
WRITE_CLASS_ENCODER(cls_version_check_op)

struct cls_version_read_ret {
  obj_version objv;

  cls_version_read_ret() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(objv, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(objv, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_object("objv", objv);
  }

  static void generate_test_instances(std::list<cls_version_read_ret*>& o) {
    o.push_back(new cls_version_read_ret);
    o.push_back(new cls_version_read_ret);
    o.back()->objv.ver = 123;
    o.back()->objv.tag = "foo";
  }
};
WRITE_CLASS_ENCODER(cls_version_read_ret)


#endif
