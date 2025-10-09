// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLS_CAS_OPS_H
#define CEPH_CLS_CAS_OPS_H

#include "include/types.h"
#include "common/hobject.h"
#include "common/Formatter.h"

struct cls_cas_chunk_create_or_get_ref_op {
  enum {
    FLAG_VERIFY = 1,  // verify content bit-for-bit if chunk already exists
  };

  hobject_t source;
  uint64_t flags = 0;
  bufferlist data;

  cls_cas_chunk_create_or_get_ref_op() {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source, bl);
    encode(flags, bl);
    encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source, bl);
    decode(flags, bl);
    decode(data, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_object("source", source);
    f->dump_unsigned("flags", flags);
    f->dump_unsigned("data_len", data.length());
  }
  static std::list<cls_cas_chunk_create_or_get_ref_op> generate_test_instances() {
    std::list<cls_cas_chunk_create_or_get_ref_op> ls;
    ls.emplace_back();
    return ls;
  }
};
WRITE_CLASS_ENCODER(cls_cas_chunk_create_or_get_ref_op)


struct cls_cas_chunk_get_ref_op {
  hobject_t source;

  cls_cas_chunk_get_ref_op() {}

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
  void dump(ceph::Formatter *f) const {
    f->dump_object("source", source);
  }
  static std::list<cls_cas_chunk_get_ref_op> generate_test_instances() {
    std::list<cls_cas_chunk_get_ref_op> ls;
    ls.emplace_back();
    return ls;
  }
};
WRITE_CLASS_ENCODER(cls_cas_chunk_get_ref_op)


struct cls_cas_chunk_put_ref_op {
  hobject_t source;

  cls_cas_chunk_put_ref_op() {}

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

  void dump(ceph::Formatter *f) const {
    f->dump_object("source", source);
  }
  static std::list<cls_cas_chunk_put_ref_op> generate_test_instances() {
    std::list<cls_cas_chunk_put_ref_op> ls;
    ls.emplace_back();
    return ls;
  }
};
WRITE_CLASS_ENCODER(cls_cas_chunk_put_ref_op)

#endif
