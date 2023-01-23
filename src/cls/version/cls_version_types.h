// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_VERSION_TYPES_H
#define CEPH_CLS_VERSION_TYPES_H

#include <cstdint>
#include <iostream>
#include <list>
#include <string>

#include "include/encoding.h"
#include "include/types.h"


class JSONObj;


struct obj_version {
  uint64_t ver = 0;
  std::string tag;

  obj_version() = default;
  obj_version(uint64_t ver, std::string tag)
    : ver(ver), tag(std::move(tag)) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(ver, bl);
    encode(tag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(ver, bl);
    decode(tag, bl);
    DECODE_FINISH(bl);
  }

  void inc() {
    ver++;
  }

  void clear() {
    ver = 0;
    tag.clear();
  }

  bool empty() const {
    return tag.empty();
  }

  bool compare(struct obj_version *v) const {
    return (ver == v->ver &&
            tag.compare(v->tag) == 0);
  }

  bool operator==(const struct obj_version& v) const {
    return (ver == v.ver &&
            tag.compare(v.tag) == 0);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_int("ver", ver);
    f->dump_string("tag", tag);
  }

  void decode_json(JSONObj *obj);
  static void generate_test_instances(std::list<obj_version*>& o);
};
WRITE_CLASS_ENCODER(obj_version)

inline std::ostream& operator <<(std::ostream& m, const obj_version& v) {
  return m << v.tag << ":" << v.ver;
}

enum VersionCond {
  VER_COND_NONE =      0,
  VER_COND_EQ,  /* equal */
  VER_COND_GT,  /* greater than */
  VER_COND_GE,  /* greater or equal */
  VER_COND_LT,  /* less than */
  VER_COND_LE,  /* less or equal */
  VER_COND_TAG_EQ,
  VER_COND_TAG_NE,
};

struct obj_version_cond {
  struct obj_version ver;
  VersionCond cond;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(ver, bl);
    uint32_t c = (uint32_t)cond;
    encode(c, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(ver, bl);
    uint32_t c;
    decode(c, bl);
    cond = (VersionCond)c;
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_object("ver", ver);
    f->dump_unsigned("cond", cond);
  }

  static void generate_test_instances(std::list<obj_version_cond*>& o) {
    o.push_back(new obj_version_cond);
    o.push_back(new obj_version_cond);
    o.back()->ver.ver = 1;
    o.back()->ver.tag = "foo";
    o.back()->cond = VER_COND_EQ;
  }
};
WRITE_CLASS_ENCODER(obj_version_cond)


#endif
