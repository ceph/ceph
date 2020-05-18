// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/Formatter.h"

#define CHUNK_REFCOUNT_ATTR "chunk_refcount"

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

  void dump(Formatter *f) const {
    f->open_array_section("refs");
    for (auto& i : refs) {
      f->dump_object("ref", i);
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<chunk_obj_refcount*>& ls) {
    ls.push_back(new chunk_obj_refcount());
  }
};
WRITE_CLASS_ENCODER(chunk_obj_refcount)
