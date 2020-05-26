// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

#include "boost/variant.hpp"

#include "include/stringify.h"
#include "common/Formatter.h"
#include "common/hobject.h"

#define CHUNK_REFCOUNT_ATTR "chunk_refs"

struct chunk_refs_t {
  enum {
    TYPE_BY_OBJECT = 1,
    TYPE_BY_HASH = 2,
    TYPE_BY_PARTIAL = 3,
    TYPE_BY_POOL = 4,
    TYPE_COUNT = 5,
  };
  static const char *type_name(int t) {
    switch (t) {
    case TYPE_BY_OBJECT: return "by_object";
    case TYPE_BY_HASH: return "by_hash";
    case TYPE_BY_POOL: return "by_pool";
    case TYPE_COUNT: return "count";
    default: return "???";
    }
  }

  struct refs_t {
    virtual ~refs_t() {}
    virtual uint8_t get_type() const = 0;
    virtual bool empty() const = 0;
    virtual uint64_t count() const = 0;
    virtual bool get(const hobject_t& o) = 0;
    virtual bool put(const hobject_t& o) = 0;
    virtual void dump(Formatter *f) const = 0;
    virtual std::string describe_encoding() const {
      return type_name(get_type());
    }
  };

  std::unique_ptr<refs_t> r;

  chunk_refs_t() {
    clear();
  }
  chunk_refs_t(const chunk_refs_t& other);

  chunk_refs_t& operator=(const chunk_refs_t&);

  void clear();

  int get_type() const {
    return r->get_type();
  }
  std::string describe_encoding() const {
    return r->describe_encoding();
  }

  bool empty() const {
    return r->empty();
  }
  uint64_t count() const {
    return r->count();
  }

  bool get(const hobject_t& o) {
    return r->get(o);
  }
  bool put(const hobject_t& o) {
    bool ret = r->put(o);
    if (r->get_type() != TYPE_BY_OBJECT &&
	r->count() == 0) {
      clear();      // reset to full resolution, yay
    }
    return ret;
  }

  void _encode_r(bufferlist& bl) const;
  void _encode_final(bufferlist& bl, bufferlist& t) const;
  void dynamic_encode(ceph::buffer::list& bl, size_t max);
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void dump(Formatter *f) const {
    r->dump(f);
  }
  static void generate_test_instances(std::list<chunk_refs_t*>& ls) {
    ls.push_back(new chunk_refs_t());
  }
};
WRITE_CLASS_ENCODER(chunk_refs_t)
