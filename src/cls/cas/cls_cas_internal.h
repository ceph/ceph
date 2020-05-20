// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "boost/variant.hpp"

#include "include/stringify.h"
#include "common/Formatter.h"

#define CHUNK_REFCOUNT_ATTR "chunk_refcount"

struct chunk_obj_refcount {
  enum {
    TYPE_BY_OBJECT = 1,
    TYPE_BY_HASH = 2,
    TYPE_BY_PARTIAL = 3,
    TYPE_BY_POOL = 4,
    TYPE_COUNT = 5,
  };

  struct refs_t {
    virtual ~refs_t() {}
    virtual uint8_t get_type() const = 0;
    virtual bool empty() const = 0;
    virtual uint64_t count() const = 0;
    virtual bool get(const hobject_t& o) = 0;
    virtual bool put(const hobject_t& o) = 0;
    virtual void encode(bufferlist& bl) const = 0;
    virtual void decode(bufferlist::const_iterator& p) = 0;
    virtual void dump(Formatter *f) const = 0;
  };

  struct refs_by_object : public refs_t {
    std::set<hobject_t> by_object;

    uint8_t get_type() const {
      return TYPE_BY_OBJECT;
    }
    bool empty() const override {
      return by_object.empty();
    }
    uint64_t count() const override {
      return by_object.size();
    }
    bool get(const hobject_t& o) override {
      if (by_object.count(o)) {
	return false;
      }
      by_object.insert(o);
      return true;
    }
    bool put(const hobject_t& o) override {
      auto p = by_object.find(o);
      if (p == by_object.end()) {
	return false;
      }
      by_object.erase(p);
      return true;
    }
    void encode(bufferlist& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(by_object, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& p) override {
      DECODE_START(1, p);
      decode(by_object, p);
      DECODE_FINISH(p);
    }
    void dump(Formatter *f) const override {
      f->dump_string("type", "by_object");
      f->dump_unsigned("count", by_object.size());
      f->open_array_section("refs");
      for (auto& i : by_object) {
	f->dump_object("ref", i);
      }
      f->close_section();
    }
  };

  struct refs_by_hash : public refs_t {
    uint64_t total = 0;
    std::map<std::pair<int64_t,uint32_t>,uint64_t> by_hash;

    refs_by_hash() {}
    refs_by_hash(const refs_by_object *o) {
      total = o->count();
      for (auto& i : o->by_object) {
	by_hash[make_pair(i.pool, i.get_hash())]++;
      }
    }

    uint8_t get_type() const {
      return TYPE_BY_HASH;
    }
    bool empty() const override {
      return by_hash.empty();
    }
    uint64_t count() const override {
      return total;
    }
    bool get(const hobject_t& o) override {
      by_hash[make_pair(o.pool, o.get_hash())]++;
      ++total;
      return true;
    }
    bool put(const hobject_t& o) override {
      auto p = by_hash.find(make_pair(o.pool, o.get_hash()));
      if (p == by_hash.end()) {
	return false;
      }
      if (--p->second == 0) {
	by_hash.erase(p);
      }
      --total;
      return true;
    }
    void encode(bufferlist& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(total, bl);
      encode(by_hash, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& p) override {
      DECODE_START(1, p);
      decode(total, p);
      decode(by_hash, p);
      DECODE_FINISH(p);
    }
    void dump(Formatter *f) const override {
      f->dump_string("type", "by_hash");
      f->dump_unsigned("count", total);
      f->open_array_section("refs");
      for (auto& i : by_hash) {
	f->open_object_section("hash");
	f->dump_int("pool", i.first.first);
	f->dump_unsigned("hash", i.first.second);
	f->dump_unsigned("count", i.second);
	f->close_section();
      }
      f->close_section();
    }
  };

  struct refs_by_pool : public refs_t {
    uint64_t total = 0;
    map<int64_t,uint64_t> by_pool;

    refs_by_pool() {}
    refs_by_pool(const refs_by_hash *o) {
      total = o->count();
      for (auto& i : o->by_hash) {
	by_pool[i.first.first] += i.second;
      }
    }

    uint8_t get_type() const {
      return TYPE_BY_POOL;
    }
    bool empty() const override {
      return by_pool.empty();
    }
    uint64_t count() const override {
      return total;
    }
    bool get(const hobject_t& o) override {
      ++by_pool[o.pool];
      ++total;
      return true;
    }
    bool put(const hobject_t& o) override {
      auto p = by_pool.find(o.pool);
      if (p == by_pool.end()) {
	return false;
      }
      --p->second;
      if (p->second == 0) {
	by_pool.erase(p);
      }
      --total;
      return true;
    }
    void encode(bufferlist& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(total, bl);
      encode(by_pool, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& p) override {
      DECODE_START(1, p);
      decode(total, p);
      decode(by_pool, p);
      DECODE_FINISH(p);
    }
    void dump(Formatter *f) const override {
      f->dump_string("type", "by_pool");
      f->dump_unsigned("count", total);
      f->open_array_section("pools");
      for (auto& i : by_pool) {
	f->open_object_section("pool");
	f->dump_unsigned("pool_id", i.first);
	f->dump_unsigned("count", i.second);
	f->close_section();
      }
      f->close_section();
    }
  };

  struct refs_count : public refs_t {
    uint64_t total = 0;

    refs_count() {}
    refs_count(const refs_t *old) {
      total = old->count();
    }

    uint8_t get_type() const {
      return TYPE_COUNT;
    }
    bool empty() const override {
      return total == 0;
    }
    uint64_t count() const override {
      return total;
    }
    bool get(const hobject_t& o) override {
      ++total;
      return true;
    }
    bool put(const hobject_t& o) override {
      if (!total) {
	return false;
      }
      --total;
      return true;
    }
    void encode(bufferlist& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(total, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& p) override {
      DECODE_START(1, p);
      decode(total, p);
      DECODE_FINISH(p);
    }
    void dump(Formatter *f) const override {
      f->dump_string("type", "count");
      f->dump_unsigned("count", total);
    }
  };


  std::unique_ptr<refs_t> r;

  chunk_obj_refcount() {
    clear();
  }

  void clear() {
    // default to most precise impl
    r.reset(new refs_by_object);
  }

  int get_type() const {
    return r->get_type();
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

  void encode(ceph::buffer::list& bl) const {
    bufferlist t;
    r->encode(t);
    _encode(bl, t);
  }

  void dynamic_encode(ceph::buffer::list& bl, size_t max = 1024) {
    bufferlist t;
    while (true) {
      r->encode(t);
      if (t.length() <= max) {
	break;
      }
      // downgrade resolution
      std::unique_ptr<refs_t> n;
      switch (r->get_type()) {
      case TYPE_BY_OBJECT:
	n.reset(new refs_by_hash(static_cast<refs_by_object*>(r.get())));
	break;
      case TYPE_BY_HASH:
	n.reset(new refs_by_pool(static_cast<refs_by_hash*>(r.get())));
	break;
      case TYPE_BY_POOL:
	n.reset(new refs_count(r.get()));
	break;
      }
      r.swap(n);
      t.clear();
    }
    _encode(bl, t);
  }

  void _encode(bufferlist& bl, bufferlist& t) const {
    ENCODE_START(1, 1, bl);
    encode(r->get_type(), bl);
    bl.claim_append(t);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    uint8_t t;
    decode(t, p);
    switch (t) {
    case TYPE_BY_OBJECT:
      r.reset(new refs_by_object());
      break;
    case TYPE_BY_HASH:
      r.reset(new refs_by_hash());
      break;
    case TYPE_BY_POOL:
      r.reset(new refs_by_pool());
      break;
    case TYPE_COUNT:
      r.reset(new refs_count());
      break;
    default:
      throw ceph::buffer::malformed_input(
	"unrecognized chunk ref encoding type "s + stringify((int)t));
    }
    r->decode(p);
    DECODE_FINISH(p);
  }

  void dump(Formatter *f) const {
    r->dump(f);
  }
  static void generate_test_instances(std::list<chunk_obj_refcount*>& ls) {
    ls.push_back(new chunk_obj_refcount());
  }
};
WRITE_CLASS_ENCODER(chunk_obj_refcount)
