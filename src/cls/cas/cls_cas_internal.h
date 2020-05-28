// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

#include "boost/variant.hpp"

#include "include/stringify.h"
#include "common/Formatter.h"
#include "common/hobject.h"

#define CHUNK_REFCOUNT_ATTR "chunk_refs"


// public type

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


// encoding specific types
// these are internal and should generally not be used directly

struct chunk_refs_by_object_t : public chunk_refs_t::refs_t {
  std::multiset<hobject_t> by_object;

  uint8_t get_type() const {
    return chunk_refs_t::TYPE_BY_OBJECT;
  }
  bool empty() const override {
    return by_object.empty();
  }
  uint64_t count() const override {
    return by_object.size();
  }
  bool get(const hobject_t& o) override {
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
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(by_object, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
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
WRITE_CLASS_ENCODER(chunk_refs_by_object_t)

struct chunk_refs_by_hash_t : public chunk_refs_t::refs_t {
  uint64_t total = 0;
  uint32_t hash_bits = 32;          ///< how many bits of mask to encode
  std::map<std::pair<int64_t,uint32_t>,uint64_t> by_hash;

  chunk_refs_by_hash_t() {}
  chunk_refs_by_hash_t(const chunk_refs_by_object_t *o) {
    total = o->count();
    for (auto& i : o->by_object) {
      by_hash[make_pair(i.pool, i.get_hash())]++;
    }
  }

  std::string describe_encoding() const {
    return "by_hash("s + stringify(hash_bits) + " bits)"s;
  }

  uint32_t mask() {
    // with the hobject_t reverse-bitwise sort, the least significant
    // hash values are actually the most significant, so preserve them
    // as we lose resolution.
    return 0xffffffff >> (32 - hash_bits);
  }

  bool shrink() {
    if (hash_bits <= 1) {
      return false;
    }
    hash_bits--;
    std::map<std::pair<int64_t,uint32_t>,uint64_t> old;
    old.swap(by_hash);
    auto m = mask();
    for (auto& i : old) {
      by_hash[make_pair(i.first.first, i.first.second & m)] = i.second;
    }
    return true;
  }

  uint8_t get_type() const {
    return chunk_refs_t::TYPE_BY_HASH;
  }
  bool empty() const override {
    return by_hash.empty();
  }
  uint64_t count() const override {
    return total;
  }
  bool get(const hobject_t& o) override {
    by_hash[make_pair(o.pool, o.get_hash() & mask())]++;
    ++total;
    return true;
  }
  bool put(const hobject_t& o) override {
    auto p = by_hash.find(make_pair(o.pool, o.get_hash() & mask()));
    if (p == by_hash.end()) {
      return false;
    }
    if (--p->second == 0) {
      by_hash.erase(p);
    }
    --total;
    return true;
  }
  DENC_HELPERS
  void bound_encode(size_t& p) const {
    p += 6 + sizeof(uint64_t) + by_hash.size() * (10 + 10);
  }
  void encode(::ceph::buffer::list::contiguous_appender& p) const {
    DENC_START(1, 1, p);
    denc_varint(total, p);
    denc_varint(hash_bits, p);
    denc_varint(by_hash.size(), p);
    int hash_bytes = (hash_bits + 7) / 8;
    for (auto& i : by_hash) {
      denc_signed_varint(i.first.first, p);
      // this may write some bytes past where we move cursor too; harmless!
      *(ceph_le32*)p.get_pos_add(hash_bytes) = i.first.second;
      denc_varint(i.second, p);
    }
    DENC_FINISH(p);
  }
  void decode(::ceph::buffer::ptr::const_iterator& p) {
    DENC_START(1, 1, p);
    denc_varint(total, p);
    denc_varint(hash_bits, p);
    uint64_t n;
    denc_varint(n, p);
    int hash_bytes = (hash_bits + 7) / 8;
    while (n--) {
      int64_t poolid;
      ceph_le32 hash;
      uint64_t count;
      denc_signed_varint(poolid, p);
      memcpy(&hash, p.get_pos_add(hash_bytes), hash_bytes);
      denc_varint(count, p);
      by_hash[make_pair(poolid, (uint32_t)hash)] = count;
    }
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const override {
    f->dump_string("type", "by_hash");
    f->dump_unsigned("count", total);
    f->dump_unsigned("hash_bits", hash_bits);
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
WRITE_CLASS_DENC(chunk_refs_by_hash_t)

struct chunk_refs_by_pool_t : public chunk_refs_t::refs_t {
  uint64_t total = 0;
  map<int64_t,uint64_t> by_pool;

  chunk_refs_by_pool_t() {}
  chunk_refs_by_pool_t(const chunk_refs_by_hash_t *o) {
    total = o->count();
    for (auto& i : o->by_hash) {
      by_pool[i.first.first] += i.second;
    }
  }

  uint8_t get_type() const {
    return chunk_refs_t::TYPE_BY_POOL;
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
  void bound_encode(size_t& p) const {
    p += 6 + sizeof(uint64_t) + by_pool.size() * (9 + 9);
  }
  DENC_HELPERS
  void encode(::ceph::buffer::list::contiguous_appender& p) const {
    DENC_START(1, 1, p);
    denc_varint(total, p);
    denc_varint(by_pool.size(), p);
    for (auto& i : by_pool) {
      denc_signed_varint(i.first, p);
      denc_varint(i.second, p);
    }
    DENC_FINISH(p);
  }
  void decode(::ceph::buffer::ptr::const_iterator& p) {
    DENC_START(1, 1, p);
    denc_varint(total, p);
    uint64_t n;
    denc_varint(n, p);
    while (n--) {
      int64_t poolid;
      uint64_t count;
      denc_signed_varint(poolid, p);
      denc_varint(count, p);
      by_pool[poolid] = count;
    }
    DENC_FINISH(p);
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
WRITE_CLASS_DENC(chunk_refs_by_pool_t)


struct chunk_refs_count_t : public chunk_refs_t::refs_t {
  uint64_t total = 0;

  chunk_refs_count_t() {}
  chunk_refs_count_t(const refs_t *old) {
    total = old->count();
  }

  uint8_t get_type() const {
    return chunk_refs_t::TYPE_COUNT;
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
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(total, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(total, p);
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const override {
    f->dump_string("type", "count");
    f->dump_unsigned("count", total);
  }
};
WRITE_CLASS_ENCODER(chunk_refs_count_t)

