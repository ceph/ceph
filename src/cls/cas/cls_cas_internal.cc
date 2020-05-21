// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls_cas_internal.h"

struct refs_by_object : public chunk_obj_refcount::refs_t {
  std::set<hobject_t> by_object;

  uint8_t get_type() const {
    return chunk_obj_refcount::TYPE_BY_OBJECT;
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
WRITE_CLASS_ENCODER(refs_by_object)

struct refs_by_hash : public chunk_obj_refcount::refs_t {
  uint64_t total = 0;
  uint32_t hash_bits = 32;          ///< how many bits of mask to encode
  std::map<std::pair<int64_t,uint32_t>,uint64_t> by_hash;

  refs_by_hash() {}
  refs_by_hash(const refs_by_object *o) {
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
    return chunk_obj_refcount::TYPE_BY_HASH;
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
      *(__le32*)p.get_pos_add(hash_bytes) = i.first.second;
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
      __le32 hash;
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
WRITE_CLASS_DENC(refs_by_hash)

struct refs_by_pool : public chunk_obj_refcount::refs_t {
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
    return chunk_obj_refcount::TYPE_BY_POOL;
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
WRITE_CLASS_DENC(refs_by_pool)

struct refs_count : public chunk_obj_refcount::refs_t {
  uint64_t total = 0;

  refs_count() {}
  refs_count(const refs_t *old) {
    total = old->count();
  }

  uint8_t get_type() const {
    return chunk_obj_refcount::TYPE_COUNT;
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
WRITE_CLASS_ENCODER(refs_count)



//

void chunk_obj_refcount::clear()
{
  // default to most precise impl
  r.reset(new refs_by_object);
}


void chunk_obj_refcount::encode(ceph::buffer::list& bl) const
{
  bufferlist t;
  _encode_r(t);
  _encode_final(bl, t);
}

void chunk_obj_refcount::_encode_r(ceph::bufferlist& bl) const
{
  using ceph::encode;
  switch (r->get_type()) {
  case TYPE_BY_OBJECT:
    encode(*(refs_by_object*)r.get(), bl);
    break;
  case TYPE_BY_HASH:
    encode(*(refs_by_hash*)r.get(), bl);
    break;
  case TYPE_BY_POOL:
    encode(*(refs_by_pool*)r.get(), bl);
    break;
  case TYPE_COUNT:
    encode(*(refs_count*)r.get(), bl);
    break;
  default:
    ceph_abort("unrecognized ref type");
  }
}

void chunk_obj_refcount::dynamic_encode(ceph::buffer::list& bl, size_t max)
{
  bufferlist t;
  while (true) {
    _encode_r(t);
    if (t.length() <= max) {
      break;
    }
    // downgrade resolution
    std::unique_ptr<refs_t> n;
    switch (r->get_type()) {
    case TYPE_BY_OBJECT:
      r.reset(new refs_by_hash(static_cast<refs_by_object*>(r.get())));
      break;
    case TYPE_BY_HASH:
      if (!static_cast<refs_by_hash*>(r.get())->shrink()) {
	r.reset(new refs_by_pool(static_cast<refs_by_hash*>(r.get())));
      }
      break;
    case TYPE_BY_POOL:
      r.reset(new refs_count(r.get()));
      break;
    }
    t.clear();
  }
  _encode_final(bl, t);
}

void chunk_obj_refcount::_encode_final(bufferlist& bl, bufferlist& t) const
{
  ENCODE_START(1, 1, bl);
  encode(r->get_type(), bl);
  bl.claim_append(t);
  ENCODE_FINISH(bl);
}

void chunk_obj_refcount::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(1, p);
  uint8_t t;
  decode(t, p);
  switch (t) {
  case TYPE_BY_OBJECT:
    {
      auto n = new refs_by_object();
      decode(*n, p);
      r.reset(n);
    }
    break;
  case TYPE_BY_HASH:
    {
      auto n = new refs_by_hash();
      decode(*n, p);
      r.reset(n);
    }
    break;
  case TYPE_BY_POOL:
    {
      auto n = new refs_by_pool();
      decode(*n, p);
      r.reset(n);
    }
    break;
  case TYPE_COUNT:
    {
      auto n = new refs_count();
      decode(*n, p);
      r.reset(n);
    }
    break;
  default:
    throw ceph::buffer::malformed_input(
      "unrecognized chunk ref encoding type "s + stringify((int)t));
  }
  DECODE_FINISH(p);
}
