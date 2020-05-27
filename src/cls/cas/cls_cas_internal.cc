// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls_cas_internal.h"


chunk_refs_t::chunk_refs_t(const chunk_refs_t& other)
{
  *this = other;
}

chunk_refs_t& chunk_refs_t::operator=(const chunk_refs_t& other)
{
  // this is inefficient, but easy.
  bufferlist bl;
  other.encode(bl);
  auto p = bl.cbegin();
  decode(p);
  return *this;
}

void chunk_refs_t::clear()
{
  // default to most precise impl
  r.reset(new chunk_refs_by_object_t);
}


void chunk_refs_t::encode(ceph::buffer::list& bl) const
{
  bufferlist t;
  _encode_r(t);
  _encode_final(bl, t);
}

void chunk_refs_t::_encode_r(ceph::bufferlist& bl) const
{
  using ceph::encode;
  switch (r->get_type()) {
  case TYPE_BY_OBJECT:
    encode(*(chunk_refs_by_object_t*)r.get(), bl);
    break;
  case TYPE_BY_HASH:
    encode(*(chunk_refs_by_hash_t*)r.get(), bl);
    break;
  case TYPE_BY_POOL:
    encode(*(chunk_refs_by_pool_t*)r.get(), bl);
    break;
  case TYPE_COUNT:
    encode(*(chunk_refs_count_t*)r.get(), bl);
    break;
  default:
    ceph_abort("unrecognized ref type");
  }
}

void chunk_refs_t::dynamic_encode(ceph::buffer::list& bl, size_t max)
{
  bufferlist t;
  while (true) {
    _encode_r(t);
    // account for the additional overhead in _encode_final
    if (t.length() + 8 <= max) {
      break;
    }
    // downgrade resolution
    std::unique_ptr<refs_t> n;
    switch (r->get_type()) {
    case TYPE_BY_OBJECT:
      r.reset(new chunk_refs_by_hash_t(
		static_cast<chunk_refs_by_object_t*>(r.get())));
      break;
    case TYPE_BY_HASH:
      if (!static_cast<chunk_refs_by_hash_t*>(r.get())->shrink()) {
	r.reset(new chunk_refs_by_pool_t(
		  static_cast<chunk_refs_by_hash_t*>(r.get())));
      }
      break;
    case TYPE_BY_POOL:
      r.reset(new chunk_refs_count_t(r.get()));
      break;
    }
    t.clear();
  }
  _encode_final(bl, t);
}

void chunk_refs_t::_encode_final(bufferlist& bl, bufferlist& t) const
{
  ENCODE_START(1, 1, bl);
  encode(r->get_type(), bl);
  bl.claim_append(t);
  ENCODE_FINISH(bl);
}

void chunk_refs_t::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(1, p);
  uint8_t t;
  decode(t, p);
  switch (t) {
  case TYPE_BY_OBJECT:
    {
      auto n = new chunk_refs_by_object_t();
      decode(*n, p);
      r.reset(n);
    }
    break;
  case TYPE_BY_HASH:
    {
      auto n = new chunk_refs_by_hash_t();
      decode(*n, p);
      r.reset(n);
    }
    break;
  case TYPE_BY_POOL:
    {
      auto n = new chunk_refs_by_pool_t();
      decode(*n, p);
      r.reset(n);
    }
    break;
  case TYPE_COUNT:
    {
      auto n = new chunk_refs_count_t();
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
