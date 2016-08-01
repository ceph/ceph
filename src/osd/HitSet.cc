// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "HitSet.h"
#include "common/Formatter.h"

// -- HitSet --

HitSet::HitSet(const HitSet::Params& params)
  : sealed(false)
{
  switch (params.get_type()) {
  case TYPE_BLOOM:
    {
      BloomHitSet::Params *p =
	static_cast<BloomHitSet::Params*>(params.impl.get());
      impl.reset(new BloomHitSet(p));
    }
    break;

  case TYPE_EXPLICIT_HASH:
    impl.reset(new ExplicitHashHitSet(static_cast<ExplicitHashHitSet::Params*>(params.impl.get())));
    break;

  case TYPE_EXPLICIT_OBJECT:
    impl.reset(new ExplicitObjectHitSet(static_cast<ExplicitObjectHitSet::Params*>(params.impl.get())));
    break;

  default:
    assert (0 == "unknown HitSet type");
  }
}

void HitSet::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(sealed, bl);
  if (impl) {
    ::encode((__u8)impl->get_type(), bl);
    impl->encode(bl);
  } else {
    ::encode((__u8)TYPE_NONE, bl);
  }
  ENCODE_FINISH(bl);
}

void HitSet::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(sealed, bl);
  __u8 type;
  ::decode(type, bl);
  switch ((impl_type_t)type) {
  case TYPE_EXPLICIT_HASH:
    impl.reset(new ExplicitHashHitSet);
    break;
  case TYPE_EXPLICIT_OBJECT:
    impl.reset(new ExplicitObjectHitSet);
    break;
  case TYPE_BLOOM:
    impl.reset(new BloomHitSet);
    break;
  case TYPE_NONE:
    impl.reset(NULL);
    break;
  default:
    throw buffer::malformed_input("unrecognized HitMap type");
  }
  if (impl)
    impl->decode(bl);
  DECODE_FINISH(bl);
}

void HitSet::dump(Formatter *f) const
{
  f->dump_string("type", get_type_name());
  f->dump_string("sealed", sealed ? "yes" : "no");
  if (impl)
    impl->dump(f);
}

void HitSet::generate_test_instances(list<HitSet*>& o)
{
  o.push_back(new HitSet);
  o.push_back(new HitSet(new BloomHitSet(10, .1, 1)));
  o.back()->insert(hobject_t());
  o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
  o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  o.push_back(new HitSet(new ExplicitHashHitSet));
  o.back()->insert(hobject_t());
  o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
  o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  o.push_back(new HitSet(new ExplicitObjectHitSet));
  o.back()->insert(hobject_t());
  o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
  o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
}

HitSet::Params::Params(const Params& o)
{
  if (o.get_type() != TYPE_NONE) {
    create_impl(o.get_type());
    // it's annoying to write virtual operator= methods; use encode/decode
    // instead.
    bufferlist bl;
    o.impl->encode(bl);
    bufferlist::iterator p = bl.begin();
    impl->decode(p);
  } // else we don't need to do anything
}

const HitSet::Params& HitSet::Params::operator=(const Params& o)
{
  create_impl(o.get_type());
  if (o.impl) {
    // it's annoying to write virtual operator= methods; use encode/decode
    // instead.
    bufferlist bl;
    o.impl->encode(bl);
    bufferlist::iterator p = bl.begin();
    impl->decode(p);
  }
  return *this;
}

void HitSet::Params::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  if (impl) {
    ::encode((__u8)impl->get_type(), bl);
    impl->encode(bl);
  } else {
    ::encode((__u8)TYPE_NONE, bl);
  }
  ENCODE_FINISH(bl);
}

bool HitSet::Params::create_impl(impl_type_t type)
{
  switch ((impl_type_t)type) {
  case TYPE_EXPLICIT_HASH:
    impl.reset(new ExplicitHashHitSet::Params);
    break;
  case TYPE_EXPLICIT_OBJECT:
    impl.reset(new ExplicitObjectHitSet::Params);
    break;
  case TYPE_BLOOM:
    impl.reset(new BloomHitSet::Params);
    break;
  case TYPE_NONE:
    impl.reset(NULL);
    break;
  default:
    return false;
  }
  return true;
}

void HitSet::Params::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  __u8 type;
  ::decode(type, bl);
  if (!create_impl((impl_type_t)type))
    throw buffer::malformed_input("unrecognized HitMap type");
  if (impl)
    impl->decode(bl);
  DECODE_FINISH(bl);
}

void HitSet::Params::dump(Formatter *f) const
{
  f->dump_string("type", HitSet::get_type_name(get_type()));
  if (impl)
    impl->dump(f);
}

void HitSet::Params::generate_test_instances(list<HitSet::Params*>& o)
{
#define loop_hitset_params(kind) \
{ \
  list<kind::Params*> params; \
  kind::Params::generate_test_instances(params); \
  for (list<kind::Params*>::iterator i = params.begin(); \
  i != params.end(); ++i) \
    o.push_back(new Params(*i)); \
}
  o.push_back(new Params);
  o.push_back(new Params(new BloomHitSet::Params));
  loop_hitset_params(BloomHitSet);
  o.push_back(new Params(new ExplicitHashHitSet::Params));
  loop_hitset_params(ExplicitHashHitSet);
  o.push_back(new Params(new ExplicitObjectHitSet::Params));
  loop_hitset_params(ExplicitObjectHitSet);
}

ostream& operator<<(ostream& out, const HitSet::Params& p) {
  out << HitSet::get_type_name(p.get_type());
  if (p.impl) {
    out << "{";
    p.impl->dump_stream(out);
  }
  out << "}";
  return out;
}


void ExplicitHashHitSet::dump(Formatter *f) const {
  f->dump_unsigned("insert_count", count);
  f->open_array_section("hash_set");
  for (ceph::unordered_set<uint32_t>::const_iterator p = hits.begin();
       p != hits.end();
       ++p)
    f->dump_unsigned("hash", *p);
  f->close_section();
}

void ExplicitObjectHitSet::dump(Formatter *f) const {
  f->dump_unsigned("insert_count", count);
  f->open_array_section("set");
  for (ceph::unordered_set<hobject_t>::const_iterator p = hits.begin();
       p != hits.end();
       ++p) {
    f->open_object_section("object");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void BloomHitSet::Params::dump(Formatter *f) const {
  f->dump_float("false_positive_probability", get_fpp());
  f->dump_int("target_size", target_size);
  f->dump_int("seed", seed);
}

void BloomHitSet::dump(Formatter *f) const {
  f->open_object_section("bloom_filter");
  bloom.dump(f);
  f->close_section();
}
