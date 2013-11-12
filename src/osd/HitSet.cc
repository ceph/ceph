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

// -- HitSet --

HitSet::HitSet(type_t type, unsigned target_size, double fpp, unsigned seed)
{
  switch (type) {
  case TYPE_BLOOM:
    impl.reset(new BloomHitSet(target_size, fpp, seed));
    break;
  case TYPE_EXPLICIT_HASH:
    impl.reset(new ExplicitHashHitSet);
    break;
  case TYPE_EXPLICIT_OBJECT:
    impl.reset(new ExplicitObjectHitSet);
    break;
  case TYPE_NONE:
    break;
  default:
    assert(0 == "unknown type");
  }
}

void HitSet::encode(bufferlist &bl) const
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

void HitSet::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  __u8 type;
  ::decode(type, bl);
  switch ((type_t)type) {
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
