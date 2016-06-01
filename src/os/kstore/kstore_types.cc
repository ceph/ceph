// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "kstore_types.h"
#include "common/Formatter.h"
#include "include/stringify.h"

// cnode_t

void kstore_cnode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(bits, bl);
  ENCODE_FINISH(bl);
}

void kstore_cnode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(bits, p);
  DECODE_FINISH(p);
}

void kstore_cnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("bits", bits);
}

void kstore_cnode_t::generate_test_instances(list<kstore_cnode_t*>& o)
{
  o.push_back(new kstore_cnode_t());
  o.push_back(new kstore_cnode_t(0));
  o.push_back(new kstore_cnode_t(123));
}


// kstore_onode_t

void kstore_onode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(nid, bl);
  ::encode(size, bl);
  ::encode(attrs, bl);
  ::encode(omap_head, bl);
  ::encode(stripe_size, bl);
  ::encode(expected_object_size, bl);
  ::encode(expected_write_size, bl);
  ::encode(alloc_hint_flags, bl);
  ENCODE_FINISH(bl);
}

void kstore_onode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(nid, p);
  ::decode(size, p);
  ::decode(attrs, p);
  ::decode(omap_head, p);
  ::decode(stripe_size, p);
  ::decode(expected_object_size, p);
  ::decode(expected_write_size, p);
  ::decode(alloc_hint_flags, p);
  DECODE_FINISH(p);
}

void kstore_onode_t::dump(Formatter *f) const
{
  f->dump_unsigned("nid", nid);
  f->dump_unsigned("size", size);
  f->open_object_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin();
       p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_unsigned("len", p->second.length());
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("omap_head", omap_head);
  f->dump_unsigned("stripe_size", stripe_size);
  f->dump_unsigned("expected_object_size", expected_object_size);
  f->dump_unsigned("expected_write_size", expected_write_size);
  f->dump_unsigned("alloc_hint_flags", alloc_hint_flags);
}

void kstore_onode_t::generate_test_instances(list<kstore_onode_t*>& o)
{
  o.push_back(new kstore_onode_t());
  // FIXME
}
