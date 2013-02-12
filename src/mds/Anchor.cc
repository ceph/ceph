// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "mds/Anchor.h"

#include "common/Formatter.h"

void Anchor::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(ino, bl);
  ::encode(dirino, bl);
  ::encode(dn_hash, bl);
  ::encode(nref, bl);
  ::encode(updated, bl);
  ENCODE_FINISH(bl);
}

void Anchor::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(ino, bl);
  ::decode(dirino, bl);
  ::decode(dn_hash, bl);
  ::decode(nref, bl);
  ::decode(updated, bl);
  DECODE_FINISH(bl);
}

void Anchor::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("dirino", dirino);
  f->dump_unsigned("dn_hash", dn_hash);
  f->dump_unsigned("num_ref", nref);
  f->dump_unsigned("updated", updated);
}

void Anchor::generate_test_instances(list<Anchor*>& ls)
{
  ls.push_back(new Anchor);
  ls.push_back(new Anchor);
  ls.back()->ino = 1;
  ls.back()->dirino = 2;
  ls.back()->dn_hash = 3;
  ls.back()->nref = 4;
  ls.back()->updated = 5;
}

ostream& operator<<(ostream& out, const Anchor &a)
{
  return out << "a(" << a.ino << " " << a.dirino << "/" << a.dn_hash << " " << a.nref << " v" << a.updated << ")";
}
