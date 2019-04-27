// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
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
  ENCODE_START(1, 1, bl);
  encode(ino, bl);
  encode(dirino, bl);
  encode(d_name, bl);
  encode(d_type, bl);
  ENCODE_FINISH(bl);
}

void Anchor::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(ino, bl);
  decode(dirino, bl);
  decode(d_name, bl);
  decode(d_type, bl);
  DECODE_FINISH(bl);
}

void Anchor::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("dirino", dirino);
  f->dump_string("d_name", d_name);
  f->dump_unsigned("d_type", d_type);
}

void Anchor::generate_test_instances(std::list<Anchor*>& ls)
{
  ls.push_back(new Anchor);
  ls.push_back(new Anchor);
  ls.back()->ino = 1;
  ls.back()->dirino = 2;
  ls.back()->d_name = "hello";
  ls.back()->d_type = DT_DIR;
}

ostream& operator<<(ostream& out, const Anchor &a)
{
  return out << "a(" << a.ino << " " << a.dirino << "/'" << a.d_name << "' " << a.d_type << ")";
}
