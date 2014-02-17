// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/histogram.h"
#include "common/Formatter.h"

// -- pow2_hist_t --
void pow2_hist_t::dump(Formatter *f) const
{
  f->open_array_section("histogram");
  for (std::vector<int32_t>::const_iterator p = h.begin(); p != h.end(); ++p)
    f->dump_int("count", *p);
  f->close_section();
  f->dump_int("upper_bound", upper_bound());
}

void pow2_hist_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(h, bl);
  ENCODE_FINISH(bl);
}

void pow2_hist_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(h, p);
  DECODE_FINISH(p);
}

void pow2_hist_t::generate_test_instances(std::list<pow2_hist_t*>& ls)
{
  ls.push_back(new pow2_hist_t);
  ls.push_back(new pow2_hist_t);
  ls.back()->h.push_back(1);
  ls.back()->h.push_back(3);
  ls.back()->h.push_back(0);
  ls.back()->h.push_back(2);
}

void pow2_hist_t::decay(int bits)
{
  for (std::vector<int32_t>::iterator p = h.begin(); p != h.end(); ++p) {
    *p >>= bits;
  }
  _contract();
}
