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

#include "DecayCounter.h"
#include "Formatter.h"

#include "include/encoding.h"

void DecayCounter::encode(ceph::buffer::list& bl) const
{
  decay();
  ENCODE_START(5, 4, bl);
  encode(val, bl);
  ENCODE_FINISH(bl);
}

void DecayCounter::decode(ceph::buffer::list::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, p);
  if (struct_v < 2) {
    double k = 0.0;
    decode(k, p);
  }
  if (struct_v < 3) {
    double k = 0.0;
    decode(k, p);
  }
  decode(val, p);
  if (struct_v < 5) {
    double delta, _;
    decode(delta, p);
    val += delta;
    decode(_, p); /* velocity */
  }
  last_decay = clock::now();
  DECODE_FINISH(p);
}

void DecayCounter::dump(ceph::Formatter *f) const
{
  decay();
  f->dump_float("value", val);
  f->dump_float("halflife", rate.get_halflife());
}

void DecayCounter::generate_test_instances(std::list<DecayCounter*>& ls)
{
  DecayCounter *counter = new DecayCounter();
  counter->val = 3.0;
  ls.push_back(counter);
  counter = new DecayCounter();
  ls.push_back(counter);
}

void DecayCounter::decay(double delta) const
{
  auto now = clock::now();
  double el = std::chrono::duration<double>(now - last_decay).count();

  // calculate new value
  double newval = val * exp(el * rate.k) + delta;
  if (newval < .01) {
    newval = 0.0;
  }

  val = newval;
  last_decay = now;
}
