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

void DecayCounter::encode(bufferlist& bl) const
{
  ENCODE_START(4, 4, bl);
  encode(val, bl);
  encode(delta, bl);
  encode(vel, bl);
  ENCODE_FINISH(bl);
}

void DecayCounter::decode(bufferlist::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, p);
  if (struct_v < 2) {
    double half_life = 0.0;
    decode(half_life, p);
  }
  if (struct_v < 3) {
    double k = 0.0;
    decode(k, p);
  }
  decode(val, p);
  decode(delta, p);
  decode(vel, p);
  last_decay = clock::now();
  DECODE_FINISH(p);
}

void DecayCounter::dump(Formatter *f) const
{
  decay(rate);
  f->dump_float("value", val);
  f->dump_float("delta", delta);
  f->dump_float("velocity", vel);
}

void DecayCounter::generate_test_instances(std::list<DecayCounter*>& ls)
{
  DecayCounter *counter = new DecayCounter();
  counter->val = 3.0;
  counter->delta = 2.0;
  counter->vel = 1.0;
  ls.push_back(counter);
  counter = new DecayCounter();
  ls.push_back(counter);
}

void DecayCounter::decay(const DecayRate &rate) const
{
  auto now = clock::now();
  if (now > last_decay) {
    double el = std::chrono::duration<double>(now - last_decay).count();
    if (el <= 0.1)
      return; /* no need to decay for small differences */

    // calculate new value
    double newval = (val+delta) * exp(el * rate.k);
    if (newval < .01) {
      newval = 0.0;
    }

    // calculate velocity approx
    vel += (newval - val) * el;
    vel *= exp(el * rate.k);

    val = newval;
    delta = 0;
    last_decay = now;
  }
}
