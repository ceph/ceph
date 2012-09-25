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

void DecayCounter::encode(bufferlist& bl) const
{
  ENCODE_START(4, 4, bl);
  ::encode(val, bl);
  ::encode(delta, bl);
  ::encode(vel, bl);
  ENCODE_FINISH(bl);
}

void DecayCounter::decode(const utime_t &t, bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, p);
  if (struct_v < 2) {
    double half_life;
    ::decode(half_life, p);
  }
  if (struct_v < 3) {
    double k;
    ::decode(k, p);
  }
  ::decode(val, p);
  ::decode(delta, p);
  ::decode(vel, p);
  DECODE_FINISH(p);
}

void DecayCounter::dump(Formatter *f) const
{
  f->dump_float("value", val);
  f->dump_float("delta", delta);
  f->dump_float("velocity", vel);
}

void DecayCounter::generate_test_instances(list<DecayCounter*>& ls)
{
  utime_t fake_time;
  DecayCounter *counter = new DecayCounter(fake_time);
  counter->val = 3.0;
  counter->delta = 2.0;
  counter->vel = 1.0;
  ls.push_back(counter);
  counter = new DecayCounter(fake_time);
  ls.push_back(counter);
}

void DecayCounter::decay(utime_t now, const DecayRate &rate)
{
  utime_t el = now;
  el -= last_decay;

  if (el.sec() >= 1) {
    // calculate new value
    double newval = (val+delta) * exp((double)el * rate.k);
    if (newval < .01)
      newval = 0.0;

    // calculate velocity approx
    vel += (newval - val) * (double)el;
    vel *= exp((double)el * rate.k);

    val = newval;
    delta = 0;
    last_decay = now;
  }
}
