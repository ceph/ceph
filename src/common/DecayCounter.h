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

#ifndef CEPH_DECAYCOUNTER_H
#define CEPH_DECAYCOUNTER_H

#include "include/utime.h"

#include <math.h>

/**
 *
 * TODO: normalize value based on some fucntion of half_life, 
 *  so that it can be interpreted as an approximation of a
 *  moving average of N seconds.  currently, changing half-life
 *  skews the scale of the value, even at steady state.  
 *
 */

class DecayRate {
  double k;             // k = ln(.5)/half_life

  friend class DecayCounter;

public:
  DecayRate() : k(0) {}
  // cppcheck-suppress noExplicitConstructor
  DecayRate(double hl) { set_halflife(hl); }
  void set_halflife(double hl) {
    k = ::log(.5) / hl;
  }    
};

class DecayCounter {
 protected:
public:
  double val;           // value
  double delta;         // delta since last decay
  double vel;           // recent velocity
  utime_t last_decay;   // time of last decay

 public:

  void encode(bufferlist& bl) const;
  void decode(const utime_t &t, bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<DecayCounter*>& ls);

  explicit DecayCounter(const utime_t &now)
    : val(0), delta(0), vel(0), last_decay(now)
  {
  }

  // these two functions are for the use of our dencoder testing infrastructure
  DecayCounter() : val(0), delta(0), vel(0), last_decay() {}

  void decode(bufferlist::iterator& p) {
    utime_t fake_time;
    decode(fake_time, p);
  }

  /**
   * reading
   */

  double get(utime_t now, const DecayRate& rate) {
    decay(now, rate);
    return val;
  }

  double get_last() {
    return val;
  }
  
  double get_last_vel() {
    return vel;
  }

  utime_t get_last_decay() { 
    return last_decay; 
  }

  /**
   * adjusting
   */

  double hit(utime_t now, const DecayRate& rate, double v = 1.0) {
    decay(now, rate);
    delta += v;
    return val+delta;
  }

  void adjust(double a) {
    val += a;
  }
  void adjust(utime_t now, const DecayRate& rate, double a) {
    decay(now, rate);
    val += a;
  }
  void scale(double f) {
    val *= f;
    delta *= f;    
    vel *= f;
  }

  /**
   * decay etc.
   */

  void reset(utime_t now) {
    last_decay = now;
    val = delta = 0;
  }

  void decay(utime_t now, const DecayRate &rate);
};

inline void encode(const DecayCounter &c, bufferlist &bl) { c.encode(bl); }
inline void decode(DecayCounter &c, const utime_t &t, bufferlist::iterator &p) {
  c.decode(t, p);
}


#endif
