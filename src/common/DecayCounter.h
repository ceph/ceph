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

#include "include/buffer.h"
#include "common/Formatter.h"
#include "common/ceph_time.h"

#include <cmath>
#include <list>
#include <sstream>

/**
 *
 * TODO: normalize value based on some fucntion of half_life, 
 *  so that it can be interpreted as an approximation of a
 *  moving average of N seconds.  currently, changing half-life
 *  skews the scale of the value, even at steady state.  
 *
 */

class DecayRate {
public:
  friend class DecayCounter;

  DecayRate() {}
  // cppcheck-suppress noExplicitConstructor
  DecayRate(double hl) { set_halflife(hl); }
  DecayRate(const DecayRate &dr) : k(dr.k) {}

  void set_halflife(double hl) {
    k = log(.5) / hl;
  }    

private:
  double k = 0;             // k = ln(.5)/half_life
};

class DecayCounter {
public:
  using time = ceph::coarse_mono_time;
  using clock = ceph::coarse_mono_clock;

  DecayCounter() : DecayCounter(DecayRate()) {}
  explicit DecayCounter(const DecayRate &rate) : last_decay(clock::now()), rate(rate) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<DecayCounter*>& ls);

  /**
   * reading
   */

  double get() const {
    decay(rate);
    return val;
  }

  double get_last() const {
    return val;
  }
  
  double get_last_vel() const {
    return vel;
  }

  time get_last_decay() const {
    return last_decay; 
  }

  /**
   * adjusting
   */

  double hit(double v = 1.0) {
    decay(rate);
    val += v;
    return val;
  }

  void adjust(double a) {
    decay(rate);
    val += a;
    if (val < 0)
      val = 0;
  }

  void scale(double f) {
    val *= f;
    vel *= f;
  }

  /**
   * decay etc.
   */

  void reset() {
    last_decay = clock::now();
    val = vel = 0;
  }

protected:
  void decay(const DecayRate &rate) const;

private:
  mutable double val = 0.0;           // value
  mutable double vel = 0.0;           // recent velocity
  mutable time last_decay = clock::zero();   // time of last decay
  DecayRate rate;
};

inline void encode(const DecayCounter &c, bufferlist &bl) {
  c.encode(bl);
}
inline void decode(DecayCounter &c, bufferlist::const_iterator &p) {
  c.decode(p);
}

inline std::ostream& operator<<(std::ostream& out, const DecayCounter& d) {
  std::ostringstream oss;
  oss.precision(2);
  double val = d.get();
  oss << "[C " << std::scientific << val << "]";
  return out << oss.str();
}

#endif
