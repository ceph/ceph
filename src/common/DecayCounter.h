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
#include "common/StackStringStream.h"
#include "common/ceph_time.h"

#include <cmath>
#include <list>
#include <sstream>

namespace ceph { class Formatter; }

/**
 *
 * TODO: normalize value based on some function of half_life, 
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
  double get_halflife() const {
    return log(.5) / k;
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

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<DecayCounter*>& ls);

  /**
   * reading
   */

  double get() const {
    decay();
    return val;
  }

  double get_last() const {
    return val;
  }
  
  time get_last_decay() const {
    return last_decay; 
  }

  /**
   * adjusting
   */

  double hit(double v = 1.0) {
    decay(v);
    return val;
  }
  void adjust(double v = 1.0) {
    decay(v);
  }

  void scale(double f) {
    val *= f;
  }

  /**
   * decay etc.
   */

  void reset() {
    last_decay = clock::now();
    val = 0;
  }

protected:
  void decay(double delta) const;
  void decay() const {decay(0.0);}

private:
  mutable double val = 0.0;           // value
  mutable time last_decay = clock::zero();   // time of last decay
  DecayRate rate;
};

inline void encode(const DecayCounter &c, ceph::buffer::list &bl) {
  c.encode(bl);
}
inline void decode(DecayCounter &c, ceph::buffer::list::const_iterator &p) {
  c.decode(p);
}

inline std::ostream& operator<<(std::ostream& out, const DecayCounter& d) {
  CachedStackStringStream css;
  css->precision(2);
  double val = d.get();
  *css << "[C " << std::scientific << val << "]";
  return out << css->strv();
}

#endif
