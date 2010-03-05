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

#ifndef __DECAYCOUNTER_H
#define __DECAYCOUNTER_H

#include <math.h>
#include "Clock.h"

#include "config.h"

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
  DecayRate(double hl) { set_halflife(hl); }
  void set_halflife(double hl) {
    k = log(.5) / hl;
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

  void encode(bufferlist& bl) const {
    __u8 struct_v = 3;
    ::encode(struct_v, bl);
    ::encode(val, bl);
    ::encode(delta, bl);
    ::encode(vel, bl);
  }
  void decode(bufferlist::iterator &p) {
    __u8 struct_v;
    ::decode(struct_v, p);
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
  }

  DecayCounter() : val(0), delta(0), vel(0) {
    reset();
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

  void reset() {
    reset(g_clock.now());
  }
  void reset(utime_t now) {
    last_decay = g_clock.now();
    val = delta = 0;
  }
  
  void decay(utime_t now, const DecayRate &rate) {
    utime_t el = now;
    el -= last_decay;

    if (el.sec() >= 1) {
      // calculate new value
      double newval = (val+delta) * exp((double)el * rate.k);
      if (newval < .01) newval = 0.0;
      
      // calculate velocity approx
      vel += (newval - val) * (double)el;
      vel *= exp((double)el * rate.k);
      
      val = newval;
      delta = 0;
      last_decay = now;
    }
  }
};

WRITE_CLASS_ENCODER(DecayCounter)


#endif
