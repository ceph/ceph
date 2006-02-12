// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#ifndef __DECAYCOUNTER_H
#define __DECAYCOUNTER_H

#include <math.h>
#include "Clock.h"

#include "config.h"

class DecayCounter {
 protected:
  double val;              // value

  double half_life;        // in seconds
  double k;                // k = ln(.5)/half_life

  utime_t last_decay;   // time of last decay

 public:
  DecayCounter() : val(0) {
	set_halflife( g_conf.mds_bal_interval );
	reset();
  }
  DecayCounter(double hl) : val(0) {
	set_halflife(hl);
	reset();
  }
  
  void adjust(double a) {
	decay();
	val += a;
  }
  void adjust_down(const DecayCounter& other) {
	// assume other has same time stamp as us...
	val -= other.val;
  }

  void set_halflife(double hl) {
	half_life = hl;
	k = log(.5) / hl;
  }

  void take(DecayCounter& other) {
	*this = other;
	other.reset();
  }

  void reset() {
	last_decay.sec_ref() = 0;
	last_decay.usec_ref() = 0;
	val = 0;
  }
  
  void decay() {
	utime_t el = g_clock.recent_now();
	el -= last_decay;
	if (el.sec() >= 1) {
	  val = val * exp((double)el * k);
	  if (val < .01) val = 0;
	  last_decay = g_clock.recent_now();
	}
  }

  double get() {
	decay();
	return val;
  }

  double hit(double v = 1.0) {
	decay();
	val += v;
	return val;
  }

};


#endif
