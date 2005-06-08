
#ifndef __DECAYCOUNTER_H
#define __DECAYCOUNTER_H

#include <math.h>
#include "Clock.h"


class DecayCounter {
 protected:
  double val;              // value

  double half_life;        // in seconds
  double k;                // k = ln(.5)/half_life

  timepair_t last_decay;   // time of last decay

 public:
  DecayCounter() {
	set_halflife( 10.0 );
	reset();
  }
  DecayCounter(double hl) {
	set_halflife(hl);
	reset();
  }
  
  void adjust(const timepair_t& now, double a) {
	decay(now);
	val += a;
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
	last_decay = timepair_t(0,0);
	val = 0;
  }
  
  void decay(const timepair_t& now) {
	double el = timepair_to_double(now) - timepair_to_double(last_decay);
	if (el > .5) {
	  val = val * exp(el * k);
	  last_decay = now;
	}
	if (val < .01) val = 0;
  }

  double get(const timepair_t& now) {
	decay(now);
	return val;
  }

  double hit(const timepair_t& now, double v = 1.0) {
	decay(now);
	val += v;
	return val;
  }

};


#endif
