
#ifndef __DECAYCOUNTER_H
#define __DECAYCOUNTER_H

#include <math.h>
#include "Clock.h"

class DecayCounter {
 protected:
  double val;              // value

  double half_life;        // in seconds
  double k;                // k = ln(.5)/half_life

  double last_decay;       // time of last decay

 public:
  DecayCounter() {
	set_halflife( 10.0 );
	reset();
  }
  DecayCounter(double hl) {
	set_halflife(hl);
	reset();
  }
  
  void adjust(double a) {
	decay();
	val += a;
  }

  void set_halflife(double hl) {
	half_life = hl;
	k = log(.5) / hl;
  }

  double getnow() {
	return g_clock.gettime();
  }

  void reset() {
	last_decay = 0.0;
	val = 0;
  }

  void decay() {
	double tnow = getnow();
	double el = tnow - last_decay;
	if (el > .5) {
	  val = val * exp(el * k);
	  last_decay = tnow;
	}
	if (val < .01) val = 0;
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
