
#ifndef __DECAYCOUNTER_H
#define __DECAYCOUNTER_H

#include <math.h>

class DecayCounter {
 protected:
  double val;              // value

  double half_life;        // in seconds
  double k;                // k = ln(.5)/half_life

  double last_decay;       // time of last decay

 public:
  DecayCounter(double hl) {
	set_halflife(hl);
	reset();
  }

  void set_halflife(double hl) {
	half_life = hl;
	k = log(.5) / hl;
  }

  double now() {
	// ??
	return 1.0;
  }

  void reset() {
	last_decay = 0.0;
	val = 0;
  }

  void decay() {
	double now = now();
	double el = now - last_decay;
	if (el > .1) {
	  val = val * exp(el * k);
	  last_decay = now;
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


#end;f
