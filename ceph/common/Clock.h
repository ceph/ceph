
#ifndef __CLOCK_H
#define __CLOCK_H

#include <sys/time.h>
#include <time.h>

#include <list>
using namespace std;

// --- time stuff ---
typedef pair<time_t, long> timepair_t;  // struct timeval is a PITA

// addition, subtraction
inline timepair_t operator+(timepair_t& l, timepair_t& r) {
  return timepair_t( l.first + r.first + (l.second+r.second)/1000000L,
					 (l.second+r.second)%1000000L );
}
inline timepair_t& operator+=(timepair_t& l, timepair_t& r) {
  l.first += r.first + (l.second+r.second)/1000000L;
  l.second += r.second;
  l.second %= 1000000L;
  return l;
}

inline timepair_t operator-(timepair_t& l, timepair_t& r) {
  return timepair_t( l.first - r.first - (l.second<r.second ? 1:0),
					 l.second - r.second + (l.second<r.second ? 1000000:0) );
}
inline timepair_t& operator-=(timepair_t& l, timepair_t& r) {
  l.first -= r.first;
  if (l.second >= r.second)
	l.second -= r.second;
  else {
	l.second += 1000000L - r.second;
	l.first--;
  }
}
inline double timepair_to_double(const timepair_t &t) {
  return (double)t.first + ((double)t.second / 1000000.0L);
}


// -- clock --
class Clock {
 protected:
  struct timeval faketime;      // if we're faking.
  struct timeval start_offset;  // time of process startup.


 public:
  Clock();
  
  time_t gettime(struct timeval *ts=0);
  timepair_t gettimepair();

  void sub(struct timeval *a, struct timeval *b) {
	a->tv_sec -= b->tv_sec;

	if (a->tv_usec - b->tv_usec >= 0)
	  a->tv_usec -= b->tv_usec;
	else { // borrow from seconds
	  a->tv_usec = a->tv_usec + 1000000 - b->tv_usec;
	  a->tv_sec--;
	}
  }
  
  
  void add(struct timeval *a, struct timeval *b) {
	a->tv_sec += b->tv_sec;
	a->tv_usec += b->tv_usec;
	if (a->tv_usec > 1000000) {
	  a->tv_sec++;
	  a->tv_usec -= 1000000;
	}
  }
  

};

extern Clock g_clock;

#endif
