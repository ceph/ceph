
#ifndef __CLOCK_H
#define __CLOCK_H

#include <sys/time.h>
#include <time.h>

class Clock {
 protected:
  struct timeval faketime;      // if we're faking.
  struct timeval start_offset;  // time of process startup.


 public:
  Clock();
  
  time_t gettime(struct timeval *ts=0);


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
