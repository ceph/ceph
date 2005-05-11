#include <sys/types.h>
#include "Clock.h"

#include "include/config.h"

#ifndef NULL
#define NULL 0
#endif

// public
Clock g_clock;

// class definition

// cons
Clock::Clock() {
  // set offset to now
  start_offset.tv_sec = 0;
  start_offset.tv_usec = 0;
  //gettime(&start_offset);
}


time_t Clock::gettime(struct timeval *ts) 
{
  if (g_conf.fake_clock) {
	faketime.tv_usec += 100;
	while (faketime.tv_usec > 1000000) {
	  faketime.tv_sec++;
	  faketime.tv_usec -= 1000000;
	}
	if (ts) 
	  *ts = faketime;
	return faketime.tv_sec;
  } else {
	// get actual time
	struct timeval curtime;
	gettimeofday(&curtime,NULL);
	
	sub(&curtime, &start_offset);

	if (ts)
	  *ts = curtime;
	return curtime.tv_sec;
  }
}

