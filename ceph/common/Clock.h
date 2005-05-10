
#ifndef __CLOCK_H
#define __CLOCK_H

#include <sys/time.h>
#include <time.h>

class Clock {
 protected:
  struct timeval curtime;

 public:
  Clock();
  
  void settime(double tm);

  double gettime();
  time_t get_unixtime() {
	return time(0);
  }

};

extern Clock g_clock;

#endif
