
#ifndef __CLOCK_H
#define __CLOCK_H

#include <sys/time.h>

class Clock {
 protected:
  struct timeval curtime;

 public:
  Clock();
  
  double gettime();
  void settime(double tm);
};

extern Clock g_clock;

#endif
