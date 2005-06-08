
#ifndef _Cond_Posix_
#define _Cond_Posix_

#include <time.h>

#include "Mutex.h"
#include "Clock.h"

#include <pthread.h>
#include <cassert>

class Cond
{
  mutable pthread_cond_t C;

  void operator=(Cond &C) {}
  Cond( const Cond &C ) {}

 public:

  Cond() {
    pthread_cond_init(&C,NULL);
  }

  virtual ~Cond() { 
	pthread_cond_destroy(&C); 
  }

  int Wait(Mutex &mutex)  { 
	int r = pthread_cond_wait(&C, &mutex.M);
	return r;
  }

  int Wait(Mutex &mutex,
		   struct timeval *tv) {
	Wait(mutex, timepair_t(tv->tv_sec, tv->tv_usec));
  }
  int Wait(Mutex &mutex,
		   timepair_t when) {
	// timeval -> timespec
	struct timespec ts;
	ts.tv_sec = when.first;
	ts.tv_nsec = when.second*1000;
	int r = pthread_cond_timedwait(&C, &mutex.M, &ts);
	return r;
  }

  int Signal() { 
	int r = pthread_cond_signal(&C);
	return r;
  }
};

#endif // !_Cond_Posix_
