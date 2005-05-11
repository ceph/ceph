
#ifndef _Cond_Posix_
#define _Cond_Posix_

#include <time.h>

#include "Mutex.h"

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
	// timeval -> timespec
	struct timespec ts;
	ts.tv_sec = tv->tv_sec;
	ts.tv_nsec = tv->tv_usec*1000;

	int r = pthread_cond_timedwait(&C, &mutex.M, &ts);
	return r;
  }

  int Signal() { 
	int r = pthread_cond_signal(&C);
	return r;
  }
};

#endif // !_Cond_Posix_
