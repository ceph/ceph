// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#ifndef _Cond_Posix_
#define _Cond_Posix_

#include <time.h>

#include "Mutex.h"
#include "Clock.h"

#include "include/Context.h"

#include <pthread.h>
#include <cassert>

class Cond
{
  mutable pthread_cond_t C;

  void operator=(Cond &C) {}
  Cond( const Cond &C ) {}

 public:

  Cond() {
    int r = pthread_cond_init(&C,NULL);
	assert(r == 0);
  }

  virtual ~Cond() { 
	pthread_cond_destroy(&C); 
  }

  int Wait(Mutex &mutex)  { 
	int r = pthread_cond_wait(&C, &mutex.M);
	return r;
  }

  int Wait(Mutex &mutex, char* s)  { 
	cout << "Wait: " << s << endl;
	int r = pthread_cond_wait(&C, &mutex.M);
	return r;
  }

  int WaitUntil(Mutex &mutex,
		   struct timeval *tv) {
	return WaitUntil(mutex, utime_t(tv->tv_sec, tv->tv_usec));
  }

  int WaitUntil(Mutex &mutex, utime_t when) {
	// timeval -> timespec
	struct timespec ts;
	memset(&ts, 0, sizeof(ts));
	ts.tv_sec = when.sec();
	ts.tv_nsec = when.nsec();
	//cout << "timedwait for " << ts.tv_sec << " sec " << ts.tv_nsec << " nsec" << endl;
	int r = pthread_cond_timedwait(&C, &mutex.M, &ts);
	return r;
  }
  int WaitInterval(Mutex &mutex, utime_t interval) {
	utime_t when = g_clock.real_now();
	when += interval;
	return WaitUntil(mutex, when);
  }

  int Signal() { 
	//int r = pthread_cond_signal(&C);
	int r = pthread_cond_broadcast(&C);
	return r;
  }
  int SignalOne() { 
	int r = pthread_cond_signal(&C);
	return r;
  }
  int SignalAll() { 
	//int r = pthread_cond_signal(&C);
	int r = pthread_cond_broadcast(&C);
	return r;
  }
};

class C_Cond : public Context {
  Cond *cond;
  bool *done;
  int *rval;
public:
  C_Cond(Cond *c, bool *d, int *r=0) : cond(c), done(d), rval(r) {
	*done = false;
  }
  void finish(int r) {
	if (rval) *rval = r;
	*done = true;
	cond->Signal();
  }
};

class C_SafeCond : public Context {
  Mutex *lock;
  Cond *cond;
  bool *done;
  int *rval;
public:
  C_SafeCond(Mutex *l, Cond *c, bool *d, int *r=0) : lock(l), cond(c), done(d), rval(r) {
	*done = false;
  }
  void finish(int r) {
	lock->Lock();
	if (rval) *rval = r;
	*done = true;
	cond->Signal();
	lock->Unlock();
  }
};

#endif // !_Cond_Posix_
