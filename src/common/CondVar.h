#ifndef CEPH_COND_VAR_H
#define CEPH_COND_VAR_H

#include "include/utime.h"

#include "Clock.h"
#include "Mutex.h"
#include "pthread.h"

class Cond {
  // my bits
  pthread_cond_t _c;

  Mutex *waiter_mutex;

  // don't allow copying.
  void operator=(Cond &C);
  Cond(const Cond &C);

 public:
  Cond() : waiter_mutex(NULL) {
    int r = pthread_cond_init(&_c,NULL);
    assert(r == 0);
  }
  virtual ~Cond() { 
    pthread_cond_destroy(&_c); 
  }

  int Wait(Mutex &mutex)  { 
    // make sure this cond is used with one mutex only
    assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    assert(mutex.is_locked());

    mutex._pre_unlock();
    int r = pthread_cond_wait(&_c, &mutex._m);
    mutex._post_lock();
    return r;
  }

  int WaitUntil(Mutex &mutex, utime_t when) {
    // make sure this cond is used with one mutex only
    assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    assert(mutex.is_locked());

    struct timespec ts;
    when.to_timespec(&ts);

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  int WaitInterval(Mutex &mutex, utime_t interval) {
    utime_t when = ceph_clock_now();
    when += interval;
    return WaitUntil(mutex, when);
  }

  template<typename Duration>
  int WaitInterval(Mutex &mutex, Duration interval) {
    ceph::real_time when(ceph::real_clock::now());
    when += interval;

    struct timespec ts = ceph::real_clock::to_timespec(when);

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  int SloppySignal() { 
    int r = pthread_cond_broadcast(&_c);
    return r;
  }
  int Signal() { 
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }
  int SignalOne() { 
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_signal(&_c);
    return r;
  }
  int SignalAll() { 
    // make sure signaler is holding the waiter's lock.
    assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }
};

#endif // CEPH_COND_VAR_H
