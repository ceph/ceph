#ifndef _CEPH_THROTTLE_H
#define _CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"

class Throttle {
  uint64_t count, want, max;
  Mutex lock;
  Cond cond;
  
public:
  Throttle(uint64_t m = 0) : count(0), max(m),
			  lock("Throttle::lock") {}

private:
  void _reset_max(uint64_t m) {
    if (m) {
      if (m < max)
	cond.SignalAll();
      max = m;
    }
  }
  bool _wait(uint64_t c) {
    bool waited = false;
    while (max && count + c > max) {
      waited = true;
      cond.Wait(lock);
    }
    return waited;
  }

public:
  uint64_t get_current() {
    Mutex::Locker l(lock);
    return count;
  }

  bool wait(uint64_t m = 0) {
    Mutex::Locker l(lock);
    _reset_max(m);
    return _wait(0);
  }

  uint64_t take(uint64_t c = 1) {
    Mutex::Locker l(lock);
    count += c;
    return count;
  }

  bool get(uint64_t c = 1, uint64_t m = 0) {
    Mutex::Locker l(lock);
    _reset_max(m);
    bool waited = _wait(c);
    count += c;
    return waited;
  }

  uint64_t put(uint64_t c = 1) {
    Mutex::Locker l(lock);
    cond.SignalAll();
    count -= c;
    return count;
  }
};


#endif
