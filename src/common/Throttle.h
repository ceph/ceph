#ifndef _CEPH_THROTTLE_H
#define _CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"

class Throttle {
  uint64_t count, max, waiting;
  Mutex lock;
  Cond cond;
  
public:
  Throttle(uint64_t m = 0) : count(0), max(m), waiting(0),
			  lock("Throttle::lock") {}

private:
  void _reset_max(uint64_t m) {
    if (m < max)
      cond.SignalOne();
    max = m;
  }
  bool _wait(uint64_t c) {
    bool waited = false;
    if (max && count + c > max) {    
      waiting += c;
      while (max && count + c > max) {
	waited = true;
	cond.Wait(lock);
      }
      waiting -= c;

      // wake up the next guy
      if (waiting)
	cond.SignalOne();
    }
    return waited;
  }

public:
  uint64_t get_current() {
    Mutex::Locker l(lock);
    return count;
  }

  uint64_t get_max() { return max; }

  bool wait(uint64_t m = 0) {
    Mutex::Locker l(lock);
    if (m)
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
    if (m)
      _reset_max(m);
    bool waited = _wait(c);
    count += c;
    return waited;
  }

  uint64_t put(uint64_t c = 1) {
    Mutex::Locker l(lock);
    cond.SignalOne();
    count -= c;
    return count;
  }
};


#endif
