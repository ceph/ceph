#ifndef _CEPH_THROTTLE_H
#define _CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"

class Throttle {
  int64_t count, max, waiting;
  Mutex lock;
  Cond cond;
  
public:
  Throttle(int64_t m = 0) : count(0), max(m), waiting(0),
			  lock("Throttle::lock") {
    assert(m >= 0);
}

private:
  void _reset_max(int64_t m) {
    if (m < max)
      cond.SignalOne();
    max = m;
  }
  bool _should_wait(int64_t c) {
    return
      max &&
      ((c < max && count + c > max) ||   // normally stay under max
       (c >= max && count > max));       // except for large c
  }
  bool _wait(int64_t c) {
    bool waited = false;
    if (_should_wait(c)) {
      waiting += c;
      do {
	waited = true;
	cond.Wait(lock);
      } while (_should_wait(c));
      waiting -= c;

      // wake up the next guy
      if (waiting)
	cond.SignalOne();
    }
    return waited;
  }

public:
  int64_t get_current() {
    Mutex::Locker l(lock);
    return count;
  }

  int64_t get_max() { return max; }

  bool wait(int64_t m = 0) {
    Mutex::Locker l(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    return _wait(0);
  }

  int64_t take(int64_t c = 1) {
    assert(c >= 0);
    Mutex::Locker l(lock);
    count += c;
    return count;
  }

  bool get(int64_t c = 1, int64_t m = 0) {
    assert(c > 0);
    Mutex::Locker l(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    bool waited = _wait(c);
    count += c;
    return waited;
  }

  int64_t put(int64_t c = 1) {
    assert(c >= 0);
    Mutex::Locker l(lock);
    if (c) {
      cond.SignalOne();
      count -= c;
      assert(count >= 0); //if count goes negative, we failed somewhere!
    }
    return count;
  }
};


#endif
