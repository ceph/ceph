#ifndef CEPH_THROTTLE_H
#define CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"
#include "include/atomic.h"

class Throttle {
  atomic_t count;
  size_t max, waiting;
  Mutex lock;
  Cond cond;
  
public:
  Throttle(size_t m = 0) : count(0), max(m), waiting(0),
			  lock("Throttle::lock") {
    assert(m >= 0);
}

private:
  void _reset_max(size_t m) {
    if (m < max)
      cond.SignalOne();
    max = m;
  }
  bool _should_wait(size_t c) {
    return
      max &&
      ((c < max && count.read() + c > max) ||   // normally stay under max
       (c >= max && count.read() > max));       // except for large c
  }
  bool _wait(size_t c) {
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
  size_t get_current() {
    return count.read();
  }

  size_t get_max() { return max; }

  bool wait(size_t m = 0) {
    Mutex::Locker l(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    return _wait(0);
  }

  size_t take(size_t c = 1) {
    assert(c >= 0);
    count.add(c);
    return count.read();
  }

  bool get(size_t c = 1, size_t m = 0) {
    assert(c >= 0);
    Mutex::Locker l(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    bool waited = _wait(c);
    count.add(c);
    return waited;
  }

  /* Returns true if it successfully got the requested amount,
   * or false if it would block.
   */
  bool get_or_fail(size_t c = 1) {
    assert (c >= 0);
    Mutex::Locker l(lock);
    if (_should_wait(c)) return false;
    count.add(c);
    return true;
  }

  size_t put(size_t c = 1) {
    assert(c >= 0);
    if (c) {
      count.sub(c);
      cond.SignalOne();
      assert(count.read() >= 0); //if count goes negative, we failed somewhere!
    }
    return count.read();
  }
};


#endif
