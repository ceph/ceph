#ifndef _CEPH_THROTTLE_H
#define _CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"

class Throttle {
  __u64 count, want, max;
  Mutex lock;
  Cond cond;
  
public:
  Throttle(__u64 m = 0) : count(0), max(m),
			  lock("Throttle::lock") {}

private:
  void _reset_max(__u64 m) {
    if (m) {
      if (m < max)
	cond.SignalAll();
      max = m;
    }
  }
  bool _wait(__u64 c) {
    bool waited = false;
    while (max && count + c > max) {
      waited = true;
      cond.Wait(lock);
    }
    return waited;
  }

public:
  __u64 get_current() {
    Mutex::Locker l(lock);
    return count;
  }

  bool wait(__u64 m = 0) {
    Mutex::Locker l(lock);
    _reset_max(m);
    return _wait(0);
  }

  __u64 take(__u64 c = 1) {
    Mutex::Locker l(lock);
    count += c;
    return count;
  }

  bool get(__u64 c = 1, __u64 m = 0) {
    Mutex::Locker l(lock);
    _reset_max(m);
    bool waited = _wait(c);
    count += c;
    return waited;
  }

  __u64 put(__u64 c = 1) {
    Mutex::Locker l(lock);
    cond.SignalAll();
    count -= c;
    return count;
  }
};


#endif
