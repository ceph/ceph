#ifndef _CEPH_THROTTLE_H
#define _CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"

class Throttle {
  __u64 count, max, waiting;
  Mutex lock;
  Cond cond;
  
public:
  Throttle(__u64 m = 0) : count(0), max(m), waiting(0),
			  lock("Throttle::lock") {}

private:
  void _reset_max(__u64 m) {
    if (m < max)
      cond.SignalOne();
    max = m;
  }
  bool _wait(__u64 c) {
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
  __u64 get_current() {
    Mutex::Locker l(lock);
    return count;
  }

  bool wait(__u64 m = 0) {
    Mutex::Locker l(lock);
    if (m)
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
    if (m)
      _reset_max(m);
    bool waited = _wait(c);
    count += c;
    return waited;
  }

  __u64 put(__u64 c = 1) {
    Mutex::Locker l(lock);
    cond.SignalOne();
    count -= c;
    return count;
  }
};


#endif
