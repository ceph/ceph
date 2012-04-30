#ifndef CEPH_THROTTLE_H
#define CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"
#include <list>

class CephContext;

class Throttle {
  CephContext *cct;
  std::string name;
  int64_t count, max;
  Mutex lock;
  list<Cond*> cond;
  
public:
  Throttle(CephContext *cct, std::string n, int64_t m = 0);
  ~Throttle();

private:
  void _reset_max(int64_t m) {
    if (m < max && !cond.empty())
      cond.front()->SignalOne();
    max = m;
  }
  bool _should_wait(int64_t c) {
    return
      max &&
      ((c <= max && count + c > max) ||   // normally stay under max
       (c >= max && count > max));       // except for large c
  }

  bool _wait(int64_t c);

public:
  int64_t get_current() {
    Mutex::Locker l(lock);
    return count;
  }

  int64_t get_max() { return max; }

  bool wait(int64_t m = 0);

  int64_t take(int64_t c = 1);
  bool get(int64_t c = 1, int64_t m = 0);

  /**
   * Returns true if it successfully got the requested amount,
   * or false if it would block.
   */
  bool get_or_fail(int64_t c = 1);
  int64_t put(int64_t c = 1);
};


#endif
