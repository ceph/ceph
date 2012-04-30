
#include "common/Throttle.h"
#include "common/dout.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "throttle(" << name << " " << (void*)this << ") "


Throttle::Throttle(CephContext *cct, std::string n, int64_t m)
  : cct(cct), name(n),
    count(0), max(m),
    lock("Throttle::lock")
{
  assert(m >= 0);
}

Throttle::~Throttle()
{
  while (!cond.empty()) {
    Cond *cv = cond.front();
    delete cv;
    cond.pop_front();
  }
}

bool Throttle::_wait(int64_t c)
{
  bool waited = false;
  if (_should_wait(c) || !cond.empty()) { // always wait behind other waiters.
    Cond *cv = new Cond;
    cond.push_back(cv);
    do {
      if (!waited)
	ldout(cct, 2) << "_wait waiting..." << dendl;
      waited = true;
      cv->Wait(lock);
    } while (_should_wait(c) || cv != cond.front());

    if (waited)
      ldout(cct, 3) << "_wait finished waiting" << dendl;

    delete cv;
    cond.pop_front();

    // wake up the next guy
    if (!cond.empty())
      cond.front()->SignalOne();
  }
  return waited;
}

bool Throttle::wait(int64_t m)
{
  Mutex::Locker l(lock);
  if (m) {
    assert(m > 0);
    _reset_max(m);
  }
  ldout(cct, 5) << "wait" << dendl;
  return _wait(0);
}

int64_t Throttle::take(int64_t c)
{
  assert(c >= 0);
  Mutex::Locker l(lock);
  ldout(cct, 5) << "take " << c << dendl;
  count += c;
  return count;
}

bool Throttle::get(int64_t c, int64_t m)
{
  assert(c >= 0);
  Mutex::Locker l(lock);
  ldout(cct, 5) << "get " << c << " (" << count << " -> " << (count + c) << ")" << dendl;
  if (m) {
    assert(m > 0);
    _reset_max(m);
  }
  bool waited = _wait(c);
  count += c;
  return waited;
}

/* Returns true if it successfully got the requested amount,
 * or false if it would block.
 */
bool Throttle::get_or_fail(int64_t c)
{
  assert (c >= 0);
  Mutex::Locker l(lock);
  if (_should_wait(c) || !cond.empty()) {
    ldout(cct, 2) << "get_or_fail " << c << " failed" << dendl;
    return false;
  } else {
    ldout(cct, 5) << "get_or_fail " << c << " success (" << count << " -> " << (count + c) << ")" << dendl;
    count += c;
    return true;
  }
}

int64_t Throttle::put(int64_t c)
{
  assert(c >= 0);
  Mutex::Locker l(lock);
  ldout(cct, 5) << "put " << c << " (" << cout << " -> " << (count-c) << ")" << dendl;
  if (c) {
    if (!cond.empty())
      cond.front()->SignalOne();
    count -= c;
    assert(count >= 0); //if count goes negative, we failed somewhere!
  }
  return count;
}
