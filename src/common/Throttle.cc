
#include "common/Throttle.h"
#include "common/dout.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "throttle(" << name << " " << (void*)this << ") "

enum {
  l_throttle_first = 532430,
  l_throttle_val,
  l_throttle_max,
  l_throttle_get,
  l_throttle_get_sum,
  l_throttle_get_or_fail_fail,
  l_throttle_get_or_fail_success,
  l_throttle_take,
  l_throttle_take_sum,
  l_throttle_put,
  l_throttle_put_sum,
  l_throttle_wait,
  l_throttle_last,
};

Throttle::Throttle(CephContext *cct, std::string n, int64_t m)
  : cct(cct), name(n),
		max(m),
    lock("Throttle::lock")
{
  assert(m >= 0);

  PerfCountersBuilder b(cct, string("throttle-") + name, l_throttle_first, l_throttle_last);
  b.add_u64_counter(l_throttle_val, "val");
  b.add_u64_counter(l_throttle_max, "max");
  b.add_u64_counter(l_throttle_get, "get");
  b.add_u64_counter(l_throttle_get_sum, "get_sum");
  b.add_u64_counter(l_throttle_get_or_fail_fail, "get_or_fail_fail");
  b.add_u64_counter(l_throttle_get_or_fail_success, "get_or_fail_success");
  b.add_u64_counter(l_throttle_take, "take");
  b.add_u64_counter(l_throttle_take_sum, "take_sum");
  b.add_u64_counter(l_throttle_put, "put");
  b.add_u64_counter(l_throttle_put_sum, "put_sum");
  b.add_fl_avg(l_throttle_wait, "wait");

  logger = b.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  logger->set(l_throttle_max, max.read());
}

Throttle::~Throttle()
{
  while (!cond.empty()) {
    Cond *cv = cond.front();
    delete cv;
    cond.pop_front();
  }

  cct->get_perfcounters_collection()->remove(logger);
  delete logger;
}

void Throttle::_reset_max(int64_t m)
{
  assert(lock.is_locked());
  if (m < ((int64_t)max.read()) && !cond.empty())
    cond.front()->SignalOne();
  logger->set(l_throttle_max, m);
  max.set((size_t)m);
}

bool Throttle::_wait(int64_t c)
{
  utime_t start;
  bool waited = false;
  if (_should_wait(c) || !cond.empty()) { // always wait behind other waiters.
    Cond *cv = new Cond;
    cond.push_back(cv);
    do {
      if (!waited) {
	ldout(cct, 2) << "_wait waiting..." << dendl;
	start = ceph_clock_now(cct);
      }
      waited = true;
      cv->Wait(lock);
    } while (_should_wait(c) || cv != cond.front());

    if (waited) {
      ldout(cct, 3) << "_wait finished waiting" << dendl;
      utime_t dur = ceph_clock_now(cct) - start;
      logger->finc(l_throttle_wait, dur);
    }

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
  ldout(cct, 10) << "wait" << dendl;
  return _wait(0);
}

int64_t Throttle::take(int64_t c)
{
  assert(c >= 0);
  ldout(cct, 10) << "take " << c << dendl;
  {
    Mutex::Locker l(lock);
    count.add(c);
  }
  logger->inc(l_throttle_take);
  logger->inc(l_throttle_take_sum, c);
  logger->set(l_throttle_val, count.read());
  return count.read();
}

bool Throttle::get(int64_t c, int64_t m)
{
  assert(c >= 0);
  ldout(cct, 10) << "get " << c << " (" << count.read() << " -> " << (count.read() + c) << ")" << dendl;
  bool waited = false;
  {
    Mutex::Locker l(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    waited = _wait(c);
    count.add(c);
  }
  logger->inc(l_throttle_get);
  logger->inc(l_throttle_get_sum, c);
  logger->set(l_throttle_val, count.read());
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
    ldout(cct, 10) << "get_or_fail " << c << " failed" << dendl;
    logger->inc(l_throttle_get_or_fail_fail);
    return false;
  } else {
    ldout(cct, 10) << "get_or_fail " << c << " success (" << count.read() << " -> " << (count.read() + c) << ")" << dendl;
    count.add(c);
    logger->inc(l_throttle_get_or_fail_success);
    logger->inc(l_throttle_get);
    logger->inc(l_throttle_get_sum, c);
    logger->set(l_throttle_val, count.read());
    return true;
  }
}

int64_t Throttle::put(int64_t c)
{
  assert(c >= 0);
  ldout(cct, 10) << "put " << c << " (" << count.read() << " -> " << (count.read()-c) << ")" << dendl;
  Mutex::Locker l(lock);
  if (c) {
    if (!cond.empty())
      cond.front()->SignalOne();
    assert(((int64_t)count.read()) >= c); //if count goes negative, we failed somewhere!
    count.sub(c);
    logger->inc(l_throttle_put);
    logger->inc(l_throttle_put_sum, c);
    logger->set(l_throttle_val, count.read());
  }
  return count.read();
}
