// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <thread>

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

Throttle::Throttle(CephContext *cct, const std::string& n, int64_t m, bool _use_perf)
  : cct(cct), name(n), logger(NULL),
    max(m),
    lock("Throttle::lock"),
    use_perf(_use_perf)
{
  assert(m >= 0);

  if (!use_perf)
    return;

  if (cct->_conf->throttler_perf_counter) {
    PerfCountersBuilder b(cct, string("throttle-") + name, l_throttle_first, l_throttle_last);
    b.add_u64(l_throttle_val, "val", "Currently available throttle");
    b.add_u64(l_throttle_max, "max", "Max value for throttle");
    b.add_u64_counter(l_throttle_get, "get", "Gets");
    b.add_u64_counter(l_throttle_get_sum, "get_sum", "Got data");
    b.add_u64_counter(l_throttle_get_or_fail_fail, "get_or_fail_fail", "Get blocked during get_or_fail");
    b.add_u64_counter(l_throttle_get_or_fail_success, "get_or_fail_success", "Successful get during get_or_fail");
    b.add_u64_counter(l_throttle_take, "take", "Takes");
    b.add_u64_counter(l_throttle_take_sum, "take_sum", "Taken data");
    b.add_u64_counter(l_throttle_put, "put", "Puts");
    b.add_u64_counter(l_throttle_put_sum, "put_sum", "Put data");
    b.add_time_avg(l_throttle_wait, "wait", "Waiting latency");

    logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
    logger->set(l_throttle_max, max.read());
  }
}

Throttle::~Throttle()
{
  while (!cond.empty()) {
    Cond *cv = cond.front();
    delete cv;
    cond.pop_front();
  }

  if (!use_perf)
    return;

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
  }
}

void Throttle::_reset_max(int64_t m)
{
  assert(lock.is_locked());
  if ((int64_t)max.read() == m)
    return;
  if (!cond.empty())
    cond.front()->SignalOne();
  if (logger)
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
    waited = true;
    ldout(cct, 2) << "_wait waiting..." << dendl;
    if (logger)
      start = ceph_clock_now(cct);

    do {
      cv->Wait(lock);
    } while (_should_wait(c) || cv != cond.front());

    ldout(cct, 2) << "_wait finished waiting" << dendl;
    if (logger) {
      utime_t dur = ceph_clock_now(cct) - start;
      logger->tinc(l_throttle_wait, dur);
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
  if (0 == max.read() && 0 == m) {
    return false;
  }

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
  if (0 == max.read()) {
    return 0;
  }
  assert(c >= 0);
  ldout(cct, 10) << "take " << c << dendl;
  {
    Mutex::Locker l(lock);
    count.add(c);
  }
  if (logger) {
    logger->inc(l_throttle_take);
    logger->inc(l_throttle_take_sum, c);
    logger->set(l_throttle_val, count.read());
  }
  return count.read();
}

bool Throttle::get(int64_t c, int64_t m)
{
  if (0 == max.read() && 0 == m) {
    return false;
  }

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
  if (logger) {
    logger->inc(l_throttle_get);
    logger->inc(l_throttle_get_sum, c);
    logger->set(l_throttle_val, count.read());
  }
  return waited;
}

/* Returns true if it successfully got the requested amount,
 * or false if it would block.
 */
bool Throttle::get_or_fail(int64_t c)
{
  if (0 == max.read()) {
    return true;
  }

  assert (c >= 0);
  Mutex::Locker l(lock);
  if (_should_wait(c) || !cond.empty()) {
    ldout(cct, 10) << "get_or_fail " << c << " failed" << dendl;
    if (logger) {
      logger->inc(l_throttle_get_or_fail_fail);
    }
    return false;
  } else {
    ldout(cct, 10) << "get_or_fail " << c << " success (" << count.read() << " -> " << (count.read() + c) << ")" << dendl;
    count.add(c);
    if (logger) {
      logger->inc(l_throttle_get_or_fail_success);
      logger->inc(l_throttle_get);
      logger->inc(l_throttle_get_sum, c);
      logger->set(l_throttle_val, count.read());
    }
    return true;
  }
}

int64_t Throttle::put(int64_t c)
{
  if (0 == max.read()) {
    return 0;
  }

  assert(c >= 0);
  ldout(cct, 10) << "put " << c << " (" << count.read() << " -> " << (count.read()-c) << ")" << dendl;
  Mutex::Locker l(lock);
  if (c) {
    if (!cond.empty())
      cond.front()->SignalOne();
    assert(((int64_t)count.read()) >= c); //if count goes negative, we failed somewhere!
    count.sub(c);
    if (logger) {
      logger->inc(l_throttle_put);
      logger->inc(l_throttle_put_sum, c);
      logger->set(l_throttle_val, count.read());
    }
  }
  return count.read();
}

void Throttle::reset()
{
  Mutex::Locker l(lock);
  if (!cond.empty())
    cond.front()->SignalOne();
  count.set(0);
  if (logger) {
    logger->set(l_throttle_val, 0);
  }
}

bool BackoffThrottle::set_params(
  double _low_threshhold,
  double _high_threshhold,
  double _expected_throughput,
  double _high_multiple,
  double _max_multiple,
  uint64_t _throttle_max,
  ostream *errstream)
{
  bool valid = true;
  if (_low_threshhold > _high_threshhold) {
    valid = false;
    if (errstream) {
      *errstream << "low_threshhold (" << _low_threshhold
		 << ") > high_threshhold (" << _high_threshhold
		 << ")" << std::endl;
    }
  }

  if (_high_multiple > _max_multiple) {
    valid = false;
    if (errstream) {
      *errstream << "_high_multiple (" << _high_multiple
		 << ") > _max_multiple (" << _max_multiple
		 << ")" << std::endl;
    }
  }

  if (_low_threshhold > 1 || _low_threshhold < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid low_threshhold (" << _low_threshhold << ")"
		 << std::endl;
    }
  }

  if (_high_threshhold > 1 || _high_threshhold < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid high_threshhold (" << _high_threshhold << ")"
		 << std::endl;
    }
  }

  if (_max_multiple < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid _max_multiple ("
		 << _max_multiple << ")"
		 << std::endl;
    }
  }

  if (_high_multiple < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid _high_multiple ("
		 << _high_multiple << ")"
		 << std::endl;
    }
  }

  if (_expected_throughput < 0) {
    valid = false;
    if (errstream) {
      *errstream << "invalid _expected_throughput("
		 << _expected_throughput << ")"
		 << std::endl;
    }
  }

  if (!valid)
    return false;

  locker l(lock);
  low_threshhold = _low_threshhold;
  high_threshhold = _high_threshhold;
  high_delay_per_count = _high_multiple / _expected_throughput;
  max_delay_per_count = _max_multiple / _expected_throughput;
  max = _throttle_max;

  if (high_threshhold - low_threshhold > 0) {
    s0 = high_delay_per_count / (high_threshhold - low_threshhold);
  } else {
    low_threshhold = high_threshhold;
    s0 = 0;
  }

  if (1 - high_threshhold > 0) {
    s1 = (max_delay_per_count - high_delay_per_count)
      / (1 - high_threshhold);
  } else {
    high_threshhold = 1;
    s1 = 0;
  }

  _kick_waiters();
  return true;
}

std::chrono::duration<double> BackoffThrottle::_get_delay(uint64_t c) const
{
  if (max == 0)
    return std::chrono::duration<double>(0);

  double r = ((double)current) / ((double)max);
  if (r < low_threshhold) {
    return std::chrono::duration<double>(0);
  } else if (r < high_threshhold) {
    return c * std::chrono::duration<double>(
      (r - low_threshhold) * s0);
  } else {
    return c * std::chrono::duration<double>(
      high_delay_per_count + ((r - high_threshhold) * s1));
  }
}

std::chrono::duration<double> BackoffThrottle::get(uint64_t c)
{
  locker l(lock);
  auto delay = _get_delay(c);

  // fast path
  if (delay == std::chrono::duration<double>(0) &&
      waiters.empty() &&
      ((max == 0) || (current == 0) || ((current + c) <= max))) {
    current += c;
    return std::chrono::duration<double>(0);
  }

  auto ticket = _push_waiter();

  while (waiters.begin() != ticket) {
    (*ticket)->wait(l);
  }

  auto start = std::chrono::system_clock::now();
  delay = _get_delay(c);
  while (true) {
    if (!((max == 0) || (current == 0) || (current + c) <= max)) {
      (*ticket)->wait(l);
    } else if (delay > std::chrono::duration<double>(0)) {
      (*ticket)->wait_for(l, delay);
    } else {
      break;
    }
    assert(ticket == waiters.begin());
    delay = _get_delay(c) - (std::chrono::system_clock::now() - start);
  }
  waiters.pop_front();
  _kick_waiters();

  current += c;
  return std::chrono::system_clock::now() - start;
}

uint64_t BackoffThrottle::put(uint64_t c)
{
  locker l(lock);
  assert(current >= c);
  current -= c;
  _kick_waiters();
  return current;
}

uint64_t BackoffThrottle::take(uint64_t c)
{
  locker l(lock);
  current += c;
  return current;
}

uint64_t BackoffThrottle::get_current()
{
  locker l(lock);
  return current;
}

uint64_t BackoffThrottle::get_max()
{
  locker l(lock);
  return max;
}

SimpleThrottle::SimpleThrottle(uint64_t max, bool ignore_enoent)
  : m_lock("SimpleThrottle"),
    m_max(max),
    m_current(0),
    m_ret(0),
    m_ignore_enoent(ignore_enoent)
{
}

SimpleThrottle::~SimpleThrottle()
{
  Mutex::Locker l(m_lock);
  assert(m_current == 0);
}

void SimpleThrottle::start_op()
{
  Mutex::Locker l(m_lock);
  while (m_max == m_current)
    m_cond.Wait(m_lock);
  ++m_current;
}

void SimpleThrottle::end_op(int r)
{
  Mutex::Locker l(m_lock);
  --m_current;
  if (r < 0 && !m_ret && !(r == -ENOENT && m_ignore_enoent))
    m_ret = r;
  m_cond.Signal();
}

bool SimpleThrottle::pending_error() const
{
  Mutex::Locker l(m_lock);
  return (m_ret < 0);
}

int SimpleThrottle::wait_for_ret()
{
  Mutex::Locker l(m_lock);
  while (m_current > 0)
    m_cond.Wait(m_lock);
  return m_ret;
}

void C_OrderedThrottle::finish(int r) {
  m_ordered_throttle->finish_op(m_tid, r);
}

OrderedThrottle::OrderedThrottle(uint64_t max, bool ignore_enoent)
  : m_lock("OrderedThrottle::m_lock"), m_max(max), m_current(0), m_ret_val(0),
    m_ignore_enoent(ignore_enoent), m_next_tid(0), m_complete_tid(0) {
}

C_OrderedThrottle *OrderedThrottle::start_op(Context *on_finish) {
  assert(on_finish != NULL);

  Mutex::Locker locker(m_lock);
  uint64_t tid = m_next_tid++;
  m_tid_result[tid] = Result(on_finish);
  C_OrderedThrottle *ctx = new C_OrderedThrottle(this, tid);

  complete_pending_ops();
  while (m_max == m_current) {
    m_cond.Wait(m_lock);
    complete_pending_ops();
  }
  ++m_current;

  return ctx;
}

void OrderedThrottle::end_op(int r) {
  Mutex::Locker locker(m_lock);
  assert(m_current > 0);

  if (r < 0 && m_ret_val == 0 && (r != -ENOENT || !m_ignore_enoent)) {
    m_ret_val = r;
  }
  --m_current;
  m_cond.Signal();
}

void OrderedThrottle::finish_op(uint64_t tid, int r) {
  Mutex::Locker locker(m_lock);

  TidResult::iterator it = m_tid_result.find(tid);
  assert(it != m_tid_result.end());

  it->second.finished = true;
  it->second.ret_val = r;
  m_cond.Signal();
}

bool OrderedThrottle::pending_error() const {
  Mutex::Locker locker(m_lock);
  return (m_ret_val < 0);
}

int OrderedThrottle::wait_for_ret() {
  Mutex::Locker locker(m_lock);
  complete_pending_ops();

  while (m_current > 0) {
    m_cond.Wait(m_lock);
    complete_pending_ops();
  }
  return m_ret_val;
}

void OrderedThrottle::complete_pending_ops() {
  assert(m_lock.is_locked());

  while (true) {
    TidResult::iterator it = m_tid_result.begin();
    if (it == m_tid_result.end() || it->first != m_complete_tid ||
        !it->second.finished) {
      break;
    }

    Result result = it->second;
    m_tid_result.erase(it);

    m_lock.Unlock();
    result.on_finish->complete(result.ret_val);
    m_lock.Lock();

    ++m_complete_tid;
  }
}
