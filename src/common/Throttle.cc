// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/scope_guard.h"

#include "common/Throttle.h"
#include "common/ceph_time.h"
#include "common/perf_counters.h"


// re-include our assert to clobber the system one; fix dout:
#include "include/assert.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "throttle(" << name << " " << (void*)this << ") "

using ceph::mono_clock;
using ceph::mono_time;
using ceph::uniquely_lock;

enum {
  l_throttle_first = 532430,
  l_throttle_val,
  l_throttle_max,
  l_throttle_get_started,
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

Throttle::Throttle(CephContext *cct, const std::string& n, int64_t m,
		   bool _use_perf)
  : cct(cct), name(n), max(m), use_perf(_use_perf)
{
  assert(m >= 0);

  if (!use_perf)
    return;

  if (cct->_conf->throttler_perf_counter) {
    PerfCountersBuilder b(cct, string("throttle-") + name, l_throttle_first, l_throttle_last);
    b.add_u64(l_throttle_val, "val", "Currently available throttle");
    b.add_u64(l_throttle_max, "max", "Max value for throttle");
    b.add_u64_counter(l_throttle_get_started, "get_started", "Number of get calls, increased before wait");
    b.add_u64_counter(l_throttle_get, "get", "Gets");
    b.add_u64_counter(l_throttle_get_sum, "get_sum", "Got data");
    b.add_u64_counter(l_throttle_get_or_fail_fail, "get_or_fail_fail", "Get blocked during get_or_fail");
    b.add_u64_counter(l_throttle_get_or_fail_success, "get_or_fail_success", "Successful get during get_or_fail");
    b.add_u64_counter(l_throttle_take, "take", "Takes");
    b.add_u64_counter(l_throttle_take_sum, "take_sum", "Taken data");
    b.add_u64_counter(l_throttle_put, "put", "Puts");
    b.add_u64_counter(l_throttle_put_sum, "put_sum", "Put data");
    b.add_time_avg(l_throttle_wait, "wait", "Waiting latency");

    logger = { b.create_perf_counters(), cct };
    cct->get_perfcounters_collection()->add(logger.get());
    logger->set(l_throttle_max, max);
  }
}

Throttle::~Throttle()
{
  auto l = uniquely_lock(lock);
  assert(conds.empty());
}

void Throttle::_reset_max(int64_t m)
{
  // lock must be held.
  if (max == m)
    return;
  if (!conds.empty())
    conds.front().notify_one();
  if (logger)
    logger->set(l_throttle_max, m);
  max = m;
}

bool Throttle::_wait(int64_t c, UNIQUE_LOCK_T(lock)& l)
{
  mono_time start;
  bool waited = false;
  if (_should_wait(c) || !conds.empty()) { // always wait behind other waiters.
    {
      auto cv = conds.emplace(conds.end());
      auto w = make_scope_guard([this, cv]() {
	  conds.erase(cv);
	});
      waited = true;
      ldout(cct, 2) << "_wait waiting..." << dendl;
      if (logger)
	start = mono_clock::now();

      cv->wait(l, [this, c, cv]() { return (!_should_wait(c) &&
					    cv == conds.begin()); });
      ldout(cct, 2) << "_wait finished waiting" << dendl;
      if (logger) {
	logger->tinc(l_throttle_wait, mono_clock::now() - start);
      }
    }
    // wake up the next guy
    if (!conds.empty())
      conds.front().notify_one();
  }
  return waited;
}

bool Throttle::wait(int64_t m)
{
  if (0 == max && 0 == m) {
    return false;
  }

  auto l = uniquely_lock(lock);
  if (m) {
    assert(m > 0);
    _reset_max(m);
  }
  ldout(cct, 10) << "wait" << dendl;
  return _wait(0, l);
}

int64_t Throttle::take(int64_t c)
{
  if (0 == max) {
    return 0;
  }
  assert(c >= 0);
  ldout(cct, 10) << "take " << c << dendl;
  {
    auto l = uniquely_lock(lock);
    count += c;
  }
  if (logger) {
    logger->inc(l_throttle_take);
    logger->inc(l_throttle_take_sum, c);
    logger->set(l_throttle_val, count);
  }
  return count;
}

bool Throttle::get(int64_t c, int64_t m)
{
  if (0 == max && 0 == m) {
    return false;
  }

  assert(c >= 0);
  ldout(cct, 10) << "get " << c << " (" << count.load() << " -> " << (count.load() + c) << ")" << dendl;
  if (logger) {
    logger->inc(l_throttle_get_started);
  }
  bool waited = false;
  {
    auto l = uniquely_lock(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    waited = _wait(c, l);
    count += c;
  }
  if (logger) {
    logger->inc(l_throttle_get);
    logger->inc(l_throttle_get_sum, c);
    logger->set(l_throttle_val, count);
  }
  return waited;
}

/* Returns true if it successfully got the requested amount,
 * or false if it would block.
 */
bool Throttle::get_or_fail(int64_t c)
{
  if (0 == max) {
    return true;
  }

  assert (c >= 0);
  auto l = uniquely_lock(lock);
  if (_should_wait(c) || !conds.empty()) {
    ldout(cct, 10) << "get_or_fail " << c << " failed" << dendl;
    if (logger) {
      logger->inc(l_throttle_get_or_fail_fail);
    }
    return false;
  } else {
    ldout(cct, 10) << "get_or_fail " << c << " success (" << count.load()
		   << " -> " << (count.load() + c) << ")" << dendl;
    count += c;
    if (logger) {
      logger->inc(l_throttle_get_or_fail_success);
      logger->inc(l_throttle_get);
      logger->inc(l_throttle_get_sum, c);
      logger->set(l_throttle_val, count);
    }
    return true;
  }
}

int64_t Throttle::put(int64_t c)
{
  if (0 == max) {
    return 0;
  }

  assert(c >= 0);
  ldout(cct, 10) << "put " << c << " (" << count.load() << " -> "
		 << (count.load()-c) << ")" << dendl;
  auto l = uniquely_lock(lock);
  if (c) {
    if (!conds.empty())
      conds.front().notify_one();
    // if count goes negative, we failed somewhere!
    assert(count >= c);
    count -= c;
    if (logger) {
      logger->inc(l_throttle_put);
      logger->inc(l_throttle_put_sum, c);
      logger->set(l_throttle_val, count);
    }
  }
  return count;
}

void Throttle::reset()
{
  auto l = uniquely_lock(lock);
  if (!conds.empty())
    conds.front().notify_one();
  count = 0;
  if (logger) {
    logger->set(l_throttle_val, 0);
  }
}

enum {
  l_backoff_throttle_first = l_throttle_last + 1,
  l_backoff_throttle_val,
  l_backoff_throttle_max,
  l_backoff_throttle_get,
  l_backoff_throttle_get_sum,
  l_backoff_throttle_take,
  l_backoff_throttle_take_sum,
  l_backoff_throttle_put,
  l_backoff_throttle_put_sum,
  l_backoff_throttle_wait,
  l_backoff_throttle_last,
};

BackoffThrottle::BackoffThrottle(CephContext *cct, const std::string& n,
				 unsigned expected_concurrency, bool _use_perf)
  : cct(cct), name(n),
    conds(expected_concurrency),///< [in] determines size of conds
    use_perf(_use_perf)
{
  if (!use_perf)
    return;

  if (cct->_conf->throttler_perf_counter) {
    PerfCountersBuilder b(cct, string("throttle-") + name,
			  l_backoff_throttle_first, l_backoff_throttle_last);
    b.add_u64(l_backoff_throttle_val, "val", "Currently available throttle");
    b.add_u64(l_backoff_throttle_max, "max", "Max value for throttle");
    b.add_u64_counter(l_backoff_throttle_get, "get", "Gets");
    b.add_u64_counter(l_backoff_throttle_get_sum, "get_sum", "Got data");
    b.add_u64_counter(l_backoff_throttle_take, "take", "Takes");
    b.add_u64_counter(l_backoff_throttle_take_sum, "take_sum", "Taken data");
    b.add_u64_counter(l_backoff_throttle_put, "put", "Puts");
    b.add_u64_counter(l_backoff_throttle_put_sum, "put_sum", "Put data");
    b.add_time_avg(l_backoff_throttle_wait, "wait", "Waiting latency");

    logger = { b.create_perf_counters(), cct };
    cct->get_perfcounters_collection()->add(logger.get());
    logger->set(l_backoff_throttle_max, max);
  }
}

BackoffThrottle::~BackoffThrottle()
{
  auto l = uniquely_lock(lock);
  assert(waiters.empty());
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

  if (logger)
    logger->set(l_backoff_throttle_max, max);

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

  if (logger) {
    logger->inc(l_backoff_throttle_get);
    logger->inc(l_backoff_throttle_get_sum, c);
  }

  // fast path
  if (delay == std::chrono::duration<double>(0) &&
      waiters.empty() &&
      ((max == 0) || (current == 0) || ((current + c) <= max))) {
    current += c;

    if (logger) {
      logger->set(l_backoff_throttle_val, current);
    }

    return std::chrono::duration<double>(0);
  }

  auto ticket = _push_waiter();
  auto wait_from = mono_clock::now();
  bool waited = false;

  while (waiters.begin() != ticket) {
    (*ticket)->wait(l);
    waited = true;
  }

  auto start = std::chrono::system_clock::now();
  delay = _get_delay(c);
  while (true) {
    if (!((max == 0) || (current == 0) || (current + c) <= max)) {
      (*ticket)->wait(l);
      waited = true;
    } else if (delay > std::chrono::duration<double>(0)) {
      (*ticket)->wait_for(l, delay);
      waited = true;
    } else {
      break;
    }
    assert(ticket == waiters.begin());
    delay = _get_delay(c) - (std::chrono::system_clock::now() - start);
  }
  waiters.pop_front();
  _kick_waiters();

  current += c;

  if (logger) {
    logger->set(l_backoff_throttle_val, current);
    if (waited) {
      logger->tinc(l_backoff_throttle_wait, mono_clock::now() - wait_from);
    }
  }

  return std::chrono::system_clock::now() - start;
}

uint64_t BackoffThrottle::put(uint64_t c)
{
  locker l(lock);
  assert(current >= c);
  current -= c;
  _kick_waiters();

  if (logger) {
    logger->inc(l_backoff_throttle_put);
    logger->inc(l_backoff_throttle_put_sum, c);
    logger->set(l_backoff_throttle_val, current);
  }

  return current;
}

uint64_t BackoffThrottle::take(uint64_t c)
{
  locker l(lock);
  current += c;

  if (logger) {
    logger->inc(l_backoff_throttle_take);
    logger->inc(l_backoff_throttle_take_sum, c);
    logger->set(l_backoff_throttle_val, current);
  }

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
  : m_max(max), m_ignore_enoent(ignore_enoent) {}

SimpleThrottle::~SimpleThrottle()
{
  auto l = uniquely_lock(m_lock);
  assert(m_current == 0);
  assert(waiters == 0);
}

void SimpleThrottle::start_op()
{
  auto l = uniquely_lock(m_lock);
  waiters++;
  m_cond.wait(l, [this]() { return m_max != m_current; });
  waiters--;
  ++m_current;
}

void SimpleThrottle::end_op(int r)
{
  auto l = uniquely_lock(m_lock);
  --m_current;
  if (r < 0 && !m_ret && !(r == -ENOENT && m_ignore_enoent))
    m_ret = r;
  m_cond.notify_all();
}

bool SimpleThrottle::pending_error() const
{
  auto l = uniquely_lock(m_lock);
  return (m_ret < 0);
}

int SimpleThrottle::wait_for_ret()
{
  auto l = uniquely_lock(m_lock);
  waiters++;
  m_cond.wait(l, [this]() { return m_current == 0; });
  waiters--;
  return m_ret;
}

void C_OrderedThrottle::finish(int r) {
  m_ordered_throttle->finish_op(m_tid, r);
}

OrderedThrottle::OrderedThrottle(uint64_t max, bool ignore_enoent)
  : m_max(max), m_ignore_enoent(ignore_enoent) {}

OrderedThrottle::~OrderedThrottle() {
  auto l  = uniquely_lock(m_lock);
  assert(waiters == 0);
}

C_OrderedThrottle *OrderedThrottle::start_op(Context *on_finish) {
  assert(on_finish);

  auto l = uniquely_lock(m_lock);
  uint64_t tid = m_next_tid++;
  m_tid_result[tid] = Result(on_finish);
  auto ctx = std::make_unique<C_OrderedThrottle>(this, tid);

  complete_pending_ops(l);
  while (m_max == m_current) {
    ++waiters;
    m_cond.wait(l);
    --waiters;
    complete_pending_ops(l);
  }
  ++m_current;

  return ctx.release();
}

void OrderedThrottle::end_op(int r) {
  auto l = uniquely_lock(m_lock);
  assert(m_current > 0);

  if (r < 0 && m_ret_val == 0 && (r != -ENOENT || !m_ignore_enoent)) {
    m_ret_val = r;
  }
  --m_current;
  m_cond.notify_all();
}

void OrderedThrottle::finish_op(uint64_t tid, int r) {
  auto l = uniquely_lock(m_lock);

  auto it = m_tid_result.find(tid);
  assert(it != m_tid_result.end());

  it->second.finished = true;
  it->second.ret_val = r;
  m_cond.notify_all();
}

bool OrderedThrottle::pending_error() const {
  auto l = uniquely_lock(m_lock);
  return (m_ret_val < 0);
}

int OrderedThrottle::wait_for_ret() {
  auto l = uniquely_lock(m_lock);
  complete_pending_ops(l);

  while (m_current > 0) {
    ++waiters;
    m_cond.wait(l);
    --waiters;
    complete_pending_ops(l);
  }
  return m_ret_val;
}

void OrderedThrottle::complete_pending_ops(UNIQUE_LOCK_T(m_lock)& l) {
  while (true) {
    auto it = m_tid_result.begin();
    if (it == m_tid_result.end() || it->first != m_complete_tid ||
        !it->second.finished) {
      break;
    }

    Result result = it->second;
    m_tid_result.erase(it);

    l.unlock();
    result.on_finish->complete(result.ret_val);
    l.lock();

    ++m_complete_tid;
  }
}

uint64_t TokenBucketThrottle::Bucket::get(uint64_t c) {
  if (0 == max) {
    return 0;
  }

  uint64_t got = 0;
  if (remain >= c) {
    // There is enough token in bucket, take c.
    got = c;
    remain -= c;
  } else {
    // There is not enough, take all remain.
    got = remain;
    remain = 0;
  }
  return got;
}

uint64_t TokenBucketThrottle::Bucket::put(uint64_t c) {
  if (0 == max) {
    return 0;
  }
  if (c) {
    // put c tokens into bucket
    uint64_t current = remain;
    if ((current + c) <= (uint64_t)max) {
      remain += c;
    } else {
      remain = (uint64_t)max;
    }
  }
  return remain;
}

void TokenBucketThrottle::Bucket::set_max(uint64_t m) {
  if (remain > m)
    remain = m;
  max = m;
}

TokenBucketThrottle::TokenBucketThrottle(
    CephContext *cct,
    uint64_t capacity,
    uint64_t avg,
    SafeTimer *timer,
    Mutex *timer_lock)
  : m_cct(cct), m_throttle(m_cct, "token_bucket_throttle", capacity),
    m_avg(avg), m_timer(timer), m_timer_lock(timer_lock),
    m_lock("token_bucket_throttle_lock")
{
  Mutex::Locker timer_locker(*m_timer_lock);
  schedule_timer();
}

TokenBucketThrottle::~TokenBucketThrottle()
{
  // cancel the timer events.
  {
    Mutex::Locker timer_locker(*m_timer_lock);
    cancel_timer();
  }

  list<Blocker> tmp_blockers;
  {
    Mutex::Locker blockers_lock(m_lock);
    tmp_blockers.splice(tmp_blockers.begin(), m_blockers, m_blockers.begin(), m_blockers.end());
  }

  for (auto b : tmp_blockers) {
    b.ctx->complete(0);
  }
}

void TokenBucketThrottle::set_max(uint64_t m) {
  Mutex::Locker lock(m_lock);
  m_throttle.set_max(m);
}

void TokenBucketThrottle::set_average(uint64_t avg) {
  m_avg = avg;
}

void TokenBucketThrottle::add_tokens() {
  list<Blocker> tmp_blockers;
  {
    // put m_avg tokens into bucket.
    Mutex::Locker lock(m_lock);
    m_throttle.put(m_avg);
    // check the m_blockers from head to tail, if blocker can get
    // enough tokens, let it go.
    while (!m_blockers.empty()) {
      Blocker blocker = m_blockers.front();
      uint64_t got = m_throttle.get(blocker.tokens_requested);
      if (got == blocker.tokens_requested) {
	// got enough tokens for front.
	tmp_blockers.splice(tmp_blockers.end(), m_blockers, m_blockers.begin());
      } else {
	// there is no more tokens.
	blocker.tokens_requested -= got;
	break;
      }
    }
  }

  for (auto b : tmp_blockers) {
    b.ctx->complete(0);
  }
}

void TokenBucketThrottle::schedule_timer() {
  add_tokens();

  m_token_ctx = new FunctionContext(
      [this](int r) {
        schedule_timer();
      });

  m_timer->add_event_after(1, m_token_ctx);
}

void TokenBucketThrottle::cancel_timer() {
  m_timer->cancel_event(m_token_ctx);
}
