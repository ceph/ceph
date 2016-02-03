// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_THROTTLE_H
#define CEPH_THROTTLE_H

#include "Mutex.h"
#include "Cond.h"
#include <list>
#include <map>
#include <iostream>
#include <condition_variable>
#include <chrono>
#include "include/atomic.h"
#include "include/Context.h"

class CephContext;
class PerfCounters;

/**
 * @class Throttle
 * Throttles the maximum number of active requests.
 *
 * This class defines the maximum number of slots currently taken away. The
 * excessive requests for more of them are delayed, until some slots are put
 * back, so @p get_current() drops below the limit after fulfills the requests.
 */
class Throttle {
  CephContext *cct;
  const std::string name;
  PerfCounters *logger;
  ceph::atomic_t count, max;
  Mutex lock;
  list<Cond*> cond;
  const bool use_perf;

public:
  Throttle(CephContext *cct, const std::string& n, int64_t m = 0, bool _use_perf = true);
  ~Throttle();

private:
  void _reset_max(int64_t m);
  bool _should_wait(int64_t c) const {
    int64_t m = max.read();
    int64_t cur = count.read();
    return
      m &&
      ((c <= m && cur + c > m) || // normally stay under max
       (c >= m && cur > m));     // except for large c
  }

  bool _wait(int64_t c);

public:
  /**
   * gets the number of currently taken slots
   * @returns the number of taken slots
   */
  int64_t get_current() const {
    return count.read();
  }

  /**
   * get the max number of slots
   * @returns the max number of slots
   */
  int64_t get_max() const { return max.read(); }

  /**
   * set the new max number, and wait until the number of taken slots drains
   * and drops below this limit.
   *
   * @param m the new max number
   * @returns true if this method is blocked, false it it returns immediately
   */
  bool wait(int64_t m = 0);

  /**
   * take the specified number of slots from the stock regardless the throttling
   * @param c number of slots to take
   * @returns the total number of taken slots
   */
  int64_t take(int64_t c = 1);

  /**
   * get the specified amount of slots from the stock, but will wait if the
   * total number taken by consumer would exceed the maximum number.
   * @param c number of slots to get
   * @param m new maximum number to set, ignored if it is 0
   * @returns true if this request is blocked due to the throttling, false 
   * otherwise
   */
  bool get(int64_t c = 1, int64_t m = 0);

  /**
   * the unblocked version of @p get()
   * @returns true if it successfully got the requested amount,
   * or false if it would block.
   */
  bool get_or_fail(int64_t c = 1);

  /**
   * put slots back to the stock
   * @param c number of slots to return
   * @returns number of requests being hold after this
   */
  int64_t put(int64_t c = 1);
  bool should_wait(int64_t c) const {
    return _should_wait(c);
  }
  void reset_max(int64_t m) {
    Mutex::Locker l(lock);
    _reset_max(m);
  }
};

/**
 * BackoffThrottle
 *
 * Creates a throttle which gradually induces delays when get() is called
 * based on params low_threshhold, high_threshhold, expected_throughput,
 * high_multiple, and max_multiple.
 *
 * In [0, low_threshhold), we want no delay.
 *
 * In [low_threshhold, high_threshhold), delays should be injected based
 * on a line from 0 at low_threshhold to
 * high_multiple * (1/expected_throughput) at high_threshhold.
 *
 * In [high_threshhold, 1), we want delays injected based on a line from
 * (high_multiple * (1/expected_throughput)) at high_threshhold to
 * (high_multiple * (1/expected_throughput)) +
 * (max_multiple * (1/expected_throughput)) at 1.
 *
 * Let the current throttle ratio (current/max) be r, low_threshhold be l,
 * high_threshhold be h, high_delay (high_multiple / expected_throughput) be e,
 * and max_delay (max_muliple / expected_throughput) be m.
 *
 * delay = 0, r \in [0, l)
 * delay = (r - l) * (e / (h - l)), r \in [l, h)
 * delay = h + (r - h)((m - e)/(1 - h))
 */
class BackoffThrottle {
  std::mutex lock;
  using locker = std::unique_lock<std::mutex>;

  unsigned next_cond = 0;

  /// allocated once to avoid constantly allocating new ones
  vector<std::condition_variable> conds;

  /// pointers into conds
  list<std::condition_variable*> waiters;

  std::list<std::condition_variable*>::iterator _push_waiter() {
    unsigned next = next_cond++;
    if (next_cond == conds.size())
      next_cond = 0;
    return waiters.insert(waiters.end(), &(conds[next]));
  }

  void _kick_waiters() {
    if (!waiters.empty())
      waiters.front()->notify_all();
  }

  /// see above, values are in [0, 1].
  double low_threshhold = 0;
  double high_threshhold = 1;

  /// see above, values are in seconds
  double high_delay_per_count = 0;
  double max_delay_per_count = 0;

  /// Filled in in set_params
  double s0 = 0; ///< e / (h - l), l != h, 0 otherwise
  double s1 = 0; ///< (m - e)/(1 - h), 1 != h, 0 otherwise

  /// max
  uint64_t max = 0;
  uint64_t current = 0;

  std::chrono::duration<double> _get_delay(uint64_t c) const;

public:
  /**
   * set_params
   *
   * Sets params.  If the params are invalid, returns false
   * and populates errstream (if non-null) with a user compreshensible
   * explanation.
   */
  bool set_params(
    double low_threshhold,
    double high_threshhold,
    double expected_throughput,
    double high_multiple,
    double max_multiple,
    uint64_t throttle_max,
    ostream *errstream);

  std::chrono::duration<double> get(uint64_t c = 1);
  std::chrono::duration<double> wait() {
    return get(0);
  }
  uint64_t put(uint64_t c = 1);
  uint64_t take(uint64_t c = 1);
  uint64_t get_current();
  uint64_t get_max();

  BackoffThrottle(
    unsigned expected_concurrency ///< [in] determines size of conds
    ) : conds(expected_concurrency) {}
};


/**
 * @class SimpleThrottle
 * This is a simple way to bound the number of concurrent operations.
 *
 * It tracks the first error encountered, and makes it available
 * when all requests are complete. wait_for_ret() should be called
 * before the instance is destroyed.
 *
 * Re-using the same instance isn't safe if you want to check each set
 * of operations for errors, since the return value is not reset.
 */
class SimpleThrottle {
public:
  SimpleThrottle(uint64_t max, bool ignore_enoent);
  ~SimpleThrottle();
  void start_op();
  void end_op(int r);
  bool pending_error() const;
  int wait_for_ret();
private:
  mutable Mutex m_lock;
  Cond m_cond;
  uint64_t m_max;
  uint64_t m_current;
  int m_ret;
  bool m_ignore_enoent;
};

class C_SimpleThrottle : public Context {
public:
  C_SimpleThrottle(SimpleThrottle *throttle) : m_throttle(throttle) {
    m_throttle->start_op();
  }
  virtual void finish(int r) {
    m_throttle->end_op(r);
  }
private:
  SimpleThrottle *m_throttle;
};

class OrderedThrottle;

class C_OrderedThrottle : public Context {
public:
  C_OrderedThrottle(OrderedThrottle *ordered_throttle, uint64_t tid)
    : m_ordered_throttle(ordered_throttle), m_tid(tid) {
  }

protected:
  virtual void finish(int r);

private:
  OrderedThrottle *m_ordered_throttle;
  uint64_t m_tid;
};

/**
 * @class OrderedThrottle
 * Throttles the maximum number of active requests and completes them in order
 *
 * Operations can complete out-of-order but their associated Context callback
 * will completed in-order during invokation of start_op() and wait_for_ret()
 */
class OrderedThrottle {
public:
  OrderedThrottle(uint64_t max, bool ignore_enoent);

  C_OrderedThrottle *start_op(Context *on_finish);
  void end_op(int r);

  bool pending_error() const;
  int wait_for_ret();

protected:
  friend class C_OrderedThrottle;

  void finish_op(uint64_t tid, int r);

private:
  struct Result {
    bool finished;
    int ret_val;
    Context *on_finish;

    Result(Context *_on_finish = NULL)
      : finished(false), ret_val(0), on_finish(_on_finish) {
    }
  };

  typedef std::map<uint64_t, Result> TidResult;

  mutable Mutex m_lock;
  Cond m_cond;
  uint64_t m_max;
  uint64_t m_current;
  int m_ret_val;
  bool m_ignore_enoent;

  uint64_t m_next_tid;
  uint64_t m_complete_tid;

  TidResult m_tid_result;

  void complete_pending_ops();
};

#endif
