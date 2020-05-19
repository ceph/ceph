// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_THROTTLE_H
#define CEPH_THROTTLE_H

#include <atomic>
#include <chrono>
#include <iostream>
#include <list>
#include <map>

#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "common/ThrottleInterface.h"
#include "common/Timer.h"
#include "common/convenience.h"
#include "common/perf_counters_collection.h"

/**
 * @class Throttle
 * Throttles the maximum number of active requests.
 *
 * This class defines the maximum number of slots currently taken away. The
 * excessive requests for more of them are delayed, until some slots are put
 * back, so @p get_current() drops below the limit after fulfills the requests.
 */
class Throttle final : public ThrottleInterface {
  CephContext *cct;
  const std::string name;
  PerfCountersRef logger;
  std::atomic<int64_t> count = { 0 }, max = { 0 };
  std::mutex lock;
  std::list<std::condition_variable> conds;
  const bool use_perf;

public:
  Throttle(CephContext *cct, const std::string& n, int64_t m = 0, bool _use_perf = true);
  ~Throttle() override;

private:
  void _reset_max(int64_t m);
  bool _should_wait(int64_t c) const {
    int64_t m = max;
    int64_t cur = count;
    return
      m &&
      ((c <= m && cur + c > m) || // normally stay under max
       (c >= m && cur > m));     // except for large c
  }

  bool _wait(int64_t c, std::unique_lock<std::mutex>& l);

public:
  /**
   * gets the number of currently taken slots
   * @returns the number of taken slots
   */
  int64_t get_current() const {
    return count;
  }

  /**
   * get the max number of slots
   * @returns the max number of slots
   */
  int64_t get_max() const { return max; }

  /**
   * return true if past midpoint
   */
  bool past_midpoint() const {
    return count >= max / 2;
  }

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
  int64_t take(int64_t c = 1) override;

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
  int64_t put(int64_t c = 1) override;
   /**
   * reset the zero to the stock
   */
  void reset();

  void reset_max(int64_t m) {
    std::lock_guard l(lock);
    _reset_max(m);
  }
};

/**
 * BackoffThrottle
 *
 * Creates a throttle which gradually induces delays when get() is called
 * based on params low_threshold, high_threshold, expected_throughput,
 * high_multiple, and max_multiple.
 *
 * In [0, low_threshold), we want no delay.
 *
 * In [low_threshold, high_threshold), delays should be injected based
 * on a line from 0 at low_threshold to
 * high_multiple * (1/expected_throughput) at high_threshold.
 *
 * In [high_threshold, 1), we want delays injected based on a line from
 * (high_multiple * (1/expected_throughput)) at high_threshold to
 * (high_multiple * (1/expected_throughput)) +
 * (max_multiple * (1/expected_throughput)) at 1.
 *
 * Let the current throttle ratio (current/max) be r, low_threshold be l,
 * high_threshold be h, high_delay (high_multiple / expected_throughput) be e,
 * and max_delay (max_multiple / expected_throughput) be m.
 *
 * delay = 0, r \in [0, l)
 * delay = (r - l) * (e / (h - l)), r \in [l, h)
 * delay = e + (r - h)((m - e)/(1 - h))
 */
class BackoffThrottle {
  CephContext *cct;
  const std::string name;
  PerfCountersRef logger;

  std::mutex lock;
  using locker = std::unique_lock<std::mutex>;

  unsigned next_cond = 0;

  /// allocated once to avoid constantly allocating new ones
  std::vector<std::condition_variable> conds;

  const bool use_perf;

  /// pointers into conds
  std::list<std::condition_variable*> waiters;

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
  double low_threshold = 0;
  double high_threshold = 1;

  /// see above, values are in seconds
  double high_delay_per_count = 0;
  double max_delay_per_count = 0;

  /// Filled in in set_params
  double s0 = 0; ///< e / (h - l), l != h, 0 otherwise
  double s1 = 0; ///< (m - e)/(1 - h), 1 != h, 0 otherwise

  /// max
  uint64_t max = 0;
  uint64_t current = 0;

  ceph::timespan _get_delay(uint64_t c) const;

public:
  /**
   * set_params
   *
   * Sets params.  If the params are invalid, returns false
   * and populates errstream (if non-null) with a user comprehensible
   * explanation.
   */
  bool set_params(
    double _low_threshold,
    double _high_threshold,
    double expected_throughput,
    double high_multiple,
    double max_multiple,
    uint64_t throttle_max,
    std::ostream *errstream);

  ceph::timespan get(uint64_t c = 1);
  ceph::timespan wait() {
    return get(0);
  }
  uint64_t put(uint64_t c = 1);
  uint64_t take(uint64_t c = 1);
  uint64_t get_current();
  uint64_t get_max();

  BackoffThrottle(CephContext *cct, const std::string& n,
    unsigned expected_concurrency, ///< [in] determines size of conds
    bool _use_perf = true);
  ~BackoffThrottle();
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
  mutable std::mutex m_lock;
  std::condition_variable m_cond;
  uint64_t m_max;
  uint64_t m_current = 0;
  int m_ret = 0;
  bool m_ignore_enoent;
  uint32_t waiters = 0;
};


class OrderedThrottle;

class C_OrderedThrottle : public Context {
public:
  C_OrderedThrottle(OrderedThrottle *ordered_throttle, uint64_t tid)
    : m_ordered_throttle(ordered_throttle), m_tid(tid) {
  }

protected:
  void finish(int r) override;

private:
  OrderedThrottle *m_ordered_throttle;
  uint64_t m_tid;
};

/**
 * @class OrderedThrottle
 * Throttles the maximum number of active requests and completes them in order
 *
 * Operations can complete out-of-order but their associated Context callback
 * will completed in-order during invocation of start_op() and wait_for_ret()
 */
class OrderedThrottle {
public:
  OrderedThrottle(uint64_t max, bool ignore_enoent);
  ~OrderedThrottle();

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

  mutable std::mutex m_lock;
  std::condition_variable m_cond;
  uint64_t m_max;
  uint64_t m_current = 0;
  int m_ret_val = 0;
  bool m_ignore_enoent;

  uint64_t m_next_tid = 0;
  uint64_t m_complete_tid = 0;

  TidResult m_tid_result;

  void complete_pending_ops(std::unique_lock<std::mutex>& l);
  uint32_t waiters = 0;
};


class TokenBucketThrottle {
  struct Bucket {
    CephContext *cct;
    const std::string name;

    uint64_t remain;
    uint64_t max;
    uint64_t capacity;
    uint64_t available;

    Bucket(CephContext *cct, const std::string &name, uint64_t m)
      : cct(cct), name(name), remain(m), max(m), capacity(m), available(m) {}

    uint64_t get(uint64_t c);
    uint64_t put(uint64_t tokens, double burst_ratio);
    void set_max(uint64_t max, uint64_t burst_seconds);
  };

  struct Blocker {
    uint64_t tokens_requested;
    Context *ctx;

    Blocker(uint64_t _tokens_requested, Context* _ctx)
      : tokens_requested(_tokens_requested), ctx(_ctx) {}
  };

  CephContext *m_cct;
  const std::string m_name;
  Bucket m_throttle;
  uint64_t m_burst = 0;
  uint64_t m_avg = 0;
  SafeTimer *m_timer;
  ceph::mutex *m_timer_lock;
  Context *m_token_ctx = nullptr;
  std::list<Blocker> m_blockers;
  ceph::mutex m_lock;

  // minimum of the filling period.
  uint64_t m_tick_min = 50;
  // tokens filling period, its unit is millisecond.
  uint64_t m_tick = 0;
  /**
   * These variables are used to calculate how many tokens need to be put into
   * the bucket within each tick.
   *
   * In actual use, the tokens to be put per tick(m_avg / m_ticks_per_second)
   * may be a floating point number, but we need an 'uint64_t' to put into the
   * bucket.
   *
   * For example, we set the value of rate to be 950, means 950 iops(or bps).
   *
   * In this case, the filling period(m_tick) should be 1000 / 950 = 1.052,
   * which is too small for the SafeTimer. So we should set the period(m_tick)
   * to be 50(m_tick_min), and 20 ticks in one second(m_ticks_per_second).
   * The tokens filled in bucket per tick is 950 / 20 = 47.5, not an integer.
   *
   * To resolve this, we use a method called tokens_filled(m_current_tick) to
   * calculate how many tokens will be put so far(until m_current_tick):
   *
   *   tokens_filled = m_current_tick / m_ticks_per_second * m_avg
   *
   * And the difference between two ticks will be the result we expect.
   *   tokens in tick 0: (1 / 20 * 950) - (0 / 20 * 950) =  47 -   0 = 47
   *   tokens in tick 1: (2 / 20 * 950) - (1 / 20 * 950) =  95 -  47 = 48
   *   tokens in tick 2: (3 / 20 * 950) - (2 / 20 * 950) = 142 -  95 = 47
   *
   * As a result, the tokens filled in one second will shown as this:
   *   tick    | 1| 2| 3| 4| 5| 6| 7| 8| 9|10|11|12|13|14|15|16|17|18|19|20|
   *   tokens  |47|48|47|48|47|48|47|48|47|48|47|48|47|48|47|48|47|48|47|48|
   */
  uint64_t m_ticks_per_second = 0;
  uint64_t m_current_tick = 0;

  // period for the bucket filling tokens, its unit is seconds.
  double m_schedule_tick = 1.0;

public:
  TokenBucketThrottle(CephContext *cct, const std::string &name,
                      uint64_t burst, uint64_t avg,
                      SafeTimer *timer, ceph::mutex *timer_lock);

  ~TokenBucketThrottle();

  const std::string &get_name() {
    return m_name;
  }

  template <typename T, typename MF, typename I>
  void add_blocker(uint64_t c, T&& t, MF&& mf, I&& item, uint64_t flag) {
    auto ctx = new LambdaContext(
      [t, mf, item=std::forward<I>(item), flag](int) mutable {
        (t->*mf)(std::forward<I>(item), flag);
      });
    m_blockers.emplace_back(c, ctx);
  }

  template <typename T, typename MF, typename I>
  bool get(uint64_t c, T&& t, MF&& mf, I&& item, uint64_t flag) {
    bool wait = false;
    uint64_t got = 0;
    std::lock_guard lock(m_lock);
    if (!m_blockers.empty()) {
      // Keep the order of requests, add item after previous blocked requests.
      wait = true;
    } else {
      if (0 == m_throttle.max || 0 == m_avg)
        return false;

      got = m_throttle.get(c);
      if (got < c) {
        // Not enough tokens, add a blocker for it.
        wait = true;
      }
    }

    if (wait) {
      add_blocker(c - got, std::forward<T>(t), std::forward<MF>(mf),
                  std::forward<I>(item), flag);
    }

    return wait;
  }

  int set_limit(uint64_t average, uint64_t burst, uint64_t burst_seconds);
  void set_schedule_tick_min(uint64_t tick);

private:
  uint64_t tokens_filled(double tick);
  uint64_t tokens_this_tick();
  void add_tokens();
  void schedule_timer();
  void cancel_timer();
};

#endif
