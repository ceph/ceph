// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_TOKEN_BUCKET_THROTTLE_H
#define CEPH_TOKEN_BUCKET_THROTTLE_H

#include <list>
#include <string>

#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "include/Context.h"

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
