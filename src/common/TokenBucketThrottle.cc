// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/TokenBucketThrottle.h"

#include "common/debug.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "TokenBucketThrottle(" << m_name << " " \
                           << (void*)this << ") "

using std::list;

uint64_t TokenBucketThrottle::Bucket::get(uint64_t c) {
  if (0 == max) {
    return 0;
  }

  uint64_t got = 0;
  if (available >= c) {
    // There is enough token in bucket, take c.
    got = c;
    available -= c;
    remain -= c;
  } else {
    // There is not enough, take all available.
    got = available;
    remain -= available;
    available = 0;
  }
  return got;
}

uint64_t TokenBucketThrottle::Bucket::put(uint64_t tokens, double burst_ratio) {
  if (0 == max) {
    return 0;
  }

  if (tokens) {
    // put tokens into bucket
    uint64_t current = remain;
    if ((current + tokens) <= capacity) {
      remain += tokens;
    } else {
      remain = capacity;
    }

    // available tokens increase at burst speed
    uint64_t available_inc = tokens;
    if (burst_ratio > 1) {
      available_inc = (uint64_t)(tokens * burst_ratio);
    }
    uint64_t inc_upper_limit = remain > max ? max : remain;
    if ((available + available_inc) <= inc_upper_limit ){
      available += available_inc;
    }else{
      available = inc_upper_limit;
    }
    
  }
  return remain;
}

void TokenBucketThrottle::Bucket::set_max(uint64_t max, uint64_t burst_seconds) {
  // the capacity of bucket should not be less than max
  if (burst_seconds < 1){
    burst_seconds = 1;
  }
  uint64_t new_capacity = max*burst_seconds;
  if (capacity != new_capacity){
    capacity = new_capacity;
    remain = capacity;
  }
  if (available > max || 0 == max) {
    available = max;
  }
  this->max = max;
}

TokenBucketThrottle::TokenBucketThrottle(
    CephContext *cct,
    const std::string &name,
    uint64_t burst,
    uint64_t avg,
    SafeTimer *timer,
    ceph::mutex *timer_lock)
  : m_cct(cct), m_name(name),
    m_throttle(m_cct, name + "_bucket", burst),
    m_burst(burst), m_avg(avg), m_timer(timer), m_timer_lock(timer_lock),
    m_lock(ceph::make_mutex(name + "_lock"))
{}

TokenBucketThrottle::~TokenBucketThrottle() {
  // cancel the timer events.
  {
    std::lock_guard timer_locker(*m_timer_lock);
    cancel_timer();
  }

  list<Blocker> tmp_blockers;
  {
    std::lock_guard blockers_lock(m_lock);
    tmp_blockers.splice(tmp_blockers.begin(), m_blockers, m_blockers.begin(), m_blockers.end());
  }

  for (auto b : tmp_blockers) {
    b.ctx->complete(0);
  }
}

int TokenBucketThrottle::set_limit(uint64_t average, uint64_t burst, uint64_t burst_seconds) {
  {
    std::lock_guard lock{m_lock};

    if (0 < burst && burst < average) {
      // the burst should never less than the average.
      return -EINVAL;
    }

    m_avg = average;
    m_burst = burst;

    if (0 == average) {
      // The limit is not set, and no tokens will be put into the bucket.
      // So, we can schedule the timer slowly, or even cancel it.
      m_tick = 1000;
    } else {
      // calculate the tick(ms), don't less than the minimum.
      m_tick = 1000 / average;
      if (m_tick < m_tick_min) {
        m_tick = m_tick_min;
      }

      // this is for the number(avg) can not be divisible.
      m_ticks_per_second = 1000 / m_tick;
      m_current_tick = 0;

      // for the default configuration of burst.
      m_throttle.set_max(0 == burst ? average : burst, burst_seconds);
    }
    // turn millisecond to second
    m_schedule_tick = m_tick / 1000.0;
  }

  // The schedule period will be changed when the average rate is set.
  {
    std::lock_guard timer_locker{*m_timer_lock};
    cancel_timer();
    schedule_timer();
  }
  return 0;
}

void TokenBucketThrottle::set_schedule_tick_min(uint64_t tick) {
  std::lock_guard lock(m_lock);
  if (tick != 0) {
    m_tick_min = tick;
  }
}

uint64_t TokenBucketThrottle::tokens_filled(double tick) {
  return (0 == m_avg) ? 0 : (tick / m_ticks_per_second * m_avg);
}

uint64_t TokenBucketThrottle::tokens_this_tick() {
  if (0 == m_avg) {
    return 0;
  }
  if (m_current_tick >= m_ticks_per_second) {
    m_current_tick = 0;
  }
  m_current_tick++;

  return tokens_filled(m_current_tick) - tokens_filled(m_current_tick - 1);
}

void TokenBucketThrottle::add_tokens() {
  list<Blocker> tmp_blockers;
  {
    std::lock_guard lock(m_lock);
    // put tokens into bucket.
    double burst_ratio = 1.0;
    if (m_throttle.max > m_avg && m_avg > 0){
      burst_ratio = (double)m_throttle.max/m_avg;
    }
    m_throttle.put(tokens_this_tick(), burst_ratio);
    if (0 == m_avg || 0 == m_throttle.max)
      tmp_blockers.swap(m_blockers);
    // check the m_blockers from head to tail, if blocker can get
    // enough tokens, let it go.
    while (!m_blockers.empty()) {
      Blocker &blocker = m_blockers.front();
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
  m_token_ctx = new LambdaContext(
      [this](int r) {
        schedule_timer();
      });
  m_timer->add_event_after(m_schedule_tick, m_token_ctx);

  add_tokens();
}

void TokenBucketThrottle::cancel_timer() {
  m_timer->cancel_event(m_token_ctx);
}
