// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>

#include "include/ceph_assert.h"
#include "common/ceph_time.h"
#include "crimson/common/log.h"

namespace crimson::common {

/**
 * intrusive_timer: timer implementation with allocation-free
 * schedule/cancel
 */
class intrusive_timer_t {
  using clock_t = ceph::coarse_real_clock;

public:
  struct callback_t : boost::intrusive::set_base_hook<
    boost::intrusive::link_mode<boost::intrusive::auto_unlink>
  > {
    const std::function<seastar::future<>()> f;
    clock_t::time_point schedule_point;

    template <typename F>
    callback_t(F &&f) : f(std::move(f)) {}

    callback_t(const callback_t &) = delete;
    callback_t(callback_t &&) = delete;
    callback_t &operator=(const callback_t &) = delete;
    callback_t &operator=(callback_t &&) = delete;

    seastar::future<> run() {
      return std::invoke(f);
    }

    auto operator<=>(const callback_t &rhs) const {
      return std::make_pair(schedule_point, this) <=>
	std::make_pair(rhs.schedule_point, &rhs);
    }
  };

private:
  bool stopping = false;
  seastar::condition_variable cv;
  boost::intrusive::set<
    callback_t,
    boost::intrusive::constant_time_size<false>
    > events;

  seastar::future<> complete;

  callback_t *peek() {
    return events.empty() ? nullptr : &*(events.begin());
  }

  seastar::future<> _run() {
    LOG_PREFIX(intrusive_timer_t::_run);
    SUBDEBUG(osd, "");
    while (true) {
      SUBDEBUG(osd, "awake");
      if (stopping) {
	SUBDEBUG(osd, "stopping");
	break;
      }

      auto next = peek();
      if (!next) {
	SUBDEBUG(osd, "empty");
	co_await cv.when();
	continue;
      }

      auto now = clock_t::now();
      if (next->schedule_point > now) {
	auto wait_until = next->schedule_point - now;
	SUBDEBUG(
	  osd,
	  "next {} scheduled at {}, now {}, waiting {}",
	  (void*)next, now, next->schedule_point, wait_until);
	try {
	  co_await cv.when(wait_until);
	} catch (const seastar::condition_variable_timed_out&) {
	  // normal outcome
	}
	continue;
      }

      SUBDEBUG(osd, "next {} running", (void*)next);
      events.erase(*next);
      co_await next->run();
    }
    co_return;
  }

public:
  intrusive_timer_t() : complete(_run()) {}

  template <typename T>
  void schedule_after(callback_t &cb, T after) {
    LOG_PREFIX(intrusive_timer_t::schedule_after);
    ceph_assert(!cb.is_linked());
    auto now = clock_t::now();
    cb.schedule_point = now + after;
    SUBDEBUG(
      osd, "now {} scheduling {} at {}",
      now, (void*)&cb, cb.schedule_point);
    events.insert(cb);
    cv.signal();
  }

  void cancel(callback_t &cb) {
    LOG_PREFIX(intrusive_timer_t::cancel);
    if (cb.is_linked()) {
      SUBDEBUG(osd, "{} is linked, canceling", (void*)&cb);
      events.erase(cb);
    }
  }

  seastar::future<> stop() {
    LOG_PREFIX(intrusive_timer_t::stop);
    SUBDEBUG(osd, "");
    stopping = true;
    cv.signal();
    return std::move(complete);
  }
};

}
