// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>
#include <condition_variable>

#include <boost/intrusive/set.hpp>

#include "common/ceph_time.h"

namespace ceph::common {

/**
 * intrusive_timer
 *
 * SafeTimer (common/Timer.h) isn't well suited to usage in high
 * usage pathways for a few reasons:
 * - Usage generally requires allocation of a fresh context for each
 *   scheduled operation.  One could override Context::complete to avoid
 *   destroying the instance, but actually reusing the instance is tricky
 *   as SafeTimer doesn't guarrantee cancelation if safe_callbacks is false.
 * - SafeTimer only guarrantees cancelation if safe_timer is true, which
 *   it generally won't be if the user needs to call into SafeTimer while
 *   holding locks taken by callbacks.
 *
 * This implementation allows the user to repeatedly schedule and cancel
 * an object inheriting from the callback_t interface below while
 * guarranteeing cancelation provided that the user holds the lock
 * associated with a particular callback while calling into intrusive_timer.
 */
class intrusive_timer {
  using clock_t = ceph::coarse_real_clock;

public:
  /**
   * callback_t
   *
   * Objects inheriting from callback_t can be scheduled
   * via intrusive_timer.
   */
  class callback_t : public boost::intrusive::set_base_hook<> {
    friend class intrusive_timer;
    clock_t::time_point schedule_point;
    unsigned incarnation = 0;

  public:
    /**
     * add_ref, dec_ref
     *
     * callback_t must remain live and all methods must remain
     * safe to call as long as calls to add_ref() outnumber calls
     * to dec_ref().
     */
    virtual void add_ref() = 0;
    virtual void dec_ref() = 0;

    /**
     * lock, unlock
     *
     * For any specific callback_t, must lock/unlock a lock held while
     * accessing intrusive_timer public methods for that callback_t
     * instance.
     */
    virtual void lock() = 0;
    virtual void unlock() = 0;

    /// Invokes callback, will be called with lock held
    virtual void invoke() = 0;

    /**
     * is_scheduled
     *
     * Return true iff callback is scheduled to be invoked.
     * May only be validly invoked while lock associated with
     * callback_t instance is held.
     */
    bool is_scheduled() const { return incarnation % 2 == 1; }
    virtual ~callback_t() = default;

    /// Order callback_t by schedule_point
    auto operator<=>(const callback_t &rhs) const {
      return std::make_pair(schedule_point, this) <=>
	std::make_pair(rhs.schedule_point, &rhs);
    }
  };

private:
  /// protects events, stopping
  std::mutex lock;

  /// stopping, cv used to signal that t should halt
  std::condition_variable cv;
  bool stopping = false;

  /// queued events ordered by callback_t::schedule_point
  boost::intrusive::set<callback_t> events;

  /// thread responsible for calling scheduled callbacks
  std::thread t;

  /// peek front of queue, null if empty
  callback_t *peek() {
    return events.empty() ? nullptr : &*(events.begin());
  }

  /// entry point for t
  void _run() {
    std::unique_lock l(lock);
    while (true) {
      if (stopping) {
	return;
      }
    
      auto next = peek();
      if (!next) {
	cv.wait(l);
	continue;
      }

      if (next->schedule_point > clock_t::now()) {
	cv.wait_until(l, next->schedule_point);
	continue;
      }

      // we release the reference below
      events.erase(*next);

      /* cancel() and schedule_after() both hold both intrusive_timer::lock
       * and the callback_t lock (precondition of both) while mutating
       * next->incarnation, so this read is safe.  We're relying on the
       * fact that only this method in this thread will access
       * next->incarnation under only one of the two. */
      auto incarnation = next->incarnation;
      l.unlock();
      {
	/* Note that intrusive_timer::cancel may observe that
	 * callback_t::is_scheduled() returns true while
	 * callback_t::is_linked() is false since we drop
	 * intrusive_timer::lock between removing next from the
	 * queue and incrementing callback_t::incarnation here
	 * under the callback_t lock.  In that case, cancel()
	 * increments incarnation logically canceling the callback
	 * but leaves the reference for us to drop.
	 */
	std::unique_lock m(*next);
	if (next->incarnation == incarnation) {
	  /* As above, cancel() and schedule_after() hold both locks so this
	   * mutation and read are safe. */
	  ++next->incarnation;
	  next->invoke();
	}
	/* else, next was canceled between l.unlock() and next->lock().
	 * Note that if incarnation does not match, we do nothing to next
	 * other than drop our reference -- it might well have been
	 * rescheduled already! */
      }
      next->dec_ref();
      l.lock();
    }
  }

public:
  intrusive_timer() : t([this] { _run(); }) {}

  /**
   * schedule_after
   *
   * Schedule cb to run after the specified period.
   * The lock associated with cb must be held.
   * cb must not already be scheduled.
   *
   * @param cb [in] callback to schedule
   * @param after [in] period after which to schedule cb
   */
  template <typename T>
  void schedule_after(callback_t &cb, T after) {
    ceph_assert(!cb.is_scheduled());
    std::unique_lock l(lock);
    ceph_assert(!cb.is_linked());

    ++cb.incarnation;
    cb.schedule_point = clock_t::now() + after;

    cb.add_ref();
    events.insert(cb);

    cv.notify_one();
  }

  /**
   * cancel
   *
   * Cancel already scheduled cb.
   * The lock associated with cb must be held.
   *
   * @param cb [in] callback to cancel
   */
  void cancel(callback_t &cb) {
    ceph_assert(cb.is_scheduled());
    std::unique_lock l(lock);
    ++cb.incarnation;

    if (cb.is_linked()) {
      events.erase(cb);
      cb.dec_ref();
    }
  }

  /// Stop intrusive_timer
  void stop() {
    {
      std::unique_lock l(lock);
      stopping = true;
      cv.notify_one();
    }
    t.join();
  }
};

}
