// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ASYNC_RESERVER_H
#define ASYNC_RESERVER_H

#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "include/Context.h"

#define rdout(x) lgeneric_subdout(cct,reserver,x)

/**
 * Manages a configurable number of asynchronous reservations.
 *
 * Memory usage is linear with the number of items queued and
 * linear with respect to the total number of priorities used
 * over all time.
 */
template <typename T, typename F>
class AsyncReserver {
  CephContext *cct;
  F *f;
  unsigned max_allowed;
  unsigned min_priority;
  ceph::mutex lock = ceph::make_mutex("AsyncReserver::lock");

  struct Reservation {
    T item;
    unsigned prio = 0;
    Context *grant = 0;
    Context *preempt = 0;
    Reservation() {}
    Reservation(T i, unsigned pr, Context *g, Context *p = 0)
      : item(i), prio(pr), grant(g), preempt(p) {}
    void dump(ceph::Formatter *f) const {
      f->dump_stream("item") << item;
      f->dump_unsigned("prio", prio);
      f->dump_bool("can_preempt", !!preempt);
    }
    friend std::ostream& operator<<(std::ostream& out, const Reservation& r) {
      return out << r.item << "(prio " << r.prio << " grant " << r.grant
		 << " preempt " << r.preempt << ")";
    }
  };

  std::map<unsigned, std::list<Reservation>> queues;
  std::map<T, std::pair<unsigned, typename std::list<Reservation>::iterator>> queue_pointers;
  std::map<T,Reservation> in_progress;
  std::set<std::pair<unsigned,T>> preempt_by_prio;  ///< in_progress that can be preempted

  void preempt_one() {
    ceph_assert(!preempt_by_prio.empty());
    auto q = in_progress.find(preempt_by_prio.begin()->second);
    ceph_assert(q != in_progress.end());
    Reservation victim = q->second;
    rdout(10) << __func__ << " preempt " << victim << dendl;
    f->queue(victim.preempt);
    victim.preempt = nullptr;
    in_progress.erase(q);
    preempt_by_prio.erase(preempt_by_prio.begin());
  }

  void do_queues() {
    rdout(20) << __func__ << ":\n";
    ceph::JSONFormatter jf(true);
    jf.open_object_section("queue");
    _dump(&jf);
    jf.close_section();
    jf.flush(*_dout);
    *_dout << dendl;

    // in case min_priority was adjusted up or max_allowed was adjusted down
    while (!preempt_by_prio.empty() &&
	   (in_progress.size() > max_allowed ||
	    preempt_by_prio.begin()->first < min_priority)) {
      preempt_one();
    }

    while (!queues.empty()) {
      // choose highest priority queue
      auto it = queues.end();
      --it;
      ceph_assert(!it->second.empty());
      if (it->first < min_priority) {
	break;
      }
      if (in_progress.size() >= max_allowed &&
	  !preempt_by_prio.empty() &&
	  it->first > preempt_by_prio.begin()->first) {
	preempt_one();
      }
      if (in_progress.size() >= max_allowed) {
	break; // no room
      }
      // grant
      Reservation p = it->second.front();
      rdout(10) << __func__ << " grant " << p << dendl;
      queue_pointers.erase(p.item);
      it->second.pop_front();
      if (it->second.empty()) {
	queues.erase(it);
      }
      if (p.grant) {
	f->queue(p.grant);
	p.grant = nullptr;
      }
      in_progress[p.item] = p;
      if (p.preempt) {
	preempt_by_prio.insert(std::make_pair(p.prio, p.item));
      }
    }
  }
public:
  AsyncReserver(
    CephContext *cct,
    F *f,
    unsigned max_allowed,
    unsigned min_priority = 0)
    : cct(cct),
      f(f),
      max_allowed(max_allowed),
      min_priority(min_priority) {}

  void set_max(unsigned max) {
    std::lock_guard l(lock);
    max_allowed = max;
    do_queues();
  }

  void set_min_priority(unsigned min) {
    std::lock_guard l(lock);
    min_priority = min;
    do_queues();
  }

  /**
   * Update the priority of a reservation
   *
   * Note, on_reserved may be called following update_priority.  Thus,
   * the callback must be safe in that case.  Callback will be called
   * with no locks held.  cancel_reservation must be called to release the
   * reservation slot.
   *
   * Cases
   * 1. Item is queued, re-queue with new priority
   * 2. Item is queued, re-queue and preempt if new priority higher than an in progress item
   * 3. Item is in progress, just adjust priority if no higher priority waiting
   * 4. Item is in progress, adjust priority if higher priority items waiting preempt item
   *
   */
  void update_priority(T item, unsigned newprio) {
    std::lock_guard l(lock);
    auto i = queue_pointers.find(item);
    if (i != queue_pointers.end()) {
      unsigned prio = i->second.first;
      if (newprio == prio)
        return;
      Reservation r = *i->second.second;
      rdout(10) << __func__ << " update " << r << " (was queued)" << dendl;
      // Like cancel_reservation() without preempting
      queues[prio].erase(i->second.second);
      if (queues[prio].empty()) {
	queues.erase(prio);
      }
      queue_pointers.erase(i);

      // Like request_reservation() to re-queue it but with new priority
      ceph_assert(!queue_pointers.count(item) &&
	   !in_progress.count(item));
      r.prio = newprio;
      queues[newprio].push_back(r);
      queue_pointers.insert(std::make_pair(item,
				    std::make_pair(newprio,--(queues[newprio]).end())));
    } else {
      auto p = in_progress.find(item);
      if (p != in_progress.end()) {
        if (p->second.prio == newprio)
          return;
	rdout(10) << __func__ << " update " << p->second
		  << " (in progress)" << dendl;
        // We want to preempt if priority goes down
        // and smaller then highest priority waiting
	if (p->second.preempt) {
	  if (newprio < p->second.prio && !queues.empty()) {
            // choose highest priority queue
            auto it = queues.end();
            --it;
            ceph_assert(!it->second.empty());
            if (it->first > newprio) {
	      rdout(10) << __func__ << " update " << p->second
		        << " lowered priority let do_queues() preempt it" << dendl;
            }
          }
	  preempt_by_prio.erase(std::make_pair(p->second.prio, p->second.item));
          p->second.prio = newprio;
	  preempt_by_prio.insert(std::make_pair(p->second.prio, p->second.item));
	} else {
          p->second.prio = newprio;
        }
      } else {
	rdout(10) << __func__ << " update " << item << " (not found)" << dendl;
      }
    }
    do_queues();
    return;
  }

  void dump(ceph::Formatter *f) {
    std::lock_guard l(lock);
    _dump(f);
  }
  void _dump(ceph::Formatter *f) {
    f->dump_unsigned("max_allowed", max_allowed);
    f->dump_unsigned("min_priority", min_priority);
    f->open_array_section("queues");
    for (auto& p : queues) {
      f->open_object_section("queue");
      f->dump_unsigned("priority", p.first);
      f->open_array_section("items");
      for (auto& q : p.second) {
	f->dump_object("item", q);
      }
      f->close_section();
      f->close_section();
    }
    f->close_section();
    f->open_array_section("in_progress");
    for (auto& p : in_progress) {
      f->dump_object("item", p.second);
    }
    f->close_section();
  }

  /**
   * Requests a reservation
   *
   * Note, on_reserved may be called following cancel_reservation.  Thus,
   * the callback must be safe in that case.  Callback will be called
   * with no locks held.  cancel_reservation must be called to release the
   * reservation slot.
   */
  void request_reservation(
    T item,                   ///< [in] reservation key
    Context *on_reserved,     ///< [in] callback to be called on reservation
    unsigned prio,            ///< [in] priority
    Context *on_preempt = 0   ///< [in] callback to be called if we are preempted (optional)
    ) {
    std::lock_guard l(lock);
    Reservation r(item, prio, on_reserved, on_preempt);
    rdout(10) << __func__ << " queue " << r << dendl;
    ceph_assert(!queue_pointers.count(item) &&
	   !in_progress.count(item));
    queues[prio].push_back(r);
    queue_pointers.insert(std::make_pair(item,
				    std::make_pair(prio,--(queues[prio]).end())));
    do_queues();
  }

  /**
   * The synchronous version of request_reservation
   * Used to handle requests from OSDs that do not support the async interface
   * to scrub replica reservations, but still must count towards the max
   * active reservations.
   */
  bool request_reservation_or_fail(
      T item		     ///< [in] reservation key
  )
  {
    std::lock_guard l(lock);
    ceph_assert(!queue_pointers.count(item) && !in_progress.count(item));

    if (in_progress.size() >= max_allowed) {
      rdout(10) << fmt::format("{}: request: {} denied", __func__, item)
		<< dendl;
      return false;
    }

    const unsigned prio = UINT_MAX;
    Reservation r(item, prio, nullptr, nullptr);
    queues[prio].push_back(r);
    queue_pointers.insert(std::make_pair(
	item, std::make_pair(prio, --(queues[prio]).end())));
    do_queues();
    // the new request should be in_progress now
    ceph_assert(in_progress.count(item));
    rdout(10) << fmt::format("{}: request: {} granted", __func__, item)
	      << dendl;
    return true;
  }

  /**
   * Cancels reservation
   *
   * Frees the reservation under key for use.
   * Note, after cancel_reservation, the reservation_callback may or
   * may not still be called. 
   */
  void cancel_reservation(
    T item                   ///< [in] key for reservation to cancel
    ) {
    std::lock_guard l(lock);
    auto i = queue_pointers.find(item);
    if (i != queue_pointers.end()) {
      unsigned prio = i->second.first;
      const Reservation& r = *i->second.second;
      rdout(10) << __func__ << " cancel " << r << " (was queued)" << dendl;
      delete r.grant;
      delete r.preempt;
      queues[prio].erase(i->second.second);
      if (queues[prio].empty()) {
	queues.erase(prio);
      }
      queue_pointers.erase(i);
    } else {
      auto p = in_progress.find(item);
      if (p != in_progress.end()) {
	rdout(10) << __func__ << " cancel " << p->second
		  << " (was in progress)" << dendl;
	if (p->second.preempt) {
	  preempt_by_prio.erase(std::make_pair(p->second.prio, p->second.item));
	  delete p->second.preempt;
	}
	in_progress.erase(p);
      } else {
	rdout(10) << __func__ << " cancel " << item << " (not found)" << dendl;
      }
    }
    do_queues();
  }

  /**
   * Has reservations
   *
   * Return true if there are reservations in progress
   */
  bool has_reservation() {
    std::lock_guard l(lock);
    return !in_progress.empty();
  }
  static const unsigned MAX_PRIORITY = (unsigned)-1;
};

#undef rdout
#endif
