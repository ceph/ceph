// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef TRACKEDREQUEST_H_
#define TRACKEDREQUEST_H_
#include <sstream>
#include <stdint.h>
#include <boost/intrusive/list.hpp>
#include <atomic>

#include "include/utime.h"
#include "common/Mutex.h"
#include "common/histogram.h"
#include "msg/Message.h"
#include "include/memory.h"
#include "common/RWLock.h"

class TrackedOp;
typedef boost::intrusive_ptr<TrackedOp> TrackedOpRef;

class OpHistory {
  set<pair<utime_t, TrackedOpRef> > arrived;
  set<pair<double, TrackedOpRef> > duration;
  Mutex ops_history_lock;
  void cleanup(utime_t now);
  bool shutdown;
  uint32_t history_size;
  uint32_t history_duration;

public:
  OpHistory() : ops_history_lock("OpHistory::Lock"), shutdown(false),
  history_size(0), history_duration(0) {}
  ~OpHistory() {
    assert(arrived.empty());
    assert(duration.empty());
  }
  void insert(utime_t now, TrackedOpRef op);
  void dump_ops(utime_t now, Formatter *f);
  void on_shutdown();
  void set_size_and_duration(uint32_t new_size, uint32_t new_duration) {
    history_size = new_size;
    history_duration = new_duration;
  }
};

struct ShardedTrackingData;
class OpTracker {
  friend class OpHistory;
  atomic64_t seq;
  vector<ShardedTrackingData*> sharded_in_flight_list;
  uint32_t num_optracker_shards;
  OpHistory history;
  float complaint_time;
  int log_threshold;
  void _mark_event(TrackedOp *op, const string &evt, utime_t now);
  bool tracking_enabled;
  RWLock       lock;

public:
  CephContext *cct;
  OpTracker(CephContext *cct_, bool tracking, uint32_t num_shards);
      
  void set_complaint_and_threshold(float time, int threshold) {
    complaint_time = time;
    log_threshold = threshold;
  }
  void set_history_size_and_duration(uint32_t new_size, uint32_t new_duration) {
    history.set_size_and_duration(new_size, new_duration);
  }
  void set_tracking(bool enable) {
    RWLock::WLocker l(lock);
    tracking_enabled = enable;
  }
  bool dump_ops_in_flight(Formatter *f, bool print_only_blocked=false);
  bool dump_historic_ops(Formatter *f);
  bool register_inflight_op(TrackedOp *i);
  void unregister_inflight_op(TrackedOp *i);

  void get_age_ms_histogram(pow2_hist_t *h);

  /**
   * Look for Ops which are too old, and insert warning
   * strings for each Op that is too old.
   *
   * @param warning_strings A vector<string> reference which is filled
   * with a warning string for each old Op.
   * @return True if there are any Ops to warn on, false otherwise.
   */
  bool check_ops_in_flight(std::vector<string> &warning_strings, int *slow = NULL);
  void mark_event(TrackedOp *op, const string &evt,
		  utime_t time = ceph_clock_now());

  void on_shutdown() {
    history.on_shutdown();
  }
  ~OpTracker();

  template <typename T, typename U>
  typename T::Ref create_request(U params)
  {
    typename T::Ref retval(new T(params, this));
    retval->tracking_start();
    return retval;
  }
};


class TrackedOp : public boost::intrusive::list_base_hook<> {
private:
  friend class OpHistory;
  friend class OpTracker;

  boost::intrusive::list_member_hook<> tracker_item;

public:
  typedef boost::intrusive::list<
  TrackedOp,
  boost::intrusive::member_hook<
    TrackedOp,
    boost::intrusive::list_member_hook<>,
    &TrackedOp::tracker_item> > tracked_op_list_t;

protected:
  OpTracker *tracker;          ///< the tracker we are associated with
  std::atomic_int nref = {0};  ///< ref count

  utime_t initiated_at;
  list<pair<utime_t, string> > events; /// list of events and their times
  mutable Mutex lock; /// to protect the events list
  string current; /// the current state the event is in
  uint64_t seq; /// a unique value set by the OpTracker

  uint32_t warn_interval_multiplier; // limits output of a given op warning

  enum {
    STATE_UNTRACKED = 0,
    STATE_LIVE,
    STATE_HISTORY
  };
  atomic<int> state = {STATE_UNTRACKED};

  TrackedOp(OpTracker *_tracker, const utime_t& initiated) :
    tracker(_tracker),
    initiated_at(initiated),
    lock("TrackedOp::lock"),
    seq(0),
    warn_interval_multiplier(1)
  { }

  /// output any type-specific data you want to get when dump() is called
  virtual void _dump(Formatter *f) const {}
  /// if you want something else to happen when events are marked, implement
  virtual void _event_marked() {}
  /// return a unique descriptor of the Op; eg the message it's attached to
  virtual void _dump_op_descriptor_unlocked(ostream& stream) const = 0;
  /// called when the last non-OpTracker reference is dropped
  virtual void _unregistered() {};

public:
  virtual ~TrackedOp() {}

  const utime_t& get_initiated() const {
    return initiated_at;
  }

  double get_duration() const {
    Mutex::Locker l(lock);
    if (!events.empty() && events.rbegin()->second.compare("done") == 0)
      return events.rbegin()->first - get_initiated();
    else
      return ceph_clock_now() - get_initiated();
  }

  void mark_event(const string &event);
  virtual const char *state_string() const {
    Mutex::Locker l(lock);
    return events.rbegin()->second.c_str();
  }
  void dump(utime_t now, Formatter *f) const;
  void tracking_start() {
    if (tracker->register_inflight_op(this)) {
      events.push_back(make_pair(initiated_at, "initiated"));
      state = STATE_LIVE;
    }
  }

  // ref counting via intrusive_ptr, with special behavior on final
  // put for historical op tracking
  friend void intrusive_ptr_add_ref(TrackedOp *o) {
    ++o->nref;
  }
  friend void intrusive_ptr_release(TrackedOp *o) {
    if (--o->nref == 0) {
      switch (o->state.load()) {
      case STATE_UNTRACKED:
	o->_unregistered();
	delete o;
	break;

      case STATE_LIVE:
	o->mark_event("done");
	o->tracker->unregister_inflight_op(o);
	break;

      case STATE_HISTORY:
	delete o;
	break;

      default:
	ceph_abort();
      }
    }
  }
};

#endif
