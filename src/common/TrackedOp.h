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

#include <atomic>
#include "common/histogram.h"
#include "common/RWLock.h"
#include "common/Thread.h"
#include "common/Clock.h"
#include "common/ceph_mutex.h"
#include "include/spinlock.h"
#include "msg/Message.h"

#define OPTRACKER_PREALLOC_EVENTS 20

class TrackedOp;
class OpHistory;

typedef boost::intrusive_ptr<TrackedOp> TrackedOpRef;

class OpHistoryServiceThread : public Thread
{
private:
  std::list<std::pair<utime_t, TrackedOpRef>> _external_queue;
  OpHistory* _ophistory;
  mutable ceph::spinlock queue_spinlock;
  bool _break_thread;
public:
  explicit OpHistoryServiceThread(OpHistory* parent)
    : _ophistory(parent),
      _break_thread(false) { }

  void break_thread();
  void insert_op(const utime_t& now, TrackedOpRef op) {
    queue_spinlock.lock();
    _external_queue.emplace_back(now, op);
    queue_spinlock.unlock();
  }

  void *entry() override;
};


class OpHistory {
  std::set<std::pair<utime_t, TrackedOpRef> > arrived;
  std::set<std::pair<double, TrackedOpRef> > duration;
  std::set<std::pair<utime_t, TrackedOpRef> > slow_op;
  ceph::mutex ops_history_lock = ceph::make_mutex("OpHistory::ops_history_lock");
  void cleanup(utime_t now);
  uint32_t history_size;
  uint32_t history_duration;
  uint32_t history_slow_op_size;
  uint32_t history_slow_op_threshold;
  std::atomic_bool shutdown;
  OpHistoryServiceThread opsvc;
  friend class OpHistoryServiceThread;

public:
  OpHistory()
    : history_size(0), history_duration(0),
      history_slow_op_size(0), history_slow_op_threshold(0),
      shutdown(false), opsvc(this) {
    opsvc.create("OpHistorySvc");
  }
  ~OpHistory() {
    ceph_assert(arrived.empty());
    ceph_assert(duration.empty());
    ceph_assert(slow_op.empty());
  }
  void insert(const utime_t& now, TrackedOpRef op)
  {
    if (shutdown)
      return;

    opsvc.insert_op(now, op);
  }

  void _insert_delayed(const utime_t& now, TrackedOpRef op);
  void dump_ops(utime_t now, ceph::Formatter *f, std::set<std::string> filters = {""}, bool by_duration=false);
  void dump_slow_ops(utime_t now, ceph::Formatter *f, std::set<std::string> filters = {""});
  void on_shutdown();
  void set_size_and_duration(uint32_t new_size, uint32_t new_duration) {
    history_size = new_size;
    history_duration = new_duration;
  }
  void set_slow_op_size_and_threshold(uint32_t new_size, uint32_t new_threshold) {
    history_slow_op_size = new_size;
    history_slow_op_threshold = new_threshold;
  }
};

struct ShardedTrackingData;
class OpTracker {
  friend class OpHistory;
  std::atomic<int64_t> seq = { 0 };
  std::vector<ShardedTrackingData*> sharded_in_flight_list;
  OpHistory history;
  uint32_t num_optracker_shards;
  float complaint_time;
  int log_threshold;
  std::atomic<bool> tracking_enabled;
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
  void set_history_slow_op_size_and_threshold(uint32_t new_size, uint32_t new_threshold) {
    history.set_slow_op_size_and_threshold(new_size, new_threshold);
  }
  bool is_tracking() const {
    return tracking_enabled;
  }
  void set_tracking(bool enable) {
    tracking_enabled = enable;
  }
  bool dump_ops_in_flight(ceph::Formatter *f, bool print_only_blocked = false, std::set<std::string> filters = {""});
  bool dump_historic_ops(ceph::Formatter *f, bool by_duration = false, std::set<std::string> filters = {""});
  bool dump_historic_slow_ops(ceph::Formatter *f, std::set<std::string> filters = {""});
  bool register_inflight_op(TrackedOp *i);
  void unregister_inflight_op(TrackedOp *i);
  void record_history_op(TrackedOpRef&& i);

  void get_age_ms_histogram(pow2_hist_t *h);

  /**
   * walk through ops in flight
   *
   * @param oldest_sec the amount of time since the oldest op was initiated
   * @param check a function consuming tracked ops, the function returns
   *              false if it don't want to be fed with more ops
   * @return True if there are any Ops to warn on, false otherwise
   */
  bool visit_ops_in_flight(utime_t* oldest_secs,
			   std::function<bool(TrackedOp&)>&& visit);
  /**
   * walk through slow ops in flight
   *
   * @param[out] oldest_sec the amount of time since the oldest op was initiated
   * @param[out] num_slow_ops total number of slow ops
   * @param[out] num_warned_ops total number of warned ops
   * @param on_warn a function consuming tracked ops, the function returns
   *                false if it don't want to be fed with more ops
   * @return True if there are any Ops to warn on, false otherwise
   */
  bool with_slow_ops_in_flight(utime_t* oldest_secs,
			       int* num_slow_ops,
			       int* num_warned_ops,
			       std::function<void(TrackedOp&)>&& on_warn);
  /**
   * Look for Ops which are too old, and insert warning
   * strings for each Op that is too old.
   *
   * @param summary[out] a std::string summarizing slow Ops.
   * @param warning_strings[out] A std::vector<std::string> reference which is filled
   * with a warning std::string for each old Op.
   * @param slow[out] total number of slow ops
   * @return True if there are any Ops to warn on, false otherwise.
   */
  bool check_ops_in_flight(std::string* summary,
			   std::vector<std::string> &warning_strings,
			   int* slow = nullptr);

  void on_shutdown() {
    history.on_shutdown();
  }
  ~OpTracker();

  template <typename T, typename U>
  typename T::Ref create_request(U params)
  {
    typename T::Ref retval(new T(params, this));
    retval->tracking_start();

    if (is_tracking()) {
      retval->mark_event("header_read", params->get_recv_stamp());
      retval->mark_event("throttled", params->get_throttle_stamp());
      retval->mark_event("all_read", params->get_recv_complete_stamp());
      retval->mark_event("dispatched", params->get_dispatch_stamp());
    }

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

  // for use when clearing lists.  e.g.,
  //   ls.clear_and_dispose(TrackedOp::Putter());
  struct Putter {
    void operator()(TrackedOp *op) {
      op->put();
    }
  };

protected:
  OpTracker *tracker;          ///< the tracker we are associated with
  std::atomic_int nref = {0};  ///< ref count

  utime_t initiated_at;

  struct Event {
    utime_t stamp;
    std::string str;

    Event(utime_t t, std::string_view s) : stamp(t), str(s) {}

    int compare(const char *s) const {
      return str.compare(s);
    }

    const char *c_str() const {
      return str.c_str();
    }

    void dump(ceph::Formatter *f) const {
      f->dump_stream("time") << stamp;
      f->dump_string("event", str);
    }
  };

  std::vector<Event> events;    ///< std::list of events and their times
  mutable ceph::mutex lock = ceph::make_mutex("TrackedOp::lock"); ///< to protect the events list
  uint64_t seq = 0;        ///< a unique value std::set by the OpTracker

  uint32_t warn_interval_multiplier = 1; //< limits output of a given op warning

  enum {
    STATE_UNTRACKED = 0,
    STATE_LIVE,
    STATE_HISTORY
  };
  std::atomic<int> state = {STATE_UNTRACKED};

  mutable std::string desc_str;   ///< protected by lock
  mutable const char *desc = nullptr;  ///< readable without lock
  mutable std::atomic<bool> want_new_desc = {false};

  TrackedOp(OpTracker *_tracker, const utime_t& initiated) :
    tracker(_tracker),
    initiated_at(initiated)
  {
    events.reserve(OPTRACKER_PREALLOC_EVENTS);
  }

  /// output any type-specific data you want to get when dump() is called
  virtual void _dump(ceph::Formatter *f) const {}
  /// if you want something else to happen when events are marked, implement
  virtual void _event_marked() {}
  /// return a unique descriptor of the Op; eg the message it's attached to
  virtual void _dump_op_descriptor_unlocked(std::ostream& stream) const = 0;
  /// called when the last non-OpTracker reference is dropped
  virtual void _unregistered() {}

  virtual bool filter_out(const std::set<std::string>& filters) { return true; }

public:
  ZTracer::Trace osd_trace;
  ZTracer::Trace pg_trace;
  ZTracer::Trace store_trace;
  ZTracer::Trace journal_trace;

  virtual ~TrackedOp() {}

  void get() {
    ++nref;
  }
  void put() {
  again:
    auto nref_snap = nref.load();
    if (nref_snap == 1) {
      switch (state.load()) {
      case STATE_UNTRACKED:
	_unregistered();
	delete this;
	break;

      case STATE_LIVE:
	mark_event("done");
	tracker->unregister_inflight_op(this);
	_unregistered();
	if (!tracker->is_tracking()) {
	  delete this;
	} else {
	  state = TrackedOp::STATE_HISTORY;
	  tracker->record_history_op(
	    TrackedOpRef(this, /* add_ref = */ false));
	}
	break;

      case STATE_HISTORY:
	delete this;
	break;

      default:
	ceph_abort();
      }
    } else if (!nref.compare_exchange_weak(nref_snap, nref_snap - 1)) {
      goto again;
    }
  }

  const char *get_desc() const {
    if (!desc || want_new_desc.load()) {
      std::lock_guard l(lock);
      _gen_desc();
    }
    return desc;
  }
private:
  void _gen_desc() const {
    std::ostringstream ss;
    _dump_op_descriptor_unlocked(ss);
    desc_str = ss.str();
    desc = desc_str.c_str();
    want_new_desc = false;
  }
public:
  void reset_desc() {
    want_new_desc = true;
  }

  const utime_t& get_initiated() const {
    return initiated_at;
  }

  double get_duration() const {
    std::lock_guard l(lock);
    if (!events.empty() && events.rbegin()->compare("done") == 0)
      return events.rbegin()->stamp - get_initiated();
    else
      return ceph_clock_now() - get_initiated();
  }

  void mark_event(std::string_view event, utime_t stamp=ceph_clock_now());

  void mark_nowarn() {
    warn_interval_multiplier = 0;
  }

  virtual std::string_view state_string() const {
    std::lock_guard l(lock);
    return events.empty() ? std::string_view() : std::string_view(events.rbegin()->str);
  }

  void dump(utime_t now, ceph::Formatter *f) const;

  void tracking_start() {
    if (tracker->register_inflight_op(this)) {
      events.emplace_back(initiated_at, "initiated");
      state = STATE_LIVE;
    }
  }

  // ref counting via intrusive_ptr, with special behavior on final
  // put for historical op tracking
  friend void intrusive_ptr_add_ref(TrackedOp *o) {
    o->get();
  }
  friend void intrusive_ptr_release(TrackedOp *o) {
    o->put();
  }
};


#endif
