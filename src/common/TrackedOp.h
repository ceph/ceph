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
#include "msg/Message.h"
#include "common/RWLock.h"

#define OPTRACKER_PREALLOC_EVENTS 20

class TrackedOp;
class OpHistory;

typedef boost::intrusive_ptr<TrackedOp> TrackedOpRef;

class OpHistoryServiceThread : public Thread
{
private:
  list<pair<utime_t, TrackedOpRef>> _external_queue;
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
  set<pair<utime_t, TrackedOpRef> > arrived;
  set<pair<double, TrackedOpRef> > duration;
  set<pair<utime_t, TrackedOpRef> > slow_op;
  Mutex ops_history_lock;
  void cleanup(utime_t now);
  uint32_t history_size;
  uint32_t history_duration;
  uint32_t history_slow_op_size;
  uint32_t history_slow_op_threshold;
  std::atomic_bool shutdown;
  OpHistoryServiceThread opsvc;
  friend class OpHistoryServiceThread;

public:
  OpHistory() : ops_history_lock("OpHistory::Lock"),
    history_size(0), history_duration(0),
    history_slow_op_size(0), history_slow_op_threshold(0),
    shutdown(false), opsvc(this) {
    opsvc.create("OpHistorySvc");
  }
  ~OpHistory() {
    assert(arrived.empty());
    assert(duration.empty());
    assert(slow_op.empty());
  }
  void insert(const utime_t& now, TrackedOpRef op)
  {
    if (shutdown)
      return;

    opsvc.insert_op(now, op);
  }

  void _insert_delayed(const utime_t& now, TrackedOpRef op);
  void dump_ops(utime_t now, Formatter *f, set<string> filters = {""});
  void dump_ops_by_duration(utime_t now, Formatter *f, set<string> filters = {""});
  void dump_slow_ops(utime_t now, Formatter *f, set<string> filters = {""});
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
  vector<ShardedTrackingData*> sharded_in_flight_list;
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
  bool dump_ops_in_flight(Formatter *f, bool print_only_blocked = false, set<string> filters = {""});
  bool dump_historic_ops(Formatter *f, bool by_duration = false, set<string> filters = {""});
  bool dump_historic_slow_ops(Formatter *f, set<string> filters = {""});
  bool register_inflight_op(TrackedOp *i);
  void unregister_inflight_op(TrackedOp *i);

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
   * @param on_warn a function consuming tracked ops, the function returns
   *                false if it don't want to be fed with more ops
   * @return True if there are any Ops to warn on, false otherwise
   */
  bool with_slow_ops_in_flight(utime_t* oldest_secs,
			       int* num_slow_ops,
			       std::function<void(TrackedOp&)>&& on_warn);
  /**
   * Look for Ops which are too old, and insert warning
   * strings for each Op that is too old.
   *
   * @param summary[out] a string summarizing slow Ops.
   * @param warning_strings[out] A vector<string> reference which is filled
   * with a warning string for each old Op.
   * @param slow[out] total number of slow ops
   * @return True if there are any Ops to warn on, false otherwise.
   */
  bool check_ops_in_flight(std::string* summary,
			   std::vector<string> &warning_strings,
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
    string str;
    const char *cstr = nullptr;

    Event(utime_t t, const string& s) : stamp(t), str(s) {}
    Event(utime_t t, const char *s) : stamp(t), cstr(s) {}

    int compare(const char *s) const {
      if (cstr)
	return strcmp(cstr, s);
      else
	return str.compare(s);
    }

    const char *c_str() const {
      if (cstr)
	return cstr;
      else
	return str.c_str();
    }

    void dump(Formatter *f) const {
      f->dump_stream("time") << stamp;
      f->dump_string("event", c_str());
    }
  };

  vector<Event> events;    ///< list of events and their times
  mutable Mutex lock = {"TrackedOp::lock"}; ///< to protect the events list
  const char *current = 0; ///< the current state the event is in
  uint64_t seq = 0;        ///< a unique value set by the OpTracker

  uint32_t warn_interval_multiplier = 1; //< limits output of a given op warning

  enum {
    STATE_UNTRACKED = 0,
    STATE_LIVE,
    STATE_HISTORY
  };
  atomic<int> state = {STATE_UNTRACKED};

  mutable string desc_str;   ///< protected by lock
  mutable const char *desc = nullptr;  ///< readable without lock
  mutable atomic<bool> want_new_desc = {false};

  TrackedOp(OpTracker *_tracker, const utime_t& initiated) :
    tracker(_tracker),
    initiated_at(initiated)
  {
    events.reserve(OPTRACKER_PREALLOC_EVENTS);
  }

  /// output any type-specific data you want to get when dump() is called
  virtual void _dump(Formatter *f) const {}
  /// if you want something else to happen when events are marked, implement
  virtual void _event_marked() {}
  /// return a unique descriptor of the Op; eg the message it's attached to
  virtual void _dump_op_descriptor_unlocked(ostream& stream) const = 0;
  /// called when the last non-OpTracker reference is dropped
  virtual void _unregistered() {}

  virtual bool filter_out(const set<string>& filters) { return true; }

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
    if (--nref == 0) {
      switch (state.load()) {
      case STATE_UNTRACKED:
	_unregistered();
	delete this;
	break;

      case STATE_LIVE:
	mark_event("done");
	tracker->unregister_inflight_op(this);
	break;

      case STATE_HISTORY:
	delete this;
	break;

      default:
	ceph_abort();
      }
    }
  }

  const char *get_desc() const {
    if (!desc || want_new_desc.load()) {
      Mutex::Locker l(lock);
      _gen_desc();
    }
    return desc;
  }
private:
  void _gen_desc() const {
    ostringstream ss;
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
    Mutex::Locker l(lock);
    if (!events.empty() && events.rbegin()->compare("done") == 0)
      return events.rbegin()->stamp - get_initiated();
    else
      return ceph_clock_now() - get_initiated();
  }

  void mark_event_string(const string &event,
			 utime_t stamp=ceph_clock_now());
  void mark_event(const char *event,
		  utime_t stamp=ceph_clock_now());

  virtual const char *state_string() const {
    Mutex::Locker l(lock);
    return events.rbegin()->c_str();
  }

  void dump(utime_t now, Formatter *f) const;

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
