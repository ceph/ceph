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
#ifndef CEPH_MDLOG_H
#define CEPH_MDLOG_H

#include "common/fair_mutex.h"
#include "include/common_fwd.h"

enum {
  l_mdl_first = 5000,
  l_mdl_evlrg,
  l_mdl_evadd,
  l_mdl_evex,
  l_mdl_evtrm,
  l_mdl_ev,
  l_mdl_evexg,
  l_mdl_evexd,
  l_mdl_segadd,
  l_mdl_segex,
  l_mdl_segtrm,
  l_mdl_seg,
  l_mdl_segmjr,
  l_mdl_segexg,
  l_mdl_segexd,
  l_mdl_expos,
  l_mdl_wrpos,
  l_mdl_rdpos,
  l_mdl_jlat,
  l_mdl_replayed,
  l_mdl_last,
};

#include "include/types.h"
#include "include/Context.h"

#include "MDSContext.h"
#include "common/Cond.h"
#include "common/DecayCounter.h"
#include "common/Finisher.h"
#include "common/Thread.h"

#include "LogSegment.h"
#include "MDSMap.h"
#include "SegmentBoundary.h"

#include <list>
#include <map>

class Journaler;
class JournalPointer;
class LogEvent;
class MDSRank;
class LogSegment;
class ESubtreeMap;

class MDLog {
public:

  MDLog(MDSRank *m);
  ~MDLog();

  const std::set<LogSegment*> &get_expiring_segments() const
  {
    return expiring_segments;
  }

  void create_logger();
  void set_write_iohint(unsigned iohint_flags);

  LogSegment *peek_current_segment() {
    return segments.empty() ? NULL : segments.rbegin()->second;
  }

  LogSegment *get_current_segment() { 
    ceph_assert(!segments.empty());
    return segments.rbegin()->second;
  }

  LogSegment *get_segment(LogSegment::seq_t seq) {
    auto it = segments.find(seq);
    if (it != segments.end()) {
      return it->second;
    } else {
      return nullptr;
    }
  }

  bool have_any_segments() const {
    return !segments.empty();
  }

  void flush_logger();

  uint64_t get_num_events() const { return num_events; }
  uint64_t get_num_segments() const { return segments.size(); }

  auto get_debug_subtrees() const {
    return events_per_segment;
  }
  auto get_max_segments() const {
    return max_segments;
  }

  uint64_t get_read_pos() const;
  uint64_t get_write_pos() const;
  uint64_t get_safe_pos() const;
  Journaler *get_journaler() { return journaler; }
  bool empty() const { return segments.empty(); }

  uint64_t get_last_major_segment_seq() const {
    ceph_assert(!major_segments.empty());
    return *major_segments.rbegin();
  }
  uint64_t get_last_segment_seq() const {
    ceph_assert(!segments.empty());
    return segments.rbegin()->first;
  }

  bool is_capped() const { return mds_is_shutting_down; }
  void cap();

  void kick_submitter();
  void shutdown();

  void submit_entry(LogEvent *e, MDSLogContextBase* c = 0) {
    std::lock_guard l(submit_mutex);
    _submit_entry(e, c);
    _segment_upkeep();
    submit_cond.notify_all();
  }

  void wait_for_safe(Context* c);
  void flush();
  bool is_flushed() const {
    return unflushed == 0;
  }

  void trim_expired_segments();
  void trim(int max=-1);
  int trim_all();

  void create(MDSContext *onfinish);  // fresh, empty log! 
  void open(MDSContext *onopen);      // append() or replay() to follow!
  void reopen(MDSContext *onopen);
  void append();
  void replay(MDSContext *onfinish);

  void standby_trim_segments();

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

  void dump_replay_status(Formatter *f) const;

  MDSRank *mds;
  // replay state
  std::map<inodeno_t, std::set<inodeno_t>> pending_exports;

protected:
  struct PendingEvent {
    PendingEvent(LogEvent *e, Context* c, bool f=false) : le(e), fin(c), flush(f) {}
    LogEvent *le;
    Context* fin;
    bool flush;
  };

  // -- replay --
  class ReplayThread : public Thread {
  public:
    explicit ReplayThread(MDLog *l) : log(l) {}
    void* entry() override {
      log->_replay_thread();
      return 0;
    }
  private:
    MDLog *log;
  } replay_thread;

  // Journal recovery/rewrite logic
  class RecoveryThread : public Thread {
  public:
    explicit RecoveryThread(MDLog *l) : log(l) {}
    void set_completion(MDSContext *c) {completion = c;}
    void* entry() override {
      log->_recovery_thread(completion);
      return 0;
    }
  private:
    MDLog *log;
    MDSContext *completion = nullptr;
  } recovery_thread;

  class SubmitThread : public Thread {
  public:
    explicit SubmitThread(MDLog *l) : log(l) {}
    void* entry() override {
      log->_submit_thread();
      return 0;
    }
  private:
    MDLog *log;
  } submit_thread;

  friend class ReplayThread;
  friend class C_MDL_Replay;
  friend class MDSLogContextBase;
  friend class SubmitThread;
  // -- subtreemaps --
  friend class ESubtreeMap;
  friend class MDCache;

  void _replay();         // old way
  void _replay_thread();  // new way

  void _recovery_thread(MDSContext *completion);
  void _reformat_journal(JournalPointer const &jp, Journaler *old_journal, MDSContext *completion);

  void set_safe_pos(uint64_t pos)
  {
    std::lock_guard l(submit_mutex);
    ceph_assert(pos >= safe_pos);
    safe_pos = pos;
  }

  void _submit_thread();

  LogSegment *get_oldest_segment() {
    return segments.begin()->second;
  }
  void remove_oldest_segment() {
    std::map<uint64_t, LogSegment*>::iterator p = segments.begin();
    delete p->second;
    segments.erase(p);
  }

  uint64_t num_events = 0; // in events
  uint64_t unflushed = 0;
  bool mds_is_shutting_down = false;

  // Log position which is persistent *and* for which
  // submit_entry wait_for_safe callbacks have already
  // been called.
  uint64_t safe_pos = 0;

  inodeno_t ino;
  Journaler *journaler = nullptr;

  PerfCounters *logger = nullptr;

  bool already_replayed = false;

  MDSContext::vec waitfor_replay;

  // -- segments --
  std::map<uint64_t,LogSegment*> segments;
  std::size_t pre_segments_size = 0;            // the num of segments when the mds finished replay-journal, to calc the num of segments growing
  LogSegment::seq_t event_seq = 0;
  uint64_t expiring_events = 0;
  uint64_t expired_events = 0;

  int64_t mdsmap_up_features = 0;
  std::map<uint64_t,std::list<PendingEvent> > pending_events; // log segment -> event list
  ceph::fair_mutex submit_mutex{"MDLog::submit_mutex"};
  std::condition_variable_any submit_cond;

private:
  friend class C_MaybeExpiredSegment;
  friend class C_MDL_Flushed;
  friend class C_OFT_Committed;

  void try_to_commit_open_file_table(uint64_t last_seq);
  LogSegment* _start_new_segment(SegmentBoundary* sb);
  void _segment_upkeep();
  void _submit_entry(LogEvent* e, MDSLogContextBase* c);

  void try_expire(LogSegment *ls, int op_prio);
  void _maybe_expired(LogSegment *ls, int op_prio);
  void _expired(LogSegment *ls);
  void _trim_expired_segments();
  void write_head(MDSContext *onfinish);

  bool debug_subtrees;
  std::atomic_uint64_t event_large_threshold; // accessed by submit thread
  uint64_t events_per_segment;
  uint64_t major_segment_event_ratio;
  int64_t max_events;
  uint64_t max_segments;
  bool pause;
  bool skip_corrupt_events;
  bool skip_unbounded_events;

  std::set<uint64_t> major_segments;
  std::set<LogSegment*> expired_segments;
  std::set<LogSegment*> expiring_segments;
  uint64_t events_since_last_major_segment = 0;

  // log trimming decay counter
  DecayCounter log_trim_counter;
};
#endif
