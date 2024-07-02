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
#include <optional>

class Journaler;
class JournalPointerStore;
class LogEvent;
class MDSRankBase;
class ESubtreeMap;

class MDLog {
  static const AutoSharedLogSegment _null_segment;
public:

  MDLog(MDSRankBase *m);
  ~MDLog();

  // Installs the callback for when all segments marked for expriy
  // will have expired, unless there aren't any to await.
  // Returns:
  //  `true`  - some segments are expiring,
  //            and the context will be finished when they expire
  //  `false` - no segments are expiring,
  //            the context WON'T finish
  bool await_expiring_segments(Context* c);

  void create_logger();
  void set_write_iohint(unsigned iohint_flags);

  const AutoSharedLogSegment& peek_current_segment() const
  {
    return unexpired_segments.empty() ? _null_segment : unexpired_segments.rbegin()->second;
  }

  const AutoSharedLogSegment& get_current_segment() const
  {
    decltype(auto) current = peek_current_segment();
    ceph_assert(_null_segment != current);
    return current;
  }

  const AutoSharedLogSegment& get_unexpired_segment(LogSegment::seq_t seq)
  {
    auto it = unexpired_segments.find(seq);
    if (it != unexpired_segments.end()) {
      return it->second;
    } else {
      return _null_segment;
    }
  }

  bool have_any_segments() const {
    return !empty();
  }

  void flush_logger();

  uint64_t get_num_events() const { return count_total_events(); }
  uint64_t get_num_segments() const { return count_total_segments(); }
  uint64_t get_num_replayed_segments() const { return num_replayed_segments; }

  auto get_debug_subtrees() const {
    return debug_subtrees;
  }
  auto get_max_segments() const {
    return max_live_segments;
  }

  uint64_t get_read_pos() const;
  uint64_t get_write_pos() const;
  uint64_t get_safe_pos() const;
  Journaler *get_journaler() { return journaler; }
  bool empty() const { return count_total_segments() == 0; }

  uint64_t get_last_major_segment_seq() const {
    ceph_assert(!major_segment_seqs.empty());
    return *major_segment_seqs.rbegin();
  }
  uint64_t get_last_segment_seq() const {
    return get_current_segment()->seq;
  }

  bool is_capped() const { return mds_is_shutting_down; }
  void cap();

  void kick_submitter();
  void shutdown();

  void submit_entry(LogEvent *e, MDSLogContextBase* c = 0);

  void wait_for_safe(Context* c);
  void flush();

  void trim_expired_segments();
  int trim_all();

  void create(MDSContext *onfinish);  // fresh, empty log! 
  void open(MDSContext *onopen);      // append() or replay() to follow!
  void reopen(MDSContext *onopen);
  void append();
  void replay(MDSContext *onfinish);

  void standby_cleanup_trimmed_segments();

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

  void dump_replay_status(Formatter *f) const;

  MDSRankBase *mds;
  // replay state
  std::map<inodeno_t, std::set<inodeno_t>> pending_exports;

private:
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
  void _reformat_journal(JournalPointerStore *jps, Journaler *old_journal, MDSContext *completion);

  void set_safe_pos(uint64_t pos)
  {
    ceph_assert(pos >= safe_pos);
    safe_pos = pos;
  }

  void _submit_thread();

  uint64_t num_live_events = 0; // in events
  uint64_t queued_flushes = 0;
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
  std::map<LogSegment::seq_t, std::vector<Context*>> expiry_waiters;

  // -- live and expiring segments --
  std::map<LogSegment::seq_t, AutoSharedLogSegment> unexpired_segments;
  LogSegment::seq_t event_seq = 0;
  std::optional<LogSegment::seq_t> last_expiring_segment_seq;
  uint64_t num_expiring_segments = 0;
  uint64_t num_expiring_events = 0;
  uint64_t num_expired_events = 0;

  int64_t mdsmap_up_features = 0;
  std::map<uint64_t,std::list<PendingEvent> > pending_events; // log segment -> event list
  ceph::fair_mutex submit_mutex{"MDLog::submit_mutex"};
  std::condition_variable_any submit_cond;

  inline uint64_t count_total_events() const
  {
    return num_live_events + num_expiring_events + num_expired_events;
  }
  inline uint64_t count_total_segments() const
  {
    return unexpired_segments.size() + expired_segments.size();
  }
  inline uint64_t count_live_segments() const
  {
    return unexpired_segments.size() - num_expiring_segments;
  }

  friend class C_MaybeExpiredSegment;
  friend class C_MDL_Flushed;
  friend class C_OFT_Committed;

  void try_to_commit_open_file_table(uint64_t last_seq);
  const AutoSharedLogSegment& _start_new_segment(SegmentBoundary* sb);
  void segment_upkeep();

  void _mark_for_expiry(const AutoSharedLogSegment& ls, int op_prio);
  void try_expire(const AutoSharedLogSegment& ls, int op_prio);
  void _maybe_expired(const AutoSharedLogSegment& ls, int op_prio);

  // we want this to take a strong reference to avoid races
  // when removing the segment from unexpired
  void _expired(AutoSharedLogSegment ls);
  void write_head(MDSContext *onfinish);

  void trim();
  void log_trim_upkeep(void);

  bool debug_subtrees;
  std::atomic_uint64_t event_large_threshold; // accessed by submit thread
  uint64_t events_per_segment = 0;
  uint64_t major_segment_event_ratio = 0;
  int64_t max_live_events = 0;
  uint64_t max_live_segments = 0;
  uint64_t num_replayed_segments = 0;
  bool pause;
  bool skip_corrupt_events;
  bool skip_unbounded_events;

  std::set<LogSegment::seq_t> major_segment_seqs;

  struct ExpiredSegmentInfo
  {
    WeakLogSegment segment;
    uint64_t offset, end;
    uint64_t num_events;
    ExpiredSegmentInfo(const AutoSharedLogSegment &src)
    {
      segment = src;
      ceph_assert(src->has_bounds());
      offset = src->get_offset();
      end = src->get_end();
      num_events = src->num_events;
    }
    ExpiredSegmentInfo() = default;
  };
  std::map<LogSegment::seq_t, ExpiredSegmentInfo> expired_segments;
  uint64_t events_since_last_major_segment = 0;

  // log trimming decay counter
  DecayCounter log_trim_counter;

  // log trimming upkeeper thread
  std::thread upkeep_thread;
  // guarded by mds_lock
  std::condition_variable_any cond;
  std::atomic<bool> upkeep_log_trim_shutdown{false};
};
#endif
