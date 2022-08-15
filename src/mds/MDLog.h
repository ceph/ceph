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

#include "include/common_fwd.h"

enum {
  l_mdl_first = 5000,
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
#include "common/Finisher.h"
#include "common/Thread.h"

#include "LogSegment.h"

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
  explicit MDLog(MDSRank *m) : mds(m),
                      replay_thread(this),
                      recovery_thread(this),
                      submit_thread(this) {}
  ~MDLog();

  const std::set<LogSegmentRef> &get_expiring_segments() const
  {
    return expiring_segments;
  }

  void create_logger();
  void set_write_iohint(unsigned iohint_flags);

  void start_new_segment() {
    std::lock_guard l(submit_mutex);
    _start_new_segment();
  }
  void prepare_new_segment() {
    std::lock_guard l(submit_mutex);
    _prepare_new_segment();
  }
  void journal_segment_subtree_map(MDSContext *onsync=NULL) {
    {
      std::lock_guard l{submit_mutex};
      _journal_segment_subtree_map(onsync);
    }
    if (onsync)
      flush();
  }

  LogSegmentRef peek_current_segment() {
    return segments.empty() ? NULL : segments.rbegin()->second;
  }

  LogSegmentRef get_current_segment() {   //return by value as it being passes by threads..
    ceph_assert(!segments.empty());
    return segments.rbegin()->second;
  }

  LogSegmentRef get_segment(LogSegment::seq_t seq) { //return by & ?
    if (segments.count(seq))
      return segments[seq];
    return NULL;
  }

  bool have_any_segments() const {
    return !segments.empty();
  }

  void flush_logger();

  size_t get_num_events() const { return num_events; }
  size_t get_num_segments() const { return segments.size(); }

  uint64_t get_read_pos() const;
  uint64_t get_write_pos() const;
  uint64_t get_safe_pos() const;
  Journaler *get_journaler() { return journaler; }
  bool empty() const { return segments.empty(); }

  bool is_capped() const { return mds_is_shutting_down; }
  void cap();

  void kick_submitter();
  void shutdown();

  void _start_entry(LogEvent *e);
  void start_entry(LogEvent *e) {
    std::lock_guard l(submit_mutex);
    _start_entry(e);
  }
  void cancel_entry(LogEvent *e);
  void _submit_entry(LogEvent *e, MDSLogContextBase *c);
  void submit_entry(LogEvent *e, MDSLogContextBase *c = 0) {
    std::lock_guard l(submit_mutex);
    _submit_entry(e, c);
    submit_cond.notify_all();
  }
  void start_submit_entry(LogEvent *e, MDSLogContextBase *c = 0) {
    std::lock_guard l(submit_mutex);
    _start_entry(e);
    _submit_entry(e, c);
    submit_cond.notify_all();
  }
  bool entry_is_open() const { return cur_event != NULL; }

  void wait_for_safe( MDSContext *c );
  void flush();
  bool is_flushed() const {
    return unflushed == 0;
  }

  void trim_expired_segments();
  void trim(int max=-1);
  int trim_all();
  bool expiry_done() const
  {
    return expiring_segments.empty() && expired_segments.empty();
  };

  void create(MDSContext *onfinish);  // fresh, empty log! 
  void open(MDSContext *onopen);      // append() or replay() to follow!
  void reopen(MDSContext *onopen);
  void append();
  void replay(MDSContext *onfinish);

  void standby_trim_segments();

  void dump_replay_status(Formatter *f) const;

  MDSRank *mds;
  // replay state
  std::map<inodeno_t, std::set<inodeno_t>> pending_exports;

protected:
  struct PendingEvent {
    PendingEvent(LogEvent *e, MDSContext *c, bool f=false) : le(e), fin(c), flush(f) {}
    LogEvent *le;
    MDSContext *fin;
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

  uint64_t get_last_segment_seq() const {
    ceph_assert(!segments.empty());
    return segments.rbegin()->first;
  }
  LogSegmentRef& get_oldest_segment() {
    return segments.begin()->second;
  }
  void remove_oldest_segment() {
    std::map<uint64_t, LogSegmentRef>::iterator p = segments.begin();
    segments.erase(p);
  }

  int num_events = 0; // in events
  int unflushed = 0;
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
  //std::map<uint64_t,LogSegment*> segments;
  //std::set<LogSegment*> expiring_segments;
  //std::set<LogSegment*> expired_segments;

  std::map<uint64_t,LogSegmentRef> segments;
  std::set<LogSegmentRef> expiring_segments;
  std::set<LogSegmentRef> expired_segments;  

  
  std::size_t pre_segments_size = 0;            // the num of segments when the mds finished replay-journal, to calc the num of segments growing
  uint64_t event_seq = 0;
  int expiring_events = 0;
  int expired_events = 0;

  int64_t mdsmap_up_features = 0;
  std::map<uint64_t,std::list<PendingEvent> > pending_events; // log segment -> event list
  ceph::mutex submit_mutex = ceph::make_mutex("MDLog::submit_mutex");
  ceph::condition_variable submit_cond;

private:
  friend class C_MaybeExpiredSegment;
  friend class C_MDL_Flushed;
  friend class C_OFT_Committed;

  // -- segments --
  void _start_new_segment();
  void _prepare_new_segment();
  void _journal_segment_subtree_map(MDSContext *onsync);

  void try_to_commit_open_file_table(uint64_t last_seq);

  void try_expire(LogSegmentRef &ls, int op_prio);
  void _maybe_expired(LogSegmentRef &ls, int op_prio);
  void _expired(LogSegmentRef &ls);
  void _trim_expired_segments();
  void write_head(MDSContext *onfinish);

  // -- events --
  LogEvent *cur_event = nullptr;
};
#endif
