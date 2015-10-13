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
  l_mdl_last,
};

#include "include/types.h"
#include "include/Context.h"

#include "common/Thread.h"
#include "common/Cond.h"

#include "LogSegment.h"

#include <list>

class Journaler;
class JournalPointer;
class LogEvent;
class MDSRank;
class LogSegment;
class ESubtreeMap;

class PerfCounters;

#include <map>
using std::map;

#include "common/Finisher.h"


class MDLog {
public:
  MDSRank *mds;
protected:
  int num_events; // in events

  int unflushed;

  bool capped;

  // Log position which is persistent *and* for which
  // submit_entry wait_for_safe callbacks have already
  // been called.
  uint64_t safe_pos;

  inodeno_t ino;
  Journaler *journaler;

  PerfCounters *logger;


  // -- replay --
  class ReplayThread : public Thread {
    MDLog *log;
  public:
    ReplayThread(MDLog *l) : log(l) {}
    void* entry() {
      log->_replay_thread();
      return 0;
    }
  } replay_thread;
  bool already_replayed;

  friend class ReplayThread;
  friend class C_MDL_Replay;

  list<MDSInternalContextBase*> waitfor_replay;

  void _replay();         // old way
  void _replay_thread();  // new way

  // Journal recovery/rewrite logic
  class RecoveryThread : public Thread {
    MDLog *log;
    MDSInternalContextBase *completion;
  public:
    void set_completion(MDSInternalContextBase *c) {completion = c;}
    RecoveryThread(MDLog *l) : log(l), completion(NULL) {}
    void* entry() {
      log->_recovery_thread(completion);
      return 0;
    }
  } recovery_thread;
  void _recovery_thread(MDSInternalContextBase *completion);
  void _reformat_journal(JournalPointer const &jp, Journaler *old_journal, MDSInternalContextBase *completion);

  // -- segments --
  map<uint64_t,LogSegment*> segments;
  set<LogSegment*> expiring_segments;
  set<LogSegment*> expired_segments;
  uint64_t event_seq;
  int expiring_events;
  int expired_events;

  struct PendingEvent {
    LogEvent *le;
    MDSInternalContextBase *fin;
    bool flush;
    PendingEvent(LogEvent *e, MDSInternalContextBase *c, bool f=false) : le(e), fin(c), flush(f) {}
  };

  map<uint64_t,list<PendingEvent> > pending_events; // log segment -> event list
  Mutex submit_mutex;
  Cond submit_cond;

  void _submit_thread();
  class SubmitThread : public Thread {
    MDLog *log;
  public:
    SubmitThread(MDLog *l) : log(l) {}
    void* entry() {
      log->_submit_thread();
      return 0;
    }
  } submit_thread;
  friend class SubmitThread;

public:
  const std::set<LogSegment*> &get_expiring_segments() const
  {
    return expiring_segments;
  }
protected:

  // -- subtreemaps --
  friend class ESubtreeMap;
  friend class MDCache;

  uint64_t get_last_segment_seq() {
    assert(!segments.empty());
    return segments.rbegin()->first;
  }
  LogSegment *get_oldest_segment() {
    return segments.begin()->second;
  }
  void remove_oldest_segment() {
    map<uint64_t, LogSegment*>::iterator p = segments.begin();
    delete p->second;
    segments.erase(p);
  }

public:
  void create_logger();
  
  // replay state
  map<inodeno_t, set<inodeno_t> >   pending_exports;

  void set_write_iohint(unsigned iohint_flags);

public:
  MDLog(MDSRank *m) : mds(m),
                      num_events(0), 
                      unflushed(0),
                      capped(false),
                      safe_pos(0),
                      journaler(0),
                      logger(0),
                      replay_thread(this),
                      already_replayed(false),
                      recovery_thread(this),
                      event_seq(0), expiring_events(0), expired_events(0),
                      submit_mutex("MDLog::submit_mutex"),
                      submit_thread(this),
                      cur_event(NULL) { }		  
  ~MDLog();


private:
  // -- segments --
  void _start_new_segment();
  void _prepare_new_segment();
  void _journal_segment_subtree_map(MDSInternalContextBase *onsync);
public:
  void start_new_segment() {
    Mutex::Locker l(submit_mutex);
    _start_new_segment();
  }
  void prepare_new_segment() {
    Mutex::Locker l(submit_mutex);
    _prepare_new_segment();
  }
  void journal_segment_subtree_map(MDSInternalContextBase *onsync=NULL) {
    submit_mutex.Lock();
    _journal_segment_subtree_map(onsync);
    submit_mutex.Unlock();
    if (onsync)
      flush();
  }

  LogSegment *peek_current_segment() {
    return segments.empty() ? NULL : segments.rbegin()->second;
  }

  LogSegment *get_current_segment() { 
    assert(!segments.empty());
    return segments.rbegin()->second;
  }

  LogSegment *get_segment(log_segment_seq_t seq) {
    if (segments.count(seq))
      return segments[seq];
    return NULL;
  }

  bool have_any_segments() {
    return !segments.empty();
  }

  void flush_logger();

  size_t get_num_events() const { return num_events; }
  size_t get_num_segments() const { return segments.size(); }

  uint64_t get_read_pos();
  uint64_t get_write_pos();
  uint64_t get_safe_pos();
  Journaler *get_journaler() { return journaler; }
  bool empty() { return segments.empty(); }

  bool is_capped() { return capped; }
  void cap();

  void shutdown();

  // -- events --
private:
  LogEvent *cur_event;
public:
  void _start_entry(LogEvent *e);
  void start_entry(LogEvent *e) {
    Mutex::Locker l(submit_mutex);
    _start_entry(e);
  }
  void cancel_entry(LogEvent *e);
  void _submit_entry(LogEvent *e, MDSInternalContextBase *c);
  void submit_entry(LogEvent *e, MDSInternalContextBase *c = 0) {
    Mutex::Locker l(submit_mutex);
    _submit_entry(e, c);
    submit_cond.Signal();
  }
  void start_submit_entry(LogEvent *e, MDSInternalContextBase *c = 0) {
    Mutex::Locker l(submit_mutex);
    _start_entry(e);
    _submit_entry(e, c);
    submit_cond.Signal();
  }
  bool entry_is_open() { return cur_event != NULL; }

  void wait_for_safe( MDSInternalContextBase *c );
  void flush();
  bool is_flushed() {
    return unflushed == 0;
  }

private:
  void try_expire(LogSegment *ls, int op_prio);
  void _maybe_expired(LogSegment *ls, int op_prio);
  void _expired(LogSegment *ls);
  void _trim_expired_segments();

  friend class C_MaybeExpiredSegment;
  friend class C_MDL_Flushed;

public:
  void trim_expired_segments();
  void trim(int max=-1);
  int trim_all();
  bool expiry_done() const
  {
    return expiring_segments.empty() && expired_segments.empty();
  };

private:
  void write_head(MDSInternalContextBase *onfinish);

public:
  void create(MDSInternalContextBase *onfinish);  // fresh, empty log! 
  void open(MDSInternalContextBase *onopen);      // append() or replay() to follow!
  void reopen(MDSInternalContextBase *onopen);
  void append();
  void replay(MDSInternalContextBase *onfinish);

  void standby_trim_segments();
};

#endif
