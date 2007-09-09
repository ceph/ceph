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


#ifndef __MDLOG_H
#define __MDLOG_H

#include "include/types.h"
#include "include/Context.h"

#include "common/Thread.h"
#include "common/Cond.h"

#include <list>

//#include <ext/hash_map>
//using __gnu_cxx::hash_mapset;

class Journaler;
class LogEvent;
class MDS;

class Logger;

/*
namespace __gnu_cxx {
  template<> struct hash<LogEvent*> {
    size_t operator()(const LogEvent *p) const { 
      static hash<unsigned long> H;
      return H((unsigned long)p); 
    }
  };
}
*/

class MDLog {
 protected:
  MDS *mds;
  size_t num_events; // in events
  size_t max_events;

  int unflushed;

  bool capped;

  inode_t log_inode;
  Journaler *journaler;

  Logger *logger;

  // -- trimming --
  map<off_t,LogEvent*> trimming;
  std::list<Context*>  trim_waiters;   // contexts waiting for trim
  bool                 trim_reading;

  bool waiting_for_read;
  friend class C_MDL_Reading;

  
  off_t get_trimmed_to() {
    if (trimming.empty())
      return get_read_pos();
    else 
      return trimming.begin()->first;
  }


  // -- replay --
  Cond replay_cond;

  class ReplayThread : public Thread {
    MDLog *log;
  public:
    ReplayThread(MDLog *l) : log(l) {}
    void* entry() {
      log->_replay_thread();
      return 0;
    }
  } replay_thread;

  friend class ReplayThread;
  friend class C_MDL_Replay;

  list<Context*> waitfor_replay;

  void _replay();         // old way
  void _replay_thread();  // new way



  // -- subtreemaps --
  set<off_t>  subtree_maps;
  map<off_t,list<Context*> > subtree_map_expire_waiters;
  bool writing_subtree_map;  // one is being written now
  bool seen_subtree_map;     // for recovery

  friend class ESubtreeMap;
  friend class C_MDS_WroteImportMap;
  friend class MDCache;

  void kick_subtree_map() {
    if (subtree_map_expire_waiters.empty()) return;
    list<Context*> ls;
    ls.swap(subtree_map_expire_waiters.begin()->second);
    subtree_map_expire_waiters.erase(subtree_map_expire_waiters.begin());
    finish_contexts(ls);
  }

public:
  off_t get_last_subtree_map_offset() {
    assert(!subtree_maps.empty());
    return *subtree_maps.rbegin();
  }


private:
  void init_journaler();
  
public:
  void reopen_logger(utime_t start);
  
  // replay state
  map<inodeno_t, set<inodeno_t> >   pending_exports;



public:
  MDLog(MDS *m) : mds(m),
		  num_events(0), max_events(g_conf.mds_log_max_len),
		  unflushed(0),
		  capped(false),
		  journaler(0),
		  logger(0),
		  trim_reading(false), waiting_for_read(false),
		  replay_thread(this),
		  writing_subtree_map(false), seen_subtree_map(false) {
  }		  
  ~MDLog();


  void set_max_events(size_t max) { max_events = max; }
  size_t get_max_events() { return max_events; }
  size_t get_num_events() { return num_events + trimming.size(); }
  size_t get_non_subtreemap_events() { return num_events + trimming.size() - subtree_map_expire_waiters.size(); }

  off_t get_read_pos();
  off_t get_write_pos();
  bool empty() {
    return get_read_pos() == get_write_pos();
  }

  bool is_capped() { return capped; }
  void cap();

  void submit_entry( LogEvent *e, Context *c = 0 );
  void wait_for_sync( Context *c );
  void flush();

  void trim(Context *c);
  void _did_read();
  void _trimmed(LogEvent *le);

  void reset();  // fresh, empty log! 
  void open(Context *onopen);
  void write_head(Context *onfinish);

  void replay(Context *onfinish);
};

#endif
