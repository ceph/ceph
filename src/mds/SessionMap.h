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

#ifndef __MDS_SESSIONMAP_H
#define __MDS_SESSIONMAP_H

#include <set>
using std::set;

#include <ext/hash_map>
using __gnu_cxx::hash_map;

#include "include/Context.h"
#include "include/xlist.h"
#include "mdstypes.h"

class CInode;

class Session {
  // -- state --
public:
  static const int STATE_OPENING = 1;
  static const int STATE_OPEN = 2;
  static const int STATE_CLOSING = 3;
  static const int STATE_STALE = 4;   // ?
  static const int STATE_RECONNECTING = 5;

private:
  int state;
  utime_t last_alive;         // last alive
public:
  entity_inst_t inst;

  // -- caps --
private:
  version_t cap_push_seq;     // cap push seq #
  xlist<CInode*> cap_inodes;  // inodes with caps; front=most recently used

public:
  version_t inc_push_seq() { return ++cap_push_seq; }
  version_t get_push_seq() const { return cap_push_seq; }

  // -- completed requests --
private:
  set<tid_t> completed_requests;
  map<tid_t, Context*> waiting_for_trim;

public:
  void add_completed_request(tid_t t) {
    completed_requests.insert(t);
  }
  void trim_completed_requests(tid_t mintid) {
    // trim
    while (completed_requests.empty() && 
	   (mintid == 0 || *completed_requests.begin() < mintid))
      completed_requests.erase(completed_requests.begin());

    // kick waiters
    list<Context*> fls;
    while (!waiting_for_trim.empty() &&
	   (mintid == 0 || waiting_for_trim.begin()->first < mintid)) {
      fls.push_back(waiting_for_trim.begin()->second);
      waiting_for_trim.erase(waiting_for_trim.begin());
    }
    finish_contexts(fls);
  }
  void add_trim_waiter(metareqid_t ri, Context *c) {
    waiting_for_trim[ri.tid] = c;
  }
  bool have_completed_request(metareqid_t ri) const {
    return completed_requests.count(ri.tid);
  }

};

class SessionMap {
private:
  MDS *mds;
  hash_map<entity_name_t, Session> session_map;
  version_t version, projected, committing, committed;

public:
  SessionMap(MDS *m) : mds(m), 
		       version(0), projected(0), committing(0), committed(0) 
  { }
    
  bool empty() { return session_map.empty(); }
  
  Session* get_session(entity_name_t w) {
    if (session_map.count(w))
      return &session_map[w];
    return 0;
  }
  entity_inst_t& get_inst(entity_name_t w) {
    assert(session_map.count(w));
    return session_map[w].inst;
  }

  Session* add_session(entity_name_t w) {
    return &session_map[w];
  }
  void remove_session(entity_name_t w) {
    session_map.erase(w);
  }
  
  
};


#endif
