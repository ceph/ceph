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

#ifndef __CLIENTMAP_H
#define __CLIENTMAP_H

#include "msg/Message.h"

#include <set>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/Context.h"
#include "mdstypes.h"

class MDS;

/*
 * this structure is used by the MDS purely so that
 * it can remember client addresses (entity_inst_t)
 * for clients with an active session.
 *
 * it is also used to keep track of recently completed
 * operations, should the client have to resubmit them
 * (after a connection failure, etc.)
 */
class ClientMap {
private:
  MDS *mds;

  version_t version;
  version_t projected;
  version_t committing;
  version_t committed;
  map<version_t, list<Context*> > commit_waiters;

public:
  version_t get_version() { return version; }
  version_t get_projected() { return projected; }
  version_t get_committing() { return committing; }
  version_t get_committed() { return committed; }

  version_t inc_projected() { return ++projected; }
  void reset_projected() { projected = version; }
  void set_committing(version_t v) { committing = v; }
  void set_committed(version_t v) { committed = v; }

private:
  // affects version
  hash_map<int,entity_inst_t> client_inst;

  // does not affect version
  set<int> sessions;
  set<int> opening;
  set<int> closing;

public:
  bool empty() {
    return client_inst.empty();
  }

  const entity_inst_t& get_inst(int client) {
    assert(client_inst.count(client));
    return client_inst[client];
  }
  const set<int>& get_session_set() { return sessions; }
  
  bool is_opening(int c) { return opening.count(c); }
  void add_opening(int c) { opening.insert(c); }
  bool is_closing(int c) { return closing.count(c); }
  void add_closing(int c) { closing.insert(c); }
  bool have_session(int client) {
    return client_inst.count(client);
  }
  void open_session(const entity_inst_t& inst) {
    opening.erase(inst.name.num());
    client_inst[inst.name.num()] = inst;
    sessions.insert(inst.name.num());
    version++;
  }
  void close_session(int client) {
    closing.erase(client);
    sessions.erase(client);
    client_inst.erase(client);
    version++;
  }
  
private:
  // -- push sequence --
  hash_map<int,version_t> client_push_seq; // seq # for messages pushed to client.

public:
  version_t inc_push_seq(int client) {
    return ++client_push_seq[client];
  }
  version_t get_push_seq(int client) {
    return client_push_seq[client];
  }


private:
  // -- completed requests --
  // client id -> tid -> result code
  map<int, set<tid_t> > completed_requests;  // completed client requests
  map<int, map<tid_t, Context*> > waiting_for_trim;
  version_t requestmapv;
 
public:
  void add_completed_request(metareqid_t ri) {
    completed_requests[ri.client].insert(ri.tid);
    requestmapv++;
  }
  void trim_completed_requests(int client, 
			       tid_t mintid) {  // zero means trim all!
    map<int, set<tid_t> >::iterator p = completed_requests.find(client);
    if (p == completed_requests.end())
      return;

    // trim
    while (!p->second.empty() && (mintid == 0 || *p->second.begin() < mintid))
      p->second.erase(p->second.begin());
    if (p->second.empty())
      completed_requests.erase(p);

    // kick waiters
    map<int, map<tid_t,Context*> >::iterator q = waiting_for_trim.find(client);
    if (q != waiting_for_trim.end()) {
      list<Context*> fls;
      while (!q->second.empty() &&
	     (mintid == 0 || q->second.begin()->first < mintid)) {
	fls.push_back(q->second.begin()->second);
	q->second.erase(q->second.begin());
      }
      if (q->second.empty())
	waiting_for_trim.erase(q);
      finish_contexts(fls);
    }
  }
  void add_trim_waiter(metareqid_t ri, Context *c) {
    waiting_for_trim[ri.client][ri.tid] = c;
  }
  bool have_completed_request(metareqid_t ri) {
    return completed_requests.count(ri.client) &&
      completed_requests[ri.client].count(ri.tid);
  }



  ClientMap(MDS *m) : mds(m),
		      version(0), projected(0), committing(0), committed(0),
		      requestmapv(0) {}


  // -- encoding --
  void encode(bufferlist& bl) {
    bl.append((char*)&version, sizeof(version));
    ::_encode(client_inst, bl);
    ::_encode(sessions, bl);
  }
  void decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(version), (char*)&version);
    off += sizeof(version);
    ::_decode(client_inst, bl, off);
    ::_decode(sessions, bl, off);

    projected = committing = committed = version;
  }


  // -- loading, saving --
  inode_t inode;
  list<Context*> waiting_for_load;

  void init_inode();
  void load(Context *onload);
  void _load_finish(bufferlist &bl);
  void save(Context *onsave, version_t needv=0);
  void _save_finish(version_t v);
};

#endif
