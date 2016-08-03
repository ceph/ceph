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

#ifndef CEPH_MDS_SESSIONMAP_H
#define CEPH_MDS_SESSIONMAP_H

#include <set>
using std::set;

#include "include/unordered_map.h"

#include "include/Context.h"
#include "include/xlist.h"
#include "include/elist.h"
#include "include/interval_set.h"
#include "mds/mdstypes.h"
#include "MDSAuthCaps.h"
#include "Capability.h"

class CInode;
class Message;
struct MDRequestImpl;


/* 
 * session
 */

class Session : public RefCountedObject {
  // -- state etc --
public:
  /*
                    
        <deleted> <-- closed <------------+
             ^         |                  |
             |         v                  |
          killing <-- opening <----+      |
             ^         |           |      |
             |         v           |      |
           stale <--> open --> closing ---+

    + additional dimension of 'importing' (with counter)

  */
  enum {
    STATE_CLOSED = 0,
    STATE_OPENING = 1,   // journaling open
    STATE_OPEN = 2,
    STATE_CLOSING = 3,   // journaling close
    STATE_STALE = 4,
    STATE_KILLING = 5
  };

  const char *get_state_name(int s) const {
    switch (s) {
    case STATE_CLOSED: return "closed";
    case STATE_OPENING: return "opening";
    case STATE_OPEN: return "open";
    case STATE_CLOSING: return "closing";
    case STATE_STALE: return "stale";
    case STATE_KILLING: return "killing";
    default: return "???";
    }
  }

private:
  int state;
  uint64_t state_seq;
  int importing_count;
  friend class SessionMap;

  // Human (friendly) name is soft state generated from client metadata
  void _update_human_name();
  std::string human_name;

  // Versions in this session was projected: used to verify
  // that appropriate mark_dirty calls follow.
  std::deque<version_t> projected;



public:

  void push_pv(version_t pv)
  {
    if (!projected.empty()) {
      assert(projected.back() != pv);
    }
    projected.push_back(pv);
  }

  void pop_pv(version_t v)
  {
    assert(!projected.empty());
    assert(projected.front() == v);
    projected.pop_front();
  }

  inline int get_state() const {return state;}
  void set_state(int new_state)
  {
    if (state != new_state) {
      state = new_state;
      state_seq++;
    }
  }
  void decode(bufferlist::iterator &p);
  void set_client_metadata(std::map<std::string, std::string> const &meta);
  std::string get_human_name() const {return human_name;}

  // Ephemeral state for tracking progress of capability recalls
  utime_t recalled_at;  // When was I asked to SESSION_RECALL?
  uint32_t recall_count;  // How many caps was I asked to SESSION_RECALL?
  uint32_t recall_release_count;  // How many caps have I actually revoked?

  session_info_t info;                         ///< durable bits

  MDSAuthCaps auth_caps;

  ConnectionRef connection;
  xlist<Session*>::item item_session_list;

  list<Message*> preopen_out_queue;  ///< messages for client, queued before they connect

  elist<MDRequestImpl*> requests;
  size_t get_request_count();

  interval_set<inodeno_t> pending_prealloc_inos; // journaling prealloc, will be added to prealloc_inos

  void notify_cap_release(size_t n_caps);
  void notify_recall_sent(int const new_limit);

  inodeno_t next_ino() {
    if (info.prealloc_inos.empty())
      return 0;
    return info.prealloc_inos.range_start();
  }
  inodeno_t take_ino(inodeno_t ino = 0) {
    assert(!info.prealloc_inos.empty());

    if (ino) {
      if (info.prealloc_inos.contains(ino))
	info.prealloc_inos.erase(ino);
      else
	ino = 0;
    }
    if (!ino) {
      ino = info.prealloc_inos.range_start();
      info.prealloc_inos.erase(ino);
    }
    info.used_inos.insert(ino, 1);
    return ino;
  }
  int get_num_projected_prealloc_inos() {
    return info.prealloc_inos.size() + pending_prealloc_inos.size();
  }

  client_t get_client() {
    return info.get_client();
  }

  int get_state() { return state; }
  const char *get_state_name() const { return get_state_name(state); }
  uint64_t get_state_seq() { return state_seq; }
  bool is_closed() const { return state == STATE_CLOSED; }
  bool is_opening() const { return state == STATE_OPENING; }
  bool is_open() const { return state == STATE_OPEN; }
  bool is_closing() const { return state == STATE_CLOSING; }
  bool is_stale() const { return state == STATE_STALE; }
  bool is_killing() const { return state == STATE_KILLING; }

  void inc_importing() {
    ++importing_count;
  }
  void dec_importing() {
    assert(importing_count > 0);
    --importing_count;
  }
  bool is_importing() { return importing_count > 0; }

  // -- caps --
private:
  version_t cap_push_seq;        // cap push seq #
  map<version_t, list<MDSInternalContextBase*> > waitfor_flush; // flush session messages

public:
  xlist<Capability*> caps;     // inodes with caps; front=most recently used
  xlist<ClientLease*> leases;  // metadata leases to clients
  utime_t last_cap_renew;

public:
  version_t inc_push_seq() { return ++cap_push_seq; }
  version_t get_push_seq() const { return cap_push_seq; }

  version_t wait_for_flush(MDSInternalContextBase* c) {
    waitfor_flush[get_push_seq()].push_back(c);
    return get_push_seq();
  }
  void finish_flush(version_t seq, list<MDSInternalContextBase*>& ls) {
    while (!waitfor_flush.empty()) {
      if (waitfor_flush.begin()->first > seq)
	break;
      ls.splice(ls.end(), waitfor_flush.begin()->second);
      waitfor_flush.erase(waitfor_flush.begin());
    }
  }

  void add_cap(Capability *cap) {
    caps.push_back(&cap->item_session_caps);
  }
  void touch_lease(ClientLease *r) {
    leases.push_back(&r->item_session_lease);
  }

  // -- leases --
  uint32_t lease_seq;

  // -- completed requests --
private:
  // Has completed_requests been modified since the last time we
  // wrote this session out?
  bool completed_requests_dirty;

  unsigned num_trim_flushes_warnings;
  unsigned num_trim_requests_warnings;
public:
  void add_completed_request(ceph_tid_t t, inodeno_t created) {
    info.completed_requests[t] = created;
    completed_requests_dirty = true;
  }
  bool trim_completed_requests(ceph_tid_t mintid) {
    // trim
    bool erased_any = false;
    while (!info.completed_requests.empty() && 
	   (mintid == 0 || info.completed_requests.begin()->first < mintid)) {
      info.completed_requests.erase(info.completed_requests.begin());
      erased_any = true;
    }

    if (erased_any) {
      completed_requests_dirty = true;
    }
    return erased_any;
  }
  bool have_completed_request(ceph_tid_t tid, inodeno_t *pcreated) const {
    map<ceph_tid_t,inodeno_t>::const_iterator p = info.completed_requests.find(tid);
    if (p == info.completed_requests.end())
      return false;
    if (pcreated)
      *pcreated = p->second;
    return true;
  }

  void add_completed_flush(ceph_tid_t tid) {
    info.completed_flushes.insert(tid);
  }
  bool trim_completed_flushes(ceph_tid_t mintid) {
    bool erased_any = false;
    while (!info.completed_flushes.empty() &&
	(mintid == 0 || *info.completed_flushes.begin() < mintid)) {
      info.completed_flushes.erase(info.completed_flushes.begin());
      erased_any = true;
    }
    if (erased_any) {
      completed_requests_dirty = true;
    }
    return erased_any;
  }
  bool have_completed_flush(ceph_tid_t tid) const {
    return info.completed_flushes.count(tid);
  }

  unsigned get_num_completed_flushes() const { return info.completed_flushes.size(); }
  unsigned get_num_trim_flushes_warnings() { return num_trim_flushes_warnings; }
  void inc_num_trim_flushes_warnings() { ++num_trim_flushes_warnings; }
  void reset_num_trim_flushes_warnings() { num_trim_flushes_warnings = 0; }

  unsigned get_num_completed_requests() const { return info.completed_requests.size(); }
  unsigned get_num_trim_requests_warnings() { return num_trim_requests_warnings; }
  void inc_num_trim_requests_warnings() { ++num_trim_requests_warnings; }
  void reset_num_trim_requests_warnings() { num_trim_requests_warnings = 0; }

  bool has_dirty_completed_requests() const
  {
    return completed_requests_dirty;
  }

  void clear_dirty_completed_requests()
  {
    completed_requests_dirty = false;
  }

  int check_access(CInode *in, unsigned mask, int caller_uid, int caller_gid,
		   int new_uid, int new_gid);


  Session() : 
    state(STATE_CLOSED), state_seq(0), importing_count(0),
    recalled_at(), recall_count(0), recall_release_count(0),
    auth_caps(g_ceph_context),
    connection(NULL), item_session_list(this),
    requests(0),  // member_offset passed to front() manually
    cap_push_seq(0),
    lease_seq(0),
    completed_requests_dirty(false),
    num_trim_flushes_warnings(0),
    num_trim_requests_warnings(0) { }
  ~Session() {
    assert(!item_session_list.is_on_list());
    while (!preopen_out_queue.empty()) {
      preopen_out_queue.front()->put();
      preopen_out_queue.pop_front();
    }
  }

  void clear() {
    pending_prealloc_inos.clear();
    info.clear_meta();

    cap_push_seq = 0;
    last_cap_renew = utime_t();

  }
};
#endif
