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

#ifndef CEPH_OSD_SESSION_H
#define CEPH_OSD_SESSION_H

#include "common/RefCountedObj.h"
#include "common/Mutex.h"
#include "include/Spinlock.h"
#include "OSDCap.h"
#include "Watch.h"
#include "OSDMap.h"

struct Session;
typedef boost::intrusive_ptr<Session> SessionRef;
struct Backoff;
typedef boost::intrusive_ptr<Backoff> BackoffRef;
class PG;
typedef boost::intrusive_ptr<PG> PGRef;

/*
 * A Backoff represents one instance of either a PG or an OID
 * being plugged at the client.  It's refcounted and linked from
 * the PG {pg_oid}_backoffs map and from the client Session
 * object.
 *
 * The Backoff has a lock that protects it's internal fields.
 *
 * The PG has a backoff_lock that protects it's maps to Backoffs.
 * This lock is *inside* of Backoff::lock.
 *
 * The Session has a backoff_lock that protects it's map of pg and
 * oid backoffs.  This lock is *inside* the Backoff::lock *and*
 * PG::backoff_lock.
 *
 * That's
 *
 *    Backoff::lock
 *       PG::backoff_lock
 *         Session::backoff_lock
 *
 * When the Session goes away, we move our backoff lists aside,
 * then we lock each of the Backoffs we
 * previously referenced and clear the Session* pointer.  If the PG
 * is still linked, we unlink it, too.
 *
 * When the PG clears the backoff, it will send an unblock message
 * if the Session* is still non-null, and unlink the session.
 *
 */

struct Backoff : public RefCountedObject {
  enum {
    STATE_NEW = 1,     ///< backoff in flight to client
    STATE_ACKED = 2,   ///< backoff acked
    STATE_DELETING = 3 ///< backoff deleted, but un-acked
  };
  std::atomic_int state = {STATE_NEW};
  uint64_t id = 0;     ///< unique id (within the Session)

  bool is_new() const {
    return state.load() == STATE_NEW;
  }
  bool is_acked() const {
    return state.load() == STATE_ACKED;
  }
  bool is_deleting() const {
    return state.load() == STATE_DELETING;
  }
  const char *get_state_name() const {
    switch (state.load()) {
    case STATE_NEW: return "new";
    case STATE_ACKED: return "acked";
    case STATE_DELETING: return "deleting";
    default: return "???";
    }
  }

  Mutex lock;
  // NOTE: the owning PG and session are either
  //   - *both* set, or
  //   - both null (teardown), or
  //   - only session is set (and state == DELETING)
  PGRef pg;             ///< owning pg
  SessionRef session;   ///< owning session
  hobject_t begin, end; ///< [) range to block, unless ==, then single obj

  Backoff(PGRef pg, SessionRef s,
	  uint64_t i,
	  const hobject_t& b, const hobject_t& e)
    : RefCountedObject(g_ceph_context, 0),
      id(i),
      lock("Backoff::lock"),
      pg(pg),
      session(s),
      begin(b),
      end(e) {}

  friend ostream& operator<<(ostream& out, const Backoff& b) {
    return out << "Backoff(" << &b << " " << b.id
	       << " " << b.get_state_name()
	       << " [" << b.begin << "," << b.end << ") "
	       << " session " << b.session
	       << " pg " << b.pg << ")";
  }
};



struct Session : public RefCountedObject {
  EntityName entity_name;
  OSDCap caps;
  int64_t auid;
  ConnectionRef con;
  WatchConState wstate;

  Mutex session_dispatch_lock;
  boost::intrusive::list<OpRequest> waiting_on_map;

  OSDMapRef osdmap;  /// Map as of which waiting_for_pg is current
  map<spg_t, boost::intrusive::list<OpRequest> > waiting_for_pg;

  Spinlock sent_epoch_lock;
  epoch_t last_sent_epoch;
  Spinlock received_map_lock;
  epoch_t received_map_epoch; // largest epoch seen in MOSDMap from here

  /// protects backoffs; orders inside Backoff::lock *and* PG::backoff_lock
  Mutex backoff_lock;
  std::atomic_int backoff_count= {0};  ///< simple count of backoffs
  map<hobject_t,set<BackoffRef>, hobject_t::BitwiseComparator> backoffs;

  std::atomic<uint64_t> backoff_seq = {0};

  explicit Session(CephContext *cct) :
    RefCountedObject(cct),
    auid(-1), con(0),
    wstate(cct),
    session_dispatch_lock("Session::session_dispatch_lock"),
    last_sent_epoch(0), received_map_epoch(0),
    backoff_lock("Session::backoff_lock")
    {}
  void maybe_reset_osdmap() {
    if (waiting_for_pg.empty()) {
      osdmap.reset();
    }
  }

  void ack_backoff(
    CephContext *cct,
    uint64_t id,
    const hobject_t& start,
    const hobject_t& end);

  Backoff *have_backoff(const hobject_t& oid) {
    if (backoff_count.load()) {
      Mutex::Locker l(backoff_lock);
      assert(backoff_count == (int)backoffs.size());
      auto p = backoffs.lower_bound(oid);
      if (p != backoffs.begin() &&
	  cmp_bitwise(p->first, oid) > 0) {
	--p;
      }
      if (p != backoffs.end()) {
	int r = cmp_bitwise(oid, p->first);
	if (r == 0 || r > 0) {
	  for (auto& q : p->second) {
	    if (r == 0 || cmp_bitwise(oid, q->end) < 0) {
	      return &(*q);
	    }
	  }
	}
      }
    }
    return nullptr;
  }

  // called by PG::release_*_backoffs and PG::clear_backoffs()
  void rm_backoff(BackoffRef b) {
    Mutex::Locker l(backoff_lock);
    assert(b->lock.is_locked_by_me());
    assert(b->session == this);
    auto p = backoffs.find(b->begin);
    // may race with clear_backoffs()
    if (p != backoffs.end()) {
      auto q = p->second.find(b);
      if (q != p->second.end()) {
	p->second.erase(q);
	if (p->second.empty()) {
	  backoffs.erase(p);
	  --backoff_count;
	}
      }
    }
    assert(backoff_count == (int)backoffs.size());
  }
  void clear_backoffs();
};

#endif
