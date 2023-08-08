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
#include "common/ceph_mutex.h"
#include "global/global_context.h"
#include "include/spinlock.h"
#include "OSDCap.h"
#include "Watch.h"
#include "OSDMap.h"
#include "PeeringState.h"

//#define PG_DEBUG_REFS

class PG;
#ifdef PG_DEBUG_REFS
#include "common/tracked_int_ptr.hpp"
typedef TrackedIntPtr<PG> PGRef;
#else
typedef boost::intrusive_ptr<PG> PGRef;
#endif

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
  std::atomic<int> state = {STATE_NEW};
  spg_t pgid;          ///< owning pgid
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

  ceph::mutex lock = ceph::make_mutex("Backoff::lock");
  // NOTE: the owning PG and session are either
  //   - *both* set, or
  //   - both null (teardown), or
  //   - only session is set (and state == DELETING)
  PGRef pg;             ///< owning pg
  ceph::ref_t<struct Session> session;   ///< owning session
  hobject_t begin, end; ///< [) range to block, unless ==, then single obj

  friend ostream& operator<<(ostream& out, const Backoff& b) {
    return out << "Backoff(" << &b << " " << b.pgid << " " << b.id
	       << " " << b.get_state_name()
	       << " [" << b.begin << "," << b.end << ") "
	       << " session " << b.session
	       << " pg " << b.pg << ")";
  }

private:
  FRIEND_MAKE_REF(Backoff);
  Backoff(spg_t pgid, PGRef pg, ceph::ref_t<Session> s,
	  uint64_t i,
	  const hobject_t& b, const hobject_t& e)
    : RefCountedObject(g_ceph_context),
      pgid(pgid),
      id(i),
      pg(pg),
      session(std::move(s)),
      begin(b),
      end(e) {}
};



struct Session : public RefCountedObject {
  EntityName entity_name;
  OSDCap caps;
  ConnectionRef con;
  entity_addr_t socket_addr;
  WatchConState wstate;

  ceph::mutex session_dispatch_lock =
    ceph::make_mutex("Session::session_dispatch_lock");
  boost::intrusive::list<OpRequest> waiting_on_map;

  ceph::spinlock projected_epoch_lock;
  epoch_t projected_epoch = 0;

  /// protects backoffs; orders inside Backoff::lock *and* PG::backoff_lock
  ceph::mutex backoff_lock = ceph::make_mutex("Session::backoff_lock");
  std::atomic<int> backoff_count= {0};  ///< simple count of backoffs
  std::map<spg_t, std::map<hobject_t, std::set<ceph::ref_t<Backoff>>>> backoffs;

  std::atomic<uint64_t> backoff_seq = {0};

  // for heartbeat connections only
  int peer = -1;
  HeartbeatStampsRef stamps;

  entity_addr_t& get_peer_socket_addr() {
    return socket_addr;
  }

  void ack_backoff(
    CephContext *cct,
    spg_t pgid,
    uint64_t id,
    const hobject_t& start,
    const hobject_t& end);

  ceph::ref_t<Backoff> have_backoff(spg_t pgid, const hobject_t& oid) {
    if (!backoff_count.load()) {
      return nullptr;
    }
    std::lock_guard l(backoff_lock);
    ceph_assert(!backoff_count == backoffs.empty());
    auto i = backoffs.find(pgid);
    if (i == backoffs.end()) {
      return nullptr;
    }
    auto p = i->second.lower_bound(oid);
    if (p != i->second.begin() &&
	(p == i->second.end() || p->first > oid)) {
      --p;
    }
    if (p != i->second.end()) {
      int r = cmp(oid, p->first);
      if (r == 0 || r > 0) {
	for (auto& q : p->second) {
	  if (r == 0 || oid < q->end) {
	    return &(*q);
	  }
	}
      }
    }
    return nullptr;
  }

  bool check_backoff(
    CephContext *cct, spg_t pgid, const hobject_t& oid, const Message *m);

  void add_backoff(ceph::ref_t<Backoff> b) {
    std::lock_guard l(backoff_lock);
    ceph_assert(!backoff_count == backoffs.empty());
    backoffs[b->pgid][b->begin].insert(std::move(b));
    ++backoff_count;
  }

  // called by PG::release_*_backoffs and PG::clear_backoffs()
  void rm_backoff(const ceph::ref_t<Backoff>& b) {
    std::lock_guard l(backoff_lock);
    ceph_assert(ceph_mutex_is_locked_by_me(b->lock));
    ceph_assert(b->session == this);
    auto i = backoffs.find(b->pgid);
    if (i != backoffs.end()) {
      // may race with clear_backoffs()
      auto p = i->second.find(b->begin);
      if (p != i->second.end()) {
	auto q = p->second.find(b);
	if (q != p->second.end()) {
	  p->second.erase(q);
	  --backoff_count;
	  if (p->second.empty()) {
	    i->second.erase(p);
	    if (i->second.empty()) {
	      backoffs.erase(i);
	    }
	  }
	}
      }
    }
    ceph_assert(!backoff_count == backoffs.empty());
  }
  void clear_backoffs();

private:
  FRIEND_MAKE_REF(Session);
  explicit Session(CephContext *cct, Connection *con_) :
    RefCountedObject(cct),
    con(con_),
    socket_addr(con_->get_peer_socket_addr()),
    wstate(cct)
    {}
};

#endif
