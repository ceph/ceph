// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2008-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "BackTrace.h"
#include "Clock.h"
#include "common/dout.h"
#include "common/environment.h"
#include "include/types.h"
#include "lockdep.h"

#include "include/unordered_map.h"
#include "include/hash_namespace.h"

#if defined(__FreeBSD__) && defined(__LP64__)	// On FreeBSD pthread_t is a pointer.
CEPH_HASH_NAMESPACE_START
  template<>
    struct hash<pthread_t>
    {
      size_t
      operator()(pthread_t __x) const
      { return (uintptr_t)__x; }
    };
CEPH_HASH_NAMESPACE_END
#endif

/******* Constants **********/
#undef DOUT_COND
#define DOUT_COND(cct, l) cct && l <= XDOUT_CONDVAR(cct, dout_subsys)
#define lockdep_dout(v) lsubdout(g_lockdep_ceph_ctx, lockdep, v)
#define MAX_LOCKS  1000   // increase me as needed
#define BACKTRACE_SKIP 2

/******* Globals **********/
int g_lockdep = get_env_int("CEPH_LOCKDEP");
struct lockdep_stopper_t {
  // disable lockdep when this module destructs.
  ~lockdep_stopper_t() {
    g_lockdep = 0;
  }
};
static pthread_mutex_t lockdep_mutex = PTHREAD_MUTEX_INITIALIZER;
static CephContext *g_lockdep_ceph_ctx = NULL;
static lockdep_stopper_t lockdep_stopper;
static ceph::unordered_map<const char *, int> lock_ids;
static map<int, const char *> lock_names;
static int last_id = 0;
static ceph::unordered_map<pthread_t, map<int,BackTrace*> > held;
static BackTrace *follows[MAX_LOCKS][MAX_LOCKS];       // follows[a][b] means b taken after a

/******* Functions **********/
void lockdep_register_ceph_context(CephContext *cct)
{
  pthread_mutex_lock(&lockdep_mutex);
  g_lockdep_ceph_ctx = cct;
  pthread_mutex_unlock(&lockdep_mutex);
}

void lockdep_unregister_ceph_context(CephContext *cct)
{
  pthread_mutex_lock(&lockdep_mutex);
  if (cct == g_lockdep_ceph_ctx) {
    // this cct is going away; shut it down!
    g_lockdep = false;
    g_lockdep_ceph_ctx = NULL;
  }
  pthread_mutex_unlock(&lockdep_mutex);
}

int lockdep_dump_locks()
{
  pthread_mutex_lock(&lockdep_mutex);

  for (ceph::unordered_map<pthread_t, map<int,BackTrace*> >::iterator p = held.begin();
       p != held.end();
       ++p) {
    lockdep_dout(0) << "--- thread " << p->first << " ---" << dendl;
    for (map<int,BackTrace*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      lockdep_dout(0) << "  * " << lock_names[q->first] << "\n";
      if (q->second)
	q->second->print(*_dout);
      *_dout << dendl;
    }
  }

  pthread_mutex_unlock(&lockdep_mutex);
  return 0;
}


int lockdep_register(const char *name)
{
  int id;

  pthread_mutex_lock(&lockdep_mutex);

  if (last_id == 0)
    for (int i=0; i<MAX_LOCKS; i++)
      for (int j=0; j<MAX_LOCKS; j++)
	follows[i][j] = NULL;

  ceph::unordered_map<const char *, int>::iterator p = lock_ids.find(name);
  if (p == lock_ids.end()) {
    assert(last_id < MAX_LOCKS);
    id = last_id++;
    lock_ids[name] = id;
    lock_names[id] = name;
    lockdep_dout(10) << "registered '" << name << "' as " << id << dendl;
  } else {
    id = p->second;
    lockdep_dout(20) << "had '" << name << "' as " << id << dendl;
  }

  pthread_mutex_unlock(&lockdep_mutex);

  return id;
}


// does a follow b?
static bool does_follow(int a, int b)
{
  if (follows[a][b]) {
    lockdep_dout(0) << "\n";
    *_dout << "------------------------------------" << "\n";
    *_dout << "existing dependency " << lock_names[a] << " (" << a << ") -> "
           << lock_names[b] << " (" << b << ") at:\n";
    follows[a][b]->print(*_dout);
    *_dout << dendl;
    return true;
  }

  for (int i=0; i<MAX_LOCKS; i++) {
    if (follows[a][i] &&
	does_follow(i, b)) {
      lockdep_dout(0) << "existing intermediate dependency " << lock_names[a]
          << " (" << a << ") -> " << lock_names[i] << " (" << i << ") at:\n";
      follows[a][i]->print(*_dout);
      *_dout << dendl;
      return true;
    }
  }

  return false;
}

int lockdep_will_lock(const char *name, int id)
{
  pthread_t p = pthread_self();
  if (id < 0) id = lockdep_register(name);

  pthread_mutex_lock(&lockdep_mutex);
  lockdep_dout(20) << "_will_lock " << name << " (" << id << ")" << dendl;

  // check dependency graph
  map<int, BackTrace *> &m = held[p];
  for (map<int, BackTrace *>::iterator p = m.begin();
       p != m.end();
       ++p) {
    if (p->first == id) {
      lockdep_dout(0) << "\n";
      *_dout << "recursive lock of " << name << " (" << id << ")\n";
      BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
      bt->print(*_dout);
      if (p->second) {
	*_dout << "\npreviously locked at\n";
	p->second->print(*_dout);
      }
      delete bt;
      *_dout << dendl;
      assert(0);
    }
    else if (!follows[p->first][id]) {
      // new dependency

      // did we just create a cycle?
      BackTrace *bt = new BackTrace(BACKTRACE_SKIP);
      if (does_follow(id, p->first)) {
	lockdep_dout(0) << "new dependency " << lock_names[p->first]
		<< " (" << p->first << ") -> " << name << " (" << id << ")"
		<< " creates a cycle at\n";
	bt->print(*_dout);
	*_dout << dendl;

	lockdep_dout(0) << "btw, i am holding these locks:" << dendl;
	for (map<int, BackTrace *>::iterator q = m.begin();
	     q != m.end();
	     ++q) {
	  lockdep_dout(0) << "  " << lock_names[q->first] << " (" << q->first << ")" << dendl;
	  if (q->second) {
	    lockdep_dout(0) << " ";
	    q->second->print(*_dout);
	    *_dout << dendl;
	  }
	}

	lockdep_dout(0) << "\n" << dendl;

	// don't add this dependency, or we'll get aMutex. cycle in the graph, and
	// does_follow() won't terminate.

	assert(0);  // actually, we should just die here.
      } else {
	follows[p->first][id] = bt;
	lockdep_dout(10) << lock_names[p->first] << " -> " << name << " at" << dendl;
	//bt->print(*_dout);
      }
    }
  }

  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

int lockdep_locked(const char *name, int id, bool force_backtrace)
{
  pthread_t p = pthread_self();

  if (id < 0) id = lockdep_register(name);

  pthread_mutex_lock(&lockdep_mutex);
  lockdep_dout(20) << "_locked " << name << dendl;
  if (g_lockdep >= 2 || force_backtrace)
    held[p][id] = new BackTrace(BACKTRACE_SKIP);
  else
    held[p][id] = 0;
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

int lockdep_will_unlock(const char *name, int id)
{
  pthread_t p = pthread_self();

  if (id < 0) {
    //id = lockdep_register(name);
    assert(id == -1);
    return id;
  }

  pthread_mutex_lock(&lockdep_mutex);
  lockdep_dout(20) << "_will_unlock " << name << dendl;

  // don't assert.. lockdep may be enabled at any point in time
  //assert(held.count(p));
  //assert(held[p].count(id));

  delete held[p][id];
  held[p].erase(id);
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}


