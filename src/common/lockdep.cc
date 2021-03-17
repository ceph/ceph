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
#include "lockdep.h"
#include <bitset>
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/valgrind.h"

/******* Constants **********/
#define lockdep_dout(v) lsubdout(g_lockdep_ceph_ctx, lockdep, v)
#define BACKTRACE_SKIP 2

/******* Globals **********/
bool g_lockdep;
struct lockdep_stopper_t {
  // disable lockdep when this module destructs.
  ~lockdep_stopper_t() {
    g_lockdep = 0;
  }
};


static pthread_mutex_t lockdep_mutex = PTHREAD_MUTEX_INITIALIZER;
static CephContext *g_lockdep_ceph_ctx = NULL;
static lockdep_stopper_t lockdep_stopper;
static ceph::unordered_map<std::string, int> lock_ids;
static std::map<int, std::string> lock_names;
static std::map<int, int> lock_refs;
static constexpr size_t MAX_LOCKS = 128 * 1024;   // increase me as needed
static std::bitset<MAX_LOCKS> free_ids; // bit set = free
static ceph::unordered_map<pthread_t, std::map<int,ceph::BackTrace*> > held;
static constexpr size_t NR_LOCKS = 4096; // the initial number of locks
static std::vector<std::bitset<MAX_LOCKS>> follows(NR_LOCKS); // follows[a][b] means b taken after a
static std::vector<std::map<int,ceph::BackTrace *>> follows_bt(NR_LOCKS);
// upper bound of lock id
unsigned current_maxid;
int last_freed_id = -1;
static bool free_ids_inited;

static bool lockdep_force_backtrace()
{
  return (g_lockdep_ceph_ctx != NULL &&
          g_lockdep_ceph_ctx->_conf->lockdep_force_backtrace);
}

/******* Functions **********/
void lockdep_register_ceph_context(CephContext *cct)
{
  static_assert((MAX_LOCKS > 0) && (MAX_LOCKS % 8 == 0),                   
    "lockdep's MAX_LOCKS needs to be divisible by 8 to operate correctly.");
  pthread_mutex_lock(&lockdep_mutex);
  if (g_lockdep_ceph_ctx == NULL) {
    ANNOTATE_BENIGN_RACE_SIZED(&g_lockdep_ceph_ctx, sizeof(g_lockdep_ceph_ctx),
                               "lockdep cct");
    ANNOTATE_BENIGN_RACE_SIZED(&g_lockdep, sizeof(g_lockdep),
                               "lockdep enabled");
    g_lockdep = true;
    g_lockdep_ceph_ctx = cct;
    lockdep_dout(1) << "lockdep start" << dendl;
    if (!free_ids_inited) {
      free_ids_inited = true;
      // FIPS zeroization audit 20191115: this memset is not security related.
      free_ids.set();
    }
  }
  pthread_mutex_unlock(&lockdep_mutex);
}

void lockdep_unregister_ceph_context(CephContext *cct)
{
  pthread_mutex_lock(&lockdep_mutex);
  if (cct == g_lockdep_ceph_ctx) {
    lockdep_dout(1) << "lockdep stop" << dendl;
    // this cct is going away; shut it down!
    g_lockdep = false;
    g_lockdep_ceph_ctx = NULL;

    // blow away all of our state, too, in case it starts up again.
    for (unsigned i = 0; i < current_maxid; ++i) {
      for (unsigned j = 0; j < current_maxid; ++j) {
        delete follows_bt[i][j];
      }
    }

    held.clear();
    lock_names.clear();
    lock_ids.clear();
    std::for_each(follows.begin(), std::next(follows.begin(), current_maxid),
                  [](auto& follow) { follow.reset(); });
    std::for_each(follows_bt.begin(), std::next(follows_bt.begin(), current_maxid),
                  [](auto& follow_bt) { follow_bt = {}; });
  }
  pthread_mutex_unlock(&lockdep_mutex);
}

int lockdep_dump_locks()
{
  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep)
    goto out;

  for (auto p = held.begin(); p != held.end(); ++p) {
    lockdep_dout(0) << "--- thread " << p->first << " ---" << dendl;
    for (auto q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      lockdep_dout(0) << "  * " << lock_names[q->first] << "\n";
      if (q->second)
	*_dout << *(q->second);
      *_dout << dendl;
    }
  }
out:
  pthread_mutex_unlock(&lockdep_mutex);
  return 0;
}

int lockdep_get_free_id(void)
{
  // if there's id known to be freed lately, reuse it
  if (last_freed_id >= 0 &&
      free_ids.test(last_freed_id)) {
    int tmp = last_freed_id;
    last_freed_id = -1;
    free_ids.reset(tmp);
    lockdep_dout(1) << "lockdep reusing last freed id " << tmp << dendl;
    return tmp;
  }
  
  // walk through entire array and locate nonzero char, then find
  // actual bit.
  for (size_t i = 0; i < free_ids.size(); ++i) {
    if (free_ids.test(i)) {
      free_ids.reset(i);
      return i;
    }
  }
  
  // not found
  lockdep_dout(0) << "failing miserably..." << dendl;
  return -1;
}

static int _lockdep_register(const char *name)
{
  int id = -1;

  if (!g_lockdep)
    return id;
  ceph::unordered_map<std::string, int>::iterator p = lock_ids.find(name);
  if (p == lock_ids.end()) {
    id = lockdep_get_free_id();
    if (id < 0) {
      lockdep_dout(0) << "ERROR OUT OF IDS .. have 0"
		      << " max " << MAX_LOCKS << dendl;
      for (auto& p : lock_names) {
	lockdep_dout(0) << "  lock " << p.first << " " << p.second << dendl;
      }
      ceph_abort();
    }
    if (current_maxid <= (unsigned)id) {
      current_maxid = (unsigned)id + 1;
      if (current_maxid == follows.size()) {
        follows.resize(current_maxid + 1);
        follows_bt.resize(current_maxid + 1);
      }
    }
    lock_ids[name] = id;
    lock_names[id] = name;
    lockdep_dout(10) << "registered '" << name << "' as " << id << dendl;
  } else {
    id = p->second;
    lockdep_dout(20) << "had '" << name << "' as " << id << dendl;
  }

  ++lock_refs[id];

  return id;
}

int lockdep_register(const char *name)
{
  int id;

  pthread_mutex_lock(&lockdep_mutex);
  id = _lockdep_register(name);
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

void lockdep_unregister(int id)
{
  if (id < 0) {
    return;
  }

  pthread_mutex_lock(&lockdep_mutex);

  std::string name;
  auto p = lock_names.find(id);
  if (p == lock_names.end())
    name = "unknown" ;
  else
    name = p->second;

  int &refs = lock_refs[id];
  if (--refs == 0) {
    if (p != lock_names.end()) {
      // reset dependency ordering
      follows[id].reset();
      for (unsigned i=0; i<current_maxid; ++i) {
        delete follows_bt[id][i];
        follows_bt[id][i] = NULL;

        delete follows_bt[i][id];
        follows_bt[i][id] = NULL;
        follows[i].reset(id);
      }

      lockdep_dout(10) << "unregistered '" << name << "' from " << id << dendl;
      lock_ids.erase(p->second);
      lock_names.erase(id);
    }
    lock_refs.erase(id);
    free_ids.set(id);
    last_freed_id = id;
  } else if (g_lockdep) {
    lockdep_dout(20) << "have " << refs << " of '" << name << "' " <<
			"from " << id << dendl;
  }
  pthread_mutex_unlock(&lockdep_mutex);
}


// does b follow a?
static bool does_follow(int a, int b)
{
  if (follows[a].test(b)) {
    lockdep_dout(0) << "\n";
    *_dout << "------------------------------------" << "\n";
    *_dout << "existing dependency " << lock_names[a] << " (" << a << ") -> "
           << lock_names[b] << " (" << b << ") at:\n";
    if (follows_bt[a][b]) {
      follows_bt[a][b]->print(*_dout);
    }
    *_dout << dendl;
    return true;
  }

  for (unsigned i=0; i<current_maxid; i++) {
    if (follows[a].test(i) &&
	does_follow(i, b)) {
      lockdep_dout(0) << "existing intermediate dependency " << lock_names[a]
          << " (" << a << ") -> " << lock_names[i] << " (" << i << ") at:\n";
      if (follows_bt[a][i]) {
        follows_bt[a][i]->print(*_dout);
      }
      *_dout << dendl;
      return true;
    }
  }

  return false;
}

int lockdep_will_lock(const char *name, int id, bool force_backtrace,
		      bool recursive)
{
  pthread_t p = pthread_self();

  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep) {
    pthread_mutex_unlock(&lockdep_mutex);
    return id;
  }

  if (id < 0)
    id = _lockdep_register(name);

  lockdep_dout(20) << "_will_lock " << name << " (" << id << ")" << dendl;

  // check dependency graph
  auto& m = held[p];
  for (auto p = m.begin(); p != m.end(); ++p) {
    if (p->first == id) {
      if (!recursive) {
	lockdep_dout(0) << "\n";
	*_dout << "recursive lock of " << name << " (" << id << ")\n";
	auto bt = new ceph::BackTrace(BACKTRACE_SKIP);
	bt->print(*_dout);
	if (p->second) {
	  *_dout << "\npreviously locked at\n";
	  p->second->print(*_dout);
	}
	delete bt;
	*_dout << dendl;
	ceph_abort();
      }
    } else if (!follows[p->first].test(id)) {
      // new dependency

      // did we just create a cycle?
      if (does_follow(id, p->first)) {
        auto bt = new ceph::BackTrace(BACKTRACE_SKIP);
	lockdep_dout(0) << "new dependency " << lock_names[p->first]
		<< " (" << p->first << ") -> " << name << " (" << id << ")"
		<< " creates a cycle at\n";
	bt->print(*_dout);
	*_dout << dendl;

	lockdep_dout(0) << "btw, i am holding these locks:" << dendl;
	for (auto q = m.begin(); q != m.end(); ++q) {
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

	ceph_abort();  // actually, we should just die here.
      } else {
	ceph::BackTrace* bt = NULL;
        if (force_backtrace || lockdep_force_backtrace()) {
          bt = new ceph::BackTrace(BACKTRACE_SKIP);
        }
        follows[p->first].set(id);
        follows_bt[p->first][id] = bt;
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

  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep)
    goto out;
  if (id < 0)
    id = _lockdep_register(name);

  lockdep_dout(20) << "_locked " << name << dendl;
  if (force_backtrace || lockdep_force_backtrace())
    held[p][id] = new ceph::BackTrace(BACKTRACE_SKIP);
  else
    held[p][id] = 0;
out:
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}

int lockdep_will_unlock(const char *name, int id)
{
  pthread_t p = pthread_self();

  if (id < 0) {
    //id = lockdep_register(name);
    ceph_assert(id == -1);
    return id;
  }

  pthread_mutex_lock(&lockdep_mutex);
  if (!g_lockdep)
    goto out;
  lockdep_dout(20) << "_will_unlock " << name << dendl;

  // don't assert.. lockdep may be enabled at any point in time
  //assert(held.count(p));
  //assert(held[p].count(id));

  delete held[p][id];
  held[p].erase(id);
out:
  pthread_mutex_unlock(&lockdep_mutex);
  return id;
}


