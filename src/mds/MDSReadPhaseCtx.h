// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <vector>

#include <mutex>
#include <utility>

#include "include/utime.h"

class CDentry;
class CInode;
class Capability;
class Session;
struct ClientLease;

/*
 * Per-worker context of a parallel read phase (see MDSReadPhase).
 *
 * While a read phase runs, the dispatch thread holds mds_lock and several
 * workers serve read-only client requests on disjoint sets of inodes.  The
 * few places those requests touch state shared between inodes (the session
 * cap list, the snaprealm cap lists, the parent dentry/dir pins, global
 * counters) record what they would have done here instead, and the
 * dispatch thread applies it, single threaded, when the phase ends.
 */
struct MDSReadPhaseCtx {
  // cap ids reserved for this worker: [next_cap_id, end_cap_id)
  uint64_t next_cap_id = 0;
  uint64_t end_cap_id = 0;

  // inodes that got their first client cap in this phase
  std::vector<CInode*> first_caps;
  // caps created in this phase, still to be put on their session's cap
  // list and on their snaprealm
  std::vector<Capability*> new_caps;
  // inodes served, for the balancer's popularity counters
  std::vector<CInode*> hits;
  // sessions served, for the session load counters
  std::vector<Session*> sessions;
  // dentries that got their first client lease in this phase (to be pinned)
  std::vector<CDentry*> lease_pins;
  // leases issued or renewed, with their ttl: to be put on the session's and
  // the cache's lease lists
  std::vector<std::pair<ClientLease*, utime_t>> leases;
  // guards Session::lease_seq, shared between workers
  std::mutex *lease_seq_lock = nullptr;

  uint64_t alloc_cap_id();

  void clear() {
    first_caps.clear();
    new_caps.clear();
    hits.clear();
    sessions.clear();
    lease_pins.clear();
    leases.clear();
  }

  // the context of the calling thread, if it is serving a read phase
  static MDSReadPhaseCtx* current() { return tl_ctx; }
  static void set_current(MDSReadPhaseCtx *c) { tl_ctx = c; }

private:
  static thread_local MDSReadPhaseCtx *tl_ctx;
};
