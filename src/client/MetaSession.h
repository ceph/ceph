// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_METASESSION_H
#define CEPH_CLIENT_METASESSION_H

#include "include/types.h"
#include "include/utime.h"
#include "include/xlist.h"
#include "mds/MDSMap.h"
#include "mds/mdstypes.h"
#include "messages/MClientCapRelease.h"

struct Cap;
struct Inode;
struct CapSnap;
struct MetaRequest;

struct MetaSession {
  mds_rank_t mds_num;
  ConnectionRef con;
  version_t seq = 0;
  uint64_t cap_gen = 0;
  utime_t cap_ttl, last_cap_renew_request;
  uint64_t cap_renew_seq = 0;
  entity_addrvec_t addrs;
  feature_bitset_t mds_features;
  feature_bitset_t mds_metric_flags;

  enum {
    STATE_NEW, // Unused
    STATE_OPENING,
    STATE_OPEN,
    STATE_CLOSING,
    STATE_CLOSED,
    STATE_STALE,
    STATE_REJECTED,
  } state = STATE_OPENING;

  enum {
    RECLAIM_NULL,
    RECLAIMING,
    RECLAIM_OK,
    RECLAIM_FAIL,
  } reclaim_state = RECLAIM_NULL;

  int mds_state = MDSMap::STATE_NULL;
  bool readonly = false;

  std::list<Context*> waiting_for_open;

  xlist<Cap*> caps;
  // dirty_list keeps all the dirty inodes before flushing in current session.
  xlist<Inode*> dirty_list;
  xlist<Inode*> flushing_caps;
  xlist<MetaRequest*> requests;
  xlist<MetaRequest*> unsafe_requests;
  std::set<ceph_tid_t> flushing_caps_tids;

  ceph::ref_t<MClientCapRelease> release;

  MetaSession(mds_rank_t mds_num, ConnectionRef con, const entity_addrvec_t& addrs)
    : mds_num(mds_num), con(con), addrs(addrs) {
  }
  ~MetaSession() {
    ceph_assert(caps.empty());
    ceph_assert(dirty_list.empty());
    ceph_assert(flushing_caps.empty());
    ceph_assert(requests.empty());
    ceph_assert(unsafe_requests.empty());
  }

  xlist<Inode*> &get_dirty_list() { return dirty_list; }

  const char *get_state_name() const;

  void dump(Formatter *f, bool cap_dump=false) const;

  void enqueue_cap_release(inodeno_t ino, uint64_t cap_id, ceph_seq_t iseq,
      ceph_seq_t mseq, epoch_t osd_barrier);
};

using MetaSessionRef = std::shared_ptr<MetaSession>;
#endif
