// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_METASESSION_H
#define CEPH_CLIENT_METASESSION_H

#include "include/types.h"
#include "include/utime.h"
#include "msg/msg_types.h"
#include "include/xlist.h"

#include "messages/MClientCapRelease.h"
#include "mds/MDSMap.h"

struct Cap;
struct Inode;
struct CapSnap;
struct MetaRequest;
class MClientCapRelease;

struct MetaSession {
  mds_rank_t mds_num;
  ConnectionRef con;
  version_t seq;
  uint64_t cap_gen;
  utime_t cap_ttl, last_cap_renew_request;
  uint64_t cap_renew_seq;
  int num_caps;
  entity_inst_t inst;

  enum {
    STATE_NEW,
    STATE_OPENING,
    STATE_OPEN,
    STATE_CLOSING,
    STATE_CLOSED,
    STATE_STALE,
  } state;

  int mds_state;
  bool readonly;

  list<Context*> waiting_for_open;

  xlist<Cap*> caps;
  xlist<Inode*> flushing_caps;
  xlist<MetaRequest*> requests;
  xlist<MetaRequest*> unsafe_requests;
  std::set<ceph_tid_t> flushing_caps_tids;
  std::set<Inode*> early_flushing_caps;

  Cap *s_cap_iterator;

  MClientCapRelease *release;
  
  MetaSession()
    : mds_num(-1), con(NULL),
      seq(0), cap_gen(0), cap_renew_seq(0), num_caps(0),
      state(STATE_NEW), mds_state(0), readonly(false),
      s_cap_iterator(NULL), release(NULL)
  {}
  ~MetaSession();

  const char *get_state_name() const;

  void dump(Formatter *f) const;

  void enqueue_cap_release(inodeno_t ino, uint64_t cap_id, ceph_seq_t iseq,
      ceph_seq_t mseq, epoch_t osd_barrier);
};

#endif
