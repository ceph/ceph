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

#ifndef CEPH_SNAPCLIENT_H
#define CEPH_SNAPCLIENT_H

#include <string_view>

#include "MDSTableClient.h"
#include "snap.h"
#include "MDSContext.h"

class MDSRank;
class LogSegment;

class SnapClient : public MDSTableClient {
public:
  explicit SnapClient(MDSRank *m) :
    MDSTableClient(m, TABLE_SNAP) {}

  void resend_queries() override;
  void handle_query_result(const cref_t<MMDSTableRequest> &m) override;
  void handle_notify_prep(const cref_t<MMDSTableRequest> &m) override;
  void notify_commit(version_t tid) override;

  void prepare_create(inodeno_t dirino, std::string_view name, utime_t stamp,
		      version_t *pstid, bufferlist *pbl, MDSContext *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_CREATE;
    encode(op, bl);
    encode(dirino, bl);
    encode(name, bl);
    encode(stamp, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }

  void prepare_create_realm(inodeno_t ino, version_t *pstid, bufferlist *pbl, MDSContext *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_CREATE;
    encode(op, bl);
    encode(ino, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }

  void prepare_destroy(inodeno_t ino, snapid_t snapid, version_t *pstid, bufferlist *pbl, MDSContext *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_DESTROY;
    encode(op, bl);
    encode(ino, bl);
    encode(snapid, bl);
    _prepare(bl, pstid, pbl, onfinish);
  }

  void prepare_update(inodeno_t ino, snapid_t snapid, std::string_view name, utime_t stamp,
		      version_t *pstid, MDSContext *onfinish) {
    bufferlist bl;
    __u32 op = TABLE_OP_UPDATE;
    encode(op, bl);
    encode(ino, bl);
    encode(snapid, bl);
    encode(name, bl);
    encode(stamp, bl);
    _prepare(bl, pstid, NULL, onfinish);
  }

  version_t get_cached_version() const { return cached_version; }
  void refresh(version_t want, MDSContext *onfinish);

  void sync(MDSContext *onfinish);

  bool is_synced() const { return synced; }
  void wait_for_sync(MDSContext *c) {
    ceph_assert(!synced);
    waiting_for_version[std::max<version_t>(cached_version, 1)].push_back(c);
  }

  snapid_t get_last_created() const { return cached_last_created; }
  snapid_t get_last_destroyed() const { return cached_last_destroyed; }
  snapid_t get_last_seq() const { return std::max(cached_last_destroyed, cached_last_created); }

  void get_snaps(std::set<snapid_t>& snaps) const;
  std::set<snapid_t> filter(const std::set<snapid_t>& snaps) const;
  const SnapInfo* get_snap_info(snapid_t snapid) const;
  void get_snap_infos(std::map<snapid_t, const SnapInfo*>& infomap, const std::set<snapid_t>& snaps) const;

  int dump_cache(Formatter *f) const;

private:
  version_t cached_version = 0;
  snapid_t cached_last_created = 0, cached_last_destroyed = 0;
  std::map<snapid_t, SnapInfo> cached_snaps;
  std::map<version_t, SnapInfo> cached_pending_update;
  std::map<version_t, std::pair<snapid_t,snapid_t> > cached_pending_destroy;

  std::set<version_t> committing_tids;

  std::map<version_t, MDSContext::vec > waiting_for_version;

  uint64_t sync_reqid = 0;
  bool synced = false;
};
#endif
