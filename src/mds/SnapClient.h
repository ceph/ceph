// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <map>
#include <set>
#include <string_view>
#include <vector>

#include "MDSTableClient.h"
#include "mds_table_types.h" // for TABLE_SNAP
#include "snap.h"

class MDSContext;
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
		      version_t *pstid, bufferlist *pbl, MDSContext *onfinish);

  void prepare_create_realm(inodeno_t ino, version_t *pstid, bufferlist *pbl, MDSContext *onfinish);

  void prepare_destroy(inodeno_t ino, snapid_t snapid, version_t *pstid, bufferlist *pbl, MDSContext *onfinish);

  void prepare_update(inodeno_t ino, snapid_t snapid, std::string_view name, utime_t stamp,
		      version_t *pstid, MDSContext *onfinish);

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

  /*
   * Snap creates/destroys are prepared on every rank (notify_prep) before
   * the snapserver agrees, but a rank other than the requester only learns
   * of the commit from MMDSSnapUpdate. Clients are notified of the new seq
   * at the same time, so they can be ahead of this rank for a while.
   *
   * Return the tid of the snap create or destroy with the smallest seq in
   * [@first, @last] that is not committed on this rank yet (prepared, or
   * committed by the snapserver without this rank having been notified),
   * or 0 if there is none.
   */
  version_t get_pending_tid(snapid_t first, snapid_t last = CEPH_NOSNAP) const;
  /*
   * complete @c once this rank is notified that @tid committed, or once
   * @tid turns out to be rolled back
   */
  void wait_for_commit(version_t tid, MDSContext *c);

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
  /*
   * creates/destroys (tid -> seq) that a refresh showed committed by the
   * snapserver before notify_commit() was called for them on this rank,
   * i.e. before this rank handled the MMDSSnapUpdate for them.
   */
  std::map<version_t, snapid_t> unnotified_commits;

  std::map<version_t, std::vector<MDSContext*> > waiting_for_version;
  std::map<version_t, std::vector<MDSContext*> > waiting_for_commit;

  bool is_pending(version_t tid) const;
  void kick_commit_waiters();

  struct prepared_op_t {
    version_t tid;
    snapid_t snapid;  // snap created or destroyed
    snapid_t seq;
    bool destroy;
  };
  std::vector<prepared_op_t> get_prepared_ops() const;
  void note_unnotified_commits(const std::vector<prepared_op_t>& ops);

  uint64_t sync_reqid = 0;
  bool synced = false;
};
#endif
