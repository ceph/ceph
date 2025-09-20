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

#ifndef CEPH_SNAPSERVER_H
#define CEPH_SNAPSERVER_H

#include <list>
#include <map>
#include <set>

#include "MDSTableServer.h"
#include "include/encoding.h"
#include "include/object.h" // for struct snapid_t

class MDSRank;
class MRemoveSnaps;
class MonClient;
struct SnapInfo;

class SnapServer : public MDSTableServer {
public:
  SnapServer(MDSRank *m, MonClient *monc);
  SnapServer();
  ~SnapServer() noexcept;

  void handle_remove_snaps(const cref_t<MRemoveSnaps> &m);

  void reset_state() override;

  bool upgrade_format() {
    // upgraded from old filesystem
    ceph_assert(is_active());
    ceph_assert(last_snap > 0);
    bool upgraded = false;
    if (get_version() == 0) {
      // version 0 confuses snapclient code
      reset();
      upgraded = true;
    }
    if (snaprealm_v2_since == CEPH_NOSNAP) {
      // new snapshots will have new format snaprealms
      snaprealm_v2_since = last_snap + 1;
      upgraded = true;
    }
    return upgraded;
  }

  void check_osd_map(bool force);

  bool can_allow_multimds_snaps() const;

  void encode(bufferlist& bl) const {
    encode_server_state(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    decode_server_state(bl);
  }

  void dump(Formatter *f) const;
  static std::list<SnapServer> generate_test_instances();

  bool force_update(snapid_t last, snapid_t v2_since,
		    std::map<snapid_t, SnapInfo>& _snaps);

protected:
  void encode_server_state(bufferlist& bl) const override;
  void decode_server_state(bufferlist::const_iterator& bl) override;

  // server bits
  void _prepare(const bufferlist &bl, uint64_t reqid, mds_rank_t bymds, bufferlist &out) override;
  void _get_reply_buffer(version_t tid, bufferlist *pbl) const override;
  void _commit(version_t tid, cref_t<MMDSTableRequest> req) override;
  void _rollback(version_t tid) override;
  void _server_update(bufferlist& bl) override;
  bool _notify_prep(version_t tid) override;
  void handle_query(const cref_t<MMDSTableRequest> &m) override;

  MonClient *mon_client = nullptr;
  snapid_t last_snap = 0;
  snapid_t last_created, last_destroyed;
  snapid_t snaprealm_v2_since;
  std::map<snapid_t, SnapInfo> snaps;
  std::map<int, std::set<snapid_t> > need_to_purge;

  std::map<version_t, SnapInfo> pending_update;
  std::map<version_t, std::pair<snapid_t,snapid_t> > pending_destroy; // (removed_snap, seq)
  std::set<version_t> pending_noop;

  version_t last_checked_osdmap = 0;
};
WRITE_CLASS_ENCODER(SnapServer)

#endif
