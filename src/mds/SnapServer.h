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

#include "MDSTableServer.h"
#include "snap.h"

class MDSRank;
class MonClient;

class SnapServer : public MDSTableServer {
protected:
  MonClient *mon_client = nullptr;
  snapid_t last_snap = 0;
  snapid_t last_created, last_destroyed;
  snapid_t snaprealm_v2_since;
  map<snapid_t, SnapInfo> snaps;
  map<int, set<snapid_t> > need_to_purge;
  
  map<version_t, SnapInfo> pending_update;
  map<version_t, pair<snapid_t,snapid_t> > pending_destroy; // (removed_snap, seq)
  set<version_t>           pending_noop;

  version_t last_checked_osdmap;

  bool root_scrubbed = false; // all snaprealms under root are converted?
  bool mdsdir_scrubbed = false; // all snaprealms under ~mds0 are converted?

  void encode_server_state(bufferlist& bl) const override {
    ENCODE_START(5, 3, bl);
    encode(last_snap, bl);
    encode(snaps, bl);
    encode(need_to_purge, bl);
    encode(pending_update, bl);
    encode(pending_destroy, bl);
    encode(pending_noop, bl);
    encode(last_created, bl);
    encode(last_destroyed, bl);
    encode(snaprealm_v2_since, bl);
    ENCODE_FINISH(bl);
  }
  void decode_server_state(bufferlist::const_iterator& bl) override {
    DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
    decode(last_snap, bl);
    decode(snaps, bl);
    decode(need_to_purge, bl);
    decode(pending_update, bl);
    if (struct_v >= 2)
      decode(pending_destroy, bl);
    else {
      map<version_t, snapid_t> t;
      decode(t, bl);
      for (map<version_t, snapid_t>::iterator p = t.begin(); p != t.end(); ++p)
	pending_destroy[p->first].first = p->second; 
    } 
    decode(pending_noop, bl);
    if (struct_v >= 4) {
      decode(last_created, bl);
      decode(last_destroyed, bl);
    } else {
      last_created = last_snap;
      last_destroyed = last_snap;
    }
    if (struct_v >= 5)
      decode(snaprealm_v2_since, bl);
    else
      snaprealm_v2_since = CEPH_NOSNAP;

    DECODE_FINISH(bl);
  }

  // server bits
  void _prepare(const bufferlist &bl, uint64_t reqid, mds_rank_t bymds, bufferlist &out) override;
  void _get_reply_buffer(version_t tid, bufferlist *pbl) const override;
  void _commit(version_t tid, MMDSTableRequest::const_ref req) override;
  void _rollback(version_t tid) override;
  void _server_update(bufferlist& bl) override;
  bool _notify_prep(version_t tid) override;
  void handle_query(const MMDSTableRequest::const_ref &m) override;

public:
  SnapServer(MDSRank *m, MonClient *monc)
    : MDSTableServer(m, TABLE_SNAP), mon_client(monc), last_checked_osdmap(0) {}
  SnapServer() : MDSTableServer(NULL, TABLE_SNAP), last_checked_osdmap(0) {}

  void reset_state() override;

  bool upgrade_format() {
    // upgraded from old filesystem
    ceph_assert(last_snap > 0);
    bool upgraded = false;
    if (get_version() == 0) {
      // version 0 confuses snapclient code
      reset_state();
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

  void mark_base_recursively_scrubbed(inodeno_t ino) {
    if (ino ==  MDS_INO_ROOT)
      root_scrubbed = true;
    else if (ino == MDS_INO_MDSDIR(rank))
      mdsdir_scrubbed = true;
    else
      ceph_abort();
  }
  bool can_allow_multimds_snaps() const {
    return (root_scrubbed && mdsdir_scrubbed) ||
	   snaps.empty() || snaps.begin()->first >= snaprealm_v2_since;
  }

  void encode(bufferlist& bl) const {
    encode_server_state(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    decode_server_state(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<SnapServer*>& ls);

  bool force_update(snapid_t last, snapid_t v2_since,
		    map<snapid_t, SnapInfo>& _snaps);
};
WRITE_CLASS_ENCODER(SnapServer)

#endif
