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

#include "SnapServer.h"
#include "MDSRank.h"
#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "mon/MonClient.h"

#include "include/types.h"
#include "messages/MMDSTableRequest.h"
#include "messages/MRemoveSnaps.h"

#include "msg/Messenger.h"

#include "common/config.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << ".snap "


void SnapServer::reset_state()
{
  last_snap = 1;  /* snapid 1 reserved for initial root snaprealm */
  snaps.clear();
  need_to_purge.clear();
  pending_update.clear();
  pending_destroy.clear();
  pending_noop.clear();

  // find any removed snapshot in data pools
  if (mds) {  // only if I'm running in a live MDS
    snapid_t first_free = 0;
    mds->objecter->with_osdmap([&](const OSDMap& o) {
	for (const auto p : mds->mdsmap->get_data_pools()) {
	  const pg_pool_t *pi = o.get_pg_pool(p);
	  if (!pi) {
	    // If pool isn't in OSDMap yet then can't have any snaps
	    // needing removal, skip.
	    continue;
	  }
	  if (pi->snap_seq > first_free) {
	    first_free = pi->snap_seq;
	  }
	}
      });
    if (first_free > last_snap)
      last_snap = first_free;
  }
  last_created = last_snap;
  last_destroyed = last_snap;
  snaprealm_v2_since = last_snap + 1;

  MDSTableServer::reset_state();
}


// SERVER

void SnapServer::_prepare(const bufferlist& bl, uint64_t reqid, mds_rank_t bymds, bufferlist& out)
{
  using ceph::decode;
  using ceph::encode;
  auto p = bl.cbegin();
  __u32 op;
  decode(op, p);

  switch (op) {
  case TABLE_OP_CREATE:
    {
      SnapInfo info;
      decode(info.ino, p);
      if (!p.end()) {
	decode(info.name, p);
	decode(info.stamp, p);
	info.snapid = ++last_snap;
	pending_update[version] = info;
	dout(10) << "prepare v" << version << " create " << info << dendl;
      } else {
	pending_noop.insert(version);
	dout(10) << "prepare v" << version << " noop" << dendl;
      }

      encode(last_snap, out);
    }
    break;

  case TABLE_OP_DESTROY:
    {
      inodeno_t ino;
      snapid_t snapid;
      decode(ino, p);    // not used, currently.
      decode(snapid, p);

      // bump last_snap... we use it as a version value on the snaprealm.
      ++last_snap;

      pending_destroy[version] = pair<snapid_t,snapid_t>(snapid, last_snap);
      dout(10) << "prepare v" << version << " destroy " << snapid << " seq " << last_snap << dendl;

      encode(last_snap, out);
    }
    break;

  case TABLE_OP_UPDATE:
    {
      SnapInfo info;
      decode(info.ino, p);
      decode(info.snapid, p);
      decode(info.name, p);
      decode(info.stamp, p);

      pending_update[version] = info;
      dout(10) << "prepare v" << version << " update " << info << dendl;
    }
    break;

  default:
    ceph_abort();
  }
  //dump();
}

void SnapServer::_get_reply_buffer(version_t tid, bufferlist *pbl) const
{
  using ceph::encode;
  auto p = pending_update.find(tid);
  if (p != pending_update.end()) {
    if (pbl && !snaps.count(p->second.snapid)) // create
      encode(p->second.snapid, *pbl);
    return;
  }
  auto q = pending_destroy.find(tid);
  if (q != pending_destroy.end()) {
    if (pbl)
      encode(q->second.second, *pbl);
    return;
  }
  auto r = pending_noop.find(tid);
  if (r != pending_noop.end()) {
    if (pbl)
      encode(last_snap, *pbl);
    return;
  }
  assert (0 == "tid not found");
}

void SnapServer::_commit(version_t tid, MMDSTableRequest::const_ref req)
{
  if (pending_update.count(tid)) {
    SnapInfo &info = pending_update[tid];
    string opname;
    if (snaps.count(info.snapid)) {
      opname = "update";
      if (info.stamp == utime_t())
	info.stamp = snaps[info.snapid].stamp;
    } else {
      opname = "create";
      if (info.snapid > last_created)
	last_created = info.snapid;
    }
    dout(7) << "commit " << tid << " " << opname << " " << info << dendl;
    snaps[info.snapid] = info;
    pending_update.erase(tid);
  }

  else if (pending_destroy.count(tid)) {
    snapid_t sn = pending_destroy[tid].first;
    snapid_t seq = pending_destroy[tid].second;
    dout(7) << "commit " << tid << " destroy " << sn << " seq " << seq << dendl;
    snaps.erase(sn);
    if (seq > last_destroyed)
      last_destroyed = seq;

    for (const auto p : mds->mdsmap->get_data_pools()) {
      need_to_purge[p].insert(sn);
      need_to_purge[p].insert(seq);
    }

    pending_destroy.erase(tid);
  }
  else if (pending_noop.count(tid)) {
    dout(7) << "commit " << tid << " noop" << dendl;
    pending_noop.erase(tid);
  }
  else
    ceph_abort();

  //dump();
}

void SnapServer::_rollback(version_t tid) 
{
  if (pending_update.count(tid)) {
    SnapInfo &info = pending_update[tid];
    string opname;
    if (snaps.count(info.snapid))
      opname = "update";
    else
      opname = "create";
    dout(7) << "rollback " << tid << " " << opname << " " << info << dendl;
    pending_update.erase(tid);
  } 

  else if (pending_destroy.count(tid)) {
    dout(7) << "rollback " << tid << " destroy " << pending_destroy[tid] << dendl;
    pending_destroy.erase(tid);
  }
  
  else if (pending_noop.count(tid)) {
    dout(7) << "rollback " << tid << " noop" << dendl;
    pending_noop.erase(tid);
  }    

  else
    ceph_abort();

  //dump();
}

void SnapServer::_server_update(bufferlist& bl)
{
  using ceph::decode;
  auto p = bl.cbegin();
  map<int, vector<snapid_t> > purge;
  decode(purge, p);

  dout(7) << "_server_update purged " << purge << dendl;
  for (map<int, vector<snapid_t> >::iterator p = purge.begin();
       p != purge.end();
       ++p) {
    for (vector<snapid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q)
      need_to_purge[p->first].erase(*q);
    if (need_to_purge[p->first].empty())
      need_to_purge.erase(p->first);
  }
}

bool SnapServer::_notify_prep(version_t tid)
{
  using ceph::encode;
  bufferlist bl;
  char type = 'F';
  encode(type, bl);
  encode(snaps, bl);
  encode(pending_update, bl);
  encode(pending_destroy, bl);
  encode(last_created, bl);
  encode(last_destroyed, bl);
  ceph_assert(version == tid);

  for (auto &p : active_clients) {
    auto m = MMDSTableRequest::create(table, TABLESERVER_OP_NOTIFY_PREP, 0, version);
    m->bl = bl;
    mds->send_message_mds(m, p);
  }
  return true;
}

void SnapServer::handle_query(const MMDSTableRequest::const_ref &req)
{
  using ceph::encode;
  using ceph::decode;
  char op;
  auto p = req->bl.cbegin();
  decode(op, p);

  auto reply = MMDSTableRequest::create(table, TABLESERVER_OP_QUERY_REPLY, req->reqid, version);

  switch (op) {
    case 'F': // full
      version_t have_version;
      decode(have_version, p);
      ceph_assert(have_version <= version);
      if (have_version == version) {
	char type = 'U';
	encode(type, reply->bl);
      } else {
	char type = 'F';
	encode(type, reply->bl);
	encode(snaps, reply->bl);
	encode(pending_update, reply->bl);
	encode(pending_destroy, reply->bl);
	encode(last_created, reply->bl);
	encode(last_destroyed, reply->bl);
      }
      // FIXME: implement incremental change
      break;
    default:
      ceph_abort();
  };

  mds->send_message(reply, req->get_connection());
}

void SnapServer::check_osd_map(bool force)
{
  if (!force && version == last_checked_osdmap) {
    dout(10) << "check_osd_map - version unchanged" << dendl;
    return;
  }
  dout(10) << "check_osd_map need_to_purge=" << need_to_purge << dendl;

  map<int, vector<snapid_t> > all_purge;
  map<int, vector<snapid_t> > all_purged;

  mds->objecter->with_osdmap(
    [this, &all_purged, &all_purge](const OSDMap& osdmap) {
      for (const auto& p : need_to_purge) {
	int id = p.first;
	const pg_pool_t *pi = osdmap.get_pg_pool(id);
	if (pi == NULL) {
	  // The pool is gone.  So are the snapshots.
	  all_purged[id] = std::vector<snapid_t>(p.second.begin(),
						 p.second.end());
	  continue;
	}

	for (const auto& q : p.second) {
	  if (pi->is_removed_snap(q)) {
	    dout(10) << " osdmap marks " << q << " as removed" << dendl;
	    all_purged[id].push_back(q);
	  } else {
	    all_purge[id].push_back(q);
	  }
	}
      }
  });

  if (!all_purged.empty()) {
    // prepare to remove from need_to_purge list
    bufferlist bl;
    using ceph::encode;
    encode(all_purged, bl);
    do_server_update(bl);
  }

  if (!all_purge.empty()) {
    dout(10) << "requesting removal of " << all_purge << dendl;
    auto m = MRemoveSnaps::create(all_purge);
    mon_client->send_mon_message(m.detach());
  }

  last_checked_osdmap = version;
}


void SnapServer::dump(Formatter *f) const
{
  f->open_object_section("snapserver");

  f->dump_int("last_snap", last_snap);
  f->dump_int("last_created", last_created);
  f->dump_int("last_destroyed", last_destroyed);

  f->open_array_section("pending_noop");
  for(set<version_t>::const_iterator i = pending_noop.begin(); i != pending_noop.end(); ++i) {
    f->dump_unsigned("version", *i);
  }
  f->close_section();

  f->open_array_section("snaps");
  for (map<snapid_t, SnapInfo>::const_iterator i = snaps.begin(); i != snaps.end(); ++i) {
    f->open_object_section("snap");
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("need_to_purge");
  for (map<int, set<snapid_t> >::const_iterator i = need_to_purge.begin(); i != need_to_purge.end(); ++i) {
    stringstream pool_id;
    pool_id << i->first;
    f->open_array_section(pool_id.str().c_str());
    for (set<snapid_t>::const_iterator s = i->second.begin(); s != i->second.end(); ++s) {
      f->dump_unsigned("snapid", s->val);
    }
    f->close_section();
  }
  f->close_section();

  f->open_array_section("pending_update");
  for(map<version_t, SnapInfo>::const_iterator i = pending_update.begin(); i != pending_update.end(); ++i) {
    f->open_object_section("snap");
    f->dump_unsigned("version", i->first);
    f->open_object_section("snapinfo");
    i->second.dump(f);
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("pending_destroy");
  for(map<version_t, pair<snapid_t, snapid_t> >::const_iterator i = pending_destroy.begin(); i != pending_destroy.end(); ++i) {
    f->open_object_section("snap");
    f->dump_unsigned("version", i->first);
    f->dump_unsigned("removed_snap", i->second.first);
    f->dump_unsigned("seq", i->second.second);
    f->close_section();
  }
  f->close_section();

  f->close_section();
}

void SnapServer::generate_test_instances(std::list<SnapServer*>& ls)
{
  list<SnapInfo*> snapinfo_instances;
  SnapInfo::generate_test_instances(snapinfo_instances);
  SnapInfo populated_snapinfo = *(snapinfo_instances.back());
  for (auto& info : snapinfo_instances) {
    delete info;
    info = nullptr;
  }

  SnapServer *blank = new SnapServer();
  ls.push_back(blank);
  SnapServer *populated = new SnapServer();
  populated->last_snap = 123;
  populated->snaps[456] = populated_snapinfo;
  populated->need_to_purge[2].insert(012);
  populated->pending_update[234] = populated_snapinfo;
  populated->pending_destroy[345].first = 567;
  populated->pending_destroy[345].second = 768;
  populated->pending_noop.insert(890);

  ls.push_back(populated);
}

bool SnapServer::force_update(snapid_t last, snapid_t v2_since,
			      map<snapid_t, SnapInfo>& _snaps)
{
  bool modified = false;
  if (last > last_snap) {
    derr << " updating last_snap " << last_snap << " -> " << last << dendl;
    last_snap = last;
    last_created = last;
    last_destroyed = last;
    modified = true;
  }
  if (v2_since > snaprealm_v2_since) {
    derr << " updating snaprealm_v2_since " << snaprealm_v2_since
	 << " -> " << v2_since << dendl;
    snaprealm_v2_since = v2_since;
    modified = true;
  }
  if (snaps != _snaps) {
    derr << " updating snaps {" << snaps << "} -> {" << _snaps << "}" << dendl;
    snaps = _snaps;
    modified = true;
  }

  if (modified) {
    need_to_purge.clear();
    pending_update.clear();
    pending_destroy.clear();
    pending_noop.clear();
    MDSTableServer::reset_state();
  }
  return modified;
}
