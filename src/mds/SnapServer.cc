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
#include "MDS.h"
#include "osd/OSDMap.h"

#include "include/types.h"
#include "messages/MMDSTableRequest.h"
#include "messages/MRemoveSnaps.h"

#include "msg/Messenger.h"

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mds->get_nodeid() << ".snap "


void SnapServer::init_inode()
{
  ino = MDS_INO_SNAPTABLE;
}

void SnapServer::reset_state()
{
  last_snap = 1;  /* snapid 1 reserved for initial root snaprealm */
  snaps.clear();
  need_to_purge.clear();
}


// SERVER

void SnapServer::_prepare(bufferlist &bl, __u64 reqid, int bymds)
{
  bufferlist::iterator p = bl.begin();
  __u32 op;
  ::decode(op, p);

  switch (op) {
  case TABLE_OP_CREATE:
    {
      version++;

      SnapInfo info;
      ::decode(info.ino, p);
      if (!p.end()) {
	::decode(info.name, p);
	::decode(info.stamp, p);
	info.snapid = ++last_snap;
	pending_create[version] = info;
	dout(10) << "prepare v" << version << " create " << info << dendl;
      } else {
	pending_noop.insert(version);
	dout(10) << "prepare v" << version << " noop" << dendl;
      }
      bl.clear();
      ::encode(last_snap, bl);
    }
    break;

  case TABLE_OP_DESTROY:
    {
      inodeno_t ino;
      snapid_t snapid;
      ::decode(ino, p);    // not used, currently.
      ::decode(snapid, p);
      version++;
      pending_destroy[version] = snapid;
      dout(10) << "prepare v" << version << " destroy " << snapid << dendl;

      // bump last_snap... we use it as a version value on the snaprealm.
      bl.clear();
      ++last_snap;
      ::encode(last_snap, bl);
    }
    break;

  default:
    assert(0);
  }
  //dump();
}

bool SnapServer::_is_prepared(version_t tid)
{
  return 
    pending_create.count(tid) ||
    pending_destroy.count(tid);
}

void SnapServer::_commit(version_t tid)
{
  if (pending_create.count(tid)) {
    dout(7) << "commit " << tid << " create " << pending_create[tid] << dendl;
    snaps[pending_create[tid].snapid] = pending_create[tid];
    pending_create.erase(tid);
  } 

  else if (pending_destroy.count(tid)) {
    snapid_t sn = pending_destroy[tid];
    dout(7) << "commit " << tid << " destroy " << sn << dendl;
    snaps.erase(sn);
    need_to_purge.insert(sn);
    pending_destroy.erase(tid);
  }
  else if (pending_noop.count(tid)) {
    dout(7) << "commit " << tid << " noop" << dendl;
    pending_noop.erase(tid);
  }
  else
    assert(0);

  // bump version.
  version++;
  //dump();
}

void SnapServer::_rollback(version_t tid) 
{
  if (pending_create.count(tid)) {
    dout(7) << "rollback " << tid << " create " << pending_create[tid] << dendl;
    pending_create.erase(tid);
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
    assert(0);

  // bump version.
  version++;
  //dump();
}

void SnapServer::_server_update(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  vector<snapid_t> purge;
  ::decode(purge, p);

  dout(7) << "_server_update purged " << purge << dendl;
  for (vector<snapid_t>::iterator p = purge.begin();
       p != purge.end();
       p++)
    need_to_purge.erase(*p);

  version++;
}

void SnapServer::handle_query(MMDSTableRequest *req)
{
  /*  bufferlist::iterator p = req->bl.begin();
  inodeno_t curino;
  ::decode(curino, p);
  dout(7) << "handle_lookup " << *req << " ino " << curino << dendl;

  vector<Anchor> trace;
  while (true) {
    assert(anchor_map.count(curino) == 1);
    Anchor &anchor = anchor_map[curino];
    
    dout(10) << "handle_lookup  adding " << anchor << dendl;
    trace.insert(trace.begin(), anchor);  // lame FIXME
    
    if (anchor.dirino < MDS_INO_BASE) break;
    curino = anchor.dirino;
  }

  // reply
  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLE_OP_QUERY_REPLY, req->reqid, version);
  ::encode(curino, req->bl);
  ::encode(trace, req->bl);
  mds->send_message_mds(reply, req->get_source().num());

  */
  delete req;
}



void SnapServer::check_osd_map(bool force)
{
  if (!force && version == last_checked_osdmap) {
    dout(10) << "check_osd_map - version unchanged" << dendl;
    return;
  }
  dout(10) << "check_osd_map need_to_purge=" << need_to_purge << dendl;

  vector<snapid_t> purge;
  vector<snapid_t> purged;

  for (set<snapid_t>::iterator p = need_to_purge.begin();
       p != need_to_purge.end();
       p++) {
    if (mds->osdmap->is_removed_snap(*p)) {
      dout(10) << " osdmap marks " << *p << " as removed" << dendl;
      purged.push_back(*p);
    } else {
      purge.push_back(*p);
    }
  }

  if (purged.size()) {
    // prepare to remove from need_to_purge list
    bufferlist bl;
    ::encode(purged, bl);
    do_server_update(bl);
  }

  if (!purge.empty()) {
    dout(10) << "requesting removal of " << purge << dendl;
    MRemoveSnaps *m = new MRemoveSnaps(purge);
    int mon = mds->monmap->pick_mon();
    mds->messenger->send_message(m, mds->monmap->get_inst(mon));
  }

  last_checked_osdmap = version;
}
