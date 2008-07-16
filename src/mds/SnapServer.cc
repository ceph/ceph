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

#include "include/types.h"
#include "messages/MMDSTableRequest.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".snap: "

void SnapServer::init_inode()
{
  ino = MDS_INO_SNAPTABLE;
  layout = g_default_file_layout;
}

void SnapServer::reset_state()
{
  last_snap = 0;
  snaps.clear();
  pending_removal.clear();
}

snapid_t SnapServer::create(inodeno_t base, const string& name, utime_t stamp, version_t *psnapv)
{
  assert(is_active());
  
  snapid_t sn = ++last_snap;
  snaps[sn].snapid = sn;
  snaps[sn].dirino = base;
  snaps[sn].name = name;
  snaps[sn].stamp = stamp;
  *psnapv = ++version;

  dout(10) << "create(" << base << "," << name << ") = " << sn << dendl;

  return sn;
}

void SnapServer::remove(snapid_t sn) 
{
  assert(is_active());

  snaps.erase(sn);
  pending_removal.insert(sn);
  version++;
}


// SERVER

void SnapServer::_prepare(bufferlist &bl, __u64 reqid, int bymds)
{
  bufferlist::iterator p = bl.begin();
  __u32 what;
  ::decode(what, p);

  switch (what) {
  case TABLE_OP_CREATE:
    {
      SnapInfo info;
      ::decode(info.dirino, p);
      ::decode(info.name, p);
      ::decode(info.stamp, p);
      info.snapid = ++version;
      pending_create[version] = info;
    }
    break;

  case TABLE_OP_DESTROY:
    {
      snapid_t snapid;
      ::decode(snapid, p);
      version++;
      pending_destroy[version] = snapid;
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
    dout(7) << "commit " << tid << " destroy " << pending_destroy[tid] << dendl;
    snaps.erase(pending_destroy[tid]);
    pending_destroy.erase(tid);
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

  else
    assert(0);

  // bump version.
  version++;
  //dump();
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


