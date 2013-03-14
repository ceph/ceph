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

#include "AnchorServer.h"
#include "MDS.h"
#include "msg/Messenger.h"
#include "messages/MMDSTableRequest.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".anchorserver "

// table

void AnchorServer::reset_state()
{
  anchor_map.clear();
  pending_create.clear();
  pending_destroy.clear();
  pending_update.clear();
  pending_for_mds.clear();
}

void AnchorServer::dump()
{
  dout(7) << "dump v " << version << dendl;
  for (map<inodeno_t, Anchor>::iterator it = anchor_map.begin();
       it != anchor_map.end();
       ++it) 
    dout(15) << "dump " << it->second << dendl;
}

void AnchorServer::dump(Formatter *f) const
{
  f->open_array_section("anchor map");
  for (map<inodeno_t, Anchor>::const_iterator i = anchor_map.begin();
       i != anchor_map.end(); ++i) {
    f->open_object_section("entry");
    f->dump_int("ino", i->first);
    f->open_object_section("Anchor");
    i->second.dump(f);
    f->close_section(); // Anchor
    f->close_section(); // entry
  }
  f->close_section(); // anchor map
}

void AnchorServer::generate_test_instances(list<AnchorServer*>& ls)
{
  AnchorServer *sample = new AnchorServer();
  sample->pending_create[0] = 0;
  sample->pending_destroy[0] = 1;
  sample->anchor_map[0] = Anchor();
  ls.push_back(sample);
}



/*
 * basic updates
 * Returns true if it changed the anchor_map contents.
 */

bool AnchorServer::add(inodeno_t ino, inodeno_t dirino, __u32 dn_hash,
                       bool replace)
{
  //dout(17) << "add " << ino << " dirfrag " << dirfrag << dendl;
  
  // parent should be there
  assert(MDS_INO_IS_BASE(dirino) ||     // base case,
         anchor_map.count(dirino));   // or have it
  
  if (anchor_map.count(ino) == 0) {
    // new item
    anchor_map[ino] = Anchor(ino, dirino, dn_hash, 0, version);
    dout(7) << "add added " << anchor_map[ino] << dendl;
  } else if (replace) {
    anchor_map[ino].dirino = dirino;
    anchor_map[ino].dn_hash = dn_hash;
    dout(7) << "add had old Anchor, updated it to "
            << anchor_map[ino] << dendl;
  } else {
    dout(7) << "add had " << anchor_map[ino] << dendl;
    return false;
  }
  return true;
}

void AnchorServer::inc(inodeno_t ino, int ref)
{
  dout(7) << "inc " << ino << dendl;

  assert(anchor_map.count(ino));

  while (1) {
    Anchor &anchor = anchor_map[ino];
    anchor.nref += ref;
    anchor.updated = version;
      
    dout(10) << "inc now " << anchor << dendl;
    ino = anchor.dirino;
    
    if (ino == 0 || MDS_INO_IS_BASE(ino)) break;
    if (anchor_map.count(ino) == 0) break;
  }
}

void AnchorServer::dec(inodeno_t ino, int ref)
{
  dout(7) << "dec " << ino << dendl;
  assert(anchor_map.count(ino));

  while (true) {
    Anchor &anchor = anchor_map[ino];
    anchor.nref -= ref;
    anchor.updated = version;

    if (anchor.nref == 0) {
      dout(10) << "dec removing " << anchor << dendl;
      inodeno_t dirino = anchor.dirino;
      anchor_map.erase(ino);
      ino = dirino;
    } else {
      dout(10) << "dec now " << anchor << dendl;
      ino = anchor.dirino;
    }
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
  }
}


// server

void AnchorServer::_prepare(bufferlist &bl, uint64_t reqid, int bymds)
{
  bufferlist::iterator p = bl.begin();
  __u32 what;
  inodeno_t ino;
  vector<Anchor> trace;
  ::decode(what, p);
  ::decode(ino, p);

  switch (what) {
  case TABLE_OP_CREATE:
    ::decode(trace, p);
    version++;

    // make sure trace is in table
    dout(25) << "trace.size=" << trace.size() << dendl;
    for (unsigned i=0; i<trace.size(); i++) {
      add(trace[i].ino, trace[i].dirino, trace[i].dn_hash, false);
      dout(25) << trace[i] << dendl;
    }
    inc(ino);
    pending_create[version] = ino;  // so we can undo
    pending_ops[ino].push_back(pair<version_t, Context*>(version, NULL));
    break;

  case TABLE_OP_DESTROY:
    version++;
    pending_destroy[version] = ino;
    pending_ops[ino].push_back(pair<version_t, Context*>(version, NULL));
    break;
    
  case TABLE_OP_UPDATE:
    ::decode(trace, p);
    version++;
    pending_update[version].first = ino;
    pending_update[version].second = trace;
    pending_ops[ino].push_back(pair<version_t, Context*>(version, NULL));
    break;

  default:
    assert(0);
  }
  //dump();
}

bool AnchorServer::check_pending(version_t tid, MMDSTableRequest *req, list<Context *>& finished)
{
  inodeno_t ino;
  if (pending_create.count(tid))
    ino = pending_create[tid];
  else if (pending_destroy.count(tid))
    ino = pending_destroy[tid];
  else if (pending_update.count(tid))
    ino = pending_update[tid].first;
  else
    assert(0);

  assert(pending_ops.count(ino));
  list< pair<version_t, Context*> >& pending = pending_ops[ino];
  list< pair<version_t, Context*> >::iterator p = pending.begin();
  if (p->first == tid) {
    assert(p->second == NULL);
  } else {
    while (p != pending.end()) {
      if (p->first == tid)
	break;
      ++p;
    }
    assert(p != pending.end());
    assert(p->second == NULL);
    // not the earliest pending operation, wait if it's a commit
    if (req) {
      p->second = new C_MDS_RetryMessage(mds, req);
      return false;
    }
  }

  pending.erase(p);
  if (pending.empty()) {
    pending_ops.erase(ino);
  } else {
    for (p = pending.begin(); p != pending.end() && p->second; ++p) {
      finished.push_back(p->second);
      p->second = NULL;
    }
  }
  return true;
}

bool AnchorServer::_commit(version_t tid, MMDSTableRequest *req)
{
  list<Context *> finished;
  if (!check_pending(tid, req, finished))
    return false;

  if (pending_create.count(tid)) {
    dout(7) << "commit " << tid << " create " << pending_create[tid] << dendl;
    pending_create.erase(tid);
  } 

  else if (pending_destroy.count(tid)) {
    inodeno_t ino = pending_destroy[tid];
    dout(7) << "commit " << tid << " destroy " << ino << dendl;
    
    dec(ino);  // destroy
    
    pending_destroy.erase(tid);
  }

  else if (pending_update.count(tid)) {
    inodeno_t ino = pending_update[tid].first;
    vector<Anchor> &trace = pending_update[tid].second;
    
    if (anchor_map.count(ino)) {
      dout(7) << "commit " << tid << " update " << ino << dendl;

      int ref = anchor_map[ino].nref;
      // remove old
      dec(ino, ref);
      
      // add new
      for (unsigned i=0; i<trace.size(); i++) 
	add(trace[i].ino, trace[i].dirino, trace[i].dn_hash, true);
      inc(ino, ref);
    } else {
      dout(7) << "commit " << tid << " update " << ino << " -- DNE" << dendl;
    }
    
    pending_update.erase(tid);
  }
  else
    assert(0);

  // bump version.
  version++;
  //dump();

  mds->queue_waiters(finished);
  return true;
}

void AnchorServer::_rollback(version_t tid) 
{
  list<Context *> finished;
  check_pending(tid, NULL, finished);

  if (pending_create.count(tid)) {
    inodeno_t ino = pending_create[tid];
    dout(7) << "rollback " << tid << " create " << ino << dendl;
    dec(ino);
    pending_create.erase(tid);
  } 

  else if (pending_destroy.count(tid)) {
    inodeno_t ino = pending_destroy[tid];
    dout(7) << "rollback " << tid << " destroy " << ino << dendl;
    pending_destroy.erase(tid);
  }

  else if (pending_update.count(tid)) {
    inodeno_t ino = pending_update[tid].first;
    dout(7) << "rollback " << tid << " update " << ino << dendl;
    pending_update.erase(tid);
  }
  else
    assert(0);

  // bump version.
  version++;
  //dump();
  mds->queue_waiters(finished);
}

/* This function DOES put the passed message before returning */
void AnchorServer::handle_query(MMDSTableRequest *req)
{
  bufferlist::iterator p = req->bl.begin();
  inodeno_t ino;
  ::decode(ino, p);
  dout(7) << "handle_lookup " << *req << " ino " << ino << dendl;

  vector<Anchor> trace;
  inodeno_t curino = ino;
  while (true) {
    assert(anchor_map.count(curino) == 1);
    Anchor &anchor = anchor_map[curino];
    
    dout(10) << "handle_lookup  adding " << anchor << dendl;
    trace.insert(trace.begin(), anchor);  // lame FIXME
    
    if (MDS_INO_IS_BASE(anchor.dirino))
      break;
    curino = anchor.dirino;
  }

  // reply
  MMDSTableRequest *reply = new MMDSTableRequest(table, TABLESERVER_OP_QUERY_REPLY, req->reqid, version);
  ::encode(ino, reply->bl);
  ::encode(trace, reply->bl);
  mds->send_message(reply, req->get_connection());

  req->put();
}


