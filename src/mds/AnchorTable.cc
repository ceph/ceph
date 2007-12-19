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

#include "AnchorTable.h"
#include "MDS.h"

#include "osdc/Filer.h"

#include "msg/Messenger.h"
#include "messages/MAnchor.h"

#include "common/Clock.h"

#include "MDLog.h"
#include "events/EAnchor.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " " << mds->messenger->get_myname() << ".anchortable "
#define derr(x)  if (x <= g_conf.debug_mds) *_derr << dbeginl << g_clock.now() << " " << mds->messenger->get_myname() << ".anchortable "


void AnchorTable::dump()
{
  dout(7) << "dump v " << version << dendl;
  for (hash_map<inodeno_t, Anchor>::iterator it = anchor_map.begin();
       it != anchor_map.end();
       it++) 
    dout(15) << "dump " << it->second << dendl;
}


/*
 * basic updates
 */

bool AnchorTable::add(inodeno_t ino, dirfrag_t dirfrag) 
{
  //dout(17) << "add " << ino << " dirfrag " << dirfrag << dendl;
  
  // parent should be there
  assert(dirfrag.ino < MDS_INO_BASE ||     // system dirino
         anchor_map.count(dirfrag.ino));   // have
  
  if (anchor_map.count(ino) == 0) {
    // new item
    anchor_map[ino] = Anchor(ino, dirfrag);
    dout(7) << "add added " << anchor_map[ino] << dendl;
    return true;
  } else {
    dout(7) << "add had " << anchor_map[ino] << dendl;
    return false;
  }
}

void AnchorTable::inc(inodeno_t ino)
{
  dout(7) << "inc " << ino << dendl;

  assert(anchor_map.count(ino));

  while (1) {
    Anchor &anchor = anchor_map[ino];
    anchor.nref++;
      
    dout(10) << "inc now " << anchor << dendl;
    ino = anchor.dirfrag.ino;
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
  }
}

void AnchorTable::dec(inodeno_t ino) 
{
  dout(7) << "dec " << ino << dendl;
  assert(anchor_map.count(ino));

  while (true) {
    Anchor &anchor = anchor_map[ino];
    anchor.nref--;
      
    if (anchor.nref == 0) {
      dout(10) << "dec removing " << anchor << dendl;
      dirfrag_t dirfrag = anchor.dirfrag;
      anchor_map.erase(ino);
      ino = dirfrag.ino;
    } else {
      dout(10) << "dec now " << anchor << dendl;
      ino = anchor.dirfrag.ino;
    }
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
  }
}


/* 
 * high level 
 */


// LOOKUP

void AnchorTable::handle_lookup(MAnchor *req)
{
  inodeno_t curino = req->get_ino();
  dout(7) << "handle_lookup " << curino << dendl;

  vector<Anchor> trace;
  while (true) {
    assert(anchor_map.count(curino) == 1);
    Anchor &anchor = anchor_map[curino];

    dout(10) << "handle_lookup  adding " << anchor << dendl;
    trace.insert(trace.begin(), anchor);  // lame FIXME

    if (anchor.dirfrag.ino < MDS_INO_BASE) break;
    curino = anchor.dirfrag.ino;
  }

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_LOOKUP_REPLY, req->get_ino());
  reply->set_trace(trace);
  mds->messenger->send_message(reply, req->get_source_inst());

  delete req;
}


// MIDLEVEL

void AnchorTable::create_prepare(inodeno_t ino, vector<Anchor>& trace, int reqmds)
{
  // make sure trace is in table
  for (unsigned i=0; i<trace.size(); i++) 
    add(trace[i].ino, trace[i].dirfrag);
  inc(ino);

  version++;
  pending_create[version] = ino;  // so we can undo
  pending_reqmds[version] = reqmds;
  //dump();
}

void AnchorTable::destroy_prepare(inodeno_t ino, int reqmds)
{
  version++;
  pending_destroy[version] = ino;
  pending_reqmds[version] = reqmds;
  //dump();
}

void AnchorTable::update_prepare(inodeno_t ino, vector<Anchor>& trace, int reqmds)
{
  version++;
  pending_update[version].first = ino;
  pending_update[version].second = trace;
  pending_reqmds[version] = reqmds;
  //dump();
}

void AnchorTable::commit(version_t atid) 
{
  if (pending_create.count(atid)) {
    dout(7) << "commit " << atid << " create " << pending_create[atid] << dendl;
    pending_create.erase(atid);
  } 

  else if (pending_destroy.count(atid)) {
    inodeno_t ino = pending_destroy[atid];
    dout(7) << "commit " << atid << " destroy " << ino << dendl;
    
    dec(ino);  // destroy
    
    pending_destroy.erase(atid);
  }

  else if (pending_update.count(atid)) {
    inodeno_t ino = pending_update[atid].first;
    vector<Anchor> &trace = pending_update[atid].second;
    
    dout(7) << "commit " << atid << " update " << ino << dendl;

    // remove old
    dec(ino);
    
    // add new
    for (unsigned i=0; i<trace.size(); i++) 
      add(trace[i].ino, trace[i].dirfrag);
    inc(ino);
    
    pending_update.erase(atid);
  }
  else
    assert(0);

  pending_reqmds.erase(atid);

  // bump version.
  version++;
  //dump();
}

void AnchorTable::rollback(version_t atid) 
{
  if (pending_create.count(atid)) {
    inodeno_t ino = pending_create[atid];
    dout(7) << "rollback " << atid << " create " << ino << dendl;
    dec(ino);
    pending_create.erase(atid);
  } 

  else if (pending_destroy.count(atid)) {
    inodeno_t ino = pending_destroy[atid];
    dout(7) << "rollback " << atid << " destroy " << ino << dendl;
    pending_destroy.erase(atid);
  }

  else if (pending_update.count(atid)) {
    inodeno_t ino = pending_update[atid].first;
    dout(7) << "rollback " << atid << " update " << ino << dendl;
    pending_update.erase(atid);
  }
  else
    assert(0);

  pending_reqmds.erase(atid);

  // bump version.
  version++;
  //dump();
}




// CREATE

class C_AT_CreatePrepare : public Context {
  AnchorTable *at;
  MAnchor *req;
  version_t atid;
public:
  C_AT_CreatePrepare(AnchorTable *a, MAnchor *r, version_t t) :
    at(a), req(r), atid(t) { }
  void finish(int r) {
    at->_create_prepare_logged(req, atid);
  }
};

void AnchorTable::handle_create_prepare(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  vector<Anchor>& trace = req->get_trace();

  dout(7) << "handle_create_prepare " << ino << dendl;
  
  create_prepare(ino, trace, req->get_source().num());

  // log it
  EAnchor *le = new EAnchor(ANCHOR_OP_CREATE_PREPARE, ino, version, req->get_source().num());
  le->set_trace(trace);
  mds->mdlog->submit_entry(le, 
			   new C_AT_CreatePrepare(this, req, version));
}
  
void AnchorTable::_create_prepare_logged(MAnchor *req, version_t atid)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "_create_prepare_logged " << ino << " atid " << atid << dendl;

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_CREATE_AGREE, ino, atid);
  mds->messenger->send_message(reply, req->get_source_inst());

  delete req;
}




// DESTROY

class C_AT_DestroyPrepare : public Context {
  AnchorTable *at;
  MAnchor *req;
  version_t atid;
public:
  C_AT_DestroyPrepare(AnchorTable *a, MAnchor *r, version_t t) :
    at(a), req(r), atid(t) { }
  void finish(int r) {
    at->_destroy_prepare_logged(req, atid);
  }
};

void AnchorTable::handle_destroy_prepare(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "handle_destroy_prepare " << ino << dendl;

  destroy_prepare(ino, req->get_source().num());

  mds->mdlog->submit_entry(new EAnchor(ANCHOR_OP_DESTROY_PREPARE, ino, version, req->get_source().num()),
			   new C_AT_DestroyPrepare(this, req, version));
}

void AnchorTable::_destroy_prepare_logged(MAnchor *req, version_t atid)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "_destroy_prepare_logged " << ino << " atid " << atid << dendl;

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_DESTROY_AGREE, ino, atid);
  mds->messenger->send_message(reply, req->get_source_inst());
  delete req;
}



// UPDATE

class C_AT_UpdatePrepare : public Context {
  AnchorTable *at;
  MAnchor *req;
  version_t atid;
public:
  C_AT_UpdatePrepare(AnchorTable *a, MAnchor *r, version_t t) :
    at(a), req(r), atid(t) { }
  void finish(int r) {
    at->_update_prepare_logged(req, atid);
  }
};

void AnchorTable::handle_update_prepare(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  vector<Anchor>& trace = req->get_trace();

  dout(7) << "handle_update_prepare " << ino << dendl;
  
  update_prepare(ino, trace, req->get_source().num());

  // log it
  EAnchor *le = new EAnchor(ANCHOR_OP_UPDATE_PREPARE, ino, version, req->get_source().num());
  le->set_trace(trace);
  mds->mdlog->submit_entry(le, 
			   new C_AT_UpdatePrepare(this, req, version));
}
  
void AnchorTable::_update_prepare_logged(MAnchor *req, version_t atid)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "_update_prepare_logged " << ino << " atid " << atid << dendl;

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_UPDATE_AGREE, ino, atid);
  mds->messenger->send_message(reply, req->get_source_inst());
  delete req;
}



// COMMIT

class C_AT_Commit : public Context {
  AnchorTable *at;
  MAnchor *req;
public:
  C_AT_Commit(AnchorTable *a, MAnchor *r) :
    at(a), req(r) { }
  void finish(int r) {
    at->_commit_logged(req);
  }
};

void AnchorTable::handle_commit(MAnchor *req)
{
  version_t atid = req->get_atid();
  dout(7) << "handle_commit " << atid << dendl;
  
  if (pending_create.count(atid) ||
      pending_destroy.count(atid) ||
      pending_update.count(atid)) {
    commit(atid);
    mds->mdlog->submit_entry(new EAnchor(ANCHOR_OP_COMMIT, atid, version));
  }
  else if (atid <= version) {
    dout(0) << "got commit for atid " << atid << " <= " << version 
	    << ", already committed, sending ack." 
	    << dendl;
    MAnchor *reply = new MAnchor(ANCHOR_OP_ACK, 0, atid);
    mds->messenger->send_message(reply, req->get_source_inst());
    delete req;
    return;
  } 
  else {
    // wtf.
    dout(0) << "got commit for atid " << atid << " > " << version << dendl;
    assert(atid <= version);
  }
  
  // wait for it to journal
  mds->mdlog->wait_for_sync(new C_AT_Commit(this, req));
}


void AnchorTable::_commit_logged(MAnchor *req)
{
  dout(7) << "_commit_logged, sending ACK" << dendl;
  MAnchor *reply = new MAnchor(ANCHOR_OP_ACK, req->get_ino(), req->get_atid());
  mds->messenger->send_message(reply, req->get_source_inst());
  delete req;
}



// ROLLBACK

void AnchorTable::handle_rollback(MAnchor *req)
{
  version_t atid = req->get_atid();
  dout(7) << "handle_rollback " << atid << dendl;
  rollback(atid);
  delete req;
}



/*
 * messages 
 */

void AnchorTable::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_ANCHOR:
    handle_anchor_request((MAnchor*)m);
    break;
    
  default:
    assert(0);
  }
}


void AnchorTable::handle_anchor_request(class MAnchor *req)
{
  // make sure i'm open!
  if (!opened) {
    dout(7) << "not open yet" << dendl;
    
    waiting_for_open.push_back(new C_MDS_RetryMessage(mds, req));
    
    if (!opening) {
      opening = true;
      load(0);
    }
    return;
  }

  dout(10) << "handle_anchor_request " << *req << dendl;

  // go
  switch (req->get_op()) {

  case ANCHOR_OP_LOOKUP:
    handle_lookup(req);
    break;

  case ANCHOR_OP_CREATE_PREPARE:
    handle_create_prepare(req);
    break;
  case ANCHOR_OP_DESTROY_PREPARE:
    handle_destroy_prepare(req);
    break;
  case ANCHOR_OP_UPDATE_PREPARE:
    handle_update_prepare(req);
    break;

  case ANCHOR_OP_COMMIT:
    handle_commit(req);
    break;

  case ANCHOR_OP_ROLLBACK:
    handle_rollback(req);
    break;

  default:
    assert(0);
  }

}




// primitive load/save for now!

// load/save entire table for now!

class C_AT_Saved : public Context {
  AnchorTable *at;
  version_t version;
public:
  C_AT_Saved(AnchorTable *a, version_t v) : at(a), version(v) {}
  void finish(int r) {
    at->_saved(version);
  }
};

void AnchorTable::save(Context *onfinish)
{
  dout(7) << "save v " << version << dendl;
  if (!opened) {
    assert(!onfinish);
    return;
  }
  
  if (onfinish)
    waiting_for_save[version].push_back(onfinish);

  if (committing_version == version) {
    dout(7) << "save already committing v " << version << dendl;
    return;
  }
  committing_version = version;

  // build up write
  bufferlist bl;

  // version
  bl.append((char*)&version, sizeof(version));

  // # anchors
  size_t size = anchor_map.size();
  bl.append((char*)&size, sizeof(size));

  // anchors
  for (hash_map<inodeno_t, Anchor>::iterator it = anchor_map.begin();
       it != anchor_map.end();
       it++) {
    it->second._encode(bl);
    dout(15) << "save encoded " << it->second << dendl;
  }

  // pending
  ::_encode(pending_reqmds, bl);
  ::_encode(pending_create, bl);
  ::_encode(pending_destroy, bl);
  
  size_t s = pending_update.size();
  bl.append((char*)&s, sizeof(s));
  for (map<version_t, pair<inodeno_t, vector<Anchor> > >::iterator p = pending_update.begin();
       p != pending_update.end();
       ++p) {
    bl.append((char*)&p->first, sizeof(p->first));
    bl.append((char*)&p->second.first, sizeof(p->second.first));
    ::_encode(p->second.second, bl);
  }

  // write!
  object_t oid = object_t(MDS_INO_ANCHORTABLE+mds->get_nodeid(), 0);
  mds->objecter->write(oid,
		       0, bl.length(),
		       mds->objecter->osdmap->file_to_object_layout(oid, g_OSD_MDAnchorTableLayout),
		       bl, 
		       NULL, new C_AT_Saved(this, version));
}

void AnchorTable::_saved(version_t v)
{
  dout(7) << "_saved v " << v << dendl;

  assert(v <= committing_version);
  assert(committed_version < v);
  committed_version = v;
  
  finish_contexts(waiting_for_save[v], 0);
  waiting_for_save.erase(v);  
}



class C_AT_Load : public Context {
  AnchorTable *at;
public:
  bufferlist bl;
  C_AT_Load(AnchorTable *a) : at(a) {}
  void finish(int result) {
    assert(result > 0);
    at->_loaded(bl);
  }
};

void AnchorTable::load(Context *onfinish)
{
  dout(7) << "load" << dendl;
  assert(!opened);

  waiting_for_open.push_back(onfinish);

  C_AT_Load *fin = new C_AT_Load(this);
  object_t oid = object_t(MDS_INO_ANCHORTABLE+mds->get_nodeid(), 0);
  mds->objecter->read(oid,
		      0, 0,
		      mds->objecter->osdmap->file_to_object_layout(oid, g_OSD_MDAnchorTableLayout),
		      &fin->bl, fin);
}

void AnchorTable::_loaded(bufferlist& bl)
{
  dout(10) << "_loaded got " << bl.length() << " bytes" << dendl;

  int off = 0;
  bl.copy(off, sizeof(version), (char*)&version);
  off += sizeof(version);

  size_t size;
  bl.copy(off, sizeof(size), (char*)&size);
  off += sizeof(size);

  for (size_t n=0; n<size; n++) {
    Anchor a;
    a._decode(bl, off);
    anchor_map[a.ino] = a;   
    dout(15) << "load_2 decoded " << a << dendl;
  }

  ::_decode(pending_reqmds, bl, off);
  ::_decode(pending_create, bl, off);
  ::_decode(pending_destroy, bl, off);

  size_t s;
  bl.copy(off, sizeof(s), (char*)&s);
  off += sizeof(s);
  for (size_t i=0; i<s; i++) {
    version_t atid;
    bl.copy(off, sizeof(atid), (char*)&atid);
    off += sizeof(atid);
    inodeno_t ino;
    bl.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);

    pending_update[atid].first = ino;
    ::_decode(pending_update[atid].second, bl, off);
  }

  assert(off == (int)bl.length());

  // done.
  opened = true;
  opening = false;
  
  finish_contexts(waiting_for_open);
}


//////

void AnchorTable::finish_recovery()
{
  dout(7) << "finish_recovery" << dendl;
  
  // resend agrees for everyone.
  for (map<version_t,int>::iterator p = pending_reqmds.begin();
       p != pending_reqmds.end();
       p++) 
    resend_agree(p->first, p->second);
}


void AnchorTable::resend_agree(version_t v, int who)
{
  if (pending_create.count(v)) {
    MAnchor *reply = new MAnchor(ANCHOR_OP_CREATE_AGREE, pending_create[v], v);
    mds->send_message_mds(reply, who);
  }
  else if (pending_destroy.count(v)) {
    MAnchor *reply = new MAnchor(ANCHOR_OP_DESTROY_AGREE, pending_destroy[v], v);
    mds->send_message_mds(reply, who);
  }
  else {
    assert(pending_update.count(v));
    MAnchor *reply = new MAnchor(ANCHOR_OP_UPDATE_AGREE, pending_update[v].first, v);
    mds->send_message_mds(reply, who);
  }
}

void AnchorTable::handle_mds_recovery(int who)
{
  dout(7) << "handle_mds_recovery mds" << who << dendl;
  
  // resend agrees for recovered mds
  for (map<version_t,int>::iterator p = pending_reqmds.begin();
       p != pending_reqmds.end();
       p++) {
    if (p->second != who) continue;
    resend_agree(p->first, p->second);
  }
}
