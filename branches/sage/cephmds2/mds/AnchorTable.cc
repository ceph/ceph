// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#undef dout
#define dout(x)  if (x <= g_conf.debug_mds) cout << g_clock.now() << " " << mds->messenger->get_myaddr() << ".anchortable "
#define derr(x)  if (x <= g_conf.debug_mds) cerr << g_clock.now() << " " << mds->messenger->get_myaddr() << ".anchortable "



/*
 * basic updates
 */

bool AnchorTable::add(inodeno_t ino, dirfrag_t dirfrag) 
{
  dout(7) << "add " << ino << " dirfrag " << dirfrag << endl;
  
  // parent should be there
  assert(dirfrag.ino < MDS_INO_BASE ||             // system dirino
         anchor_map.count(dirfrag.ino));   // have
  
  if (anchor_map.count(ino) == 0) {
    // new item
    anchor_map[ino] = Anchor(ino, dirfrag);
    dout(10) << "  add: added " << anchor_map[ino] << endl;
    return true;
  } else {
    dout(10) << "  add: had " << anchor_map[ino] << endl;
    return false;
  }
}

void AnchorTable::inc(inodeno_t ino)
{
  dout(7) << "inc " << ino << endl;

  assert(anchor_map.count(ino));
  Anchor &anchor = anchor_map[ino];

  while (1) {
    anchor.nref++;
      
    dout(10) << "  inc: record now " << anchor << endl;
    ino = anchor.dirfrag.ino;
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
    anchor = anchor_map[ino];      
  }
}

void AnchorTable::dec(inodeno_t ino) 
{
  dout(7) << "dec " << ino << endl;

  assert(anchor_map.count(ino));
  Anchor &anchor = anchor_map[ino];

  while (true) {
    anchor.nref--;
      
    if (anchor.nref == 0) {
      dout(10) << "  dec: record " << anchor << " now 0, removing" << endl;
      dirfrag_t dirfrag = anchor.dirfrag;
      anchor_map.erase(ino);
      ino = dirfrag.ino;
    } else {
      dout(10) << "  dec: record now " << anchor << endl;
      ino = anchor.dirfrag.ino;
    }
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
    anchor = anchor_map[ino];      
  }
}


/* 
 * high level 
 */


// LOOKUP

void AnchorTable::handle_lookup(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "handle_lookup " << ino << endl;

  assert(anchor_map.count(ino) == 1);
  Anchor &anchor = anchor_map[ino];

  vector<Anchor> trace;
  while (true) {
    dout(10) << "handle_lookup  adding " << anchor << endl;
    trace.insert(trace.begin(), anchor);  // lame FIXME

    if (anchor.dirfrag.ino < MDS_INO_BASE) break;

    assert(anchor_map.count(anchor.dirfrag.ino) == 1);
    anchor = anchor_map[anchor.dirfrag.ino];
  }

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_LOOKUP_REPLY, ino);
  reply->set_trace(trace);
  mds->messenger->send_message(reply, req->get_source_inst(), req->get_source_port());

  delete req;
}


// MIDLEVEL

void AnchorTable::create_prepare(inodeno_t ino, vector<Anchor>& trace)
{
  // make sure trace is in table
  for (unsigned i=0; i<trace.size(); i++) 
    add(trace[i].ino, trace[i].dirfrag);
  inc(ino);
  pending_create.insert(ino);  // so we can undo
  version++;
}

void AnchorTable::create_commit(inodeno_t ino)
{
  pending_create.erase(ino);
  version++;
}

void AnchorTable::destroy_prepare(inodeno_t ino)
{
  pending_destroy.insert(ino);
  version++;
}

void AnchorTable::destroy_commit(inodeno_t ino)
{
  // apply
  dec(ino);  
  pending_destroy.erase(ino);
  version++;
}

void AnchorTable::update_prepare(inodeno_t ino, vector<Anchor>& trace)
{
  pending_update[ino] = trace;
  version++;
}

void AnchorTable::update_commit(inodeno_t ino) 
{
  // remove old
  dec(ino);

  // add new
  // make sure new trace is in table
  vector<Anchor> &trace = pending_update[ino];
  for (unsigned i=0; i<trace.size(); i++) 
    add(trace[i].ino, trace[i].dirfrag);
  inc(ino);

  pending_update.erase(ino);
  version++;
}


// CREATE

class C_AT_CreatePrepare : public Context {
  AnchorTable *at;
  MAnchor *req;
public:
  C_AT_CreatePrepare(AnchorTable *a, MAnchor *r) :
    at(a), req(r) { }
  void finish(int r) {
    at->_create_prepare_logged(req);
  }
};

void AnchorTable::handle_create_prepare(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  vector<Anchor>& trace = req->get_trace();

  dout(7) << "handle_create_prepare " << ino << endl;
  
  create_prepare(ino, trace);

  // log it
  EAnchor *le = new EAnchor(ANCHOR_OP_CREATE_PREPARE, ino, version);
  le->set_trace(trace);
  mds->mdlog->submit_entry(le, 
			   new C_AT_CreatePrepare(this, req));
}
  
void AnchorTable::_create_prepare_logged(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "_create_prepare_logged " << ino << endl;

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_CREATE_ACK, ino);
  mds->messenger->send_message(reply, req->get_source_inst(), req->get_source_port());

  delete req;
}

void AnchorTable::handle_create_commit(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "handle_create_commit " << ino << endl;
  
  create_commit(ino);

  mds->mdlog->submit_entry(new EAnchor(ANCHOR_OP_CREATE_COMMIT, ino, version));
}


// DESTROY

class C_AT_DestroyPrepare : public Context {
  AnchorTable *at;
  MAnchor *req;
public:
  C_AT_DestroyPrepare(AnchorTable *a, MAnchor *r) :
    at(a), req(r) { }
  void finish(int r) {
    at->_destroy_prepare_logged(req);
  }
};

void AnchorTable::handle_destroy_prepare(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "handle_destroy_prepare " << ino << endl;

  destroy_prepare(ino);

  mds->mdlog->submit_entry(new EAnchor(ANCHOR_OP_DESTROY_PREPARE, ino, version),
			   new C_AT_DestroyPrepare(this, req));
}

void AnchorTable::_destroy_prepare_logged(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "_destroy_prepare_logged " << ino << endl;

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_DESTROY_ACK, ino);
  mds->messenger->send_message(reply, req->get_source_inst(), req->get_source_port());

  delete req;
}

void AnchorTable::handle_destroy_commit(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "handle_destroy_commit " << ino << endl;
  
  destroy_commit(ino);

  // log
  mds->mdlog->submit_entry(new EAnchor(ANCHOR_OP_DESTROY_COMMIT, ino, version));
}


// UPDATE

class C_AT_UpdatePrepare : public Context {
  AnchorTable *at;
  MAnchor *req;
public:
  C_AT_UpdatePrepare(AnchorTable *a, MAnchor *r) :
    at(a), req(r) { }
  void finish(int r) {
    at->_update_prepare_logged(req);
  }
};

void AnchorTable::handle_update_prepare(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  vector<Anchor>& trace = req->get_trace();

  dout(7) << "handle_update_prepare " << ino << endl;
  
  update_prepare(ino, trace);

  // log it
  EAnchor *le = new EAnchor(ANCHOR_OP_UPDATE_PREPARE, ino, version);
  le->set_trace(trace);
  mds->mdlog->submit_entry(le, 
			   new C_AT_UpdatePrepare(this, req));
}
  
void AnchorTable::_update_prepare_logged(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "_update_prepare_logged " << ino << endl;

  // reply
  MAnchor *reply = new MAnchor(ANCHOR_OP_UPDATE_ACK, ino);
  mds->messenger->send_message(reply, req->get_source_inst(), req->get_source_port());

  delete req;
}

void AnchorTable::handle_update_commit(MAnchor *req)
{
  inodeno_t ino = req->get_ino();
  dout(7) << "handle_update_commit " << ino << endl;
  
  update_commit(ino);

  mds->mdlog->submit_entry(new EAnchor(ANCHOR_OP_UPDATE_COMMIT, ino, version));
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
    dout(7) << "not open yet" << endl;
    
    waiting_for_open.push_back(new C_MDS_RetryMessage(mds, req));
    
    if (!opening) {
      opening = true;
      load(0);
    }
    return;
  }

  // go
  switch (req->get_op()) {

  case ANCHOR_OP_LOOKUP:
    handle_lookup(req);
    break;

  case ANCHOR_OP_CREATE_PREPARE:
    handle_create_prepare(req);
    break;
  case ANCHOR_OP_CREATE_COMMIT:
    handle_create_commit(req);
    break;

  case ANCHOR_OP_DESTROY_PREPARE:
    handle_destroy_prepare(req);
    break;
  case ANCHOR_OP_DESTROY_COMMIT:
    handle_destroy_commit(req);
    break;


  case ANCHOR_OP_UPDATE_PREPARE:
    handle_update_prepare(req);
    break;
  case ANCHOR_OP_UPDATE_COMMIT:
    handle_update_commit(req);
    break;

  default:
    assert(0);
  }

}




// primitive load/save for now!

// load/save entire table for now!

void AnchorTable::save(Context *onfinish)
{
  dout(7) << "save v " << version << endl;
  if (!opened) return;
  
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
    dout(15) << "save encoded " << it->second << endl;
  }

  // pending
  ::_encode(pending_create, bl);
  ::_encode(pending_destroy, bl);
  
  size_t s = pending_update.size();
  bl.append((char*)&s, sizeof(s));
  for (map<inodeno_t, vector<Anchor> >::iterator p = pending_update.begin();
       p != pending_update.end();
       ++p) {
    bl.append((char*)&p->first, sizeof(p->first));
    ::_encode(p->second, bl);
  }

  // write!
  mds->objecter->write(object_t(MDS_INO_ANCHORTABLE+mds->get_nodeid(), 0),
		       0, bl.length(),
		       bl, 
		       NULL, onfinish);
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
  dout(7) << "load" << endl;
  assert(!opened);

  waiting_for_open.push_back(onfinish);

  C_AT_Load *fin = new C_AT_Load(this);
  mds->objecter->read(object_t(MDS_INO_ANCHORTABLE+mds->get_nodeid(), 0),
		      0, 0, &fin->bl, fin);
}

void AnchorTable::_loaded(bufferlist& bl)
{
  dout(10) << "_loaded got " << bl.length() << " bytes" << endl;

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
    dout(15) << "load_2 decoded " << a << endl;
  }

  ::_decode(pending_create, bl, off);
  ::_decode(pending_destroy, bl, off);

  size_t s;
  bl.copy(off, sizeof(s), (char*)&s);
  off += sizeof(s);
  for (size_t i=0; i<s; i++) {
    inodeno_t ino;
    bl.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    ::_decode(pending_update[ino], bl, off);
  }

  assert(off == (int)bl.length());

  // done.
  opened = true;
  opening = false;
  
  finish_contexts(waiting_for_open);
}

