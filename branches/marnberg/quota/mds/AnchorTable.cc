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
#include "messages/MAnchorRequest.h"
#include "messages/MAnchorReply.h"

#include "common/Clock.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug_mds) cout << g_clock.now() << " " << mds->messenger->get_myaddr() << ".anchortable "
#define derr(x)  if (x <= g_conf.debug_mds) cerr << g_clock.now() << " " << mds->messenger->get_myaddr() << ".anchortable "

AnchorTable::AnchorTable(MDS *mds)
{
  this->mds = mds;
  opening = false;
  opened = false;
}

void AnchorTable::init_inode()
{
  memset(&table_inode, 0, sizeof(table_inode));
  table_inode.ino = MDS_INO_ANCHORTABLE+mds->get_nodeid();
  table_inode.layout = g_OSD_FileLayout;
}

void AnchorTable::reset()
{  
  init_inode();
  opened = true;
  anchor_map.clear();
}

/*
 * basic updates
 */

bool AnchorTable::add(inodeno_t ino, inodeno_t dirino, string& ref_dn) 
{
  dout(7) << "add " << std::hex << ino << " dirino " << dirino << std::dec << " ref_dn " << ref_dn << endl;
  
  // parent should be there
  assert(dirino < 1000 ||             // system dirino
         anchor_map.count(dirino));   // have
  
  if (anchor_map.count(ino) == 0) {
    // new item
    anchor_map[ ino ] = new Anchor(ino, dirino, ref_dn);
    dout(10) << "  add: added " << std::hex << ino << std::dec << endl;
    return true;
  } else {
    dout(10) << "  add: had " << std::hex << ino << std::dec << endl;
    return false;
  }
}

void AnchorTable::inc(inodeno_t ino)
{
  dout(7) << "inc " << std::hex << ino << std::dec << endl;

  assert(anchor_map.count(ino) != 0);
  Anchor *anchor = anchor_map[ino];
  assert(anchor);

  while (1) {
    anchor->nref++;
      
    dout(10) << "  inc: record " << std::hex << ino << std::dec << " now " << anchor->nref << endl;
    ino = anchor->dirino;
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
    anchor = anchor_map[ino];      
    assert(anchor);
  }
}

void AnchorTable::dec(inodeno_t ino) 
{
  dout(7) << "dec " << std::hex << ino << std::dec << endl;

  assert(anchor_map.count(ino) != 0);
  Anchor *anchor = anchor_map[ino];
  assert(anchor);

  while (true) {
    anchor->nref--;
      
    if (anchor->nref == 0) {
      dout(10) << "  dec: record " << std::hex << ino << std::dec << " now 0, removing" << endl;
      inodeno_t dirino = anchor->dirino;
      anchor_map.erase(ino);
      delete anchor;
      ino = dirino;
    } else {
      dout(10) << "  dec: record " << std::hex << ino << std::dec << " now " << anchor->nref << endl;
      ino = anchor->dirino;
    }
    
    if (ino == 0) break;
    if (anchor_map.count(ino) == 0) break;
    anchor = anchor_map[ino];      
    assert(anchor);
  }
}


/* 
 * high level 
 */

void AnchorTable::lookup(inodeno_t ino, vector<Anchor*>& trace)
{
  dout(7) << "lookup " << std::hex << ino << std::dec << endl;

  assert(anchor_map.count(ino) == 1);
  Anchor *anchor = anchor_map[ino];
  assert(anchor);

  while (true) {
    dout(10) << "  record " << std::hex << anchor->ino << " dirino " << anchor->dirino << std::dec << " ref_dn " << anchor->ref_dn << endl;
    trace.insert(trace.begin(), anchor);  // lame FIXME

    if (anchor->dirino < MDS_INO_BASE) break;

    assert(anchor_map.count(anchor->dirino) == 1);
    anchor = anchor_map[anchor->dirino];
    assert(anchor);
  }
}

void AnchorTable::create(inodeno_t ino, vector<Anchor*>& trace)
{
  dout(7) << "create " << std::hex << ino << std::dec << endl;
  
  // make sure trace is in table
  for (unsigned i=0; i<trace.size(); i++) 
    add(trace[i]->ino, trace[i]->dirino, trace[i]->ref_dn);

  inc(ino);  // ok!
}

void AnchorTable::destroy(inodeno_t ino)
{
  dec(ino);
}



/*
 * messages 
 */

void AnchorTable::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_ANCHORREQUEST:
    handle_anchor_request((MAnchorRequest*)m);
    break;
    
  default:
    assert(0);
  }
}



void AnchorTable::handle_anchor_request(class MAnchorRequest *m)
{
  // make sure i'm open!
  if (!opened) {
    dout(7) << "not open yet" << endl;
    
    waiting_for_open.push_back(new C_MDS_RetryMessage(mds,m));
    
    if (!opening) {
      opening = true;
      load(0);
    }
    return;
  }

  // go
  MAnchorReply *reply = new MAnchorReply(m);
  
  switch (m->get_op()) {

  case ANCHOR_OP_LOOKUP:
    lookup( m->get_ino(), reply->get_trace() );
    break;

  case ANCHOR_OP_UPDATE:
    destroy( m->get_ino() );
    create( m->get_ino(), m->get_trace() );
    break;

  case ANCHOR_OP_CREATE:
    create( m->get_ino(), m->get_trace() );
    break;

  case ANCHOR_OP_DESTROY:
    destroy( m->get_ino() );
    break;

  default:
    assert(0);
  }

  // send reply
  mds->messenger->send_message(reply, m->get_source_inst(), m->get_source_port());
  delete m;
}




// primitive load/save for now!

// load/save entire table for now!

void AnchorTable::save(Context *onfinish)
{
  dout(7) << "save" << endl;
  if (!opened) return;
  
  // build up write
  bufferlist tabbl;

  int num = anchor_map.size();
  tabbl.append((char*)&num, sizeof(int));

  for (hash_map<inodeno_t, Anchor*>::iterator it = anchor_map.begin();
       it != anchor_map.end();
       it++) {
    dout(14) << " saving anchor for " << std::hex << it->first << std::dec << endl;
    Anchor *a = it->second;
    assert(a);
    a->_encode(tabbl);
  }

  bufferlist bl;
  size_t size = tabbl.length();
  bl.append((char*)&size, sizeof(size));
  bl.claim_append(tabbl);

  dout(7) << " " << num << " anchors, " << size << " bytes" << endl;
  
  // write!
  mds->filer->write(table_inode,
                    0, bl.length(),
                    bl, 0, 
                    NULL, onfinish);
}



class C_AT_Load : public Context {
  AnchorTable *at;
public:
  size_t size;
  bufferlist bl;
  C_AT_Load(size_t size, AnchorTable *at) {
    this->size = size;
    this->at = at;
  }
  void finish(int result) {
    assert(result > 0);

    at->load_2(size, bl);
  }
};

class C_AT_LoadSize : public Context {
  AnchorTable *at;
  MDS *mds;
public:
  bufferlist bl;
  C_AT_LoadSize(AnchorTable *at, MDS *mds) {
    this->at = at;
    this->mds = mds;
  }
  void finish(int r) {
    size_t size = 0;
    assert(bl.length() >= sizeof(size));
    bl.copy(0, sizeof(size), (char*)&size);
    cout << "r is " << r << " size is " << size << endl;
    if (r > 0 && size > 0) {
      C_AT_Load *c = new C_AT_Load(size, at);
      mds->filer->read(at->table_inode,
                       sizeof(size), size,
                       &c->bl,
                       c);
    } else {
      // fail
      bufferlist empty;
      at->load_2(0, empty);
    }
  }
};

void AnchorTable::load(Context *onfinish)
{
  dout(7) << "load" << endl;
  init_inode();

  assert(!opened);

  waiting_for_open.push_back(onfinish);
  
  C_AT_LoadSize *c = new C_AT_LoadSize(this, mds);
  mds->filer->read(table_inode,
                   0, sizeof(size_t),
                   &c->bl,
                   c);
}

void AnchorTable::load_2(size_t size, bufferlist& bl)
{
  // num
  int off = 0;
  int num;
  bl.copy(0, sizeof(num), (char*)&num);
  off += sizeof(num);
  
  // parse anchors
  for (int i=0; i<num; i++) {
    Anchor *a = new Anchor;
    a->_decode(bl, off);
    dout(10) << "load_2 decoded " << std::hex << a->ino << " dirino " << a->dirino << std::dec << " ref_dn " << a->ref_dn << endl;
    anchor_map[a->ino] = a;
  }

  dout(7) << "load_2 got " << num << " anchors" << endl;

  opened = true;
  opening = false;

  // finish
  finish_contexts(waiting_for_open);
}

