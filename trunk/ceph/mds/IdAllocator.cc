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



#define DBLEVEL  20

#include "IdAllocator.h"
#include "MDS.h"
#include "MDLog.h"

#include "osdc/Filer.h"

#include "include/types.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".idalloc: "


void IdAllocator::init_inode()
{
  memset(&inode, 0, sizeof(inode));
  inode.ino = MDS_INO_IDS_OFFSET + mds->get_nodeid();
  inode.layout = g_OSD_FileLayout;
}


inodeno_t IdAllocator::alloc_id() 
{
  assert(is_active());
  
  // pick one
  inodeno_t id = free.start();
  free.erase(id);
  dout(10) << "idalloc " << this << ": alloc id " << id << dendl;

  version++;
  
  // log it
  /*
  if (!replay)
    mds->mdlog->submit_entry(new EAlloc(IDTYPE_INO, id, EALLOC_EV_ALLOC, version));
  */

  return id;
}

void IdAllocator::reclaim_id(inodeno_t id) 
{
  assert(is_active());
  
  dout(10) << "idalloc " << this << ": reclaim id " << id << dendl;
  free.insert(id);

  version++;
  
  /*
  if (!replay)
    mds->mdlog->submit_entry(new EAlloc(IDTYPE_INO, id, EALLOC_EV_FREE, version));
  */
}



class C_ID_Save : public Context {
  IdAllocator *ida;
  version_t version;
public:
  C_ID_Save(IdAllocator *i, version_t v) : ida(i), version(v) {}
  void finish(int r) {
    ida->save_2(version);
  }
};

void IdAllocator::save(Context *onfinish, version_t v)
{
  if (v > 0 && v <= committing_version) {
    dout(10) << "save v " << version << " - already saving "
	     << committing_version << " >= needed " << v << dendl;
    waitfor_save[v].push_back(onfinish);
    return;
  }
  
  dout(10) << "save v " << version << dendl;
  assert(is_active());
  
  bufferlist bl;

  bl.append((char*)&version, sizeof(version));
  ::_encode(free.m, bl);

  committing_version = version;

  if (onfinish)
    waitfor_save[version].push_back(onfinish);

  // write (async)
  mds->filer->write(inode,
                    0, bl.length(), bl,
                    0,
		    0, new C_ID_Save(this, version));
}

void IdAllocator::save_2(version_t v)
{
  dout(10) << "save_2 v " << v << dendl;
  
  committed_version = v;
  
  list<Context*> ls;
  while (!waitfor_save.empty()) {
    if (waitfor_save.begin()->first > v) break;
    ls.splice(ls.end(), waitfor_save.begin()->second);
    waitfor_save.erase(waitfor_save.begin());
  }
  finish_contexts(ls,0);
}


void IdAllocator::reset()
{
  init_inode();

  // use generic range. FIXME THIS IS CRAP
  free.clear();
#ifdef __LP64__
  uint64_t start = (uint64_t)(mds->get_nodeid()+1) << 40;
  uint64_t end = ((uint64_t)(mds->get_nodeid()+2) << 40) - 1;
#else
# warning this looks like a 32-bit system, using small inode numbers.
  uint64_t start = (uint64_t)(mds->get_nodeid()+1) << 25;
  uint64_t end = ((uint64_t)(mds->get_nodeid()+2) << 25) - 1;
#endif
  free.insert(start, end);

  state = STATE_ACTIVE;
}



// -----------------------

class C_ID_Load : public Context {
public:
  IdAllocator *ida;
  Context *onfinish;
  bufferlist bl;
  C_ID_Load(IdAllocator *i, Context *o) : ida(i), onfinish(o) {}
  void finish(int r) {
    ida->load_2(r, bl, onfinish);
  }
};

void IdAllocator::load(Context *onfinish)
{ 
  dout(10) << "load" << dendl;

  init_inode();

  assert(is_undef());
  state = STATE_OPENING;

  C_ID_Load *c = new C_ID_Load(this, onfinish);
  mds->filer->read(inode,
                   0, inode.layout.fl_stripe_unit,
                   &c->bl,
                   c);
}

void IdAllocator::load_2(int r, bufferlist& bl, Context *onfinish)
{
  assert(is_opening());
  state = STATE_ACTIVE;

  if (r > 0) {
    dout(10) << "load_2 got " << bl.length() << " bytes" << dendl;
    int off = 0;
    bl.copy(off, sizeof(version), (char*)&version);
    off += sizeof(version);
    ::_decode(free.m, bl, off);
    committed_version = version;
  }
  else {
    dout(10) << "load_2 found no alloc file" << dendl;
    assert(0); // this shouldn't happen if mkfs finished.
    reset();   
  }

  if (onfinish) {
    onfinish->finish(0);
    delete onfinish;
  }
}
