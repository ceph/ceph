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

#include "include/types.h"

#include "MDS.h"
#include "ClientMap.h"

#include "osdc/Filer.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".clientmap "



void ClientMap::init_inode()
{
  memset(&inode, 0, sizeof(inode));
  inode.ino = MDS_INO_CLIENTMAP_OFFSET + mds->get_nodeid();
  inode.layout = g_OSD_FileLayout;
}


// ----------------
// LOAD

class C_CM_Load : public Context {
  ClientMap *clientmap;
public:
  bufferlist bl;
  C_CM_Load(ClientMap *cm) : clientmap(cm) {}
  void finish(int r) {
	clientmap->_load_finish(bl);
  }
};

void ClientMap::load(Context *onload)
{
  dout(10) << "load" << endl;

  init_inode();

  if (onload)
	waiting_for_load.push_back(onload);
  
  C_CM_Load *c = new C_CM_Load(this);
  mds->filer->read(inode,
                   0, inode.layout.stripe_unit,
                   &c->bl,
                   c);

}

void ClientMap::_load_finish(bufferlist &bl)
{ 
  int off = 0;
  decode(bl, off);
  dout(10) << "_load_finish v " << version 
		   << ", " << client_inst.size() << " clients, "
		   << bl.length() << " bytes"
		   << endl;
  projected = committing = committed = version;
  finish_contexts(waiting_for_load);
}


// ----------------
// SAVE

class C_CM_Save : public Context {
  ClientMap *clientmap;
  version_t version;
public:
  C_CM_Save(ClientMap *cm, version_t v) : clientmap(cm), version(v) {}
  void finish(int r) {
	clientmap->_save_finish(version);
  }
};

void ClientMap::save(Context *onsave, version_t needv)
{
  dout(10) << "save needv " << needv << ", v " << version << endl;
  commit_waiters[version].push_back(onsave);
  
  if (needv && committing >= needv) return;
  
  bufferlist bl;
  
  init_inode();
  encode(bl);
  committing = version;
  mds->filer->write(inode,
                    0, bl.length(), bl,
                    0,
					0, new C_CM_Save(this, version));
}

void ClientMap::_save_finish(version_t v)
{
  dout(10) << "_save_finish v" << v << endl;
  committed = v;

  finish_contexts(commit_waiters[v]);
  commit_waiters.erase(v);
}
