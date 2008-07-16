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

#include "InoTable.h"
#include "MDS.h"

#include "include/types.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".inotable: "

void InoTable::init_inode()
{
  ino = MDS_INO_IDS_OFFSET + mds->get_nodeid();
  layout = g_default_file_layout;
}

void InoTable::reset_state()
{
  // use generic range. FIXME THIS IS CRAP
  free.clear();
  //#ifdef __LP64__
  uint64_t start = (uint64_t)(mds->get_nodeid()+1) << 40;
  uint64_t end = ((uint64_t)(mds->get_nodeid()+2) << 40) - 1;
  //#else
  //# warning this looks like a 32-bit system, using small inode numbers.
  //  uint64_t start = (uint64_t)(mds->get_nodeid()+1) << 25;
  //  uint64_t end = ((uint64_t)(mds->get_nodeid()+2) << 25) - 1;
  //#endif
  free.insert(start, end);
}

inodeno_t InoTable::alloc_id() 
{
  assert(is_active());
  
  // pick one
  inodeno_t id = free.start();
  free.erase(id);
  dout(10) << "alloc id " << id << dendl;

  version++;
  
  return id;
}

