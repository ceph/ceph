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

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mds->get_nodeid() << "." << table_name << ": "

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

  projected_free.m = free.m;
}

inodeno_t InoTable::project_alloc_id(inodeno_t id) 
{
  assert(is_active());
  if (!id)
    id = projected_free.start();
  projected_free.erase(id);
  dout(10) << "project_alloc_id " << id << dendl;
  ++projected_version;
  return id;
}
void InoTable::apply_alloc_id(inodeno_t id)
{
  dout(10) << "apply_alloc_id " << id << dendl;
  free.erase(id);
  ++version;
}

void InoTable::project_alloc_ids(deque<inodeno_t>& ids, int want) 
{
  assert(is_active());
  for (int i=0; i<want; i++) {
    inodeno_t id = projected_free.start();
    projected_free.erase(id);
    ids.push_back(id);
  }
  dout(10) << "project_alloc_ids " << ids << dendl;
  ++projected_version;
}
void InoTable::apply_alloc_ids(deque<inodeno_t>& ids)
{
  dout(10) << "apply_alloc_ids " << ids << dendl;
  for (deque<inodeno_t>::iterator p = ids.begin();
       p != ids.end();
       p++) 
    free.erase(*p);
  ++version;
}


void InoTable::project_release_ids(deque<inodeno_t>& ids) 
{
  dout(10) << "project_release_ids " << ids << dendl;
  for (deque<inodeno_t>::iterator p = ids.begin(); p != ids.end(); p++)
    projected_free.insert(*p);
  ++projected_version;
}
void InoTable::apply_release_ids(deque<inodeno_t>& ids) 
{
  dout(10) << "apply_release_ids " << ids << dendl;
  for (deque<inodeno_t>::iterator p = ids.begin(); p != ids.end(); p++)
    free.insert(*p);
  ++version;
}


//

void InoTable::replay_alloc_id(inodeno_t id) 
{
  dout(10) << "replay_alloc_id " << id << dendl;
  free.erase(id);
  projected_free.erase(id);
  projected_version = ++version;
}
void InoTable::replay_alloc_ids(deque<inodeno_t>& ids) 
{
  dout(10) << "replay_alloc_ids " << ids << dendl;
  for (deque<inodeno_t>::iterator p = ids.begin(); p != ids.end(); p++) {
    free.erase(*p);
    projected_free.erase(*p);
  }
  projected_version = ++version;
}
void InoTable::replay_release_ids(deque<inodeno_t>& ids) 
{
  dout(10) << "replay_release_ids " << ids << dendl;
  for (deque<inodeno_t>::iterator p = ids.begin(); p != ids.end(); p++) {
    free.insert(*p);
    projected_free.insert(*p);
  }
  projected_version = ++version;
}

