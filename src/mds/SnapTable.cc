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

#include "SnapTable.h"
#include "MDS.h"

#include "include/types.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".snap: "

void SnapTable::init_inode()
{
  ino = MDS_INO_SNAPTABLE;
  layout = g_default_file_layout;
}

void SnapTable::reset_state()
{
  last_snap = 0;
  snaps.clear();
  pending_removal.clear();
}

snapid_t SnapTable::create(inodeno_t base, const string& name, utime_t stamp)
{
  assert(is_active());
  
  snapid_t sn = ++last_snap;
  snaps[sn].snapid = sn;
  snaps[sn].base = base;
  snaps[sn].name = name;
  snaps[sn].stamp = stamp;
  version++;

  dout(10) << "create(" << base << "," << name << ") = " << sn << dendl;

  return sn;
}

void SnapTable::remove(snapid_t sn) 
{
  assert(is_active());

  snaps.erase(sn);
  pending_removal.insert(sn);
  version++;
}

