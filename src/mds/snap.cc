// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004- Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "snap.h"
#include "MDCache.h"
#include "MDS.h"


/*
 * SnapRealm
 */

#define dout(x) if (x < g_conf.debug_mds) *_dout << dbeginl << g_clock.now() \
						 << " mds" << mdcache->mds->get_nodeid() \
						 << ".snaprealm(" << inode->ino() << ") "

bool SnapRealm::open_parents(MDRequest *mdr)
{
  dout(10) << "open_parents" << dendl;
  for (multimap<snapid_t, snaplink_t>::iterator p = parents.begin();
       p != parents.end();
       p++) {
    CInode *parent = mdcache->get_inode(p->second.dirino);
    if (parent)
      continue;
    mdcache->open_remote_ino(p->second.dirino, mdr, 
			     new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  return true;
}

/*
 * get list of snaps for this realm.  we must include parents' snaps
 * for the intervals during which they were our parent.
 */
void SnapRealm::get_snap_list(set<snapid_t> &s)
{
  // start with my snaps
  for (map<snapid_t, SnapInfo>::iterator p = snaps.begin();
       p != snaps.end();
       p++)
    s.insert(p->first);

  // include parent snaps
  for (multimap<snapid_t, snaplink_t>::iterator p = parents.begin();
       p != parents.end();
       p++) {
    CInode *parent = mdcache->get_inode(p->second.dirino);
    assert(parent);  // call open_parents first!
    assert(parent->snaprealm);

    for (map<snapid_t, SnapInfo>::iterator q = parent->snaprealm->snaps.begin();
	 q != parent->snaprealm->snaps.end();
	 q++)
      if (q->first <= p->first && 
	  q->first >= p->second.first)
	s.insert(q->first);
  }
  dout(10) << "build_snap_list " << s << dendl;
}
