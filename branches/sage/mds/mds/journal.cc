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

#include "events/EString.h"
#include "events/ESubtreeMap.h"
#include "events/ESession.h"

#include "events/EMetaBlob.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"

#include "events/EPurgeFinish.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/EAnchor.h"
#include "events/EAnchorClient.h"

#include "LogSegment.h"

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Server.h"
#include "Migrator.h"
#include "AnchorTable.h"
#include "AnchorClient.h"
#include "IdAllocator.h"


#include "config.h"

#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "


// -----------------------
// LogSegment

class C_MDL_RetryExpireSegment : public Context {
public:
  MDS *mds;
  LogSegment *ls;
  C_MDL_RetryExpireSegment(MDS *m, LogSegment *l) : mds(m), ls(l) {}
  void finish(int r) {
    ls->try_to_expire(mds);
  }
};

C_Gather *LogSegment::try_to_expire(MDS *mds)
{
  C_Gather *gather = 0;

  set<CDir*> commit;

  dout(6) << "LogSegment(" << offset << ").try_to_expire" << dendl;

  // commit dirs
  for (xlist<CDir*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) 
    commit.insert(*p);
  for (xlist<CDentry*>::iterator p = dirty_dentries.begin(); !p.end(); ++p) 
    commit.insert((*p)->get_dir());
  for (xlist<CInode*>::iterator p = dirty_inodes.begin(); !p.end(); ++p) 
    commit.insert((*p)->get_parent_dn()->get_dir());

  if (!commit.empty()) {
    if (!gather) gather = new C_Gather;
    
    for (set<CDir*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p) {
      CDir *dir = *p;
      if (dir->can_auth_pin()) {
	dout(10) << " committing " << *dir << dendl;
	dir->commit(0, gather->new_sub());
      } else {
	dout(10) << " waiting for unfreeze on " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_UNFREEZE, gather->new_sub());
      }
    }
  }

  // dirty non-auth mtimes
  for (xlist<CInode*>::iterator p = dirty_inode_mtimes.begin(); !p.end(); ++p) {
    dout(10) << " waiting for dirlock mtime flush on " << *p << dendl;
    if (!gather) gather = new C_Gather;
    (*p)->dirlock.add_waiter(SimpleLock::WAIT_STABLE, gather->new_sub());
  }
  
  // pending commit atids
  for (hash_set<version_t>::iterator p = pending_commit_atids.begin();
       p != pending_commit_atids.end();
       ++p) {
    if (!gather) gather = new C_Gather;
    assert(!mds->anchorclient->has_committed(*p));
    dout(10) << " anchor transaction " << *p 
	     << " pending commit (not yet acked), waiting" << dendl;
    mds->anchorclient->wait_for_ack(*p, gather->new_sub());
  }

  return gather;
}


// -----------------------
// EString

bool EString::has_expired(MDS *mds) {
  dout(10) << "EString.has_expired " << event << dendl; 
  return true;
}
void EString::expire(MDS *mds, Context *c)
{
  dout(10) << "EString.expire " << event << dendl; 
}
void EString::replay(MDS *mds)
{
  dout(10) << "EString.replay " << event << dendl; 
}



// -----------------------
// EMetaBlob

EMetaBlob::EMetaBlob(MDLog *mdlog) :
  last_subtree_map(mdlog->get_last_segment_offset()),
  my_offset(mdlog->get_write_pos()) 
{
}


/*
 * we need to ensure that a journaled item has either
 * 
 * - been safely committed to its dirslice.
 *
 * - has been safely exported.  i.e., authority().first != us.  
 *   in particular, auth of <us, them> is not enough, we need to
 *   wait for <them,-2>.  
 *
 * note that this check is overly conservative, in that we'll
 * try to flush the dir again if we reimport the subtree, even though
 * later journal entries contain the same dirty data (from the import).
 *
 */
bool EMetaBlob::has_expired(MDS *mds)
{
/*
  // examine dirv's for my lumps
  for (map<dirfrag_t,dirlump>::iterator lp = lump_map.begin();
       lp != lump_map.end();
       ++lp) {
    CDir *dir = mds->mdcache->get_dirfrag(lp->first);
    if (!dir) 
      continue;       // we expired it

    // FIXME: check the slice only

    if (dir->authority().first != mds->get_nodeid()) {
      dout(10) << "EMetaBlob.has_expired not auth, needed dirv " << lp->second.dirv
	       << " for " << *dir << dendl;
      continue;       // not our problem
    }

    if (g_conf.mds_hack_log_expire_for_better_stats) {
      // FIXME HACK: this makes logger stats more accurage, for journal stats, 
      //             but is not perfectly safe.  for benchmarking ONLY!
      if (dir->get_committing_version() >= lp->second.dirv ||   // committING, not committED.
	  dir->get_committed_version_equivalent() >= lp->second.dirv) {
	dout(10) << "EMetaBlob.has_expired have|committING (unsafe hack!) dirv " << lp->second.dirv
		 << " for " << *dir << dendl;
	continue;       // yay
      }
    } else {
      // this is the proper (safe) way
      if (dir->get_committed_version() >= lp->second.dirv ||
	  dir->get_committed_version_equivalent() >= lp->second.dirv) {
	dout(10) << "EMetaBlob.has_expired have dirv " << lp->second.dirv
		 << " for " << *dir << dendl;
	continue;       // yay
      }
    }
    
    if (dir->is_ambiguous_dir_auth()) {
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      if (ex->is_exporting()) {
	// wait until export is acked (logged on remote) and committed (logged locally)
	dout(10) << "EMetaBlob.has_expired ambiguous auth for " << *dir
		 << ", exporting on " << *ex << dendl;
	return false;
      } else {
	dout(10) << "EMetaBlob.has_expired ambiguous auth for " << *dir
		 << ", importing on " << *ex << dendl;
	return false;
      }
    }

    if (dir->get_committed_version() < lp->second.dirv) {
      dout(10) << "EMetaBlob.has_expired need dirv " << lp->second.dirv
	       << " for " << *dir << dendl;
      return false;  // not committed.
    }

    assert(0);  // i goofed the logic
  }

  // have my anchortable ops committed?
  for (list<version_t>::iterator p = atids.begin();
       p != atids.end();
       ++p) {
    if (!mds->anchorclient->has_committed(*p)) {
      dout(10) << "EMetaBlob.has_expired anchor transaction " << *p 
	       << " not yet acked" << dendl;
      return false;
    }
  }
  
  if (!dirty_inode_mtimes.empty())
    for (map<inodeno_t,utime_t>::iterator p = dirty_inode_mtimes.begin();
	 p != dirty_inode_mtimes.end();
	 ++p) {
      CInode *in = mds->mdcache->get_inode(p->first);
      if (in) {
	if (in->inode.ctime == p->second &&
	    in->dirlock.is_updated()) {
	  dout(10) << "EMetaBlob.has_expired dirty mtime dirlock hasn't flushed on " << *in << dendl;
	  return false;
	}
      }
    }

  // allocated_ios
  if (!allocated_inos.empty()) {
    version_t cv = mds->idalloc->get_committed_version();
    if (cv < alloc_tablev) {
      dout(10) << "EMetaBlob.has_expired idalloc tablev " << alloc_tablev << " > " << cv
	       << ", still dirty" << dendl;
      return false;   // still dirty
    } else {
      dout(10) << "EMetaBlob.has_expired idalloc tablev " << alloc_tablev << " <= " << cv
	       << ", already flushed" << dendl;
    }
  }


  // truncated inodes
  for (list< pair<inode_t,off_t> >::iterator p = truncated_inodes.begin();
       p != truncated_inodes.end();
       ++p) {
    if (mds->mdcache->is_purging(p->first.ino, p->second)) {
      dout(10) << "EMetaBlob.has_expired still purging inode " << p->first.ino 
	       << " to " << p->second << dendl;
      return false;
    }
  }  

  // client requests
  for (list<metareqid_t>::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p) {
    if (mds->clientmap.have_completed_request(*p)) {
      dout(10) << "EMetaBlob.has_expired still have completed request " << *p
	       << dendl;
      return false;
    }
  }

  
  */
  return true;  // all dirlumps expired, etc.
}


void EMetaBlob::expire(MDS *mds, Context *c)
{
/*
  map<CDir*,version_t> commit;  // dir -> version needed
  list<CDir*> waitfor_export;
  list<CDir*> waitfor_import;
  int ncommit = 0;

  // examine dirv's for my lumps
  // make list of dir slices i need to commit
  for (map<dirfrag_t,dirlump>::iterator lp = lump_map.begin();
       lp != lump_map.end();
       ++lp) {
    CDir *dir = mds->mdcache->get_dirfrag(lp->first);
    if (!dir) 
      continue;       // we expired it
    
    // FIXME: check the slice only

    if (dir->authority().first != mds->get_nodeid()) {
      dout(10) << "EMetaBlob.expire not auth, needed dirv " << lp->second.dirv
	       << " for " << *dir << dendl;
      continue;     // not our problem
    }
    if (dir->get_committed_version() >= lp->second.dirv ||
	dir->get_committed_version_equivalent() >= lp->second.dirv) {
      dout(10) << "EMetaBlob.expire have dirv " << lp->second.dirv
	       << " on " << *dir << dendl;
      continue;   // yay
    }
    
    if (dir->is_ambiguous_dir_auth()) {
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      if (ex->is_exporting()) {
	// wait until export is acked (logged on remote) and committed (logged locally)
	dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
		 << ", waiting for export finish on " << *ex << dendl;
	waitfor_export.push_back(ex);
	continue;
      } else {
	dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
		 << ", waiting for import finish on " << *ex << dendl;
	waitfor_import.push_back(ex);
	continue;
      }
    }
    
    assert(dir->get_committed_version() < lp->second.dirv);
    dout(10) << "EMetaBlob.expire need dirv " << lp->second.dirv
	     << ", committing " << *dir << dendl;
    commit[dir] = MAX(commit[dir], lp->second.dirv);
    ncommit++;
  }

  // set up gather context
  C_Gather *gather = new C_Gather(c);

  // do or wait for exports and commits
  for (map<CDir*,version_t>::iterator p = commit.begin();
       p != commit.end();
       ++p) {
    if (p->first->can_auth_pin())
      p->first->commit(p->second, gather->new_sub());
    else
      // pbly about to export|split|merge. 
      // just wait for it to unfreeze, then retry
      p->first->add_waiter(CDir::WAIT_UNFREEZE, gather->new_sub());  
  }
  for (list<CDir*>::iterator p = waitfor_export.begin();
       p != waitfor_export.end();
       ++p) 
    mds->mdcache->migrator->add_export_finish_waiter(*p, gather->new_sub());
  for (list<CDir*>::iterator p = waitfor_import.begin();
       p != waitfor_import.end();
       ++p) 
    (*p)->add_waiter(CDir::WAIT_UNFREEZE, gather->new_sub());
  

  // have my anchortable ops committed?
  for (list<version_t>::iterator p = atids.begin();
       p != atids.end();
       ++p) {
    if (!mds->anchorclient->has_committed(*p)) {
      dout(10) << "EMetaBlob.expire anchor transaction " << *p 
	       << " not yet acked, waiting" << dendl;
      mds->anchorclient->wait_for_ack(*p, gather->new_sub());
    }
  }

  // dirtied inode mtimes
  if (!dirty_inode_mtimes.empty())
    for (map<inodeno_t,utime_t>::iterator p = dirty_inode_mtimes.begin();
	 p != dirty_inode_mtimes.end();
	 ++p) {
      CInode *in = mds->mdcache->get_inode(p->first);
      if (in) {
	if (in->inode.ctime == p->second &&
	    in->dirlock.is_updated()) {
	  dout(10) << "EMetaBlob.expire dirty mtime dirlock hasn't flushed, waiting on " 
		   << *in << dendl;
	  in->dirlock.add_waiter(SimpleLock::WAIT_STABLE, gather->new_sub());
	}
      }
    }

  // allocated_inos
  if (!allocated_inos.empty()) {
    version_t cv = mds->idalloc->get_committed_version();
    if (cv < alloc_tablev) {
      dout(10) << "EMetaBlob.expire saving idalloc table, need " << alloc_tablev << dendl;
      mds->idalloc->save(gather->new_sub(), alloc_tablev);
    }
  }

  // truncated inodes
  for (list< pair<inode_t,off_t> >::iterator p = truncated_inodes.begin();
       p != truncated_inodes.end();
       ++p) {
    if (mds->mdcache->is_purging(p->first.ino, p->second)) {
      dout(10) << "EMetaBlob.expire waiting for purge of inode " << p->first.ino
	       << " to " << p->second << dendl;
      mds->mdcache->wait_for_purge(p->first.ino, p->second, gather->new_sub());
    }
  }

  // client requests
  for (list<metareqid_t>::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p) {
    if (mds->clientmap.have_completed_request(*p)) {
      dout(10) << "EMetaBlob.expire waiting on completed request " << *p
	       << dendl;
      mds->clientmap.add_trim_waiter(*p, gather->new_sub());
    }
  }

  dout(10) << "my gather finsher is " << gather << " with " << gather->get_num() << dendl;

*/
}

void EMetaBlob::update_segment(LogSegment *ls)
{
  // atids?
  //for (list<version_t>::iterator p = atids.begin(); p != atids.end(); ++p)
  //  ls->pending_commit_atids[*p] = ls;
  // -> handled directly by AnchorClient

  // dirty inode mtimes
  // -> handled directly by Server.cc, replay()

  // alloc table update?
  if (!allocated_inos.empty())
    ls->allocv = alloc_tablev;

  // truncated inodes
  // -> handled directly by Server.cc

  // client requests
  //  note the newest request per client
  //if (!client_reqs.empty())
    //    ls->last_client_tid[client_reqs.rbegin()->client] = client_reqs.rbegin()->tid);
}

void EMetaBlob::replay(MDS *mds, LogSegment *logseg)
{
  dout(10) << "EMetaBlob.replay " << lump_map.size() << " dirlumps" << dendl;

  if (!logseg) logseg = _segment;
  assert(logseg);

  // walk through my dirs (in order!)
  for (list<dirfrag_t>::iterator lp = lump_order.begin();
       lp != lump_order.end();
       ++lp) {
    dout(10) << "EMetaBlob.replay dir " << *lp << dendl;
    dirlump &lump = lump_map[*lp];

    // the dir 
    CDir *dir = mds->mdcache->get_dirfrag(*lp);
    if (!dir) {
      // hmm.  do i have the inode?
      CInode *diri = mds->mdcache->get_inode((*lp).ino);
      if (!diri) {
	if ((*lp).ino == MDS_INO_ROOT) {
	  diri = mds->mdcache->create_root_inode();
	  dout(10) << "EMetaBlob.replay created root " << *diri << dendl;
	} else if (MDS_INO_IS_STRAY((*lp).ino)) {
	  int whose = (*lp).ino - MDS_INO_STRAY_OFFSET;
	  diri = mds->mdcache->create_stray_inode(whose);
	  dout(10) << "EMetaBlob.replay created stray " << *diri << dendl;
	} else {
	  dout(0) << "EMetaBlob.replay missing dir ino  " << (*lp).ino << dendl;
	  assert(0);
	}
      }
      // create the dirfrag
      dir = diri->get_or_open_dirfrag(mds->mdcache, (*lp).frag);

      if ((*lp).ino < MDS_INO_BASE) 
	mds->mdcache->adjust_subtree_auth(dir, CDIR_AUTH_UNKNOWN);

      dout(10) << "EMetaBlob.replay added dir " << *dir << dendl;  
    }
    dir->set_version( lump.dirv );
    if (lump.is_dirty())
      dir->_mark_dirty(logseg);
    if (lump.is_complete())
      dir->mark_complete();
    
    // decode bits
    lump._decode_bits();

    // full dentry+inode pairs
    for (list<fullbit>::iterator p = lump.get_dfull().begin();
	 p != lump.get_dfull().end();
	 p++) {
      CDentry *dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_null_dentry(p->dn);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay had " << *dn << dendl;
      }

      CInode *in = mds->mdcache->get_inode(p->inode.ino);
      if (!in) {
	in = new CInode(mds->mdcache);
	in->inode = p->inode;
	in->dirfragtree = p->dirfragtree;
	if (in->inode.is_symlink()) in->symlink = p->symlink;
	mds->mdcache->add_inode(in);
	if (!dn->is_null()) {
	  if (dn->is_primary())
	    dout(-10) << "EMetaBlob.replay FIXME had dentry linked to wrong inode " << *dn 
		     << " " << *dn->get_inode()
		     << " should be " << p->inode.ino
		     << dendl;
	  dir->unlink_inode(dn);
	  //assert(0); // hrm!  fallout from sloppy unlink?  or?  hmmm FIXME investigate further
	}
	dir->link_primary_inode(dn, in);
	if (p->dirty) in->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *in << dendl;
      } else {
	if (dn->get_inode() != in && in->get_parent_dn()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *in << dendl;
	  in->get_parent_dn()->get_dir()->unlink_inode(in->get_parent_dn());
	}
	in->inode = p->inode;
	in->dirfragtree = p->dirfragtree;
	if (in->inode.is_symlink()) in->symlink = p->symlink;
	if (p->dirty) in->_mark_dirty(logseg);
	if (dn->get_inode() != in) {
	  dir->link_primary_inode(dn, in);
	  dout(10) << "EMetaBlob.replay linked " << *in << dendl;
	} else {
	  dout(10) << "EMetaBlob.replay had " << *in << dendl;
	}
      }
    }

    // remote dentries
    for (list<remotebit>::iterator p = lump.get_dremote().begin();
	 p != lump.get_dremote().end();
	 p++) {
      CDentry *dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_remote_dentry(p->dn, p->ino, p->d_type);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  dir->unlink_inode(dn);
	}
	dn->set_remote(p->ino, p->d_type);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay had " << *dn << dendl;
      }
    }

    // null dentries
    for (list<nullbit>::iterator p = lump.get_dnull().begin();
	 p != lump.get_dnull().end();
	 p++) {
      CDentry *dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_null_dentry(p->dn);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay added " << *dn << dendl;
      } else {
	if (!dn->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << dendl;
	  dir->unlink_inode(dn);
	}
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty(logseg);
	dout(10) << "EMetaBlob.replay had " << *dn << dendl;
      }
    }
  }

  // anchor transactions
  for (list<version_t>::iterator p = atids.begin();
       p != atids.end();
       ++p) {
    dout(10) << "EMetaBlob.replay noting anchor transaction " << *p << dendl;
    mds->anchorclient->got_journaled_agree(*p, logseg);
  }

  // dirtied inode mtimes
  if (!dirty_inode_mtimes.empty())
    for (map<inodeno_t,utime_t>::iterator p = dirty_inode_mtimes.begin();
	 p != dirty_inode_mtimes.end();
	 ++p) {
      CInode *in = mds->mdcache->get_inode(p->first);
      dout(10) << "EMetaBlob.replay setting dirlock updated flag on " << *in << dendl;
      in->dirlock.set_updated();
      logseg->dirty_inode_mtimes.push_back(&in->xlist_dirty_inode_mtime);
    }

  // allocated_inos
  if (!allocated_inos.empty()) {
    if (mds->idalloc->get_version() >= alloc_tablev) {
      dout(10) << "EMetaBlob.replay idalloc tablev " << alloc_tablev
	       << " <= table " << mds->idalloc->get_version() << dendl;
    } else {
      for (list<inodeno_t>::iterator p = allocated_inos.begin();
	   p != allocated_inos.end();
	   ++p) {
	dout(10) << " EMetaBlob.replay idalloc " << *p << " tablev " << alloc_tablev
		 << " - 1 == table " << mds->idalloc->get_version() << dendl;
	assert(alloc_tablev-1 == mds->idalloc->get_version());
	
	inodeno_t ino = mds->idalloc->alloc_id();
	assert(ino == *p);       // this should match.
      }	
      assert(alloc_tablev == mds->idalloc->get_version());
    }
  }

  // truncated inodes
  for (list< triple<inodeno_t,off_t,off_t> >::iterator p = truncated_inodes.begin();
       p != truncated_inodes.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(p->first);
    assert(in);
    dout(10) << "EMetaBlob.replay will purge truncated " 
	     << p->third << " -> " << p->second
	     << " on " << *in << dendl;
    mds->mdcache->add_recovered_purge(in, p->second, p->third, logseg);
  }

  // client requests
  for (list<metareqid_t>::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p)
    mds->clientmap.add_completed_request(*p);


  // update segment
  update_segment(logseg);
}

// -----------------------
// ESession
bool ESession::has_expired(MDS *mds) 
{
  if (mds->clientmap.get_committed() >= cmapv) {
    dout(10) << "ESession.has_expired newer clientmap " << mds->clientmap.get_committed() 
	     << " >= " << cmapv << " has committed" << dendl;
    return true;
  } else if (mds->clientmap.get_committing() >= cmapv) {
    dout(10) << "ESession.has_expired newer clientmap " << mds->clientmap.get_committing() 
	     << " >= " << cmapv << " is still committing" << dendl;
    return false;
  } else {
    dout(10) << "ESession.has_expired clientmap " << mds->clientmap.get_version() 
	     << " > " << cmapv << ", need to save" << dendl;
    return false;
  }
}

void ESession::expire(MDS *mds, Context *c)
{  
  dout(10) << "ESession.expire saving clientmap" << dendl;
  mds->clientmap.save(c, cmapv);
}

void ESession::update_segment()
{
  _segment->clientmapv = cmapv;
}

void ESession::replay(MDS *mds)
{
  if (mds->clientmap.get_version() >= cmapv) {
    dout(10) << "ESession.replay clientmap " << mds->clientmap.get_version() 
	     << " >= " << cmapv << ", noop" << dendl;

    // hrm, this isn't very pretty.
    if (!open)
      mds->clientmap.trim_completed_requests(client_inst.name.num(), 0);

  } else {
    dout(10) << "ESession.replay clientmap " << mds->clientmap.get_version() 
	     << " < " << cmapv << dendl;
    assert(mds->clientmap.get_version() + 1 == cmapv);
    if (open) {
      mds->clientmap.open_session(client_inst);
    } else {
      mds->clientmap.close_session(client_inst.name.num());
      mds->clientmap.trim_completed_requests(client_inst.name.num(), 0);
    }
    mds->clientmap.reset_projected(); // make it follow version.
  }
}



// -----------------------
// EAnchor

bool EAnchor::has_expired(MDS *mds) 
{
  version_t cv = mds->anchortable->get_committed_version();
  if (cv < version) {
    dout(10) << "EAnchor.has_expired v " << version << " > " << cv
	     << ", still dirty" << dendl;
    return false;   // still dirty
  } else {
    dout(10) << "EAnchor.has_expired v " << version << " <= " << cv
	     << ", already flushed" << dendl;
    return true;    // already flushed
  }
}

void EAnchor::expire(MDS *mds, Context *c)
{
  dout(10) << "EAnchor.expire saving anchor table" << dendl;
  mds->anchortable->save(c);
}

void EAnchor::update_segment()
{
  _segment->anchortablev = version;
}

void EAnchor::replay(MDS *mds)
{
  if (mds->anchortable->get_version() >= version) {
    dout(10) << "EAnchor.replay event " << version
	     << " <= table " << mds->anchortable->get_version() << dendl;
  } else {
    dout(10) << " EAnchor.replay event " << version
	     << " - 1 == table " << mds->anchortable->get_version() << dendl;
    assert(version-1 == mds->anchortable->get_version());
    
    switch (op) {
      // anchortable
    case ANCHOR_OP_CREATE_PREPARE:
      mds->anchortable->create_prepare(ino, trace, reqmds);
      break;
    case ANCHOR_OP_DESTROY_PREPARE:
      mds->anchortable->destroy_prepare(ino, reqmds);
      break;
    case ANCHOR_OP_UPDATE_PREPARE:
      mds->anchortable->update_prepare(ino, trace, reqmds);
      break;
    case ANCHOR_OP_COMMIT:
      mds->anchortable->commit(atid);
      break;

    default:
      assert(0);
    }
    
    assert(version == mds->anchortable->get_version());
  }
}


// EAnchorClient

bool EAnchorClient::has_expired(MDS *mds) 
{
  return true;
}

void EAnchorClient::expire(MDS *mds, Context *c)
{
  assert(0);
}

void EAnchorClient::replay(MDS *mds)
{
  dout(10) << " EAnchorClient.replay op " << op << " atid " << atid << dendl;
    
  switch (op) {
    // anchorclient
  case ANCHOR_OP_ACK:
    mds->anchorclient->got_journaled_ack(atid);
    break;
    
  default:
    assert(0);
  }
}


// -----------------------
// EUpdate

void EUpdate::update_segment()
{
  metablob.update_segment(_segment);
}

void EUpdate::replay(MDS *mds)
{
  metablob.replay(mds, _segment);
}


// ------------------------
// EOpen

bool EOpen::has_expired(MDS *mds)
{
  for (list<inodeno_t>::iterator p = inos.begin(); p != inos.end(); ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (in &&
	in->is_any_caps() &&
	!(in->last_open_journaled > get_start_off() ||
	  in->last_open_journaled == 0)) {
      dout(10) << "EOpen.has_expired still refer to caps on " << *in << dendl;
      return false;
    }
  }
  return true;
}

void EOpen::expire(MDS *mds, Context *c)
{
  dout(10) << "EOpen.expire " << dendl;
  
  if (mds->mdlog->is_capped()) {
    dout(0) << "uh oh, log is capped, but i have unexpired opens." << dendl;
    assert(0);
  }

  for (list<inodeno_t>::iterator p = inos.begin(); p != inos.end(); ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (!in) continue;
    if (!in->is_any_caps()) continue;
    
    dout(10) << "EOpen.expire " << in->ino()
	     << " last_open_journaled " << in->last_open_journaled << dendl;

    mds->server->queue_journal_open(in);
  }
  mds->server->add_journal_open_waiter(c);
  mds->server->maybe_journal_opens();
}

void EOpen::update_segment()
{
  // ??
}

void EOpen::replay(MDS *mds)
{
  dout(10) << "EOpen.replay " << dendl;
  metablob.replay(mds, _segment);
}


// -----------------------
// ESlaveUpdate

bool ESlaveUpdate::has_expired(MDS *mds)
{
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    if (mds->mdcache->ambiguous_slave_updates.count(reqid) == 0) {
      dout(10) << "ESlaveUpdate.has_expired prepare " << reqid << " for mds" << master 
	       << ": haven't yet seen commit|rollback" << dendl;
      return false;
    } 
    else if (mds->mdcache->ambiguous_slave_updates[reqid]) {
      dout(10) << "ESlaveUpdate.has_expired prepare " << reqid << " for mds" << master 
	       << ": committed, checking metablob" << dendl;
      bool exp = metablob.has_expired(mds);
      if (exp) 
	mds->mdcache->ambiguous_slave_updates.erase(reqid);
      return exp;      
    }
    else {
      dout(10) << "ESlaveUpdate.has_expired prepare " << reqid << " for mds" << master 
	       << ": aborted" << dendl;
      mds->mdcache->ambiguous_slave_updates.erase(reqid);
      return true;
    }
    
  case ESlaveUpdate::OP_COMMIT:
  case ESlaveUpdate::OP_ROLLBACK:
    if (mds->mdcache->waiting_for_slave_update_commit.count(reqid)) {
      dout(10) << "ESlaveUpdate.has_expired "
	       << ((op == ESlaveUpdate::OP_COMMIT) ? "commit ":"rollback ")
	       << reqid << " for mds" << master 
	       << ": noting commit, kicking prepare waiter" << dendl;
      mds->mdcache->ambiguous_slave_updates[reqid] = (op == ESlaveUpdate::OP_COMMIT);
      mds->mdcache->waiting_for_slave_update_commit[reqid]->finish(0);
      delete mds->mdcache->waiting_for_slave_update_commit[reqid];
      mds->mdcache->waiting_for_slave_update_commit.erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.has_expired "
	       << ((op == ESlaveUpdate::OP_COMMIT) ? "commit ":"rollback ")
	       << reqid << " for mds" << master 
	       << ": no prepare waiter, ignoring" << dendl;
    }
    return true;

  default:
    assert(0);
    return false;
  }
}

void ESlaveUpdate::expire(MDS *mds, Context *c)
{
  assert(op == ESlaveUpdate::OP_PREPARE);

  if (mds->mdcache->ambiguous_slave_updates.count(reqid) == 0) {
    // wait
    dout(10) << "ESlaveUpdate.expire prepare " << reqid << " for mds" << master
	     << ": waiting for commit|rollback" << dendl;
    mds->mdcache->waiting_for_slave_update_commit[reqid] = c;
  } else {
    // we committed.. expire the metablob
    assert(mds->mdcache->ambiguous_slave_updates[reqid] == true); 
    dout(10) << "ESlaveUpdate.expire prepare " << reqid << " for mds" << master
	     << ": waiting for metablob to expire" << dendl;
    //metablob.expire(mds, c);
  }
}

void ESlaveUpdate::replay(MDS *mds)
{
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    // FIXME: horribly inefficient copy; EMetaBlob needs a swap() or something
    dout(10) << "ESlaveUpdate.replay prepare " << reqid << " for mds" << master 
	     << ": saving blob for later commit" << dendl;
    assert(mds->mdcache->uncommitted_slave_updates[master].count(reqid) == 0);
    metablob._segment = _segment;  // may need this later
    mds->mdcache->uncommitted_slave_updates[master][reqid] = metablob;
    break;

  case ESlaveUpdate::OP_COMMIT:
    if (mds->mdcache->uncommitted_slave_updates[master].count(reqid)) {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds" << master
	       << ": applying previously saved blob" << dendl;
      mds->mdcache->uncommitted_slave_updates[master][reqid].replay(mds, _segment);
      mds->mdcache->uncommitted_slave_updates[master].erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds" << master 
	       << ": ignoring, no previously saved blob" << dendl;
    }
    break;

  case ESlaveUpdate::OP_ROLLBACK:
    if (mds->mdcache->uncommitted_slave_updates[master].count(reqid)) {
      dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds" << master
	       << ": discarding previously saved blob" << dendl;
      assert(mds->mdcache->uncommitted_slave_updates[master].count(reqid));
      mds->mdcache->uncommitted_slave_updates[master].erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds" << master 
	       << ": ignoring, no previously saved blob" << dendl;
    }
    break;

  default:
    assert(0);
  }
}


// -----------------------
// ESubtreeMap

void ESubtreeMap::replay(MDS *mds) 
{
  // suck up the subtree map?
  if (mds->mdcache->is_subtrees()) {
    dout(10) << "ESubtreeMap.replay -- ignoring, already have import map" << dendl;
    return;
  }

  dout(10) << "ESubtreeMap.replay -- reconstructing (auth) subtree spanning tree" << dendl;
  
  // first, stick the spanning tree in my cache
  //metablob.print(cout);
  metablob.replay(mds, _segment);
  
  // restore import/export maps
  for (map<dirfrag_t, list<dirfrag_t> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = mds->mdcache->get_dirfrag(p->first);
    mds->mdcache->adjust_bounded_subtree_auth(dir, p->second, mds->get_nodeid());
  }
  
  mds->mdcache->show_subtrees();
}



// -----------------------
// EFragment

void EFragment::replay(MDS *mds)
{
  dout(10) << "EFragment.replay " << ino << " " << basefrag << " by " << bits << dendl;
  
  CInode *in = mds->mdcache->get_inode(ino);
  assert(in);

  list<CDir*> resultfrags;
  list<Context*> waiters;
  mds->mdcache->adjust_dir_fragments(in, basefrag, bits, resultfrags, waiters);

  metablob.replay(mds, _segment);
}



// -----------------------
// EPurgeFinish


bool EPurgeFinish::has_expired(MDS *mds)
{
  return true;
}

void EPurgeFinish::expire(MDS *mds, Context *c)
{
  assert(0);
}

void EPurgeFinish::update_segment()
{
  // ** update purge lists?
}

void EPurgeFinish::replay(MDS *mds)
{
  dout(10) << "EPurgeFinish.replay " << ino << " " << oldsize << " -> " << newsize << dendl;
  CInode *in = mds->mdcache->get_inode(ino);
  assert(in);
  mds->mdcache->remove_recovered_purge(in, newsize, oldsize);
}





// =========================================================================

// -----------------------
// EExport

bool EExport::has_expired(MDS *mds)
{
  CDir *dir = mds->mdcache->get_dirfrag(base);
  if (dir && mds->mdcache->migrator->is_exporting(dir)) {
    dout(10) << "EExport.has_expired still exporting " << *dir << dendl;
    return false;
  }
  return true;
}

void EExport::expire(MDS *mds, Context *c)
{
  CDir *dir = mds->mdcache->get_dirfrag(base);
  assert(dir);
  assert(mds->mdcache->migrator->is_exporting(dir));

  dout(10) << "EExport.expire waiting for export of " << *dir << dendl;
  mds->mdcache->migrator->add_export_finish_waiter(dir, c);
}

void EExport::replay(MDS *mds)
{
  dout(10) << "EExport.replay " << base << dendl;
  metablob.replay(mds, _segment);
  
  CDir *dir = mds->mdcache->get_dirfrag(base);
  assert(dir);
  
  set<CDir*> realbounds;
  for (set<dirfrag_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = mds->mdcache->get_dirfrag(*p);
    assert(bd);
    realbounds.insert(bd);
  }

  // adjust auth away
  mds->mdcache->adjust_bounded_subtree_auth(dir, realbounds, pair<int,int>(CDIR_AUTH_UNKNOWN, CDIR_AUTH_UNKNOWN));
  mds->mdcache->try_subtree_merge(dir);
}


// -----------------------
// EImportStart

void EImportStart::replay(MDS *mds)
{
  dout(10) << "EImportStart.replay " << base << dendl;
  metablob.replay(mds, _segment);

  // put in ambiguous import list
  mds->mdcache->add_ambiguous_import(base, bounds);
}

// -----------------------
// EImportFinish

bool EImportFinish::has_expired(MDS *mds)
{
  return true;
}
void EImportFinish::expire(MDS *mds, Context *c)
{
  assert(0);  // shouldn't ever happen
}

void EImportFinish::replay(MDS *mds)
{
  if (mds->mdcache->have_ambiguous_import(base)) {
    dout(10) << "EImportFinish.replay " << base << " success=" << success << dendl;
    if (success) 
      mds->mdcache->finish_ambiguous_import(base);
    else
      mds->mdcache->cancel_ambiguous_import(base);
  } else {
    dout(10) << "EImportFinish.replay " << base << " success=" << success
	     << ", predates my subtree_map start point, ignoring" 
	     << dendl;
    // verify that?
  }
}





