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

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Server.h"
#include "Migrator.h"
#include "AnchorTable.h"
#include "AnchorClient.h"
#include "IdAllocator.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "


// -----------------------
// EString

bool EString::has_expired(MDS *mds) {
  dout(10) << "EString.has_expired " << event << endl; 
  return true;
}
void EString::expire(MDS *mds, Context *c)
{
  dout(10) << "EString.expire " << event << endl; 
}
void EString::replay(MDS *mds)
{
  dout(10) << "EString.replay " << event << endl; 
}



// -----------------------
// EMetaBlob

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
	       << " for " << *dir << endl;
      continue;       // not our problem
    }
    if (dir->get_committed_version() >= lp->second.dirv ||
	dir->get_committed_version_equivalent() >= lp->second.dirv) {
      dout(10) << "EMetaBlob.has_expired have dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      continue;       // yay
    }
    
    if (dir->is_ambiguous_dir_auth()) {
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      if (ex->is_exporting()) {
	// wait until export is acked (logged on remote) and committed (logged locally)
	dout(10) << "EMetaBlob.has_expired ambiguous auth for " << *dir
		 << ", exporting on " << *ex << endl;
	return false;
      } else {
	dout(10) << "EMetaBlob.has_expired ambiguous auth for " << *dir
		 << ", importing on " << *ex << endl;
	return false;
      }
    }

    if (dir->get_committed_version() < lp->second.dirv) {
      dout(10) << "EMetaBlob.has_expired need dirv " << lp->second.dirv
	       << " for " << *dir << endl;
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
	       << " not yet acked" << endl;
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
	  dout(10) << "EMetaBlob.has_expired dirty mtime dirlock hasn't flushed on " << *in << endl;
	  return false;
	}
      }
    }

  // allocated_ios
  if (!allocated_inos.empty()) {
    version_t cv = mds->idalloc->get_committed_version();
    if (cv < alloc_tablev) {
      dout(10) << "EMetaBlob.has_expired idalloc tablev " << alloc_tablev << " > " << cv
	       << ", still dirty" << endl;
      return false;   // still dirty
    } else {
      dout(10) << "EMetaBlob.has_expired idalloc tablev " << alloc_tablev << " <= " << cv
	       << ", already flushed" << endl;
    }
  }


  // truncated inodes
  for (list< pair<inode_t,off_t> >::iterator p = truncated_inodes.begin();
       p != truncated_inodes.end();
       ++p) {
    if (mds->mdcache->is_purging(p->first.ino, p->second)) {
      dout(10) << "EMetaBlob.has_expired still purging inode " << p->first.ino 
	       << " to " << p->second << endl;
      return false;
    }
  }  

  // client requests
  for (list<metareqid_t>::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p) {
    if (mds->clientmap.have_completed_request(*p)) {
      dout(10) << "EMetaBlob.has_expired still have completed request " << *p
	       << endl;
      return false;
    }
  }

  
  return true;  // all dirlumps expired, etc.
}


void EMetaBlob::expire(MDS *mds, Context *c)
{
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
	       << " for " << *dir << endl;
      continue;     // not our problem
    }
    if (dir->get_committed_version() >= lp->second.dirv ||
	dir->get_committed_version_equivalent() >= lp->second.dirv) {
      dout(10) << "EMetaBlob.expire have dirv " << lp->second.dirv
	       << " on " << *dir << endl;
      continue;   // yay
    }
    
    if (dir->is_ambiguous_dir_auth()) {
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      if (ex->is_exporting()) {
	// wait until export is acked (logged on remote) and committed (logged locally)
	dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
		 << ", waiting for export finish on " << *ex << endl;
	waitfor_export.push_back(ex);
	continue;
      } else {
	dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
		 << ", waiting for import finish on " << *ex << endl;
	waitfor_import.push_back(ex);
	continue;
      }
    }
    
    assert(dir->get_committed_version() < lp->second.dirv);
    dout(10) << "EMetaBlob.expire need dirv " << lp->second.dirv
	     << ", committing " << *dir << endl;
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
      p->first->add_waiter(CDir::WAIT_AUTHPINNABLE, gather->new_sub());  
  }
  for (list<CDir*>::iterator p = waitfor_export.begin();
       p != waitfor_export.end();
       ++p) 
    mds->mdcache->migrator->add_export_finish_waiter(*p, gather->new_sub());
  for (list<CDir*>::iterator p = waitfor_import.begin();
       p != waitfor_import.end();
       ++p) 
    (*p)->add_waiter(CDir::WAIT_IMPORTED, gather->new_sub());
  

  // have my anchortable ops committed?
  for (list<version_t>::iterator p = atids.begin();
       p != atids.end();
       ++p) {
    if (!mds->anchorclient->has_committed(*p)) {
      dout(10) << "EMetaBlob.expire anchor transaction " << *p 
	       << " not yet acked, waiting" << endl;
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
		   << *in << endl;
	  in->dirlock.add_waiter(SimpleLock::WAIT_STABLE, gather->new_sub());
	}
      }
    }

  // allocated_inos
  if (!allocated_inos.empty()) {
    version_t cv = mds->idalloc->get_committed_version();
    if (cv < alloc_tablev) {
      dout(10) << "EMetaBlob.expire saving idalloc table, need " << alloc_tablev << endl;
      mds->idalloc->save(gather->new_sub(), alloc_tablev);
    }
  }

  // truncated inodes
  for (list< pair<inode_t,off_t> >::iterator p = truncated_inodes.begin();
       p != truncated_inodes.end();
       ++p) {
    if (mds->mdcache->is_purging(p->first.ino, p->second)) {
      dout(10) << "EMetaBlob.expire waiting for purge of inode " << p->first.ino
	       << " to " << p->second << endl;
      mds->mdcache->wait_for_purge(p->first.ino, p->second, gather->new_sub());
    }
  }

  // client requests
  for (list<metareqid_t>::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p) {
    if (mds->clientmap.have_completed_request(*p)) {
      dout(10) << "EMetaBlob.expire waiting on completed request " << *p
	       << endl;
      mds->clientmap.add_trim_waiter(*p, gather->new_sub());
    }
  }

}

void EMetaBlob::replay(MDS *mds)
{
  dout(10) << "EMetaBlob.replay " << lump_map.size() << " dirlumps" << endl;

  // walk through my dirs (in order!)
  for (list<dirfrag_t>::iterator lp = lump_order.begin();
       lp != lump_order.end();
       ++lp) {
    dout(10) << "EMetaBlob.replay dir " << *lp << endl;
    dirlump &lump = lump_map[*lp];

    // the dir 
    CDir *dir = mds->mdcache->get_dirfrag(*lp);
    if (!dir) {
      // hmm.  do i have the inode?
      CInode *diri = mds->mdcache->get_inode((*lp).ino);
      if (!diri) {
	if ((*lp).ino == MDS_INO_ROOT) {
	  diri = mds->mdcache->create_root_inode();
	  dout(10) << "EMetaBlob.replay created root " << *diri << endl;
	} else if (MDS_INO_IS_STRAY((*lp).ino)) {
	  int whose = (*lp).ino - MDS_INO_STRAY_OFFSET;
	  diri = mds->mdcache->create_stray_inode(whose);
	  dout(10) << "EMetaBlob.replay created stray " << *diri << endl;
	} else {
	  assert(0);
	}
      }
      // create the dirfrag
      dir = diri->get_or_open_dirfrag(mds->mdcache, (*lp).frag);

      if ((*lp).ino < MDS_INO_BASE) 
	mds->mdcache->adjust_subtree_auth(dir, CDIR_AUTH_UNKNOWN);

      dout(10) << "EMetaBlob.replay added dir " << *dir << endl;  
    }
    dir->set_version( lump.dirv );
    if (lump.is_dirty())
      dir->_mark_dirty();
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
	if (p->dirty) dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *dn << endl;
      } else {
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay had " << *dn << endl;
      }

      CInode *in = mds->mdcache->get_inode(p->inode.ino);
      if (!in) {
	in = new CInode(mds->mdcache);
	in->inode = p->inode;
	if (in->inode.is_symlink()) in->symlink = p->symlink;
	mds->mdcache->add_inode(in);
	dir->link_primary_inode(dn, in);
	if (p->dirty) in->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *in << endl;
      } else {
	if (in->get_parent_dn()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *in << endl;
	  in->get_parent_dn()->get_dir()->unlink_inode(in->get_parent_dn());
	}
	in->inode = p->inode;
	if (in->inode.is_symlink()) in->symlink = p->symlink;
	dir->link_primary_inode(dn, in);
	if (p->dirty) in->_mark_dirty();
	dout(10) << "EMetaBlob.replay linked " << *in << endl;
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
	if (p->dirty) dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *dn << endl;
      } else {
	if (!dn->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << endl;
	  dir->unlink_inode(dn);
	}
	dn->set_remote(p->ino, p->d_type);
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay had " << *dn << endl;
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
	if (p->dirty) dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *dn << endl;
      } else {
	if (!dn->is_null()) {
	  dout(10) << "EMetaBlob.replay unlinking " << *dn << endl;
	  dir->unlink_inode(dn);
	}
	dn->set_version(p->dnv);
	if (p->dirty) dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay had " << *dn << endl;
      }
    }
  }

  // anchor transactions
  for (list<version_t>::iterator p = atids.begin();
       p != atids.end();
       ++p) {
    dout(10) << "EMetaBlob.replay noting anchor transaction " << *p << endl;
    mds->anchorclient->got_journaled_agree(*p);
  }

  // dirtied inode mtimes
  if (!dirty_inode_mtimes.empty())
    for (map<inodeno_t,utime_t>::iterator p = dirty_inode_mtimes.begin();
	 p != dirty_inode_mtimes.end();
	 ++p) {
      CInode *in = mds->mdcache->get_inode(p->first);
      dout(10) << "EMetaBlob.replay setting dirlock updated flag on " << *in << endl;
      in->dirlock.set_updated();
    }

  // allocated_inos
  if (!allocated_inos.empty()) {
    if (mds->idalloc->get_version() >= alloc_tablev) {
      dout(10) << "EMetaBlob.replay idalloc tablev " << alloc_tablev
	       << " <= table " << mds->idalloc->get_version() << endl;
    } else {
      for (list<inodeno_t>::iterator p = allocated_inos.begin();
	   p != allocated_inos.end();
	   ++p) {
	dout(10) << " EMetaBlob.replay idalloc " << *p << " tablev " << alloc_tablev
		 << " - 1 == table " << mds->idalloc->get_version() << endl;
	assert(alloc_tablev-1 == mds->idalloc->get_version());
	
	inodeno_t ino = mds->idalloc->alloc_id();
	assert(ino == *p);       // this should match.
    
	assert(alloc_tablev == mds->idalloc->get_version());
      }	
    }
  }

  // truncated inodes
  for (list< pair<inode_t,off_t> >::iterator p = truncated_inodes.begin();
       p != truncated_inodes.end();
       ++p) {
    dout(10) << "EMetaBlob.replay will purge truncated inode " << p->first.ino
	     << " to " << p->second << endl;
    mds->mdcache->add_recovered_purge(p->first, p->second);  
  }

  // client requests
  for (list<metareqid_t>::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p)
    mds->clientmap.add_completed_request(*p);
}

// -----------------------
// ESession
bool ESession::has_expired(MDS *mds) 
{
  if (mds->clientmap.get_committed() >= cmapv) {
    dout(10) << "ESession.has_expired newer clientmap " << mds->clientmap.get_committed() 
	     << " >= " << cmapv << " has committed" << endl;
    return true;
  } else if (mds->clientmap.get_committing() >= cmapv) {
    dout(10) << "ESession.has_expired newer clientmap " << mds->clientmap.get_committing() 
	     << " >= " << cmapv << " is still committing" << endl;
    return false;
  } else {
    dout(10) << "ESession.has_expired clientmap " << mds->clientmap.get_version() 
	     << " > " << cmapv << ", need to save" << endl;
    return false;
  }
}

void ESession::expire(MDS *mds, Context *c)
{  
  dout(10) << "ESession.expire saving clientmap" << endl;
  mds->clientmap.save(c, cmapv);
}

void ESession::replay(MDS *mds)
{
  if (mds->clientmap.get_version() >= cmapv) {
    dout(10) << "ESession.replay clientmap " << mds->clientmap.get_version() 
	     << " >= " << cmapv << ", noop" << endl;

    // hrm, this isn't very pretty.
    if (!open)
      mds->clientmap.trim_completed_requests(client_inst.name.num(), 0);

  } else {
    dout(10) << "ESession.replay clientmap " << mds->clientmap.get_version() 
	     << " < " << cmapv << endl;
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
	     << ", still dirty" << endl;
    return false;   // still dirty
  } else {
    dout(10) << "EAnchor.has_expired v " << version << " <= " << cv
	     << ", already flushed" << endl;
    return true;    // already flushed
  }
}

void EAnchor::expire(MDS *mds, Context *c)
{
  dout(10) << "EAnchor.expire saving anchor table" << endl;
  mds->anchortable->save(c);
}

void EAnchor::replay(MDS *mds)
{
  if (mds->anchortable->get_version() >= version) {
    dout(10) << "EAnchor.replay event " << version
	     << " <= table " << mds->anchortable->get_version() << endl;
  } else {
    dout(10) << " EAnchor.replay event " << version
	     << " - 1 == table " << mds->anchortable->get_version() << endl;
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
  dout(10) << " EAnchorClient.replay op " << op << " atid " << atid << endl;
    
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

bool EUpdate::has_expired(MDS *mds)
{
  return metablob.has_expired(mds);
}

void EUpdate::expire(MDS *mds, Context *c)
{
  metablob.expire(mds, c);
}

void EUpdate::replay(MDS *mds)
{
  metablob.replay(mds);
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
      dout(10) << "EOpen.has_expired still refer to caps on " << *in << endl;
      return false;
    }
  }
  return true;
}

void EOpen::expire(MDS *mds, Context *c)
{
  dout(10) << "EOpen.expire " << endl;
  
  if (mds->mdlog->is_capped()) {
    dout(0) << "uh oh, log is capped, but i have unexpired opens." << endl;
    assert(0);
  }

  for (list<inodeno_t>::iterator p = inos.begin(); p != inos.end(); ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (!in) continue;
    if (!in->is_any_caps()) continue;
    
    dout(10) << "EOpen.expire " << in->ino()
	     << " last_open_journaled " << in->last_open_journaled << endl;

    mds->server->queue_journal_open(in);
  }
  mds->server->add_journal_open_waiter(c);
  mds->server->maybe_journal_opens();
}

void EOpen::replay(MDS *mds)
{
  dout(10) << "EOpen.replay " << endl;
  metablob.replay(mds);
}


// -----------------------
// ESlaveUpdate

bool ESlaveUpdate::has_expired(MDS *mds)
{
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    if (mds->mdcache->ambiguous_slave_updates.count(reqid) == 0) {
      dout(10) << "ESlaveUpdate.has_expired prepare " << reqid << " for mds" << master 
	       << ": haven't yet seen commit|rollback" << endl;
      return false;
    } 
    else if (mds->mdcache->ambiguous_slave_updates[reqid]) {
      dout(10) << "ESlaveUpdate.has_expired prepare " << reqid << " for mds" << master 
	       << ": committed, checking metablob" << endl;
      bool exp = metablob.has_expired(mds);
      if (exp) 
	mds->mdcache->ambiguous_slave_updates.erase(reqid);
      return exp;      
    }
    else {
      dout(10) << "ESlaveUpdate.has_expired prepare " << reqid << " for mds" << master 
	       << ": aborted" << endl;
      mds->mdcache->ambiguous_slave_updates.erase(reqid);
      return true;
    }
    
  case ESlaveUpdate::OP_COMMIT:
  case ESlaveUpdate::OP_ROLLBACK:
    if (mds->mdcache->waiting_for_slave_update_commit.count(reqid)) {
      dout(10) << "ESlaveUpdate.has_expired "
	       << ((op == ESlaveUpdate::OP_COMMIT) ? "commit ":"rollback ")
	       << reqid << " for mds" << master 
	       << ": noting commit, kicking prepare waiter" << endl;
      mds->mdcache->ambiguous_slave_updates[reqid] = (op == ESlaveUpdate::OP_COMMIT);
      mds->mdcache->waiting_for_slave_update_commit[reqid]->finish(0);
      delete mds->mdcache->waiting_for_slave_update_commit[reqid];
      mds->mdcache->waiting_for_slave_update_commit.erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.has_expired "
	       << ((op == ESlaveUpdate::OP_COMMIT) ? "commit ":"rollback ")
	       << reqid << " for mds" << master 
	       << ": no prepare waiter, ignoring" << endl;
    }
    return true;

  default:
    assert(0);
  }
}

void ESlaveUpdate::expire(MDS *mds, Context *c)
{
  assert(op == ESlaveUpdate::OP_PREPARE);

  if (mds->mdcache->ambiguous_slave_updates.count(reqid) == 0) {
    // wait
    dout(10) << "ESlaveUpdate.expire prepare " << reqid << " for mds" << master
	     << ": waiting for commit|rollback" << endl;
    mds->mdcache->waiting_for_slave_update_commit[reqid] = c;
  } else {
    // we committed.. expire the metablob
    assert(mds->mdcache->ambiguous_slave_updates[reqid] == true); 
    dout(10) << "ESlaveUpdate.expire prepare " << reqid << " for mds" << master
	     << ": waiting for metablob to expire" << endl;
    metablob.expire(mds, c);
  }
}

void ESlaveUpdate::replay(MDS *mds)
{
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    // FIXME: horribly inefficient copy; EMetaBlob needs a swap() or something
    dout(10) << "ESlaveUpdate.replay prepare " << reqid << " for mds" << master 
	     << ": saving blob for later commit" << endl;
    assert(mds->mdcache->uncommitted_slave_updates[master].count(reqid) == 0);
    mds->mdcache->uncommitted_slave_updates[master][reqid] = metablob;
    break;

  case ESlaveUpdate::OP_COMMIT:
    if (mds->mdcache->uncommitted_slave_updates[master].count(reqid)) {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds" << master
	       << ": applying previously saved blob" << endl;
      mds->mdcache->uncommitted_slave_updates[master][reqid].replay(mds);
      mds->mdcache->uncommitted_slave_updates[master].erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds" << master 
	       << ": ignoring, no previously saved blob" << endl;
    }
    break;

  case ESlaveUpdate::OP_ROLLBACK:
    if (mds->mdcache->uncommitted_slave_updates[master].count(reqid)) {
      dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds" << master
	       << ": discarding previously saved blob" << endl;
      assert(mds->mdcache->uncommitted_slave_updates[master].count(reqid));
      mds->mdcache->uncommitted_slave_updates[master].erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds" << master 
	       << ": ignoring, no previously saved blob" << endl;
    }
    break;

  default:
    assert(0);
  }
}


// -----------------------
// ESubtreeMap

bool ESubtreeMap::has_expired(MDS *mds)
{
  if (mds->mdlog->get_last_subtree_map_offset() > get_start_off()) {
    dout(10) << "ESubtreeMap.has_expired -- there's a newer map" << endl;
    return true;
  } else if (mds->mdlog->is_capped()) {
    dout(10) << "ESubtreeMap.has_expired -- log is capped, allowing map to expire" << endl;
    return true;
  } else {
    dout(10) << "ESubtreeMap.has_expired -- not until there's a newer map written" 
	     << " (" << get_start_off() << " >= " << mds->mdlog->get_last_subtree_map_offset() << ")"
	     << endl;
    return false;
  }
}

void ESubtreeMap::expire(MDS *mds, Context *c)
{
  dout(10) << "ESubtreeMap.has_expire -- waiting for a newer map to be written (or for shutdown)" << endl;
  mds->mdlog->add_subtree_map_expire_waiter(c);
}

void ESubtreeMap::replay(MDS *mds) 
{
  if (mds->mdcache->is_subtrees()) {
    dout(10) << "ESubtreeMap.replay -- ignoring, already have import map" << endl;
  } else {
    dout(10) << "ESubtreeMap.replay -- reconstructing (auth) subtree spanning tree" << endl;
    
    // first, stick the spanning tree in my cache
    //metablob.print(cout);
    metablob.replay(mds);
    
    // restore import/export maps
    for (map<dirfrag_t, list<dirfrag_t> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      CDir *dir = mds->mdcache->get_dirfrag(p->first);
      mds->mdcache->adjust_bounded_subtree_auth(dir, p->second, mds->get_nodeid());
    }
  }
  mds->mdcache->show_subtrees();
}



// -----------------------
// EFragment

bool EFragment::has_expired(MDS *mds)
{
  return metablob.has_expired(mds);
}

void EFragment::expire(MDS *mds, Context *c)
{
  metablob.expire(mds, c);
}

void EFragment::replay(MDS *mds)
{
  dout(10) << "EFragment.replay " << ino << " " << basefrag << " by " << bits << endl;
  
  CInode *in = mds->mdcache->get_inode(ino);
  assert(in);

  //in->fragment_dir(basefrag, bits);
  metablob.replay(mds);
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

void EPurgeFinish::replay(MDS *mds)
{
  dout(10) << "EPurgeFinish.replay " << ino << " to " << newsize << endl;
  mds->mdcache->remove_recovered_purge(ino, newsize);
}





// =========================================================================

// -----------------------
// EExport

bool EExport::has_expired(MDS *mds)
{
  CDir *dir = mds->mdcache->get_dirfrag(base);
  if (dir && mds->mdcache->migrator->is_exporting(dir)) {
    dout(10) << "EExport.has_expired still exporting " << *dir << endl;
    return false;
  }
  return true;
}

void EExport::expire(MDS *mds, Context *c)
{
  CDir *dir = mds->mdcache->get_dirfrag(base);
  assert(dir);
  assert(mds->mdcache->migrator->is_exporting(dir));

  dout(10) << "EExport.expire waiting for export of " << *dir << endl;
  mds->mdcache->migrator->add_export_finish_waiter(dir, c);
}

void EExport::replay(MDS *mds)
{
  dout(10) << "EExport.replay " << base << endl;
  metablob.replay(mds);
  
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

bool EImportStart::has_expired(MDS *mds)
{
  return metablob.has_expired(mds);
}

void EImportStart::expire(MDS *mds, Context *c)
{
  dout(10) << "EImportStart.expire " << base << endl;
  metablob.expire(mds, c);
}

void EImportStart::replay(MDS *mds)
{
  dout(10) << "EImportStart.replay " << base << endl;
  metablob.replay(mds);

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
    dout(10) << "EImportFinish.replay " << base << " success=" << success << endl;
    if (success) 
      mds->mdcache->finish_ambiguous_import(base);
    else
      mds->mdcache->cancel_ambiguous_import(base);
  } else {
    dout(10) << "EImportFinish.replay " << base << " success=" << success
	     << ", predates my subtree_map start point, ignoring" 
	     << endl;
    // verify that?
  }
}





