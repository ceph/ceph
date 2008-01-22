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
#include "events/ESessions.h"

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
#include "Locker.h"


#include "config.h"

#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log || l <= g_conf.debug_mds_log_expire) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log || l <= g_conf.debug_mds_log_expire) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "


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
  for (xlist<CDir*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) {
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (xlist<CDentry*>::iterator p = dirty_dentries.begin(); !p.end(); ++p) {
    assert((*p)->is_auth());
    commit.insert((*p)->get_dir());
  }
  for (xlist<CInode*>::iterator p = dirty_inodes.begin(); !p.end(); ++p) {
    assert((*p)->is_auth());
    commit.insert((*p)->get_parent_dn()->get_dir());
  }

  if (!commit.empty()) {
    if (!gather) gather = new C_Gather;
    
    for (set<CDir*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p) {
      CDir *dir = *p;
      assert(dir->is_auth());
      if (dir->can_auth_pin()) {
	dout(15) << "try_to_expire committing " << *dir << dendl;
	dir->commit(0, gather->new_sub());
      } else {
	dout(15) << "try_to_expire waiting for unfreeze on " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_UNFREEZE, gather->new_sub());
      }
    }
  }

  // dirty non-auth mtimes
  for (xlist<CInode*>::iterator p = dirty_inode_mtimes.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dirlock mtime flush on " << *in << dendl;
    if (!gather) gather = new C_Gather;

    if (in->is_ambiguous_auth()) {
      dout(10) << " waiting for single auth on " << *in << dendl;
      in->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, gather->new_sub());
    } else if (in->is_auth()) {
      dout(10) << " i'm auth, unscattering dirlock on " << *in << dendl;
      assert(in->is_replicated()); // hrm!
      mds->locker->scatter_lock(&in->dirlock);
      in->dirlock.add_waiter(SimpleLock::WAIT_STABLE, gather->new_sub());
    } else {
      dout(10) << " i'm a replica, requesting dirlock unscatter of " << *in << dendl;
      mds->locker->scatter_try_unscatter(&in->dirlock, gather->new_sub());
    }
    //(*p)->dirlock.add_waiter(SimpleLock::WAIT_STABLE, gather->new_sub());
  }
  
  // open files
  if (!open_files.empty()) {
    assert(!mds->mdlog->is_capped()); // hmm FIXME
    for (xlist<CInode*>::iterator p = open_files.begin(); !p.end(); ++p) {
      dout(20) << "try_to_expire requeueing open file " << **p << dendl;
      mds->server->queue_journal_open(*p);
    }
    if (!gather) gather = new C_Gather;
    mds->server->add_journal_open_waiter(gather->new_sub());
    mds->server->maybe_journal_opens();
    dout(10) << "try_to_expire waiting for open files to rejournal" << dendl;
  }

  // slave updates
  for (xlist<MDSlaveUpdate*>::iterator p = slave_updates.begin(); !p.end(); ++p) {
    MDSlaveUpdate *su = *p;
    dout(10) << "try_to_expire waiting on slave update " << su << dendl;
    assert(su->waiter == 0);
    if (!gather) gather = new C_Gather;
    su->waiter = gather->new_sub();
  }

  // idalloc
  if (allocv > mds->idalloc->get_committed_version()) {
    dout(10) << "try_to_expire saving idalloc table, need " << allocv
	      << ", committed is " << mds->idalloc->get_committed_version()
	      << " (" << mds->idalloc->get_committing_version() << ")"
	      << dendl;
    if (!gather) gather = new C_Gather;
    mds->idalloc->save(gather->new_sub(), allocv);
  }

  // sessionmap
  if (sessionmapv > mds->sessionmap.committed) {
    dout(10) << "try_to_expire saving sessionmap, need " << sessionmapv 
	      << ", committed is " << mds->sessionmap.committed
	      << " (" << mds->sessionmap.committing << ")"
	      << dendl;
    if (!gather) gather = new C_Gather;
    mds->sessionmap.save(gather->new_sub(), sessionmapv);
  }

  // pending commit atids
  for (hash_set<version_t>::iterator p = pending_commit_atids.begin();
       p != pending_commit_atids.end();
       ++p) {
    if (!gather) gather = new C_Gather;
    assert(!mds->anchorclient->has_committed(*p));
    dout(10) << "try_to_expire anchor transaction " << *p 
	     << " pending commit (not yet acked), waiting" << dendl;
    mds->anchorclient->wait_for_ack(*p, gather->new_sub());
  }
  
  // anchortable
  if (anchortablev > mds->anchortable->get_committed_version()) {
    dout(10) << "try_to_expire waiting for anchor table to save, need " << anchortablev << dendl;
    if (!gather) gather = new C_Gather;
    mds->anchortable->save(gather->new_sub());
  }

  // FIXME client requests...?
  // audit handling of anchor transactions?

  if (gather) {
    dout(6) << "LogSegment(" << offset << ").try_to_expire waiting" << dendl;
  } else {
    dout(6) << "LogSegment(" << offset << ").try_to_expire success" << dendl;
  }
  return gather;
}



#undef dout
#undef derr
#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "


// -----------------------
// EString

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
    mds->sessionmap.add_completed_request(*p);


  // update segment
  update_segment(logseg);
}

// -----------------------
// ESession

void ESession::update_segment()
{
  _segment->sessionmapv = cmapv;
}

void ESession::replay(MDS *mds)
{
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.version 
	     << " >= " << cmapv << ", noop" << dendl;

    // hrm, this isn't very pretty.
    if (!open)
      mds->sessionmap.trim_completed_requests(client_inst.name, 0);

  } else {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.version
	     << " < " << cmapv << dendl;
    mds->sessionmap.projected = ++mds->sessionmap.version;
    assert(mds->sessionmap.version == cmapv);
    if (open) {
      Session *session = mds->sessionmap.get_or_add_session(client_inst);
      session->last_cap_renew = g_clock.now();
      mds->sessionmap.set_state(session, Session::STATE_OPEN);
    } else {
      Session *session = mds->sessionmap.get_session(client_inst.name);
      if (session)
	mds->sessionmap.remove_session(session);
    }
  }
}

void ESessions::update_segment()
{
  _segment->sessionmapv = cmapv;
}

void ESessions::replay(MDS *mds)
{
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.version
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.version
	     << " < " << cmapv << dendl;
    mds->sessionmap.open_sessions(client_map);
    assert(mds->sessionmap.version == cmapv);
    mds->sessionmap.projected = mds->sessionmap.version;
  }
}



// -----------------------
// EAnchor

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

void ESlaveUpdate::replay(MDS *mds)
{
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    // FIXME: horribly inefficient copy; EMetaBlob needs a swap() or something
    dout(10) << "ESlaveUpdate.replay prepare " << reqid << " for mds" << master 
	     << ": saving blobs for later commit" << dendl;
    assert(mds->mdcache->uncommitted_slave_updates[master].count(reqid) == 0);
    commit._segment = _segment;  // may need this later
    rollback._segment = _segment;  // may need this later
    mds->mdcache->uncommitted_slave_updates[master][reqid] = 
      new MDSlaveUpdate(commit, rollback, _segment->slave_updates);
    break;

  case ESlaveUpdate::OP_COMMIT:
    if (mds->mdcache->uncommitted_slave_updates[master].count(reqid)) {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds" << master
	       << ": applying commit blob" << dendl;
      mds->mdcache->uncommitted_slave_updates[master][reqid]->commit.replay(mds, _segment);
      delete mds->mdcache->uncommitted_slave_updates[master][reqid];
      mds->mdcache->uncommitted_slave_updates[master].erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds" << master 
	       << ": ignoring, no previously saved blobs" << dendl;
    }
    break;

  case ESlaveUpdate::OP_ROLLBACK:
    if (mds->mdcache->uncommitted_slave_updates[master].count(reqid)) {
      dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds" << master
	       << ": applying rollback blob" << dendl;
      assert(mds->mdcache->uncommitted_slave_updates[master].count(reqid));
      mds->mdcache->uncommitted_slave_updates[master][reqid]->rollback.replay(mds, _segment);
      delete mds->mdcache->uncommitted_slave_updates[master][reqid];
      mds->mdcache->uncommitted_slave_updates[master].erase(reqid);
    } else {
      dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds" << master 
	       << ": ignoring, no previously saved blobs" << dendl;
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

  // open client sessions?
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "EImportStart.replay sessionmap " << mds->sessionmap.version 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "EImportStart.replay sessionmap " << mds->sessionmap.version 
	     << " < " << cmapv << dendl;
    map<int,entity_inst_t> cm;
    bufferlist::iterator blp = client_map.begin();
    ::_decode_simple(cm, blp);
    mds->sessionmap.open_sessions(cm);
    assert(mds->sessionmap.version == cmapv);
    mds->sessionmap.projected = mds->sessionmap.version;
  }
}

// -----------------------
// EImportFinish

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





