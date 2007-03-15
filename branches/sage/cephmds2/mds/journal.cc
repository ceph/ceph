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

#include "events/EString.h"

#include "events/EMetaBlob.h"
#include "events/EAlloc.h"
#include "events/EAnchor.h"
#include "events/EUpdate.h"
#include "events/EImportMap.h"

#include "events/EMount.h"
#include "events/EClientMap.h"

#include "events/EPurgeFinish.h"
#include "events/EUnlink.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Migrator.h"
#include "AnchorTable.h"
#include "AnchorClient.h"

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
    if (dir->get_committed_version() >= lp->second.dirv) {
      dout(10) << "EMetaBlob.has_expired have dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      continue;       // yay
    }
    
    if (dir->auth_is_ambiguous()) {
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      if (ex->is_exporting()) {
	// wait until export is acked (logged on remote) and committed (logged locally)
	dout(10) << "EMetaBlob.has_expired ambiguous auth for " << *dir
		 << ", exporting (not yet safe) on " << *ex << endl;
	return false;
      } else {
	dout(10) << "EMetaBlob.has_expired ambiguous auth for " << *dir
		 << ", _importing_ (safe) on " << *ex << endl;
	return true;
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

  // destroyed inodes
  for (list<inode_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       ++p) {
    if (mds->mdcache->is_purging(p->ino)) {
      dout(10) << "EMetaBlob.has_expired still purging destroyed inode " << p->ino << endl;
      return false;
    }
  }  
  
  return true;  // all dirlumps expired, etc.
}


void EMetaBlob::expire(MDS *mds, Context *c)
{
  map<CDir*,version_t> commit;  // dir -> version needed
  list<CDir*> waitfor_export;
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
    if (dir->get_committed_version() >= lp->second.dirv) {
      dout(10) << "EMetaBlob.expire have dirv " << lp->second.dirv
	       << " on " << *dir << endl;
      continue;   // yay
    }
    
    if (dir->auth_is_ambiguous()) {
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      if (ex->is_exporting()) {
	// wait until export is acked (logged on remote) and committed (logged locally)
	dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
		 << ", waiting for export finish on " << *ex << endl;
	waitfor_export.push_back(ex);
	continue;
      } else {
	dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
		 << ", but _importing_, we're safe on " << *ex << endl;
	continue;
      }
    }
    if (dir->get_committed_version() < lp->second.dirv) {
      dout(10) << "EMetaBlob.expire need dirv " << lp->second.dirv
	       << ", committing " << *dir << endl;
      commit[dir] = MAX(commit[dir], lp->second.dirv);
      ncommit++;
      continue;
    }

    assert(0);  // hrm
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

  // destroyed inodes
  for (list<inode_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       ++p) {
    if (mds->mdcache->is_purging(p->ino)) {
      dout(10) << "EMetaBlob.expire waiting for purge of destroyed inode " << p->ino << endl;
      mds->mdcache->wait_for_purge(p->ino, gather->new_sub());
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
	assert((*lp).ino == 1);
	diri = mds->mdcache->create_root_inode();
	dout(10) << "EMetaBlob.replay created root " << *diri << endl;
      }
      // create the dirfrag
      dir = diri->get_or_open_dirfrag(mds->mdcache, (*lp).frag);
      if ((*lp).ino == 1) 
	dir->set_dir_auth(CDIR_AUTH_UNKNOWN);  // FIXME: can root dir be fragmented?  hrm.
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
      CInode *in = mds->mdcache->get_inode(p->inode.ino);
      if (!in) {
	// inode
	in = new CInode(mds->mdcache);
	in->inode = p->inode;
	if (in->inode.is_symlink()) in->symlink = p->symlink;
	mds->mdcache->add_inode(in);
	// dentry
	CDentry *dn = dir->add_dentry( p->dn, in );
	dn->set_version(p->dnv);
	dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *dn << " " << *in << endl;
      } else {
	// inode
	in->inode = p->inode;
	if (in->inode.is_symlink()) in->symlink = p->symlink;
	// dentry
	CDentry *dn = in->get_parent_dn();
	dn->set_version(p->dnv);
	dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay had " << *in->get_parent_dn() << " " << *in << endl;
      }
    }

    // remote dentries
    for (list<remotebit>::iterator p = lump.get_dremote().begin();
	 p != lump.get_dremote().end();
	 p++) {
      CDentry *dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_dentry(p->dn, p->ino);
	dn->set_remote_ino(p->ino);
	dn->set_version(p->dnv);
	dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *dn << endl;
      } else {
	dn->set_remote_ino(p->ino);
	dn->set_version(p->dnv);
	dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay had " << *dn << endl;
      }
    }

    // null dentries
    for (list<nullbit>::iterator p = lump.get_dnull().begin();
	 p != lump.get_dnull().end();
	 p++) {
      CDentry *dn = dir->lookup(p->dn);
      if (!dn) {
	dn = dir->add_dentry(p->dn);
	dn->set_version(p->dnv);
	dn->_mark_dirty();
	dout(10) << "EMetaBlob.replay added " << *dn << endl;
      } else {
	dn->set_version(p->dnv);
	dn->_mark_dirty();
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

  // destroyed inodes
  for (list<inode_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       ++p) {
    dout(10) << "EMetaBlob.replay will purge destroyed inode " << p->ino << endl;
    mds->mdcache->add_recovered_purge(*p);  
  }
}

// -----------------------
// EClientMap

bool EClientMap::has_expired(MDS *mds) 
{
  if (mds->clientmap.get_committed() >= cmapv) {
    dout(10) << "EClientMap.has_expired newer clientmap " << mds->clientmap.get_committed() 
	     << " >= " << cmapv << " has committed" << endl;
    return true;
  } else if (mds->clientmap.get_committing() >= cmapv) {
    dout(10) << "EClientMap.has_expired newer clientmap " << mds->clientmap.get_committing() 
	     << " >= " << cmapv << " is still committing" << endl;
    return false;
  } else {
    dout(10) << "EClientMap.has_expired clientmap " << mds->clientmap.get_version() 
	     << " not empty" << endl;
    return false;
  }
}

void EClientMap::expire(MDS *mds, Context *c)
{
  if (mds->clientmap.get_committing() >= cmapv) {
    dout(10) << "EClientMap.expire logging clientmap" << endl;
    assert(mds->clientmap.get_committing() > mds->clientmap.get_committed());
    mds->clientmap.add_commit_waiter(c);
  } else {
    dout(10) << "EClientMap.expire logging clientmap" << endl;
    mds->log_clientmap(c);
  }
}

void EClientMap::replay(MDS *mds)
{
  dout(10) << "EClientMap.replay v " << cmapv << endl;
  int off = 0;
  mds->clientmap.decode(mapbl, off);
  mds->clientmap.set_committed(mds->clientmap.get_version());
  mds->clientmap.set_committing(mds->clientmap.get_version());
}


// EMount
bool EMount::has_expired(MDS *mds) 
{
  if (mds->clientmap.get_committed() >= cmapv) {
    dout(10) << "EMount.has_expired newer clientmap " << mds->clientmap.get_committed() 
	     << " >= " << cmapv << " has committed" << endl;
    return true;
  } else if (mds->clientmap.get_committing() >= cmapv) {
    dout(10) << "EMount.has_expired newer clientmap " << mds->clientmap.get_committing() 
	     << " >= " << cmapv << " is still committing" << endl;
    return false;
  } else {
    dout(10) << "EMount.has_expired clientmap " << mds->clientmap.get_version() 
	     << " not empty" << endl;
    return false;
  }
}

void EMount::expire(MDS *mds, Context *c)
{
  if (mds->clientmap.get_committing() >= cmapv) {
    dout(10) << "EMount.expire logging clientmap" << endl;
    assert(mds->clientmap.get_committing() > mds->clientmap.get_committed());
    mds->clientmap.add_commit_waiter(c);
  } else {
    dout(10) << "EMount.expire logging clientmap" << endl;
    mds->log_clientmap(c);
  }
}

void EMount::replay(MDS *mds)
{
  dout(10) << "EMount.replay" << endl;
  if (mounted)
    mds->clientmap.add_mount(client_inst);
  else
    mds->clientmap.rem_mount(client_inst.name.num());
}



// -----------------------
// EAlloc

bool EAlloc::has_expired(MDS *mds) 
{
  version_t cv = mds->idalloc->get_committed_version();
  if (cv < table_version) {
    dout(10) << "EAlloc.has_expired v " << table_version << " > " << cv
	     << ", still dirty" << endl;
    return false;   // still dirty
  } else {
    dout(10) << "EAlloc.has_expired v " << table_version << " <= " << cv
	     << ", already flushed" << endl;
    return true;    // already flushed
  }
}

void EAlloc::expire(MDS *mds, Context *c)
{
  dout(10) << "EAlloc.expire saving idalloc table" << endl;
  mds->idalloc->save(c, table_version);
}

void EAlloc::replay(MDS *mds)
{
  if (mds->idalloc->get_version() >= table_version) {
    dout(10) << "EAlloc.replay event " << table_version
	     << " <= table " << mds->idalloc->get_version() << endl;
  } else {
    dout(10) << " EAlloc.replay event " << table_version
	     << " - 1 == table " << mds->idalloc->get_version() << endl;
    assert(table_version-1 == mds->idalloc->get_version());
    
    if (what == EALLOC_EV_ALLOC) {
      idno_t nid = mds->idalloc->alloc_id(true);
      assert(nid == id);       // this should match.
    } 
    else if (what == EALLOC_EV_FREE) {
      mds->idalloc->reclaim_id(id, true);
    } 
    else
      assert(0);
    
    assert(table_version == mds->idalloc->get_version());
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
      mds->anchortable->create_prepare(ino, trace);
      break;
    case ANCHOR_OP_DESTROY_PREPARE:
      mds->anchortable->destroy_prepare(ino);
      break;
    case ANCHOR_OP_UPDATE_PREPARE:
      mds->anchortable->update_prepare(ino, trace);
      break;
    case ANCHOR_OP_COMMIT:
      mds->anchortable->commit(atid);
      break;

      // anchorclient
    case ANCHOR_OP_ACK:
      mds->anchorclient->got_journaled_ack(atid);
      break;

    default:
      assert(0);
    }
    
    assert(version == mds->anchortable->get_version());
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


// -----------------------
// EImportMap

bool EImportMap::has_expired(MDS *mds)
{
  if (mds->mdlog->last_import_map > get_end_off()) {
    dout(10) << "EImportMap.has_expired -- there's a newer map" << endl;
    return true;
  } 
  else if (mds->mdlog->is_capped()) {
    dout(10) << "EImportMap.has_expired -- log is capped, allowing map to expire" << endl;
    return true;
  } else {
    dout(10) << "EImportMap.has_expired -- not until there's a newer map written" << endl;
    return false;
  }
}

/*
class C_MDS_ImportMapFlush : public Context {
  MDS *mds;
  off_t end_off;
public:
  C_MDS_ImportMapFlush(MDS *m, off_t eo) : mds(m), end_off(eo) { }
  void finish(int r) {
    // am i the last thing in the log?
    if (mds->mdlog->get_write_pos() == end_off) {
      // yes.  we're good.
    } else {
      // no.  submit another import_map so that we can go away.
    }
  }
};
*/

void EImportMap::expire(MDS *mds, Context *c)
{
  dout(10) << "EImportMap.has_expire -- waiting for a newer map to be written (or for shutdown)" << endl;
  mds->mdlog->import_map_expire_waiters.push_back(c);
}

void EImportMap::replay(MDS *mds) 
{
  if (!mds->mdcache->subtrees.empty()) {
    dout(10) << "EImportMap.replay -- ignoring, already have import map" << endl;
  } else {
    dout(10) << "EImportMap.replay -- reconstructing (auth) subtree spanning tree" << endl;
    
    // first, stick the spanning tree in my cache
    metablob.replay(mds);
    
    // restore import/export maps
    for (set<dirfrag_t>::iterator p = imports.begin();
	 p != imports.end();
	 ++p) {
      CDir *dir = mds->mdcache->get_dirfrag(*p);
      mds->mdcache->adjust_subtree_auth(dir, mds->get_nodeid());
    }
  }
  mds->mdcache->show_subtrees();
}



// -----------------------
// EUnlink

bool EUnlink::has_expired(MDS *mds)
{
  /*
  // dir
  CInode *diri = mds->mdcache->get_inode( diritrace.back().inode.ino );
  CDir *dir = 0;
  if (diri) dir = diri->dir;

  if (dir && dir->get_last_committed_version() < dirv) return false;

  if (!inodetrace.trace.empty()) {
    // inode
    CInode *in = mds->mdcache->get_inode( inodetrace.back().inode.ino );
    if (in && in->get_last_committed_version() < inodetrace.back().inode.version)
      return false;
  }
  */
  return true;
}

void EUnlink::expire(MDS *mds, Context *c)
{
  /*
  CInode *diri = mds->mdcache->get_inode( diritrace.back().inode.ino );
  CDir *dir = diri->dir;
  assert(dir);
  
  // okay!
  dout(7) << "commiting dirty (from unlink) dir " << *dir << endl;
  mds->mdstore->commit_dir(dir, dirv, c);
  */
}

void EUnlink::replay(MDS *mds)
{
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
  dout(10) << "EPurgeFinish.replay " << ino << endl;
  mds->mdcache->remove_recovered_purge(ino);
}





// =========================================================================

// -----------------------
// EExport

bool EExport::has_expired(MDS *mds)
{
  CDir *dir = mds->mdcache->get_dirfrag(base);
  if (!dir) return true;
  if (!mds->mdcache->migrator->is_exporting(dir))
    return true;
  dout(10) << "EExport.has_expired still exporting " << *dir << endl;
  return false;
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
  dout(10) << "EImportFinish.replay " << base << " success=" << success << endl;
  if (success) 
    mds->mdcache->finish_ambiguous_import(base);
  else
    mds->mdcache->cancel_ambiguous_import(base);
}





