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
 * - has been safely exported. note that !is_auth() && !is_proxy()
 * implies safely exported.  if !is_auth() && is_proxy(), we need to
 * add a waiter for the export to complete.
 *
 */
bool EMetaBlob::has_expired(MDS *mds)
{
  // examine dirv's for my lumps
  for (map<inodeno_t,dirlump>::iterator lp = lump_map.begin();
       lp != lump_map.end();
       ++lp) {
    CInode *diri = mds->mdcache->get_inode(lp->first);
    if (!diri) 
      continue;       // we expired it
    CDir *dir = diri->dir;
    if (!dir) 
      continue;       // we expired it

    // FIXME: check the slice only

    if (dir->is_proxy()) {
      dout(10) << "EMetaBlob.has_expired am proxy, needed dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      return false;      // we need to wait until the export flushes!
    }
    if (!dir->is_auth()) {
      dout(10) << "EMetaBlob.has_expired not auth, needed dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      continue;       // not our problem
    }

    if (dir->get_committed_version() < lp->second.dirv) {
      dout(10) << "EMetaBlob.has_expired need dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      return false;  // not committed.
    } else {
      dout(10) << "EMetaBlob.has_expired have dirv " << lp->second.dirv
	       << " for " << *dir << endl;
    }
  }

  return true;  // all dirlumps expired.
}

void EMetaBlob::expire(MDS *mds, Context *c)
{
  map<CDir*,version_t> commit;  // dir -> version needed
  list<CDir*> waitfor_export;
  int ncommit = 0;

  // examine dirv's for my lumps
  // make list of dir slices i need to commit
  for (map<inodeno_t,dirlump>::iterator lp = lump_map.begin();
       lp != lump_map.end();
       ++lp) {
    CInode *diri = mds->mdcache->get_inode(lp->first);
    if (!diri) 
      continue;       // we expired it
    CDir *dir = diri->dir;
    if (!dir) 
      continue;       // we expired it
    
    // FIXME: check the slice only

    if (dir->auth_is_ambiguous()) {
      // wait until export is acked (logged on remote) and committed (logged locally)
      CDir *ex = mds->mdcache->get_subtree_root(dir);
      dout(10) << "EMetaBlob.expire ambiguous auth for " << *dir
	       << ", waiting for export finish on " << *ex << endl;
      waitfor_export.push_back(ex);
      continue;
    }
    if (!dir->is_auth()) {
      dout(10) << "EMetaBlob.expire not auth, needed dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      continue;     // not our problem
    }
    if (dir->get_committed_version() < lp->second.dirv) {
      dout(10) << "EMetaBlob.expire need dirv " << lp->second.dirv
	       << ", committing " << *dir << endl;
      commit[dir] = MAX(commit[dir], lp->second.dirv);
      ncommit++;
    } else {
      dout(10) << "EMetaBlob.expire have dirv " << lp->second.dirv
	       << " on " << *dir << endl;
    }
  }

  // commit
  assert(!commit.empty());

  C_Gather *gather = new C_Gather(c);
  for (map<CDir*,version_t>::iterator p = commit.begin();
       p != commit.end();
       ++p)
    p->first->commit(p->second, gather->new_sub());
  for (list<CDir*>::iterator p = waitfor_export.begin();
       p != waitfor_export.end();
       ++p) 
    mds->mdcache->migrator->add_export_finish_waiter(*p, gather->new_sub());
}

void EMetaBlob::replay(MDS *mds)
{
  dout(10) << "EMetaBlob.replay " << lump_map.size() << " dirlumps" << endl;

  // walk through my dirs (in order!)
  for (list<inodeno_t>::iterator lp = lump_order.begin();
       lp != lump_order.end();
       ++lp) {
    dout(10) << "EMetaBlob.replay dir " << *lp << endl;
    dirlump &lump = lump_map[*lp];

    // the dir 
    CInode *diri = mds->mdcache->get_inode(*lp);
    CDir *dir;
    if (!diri) {
      assert(*lp == 1);
      diri = mds->mdcache->create_root_inode();
      dout(10) << "EMetaBlob.replay created root " << *diri << endl;
    }
    if (diri->dir) {
      dir = diri->dir;
      dout(20) << "EMetaBlob.replay had dir " << *dir << endl;
    } else {
      dir = diri->get_or_open_dir(mds->mdcache);
      if (*lp == 1) 
	dir->set_dir_auth(CDIR_AUTH_UNKNOWN);
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
    for (set<inodeno_t>::iterator p = imports.begin();
	 p != imports.end();
	 ++p) {
      CInode *diri = mds->mdcache->get_inode(*p);
      CDir *dir = diri->dir;
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
}

void EPurgeFinish::replay(MDS *mds)
{
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
  dout(10) << "EImportStart.expire " << dirino << endl;
  metablob.expire(mds, c);
}

void EImportStart::replay(MDS *mds)
{
  dout(10) << "EImportStart.replay " << dirino << endl;
  metablob.replay(mds);

  // put in ambiguous import list
  mds->mdcache->add_ambiguous_import(dirino, bounds);
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
  dout(10) << "EImportFinish.replay " << dirino << " success=" << success << endl;
  if (success) 
    mds->mdcache->finish_ambiguous_import(dirino);
  else
    mds->mdcache->cancel_ambiguous_import(dirino);
}





