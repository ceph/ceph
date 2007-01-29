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

#include "events/EMetaBlob.h"
#include "events/EAlloc.h"
#include "events/EUpdate.h"
#include "events/EImportMap.h"

#include "events/EPurgeFinish.h"
#include "events/EUnlink.h"
#include "events/EExportStart.h"
#include "events/EExportFinish.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "MDStore.h"
#include "Migrator.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "


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

    if (dir->get_last_committed_version() < lp->second.dirv) {
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
  list<CDir*> commit;
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

    if (dir->is_proxy()) {
      // wait until export is acked (logged on remote) and committed (logged locally)
      CDir *ex = mds->mdcache->get_export_container(dir);
      dout(10) << "EMetaBlob.expire proxy for " << *dir
	       << ", waiting for export finish on " << *ex << endl;
      waitfor_export.push_back(ex);
      continue;
    }
    if (!dir->is_auth()) {
      dout(10) << "EMetaBlob.expire not auth, needed dirv " << lp->second.dirv
	       << " for " << *dir << endl;
      continue;     // not our problem
    }
    if (dir->get_last_committed_version() < lp->second.dirv) {
      dout(10) << "EMetaBlob.expire need dirv " << lp->second.dirv
	       << ", committing " << *dir << endl;
      commit.push_back(dir);
      ncommit++;
    } else {
      dout(10) << "EMetaBlob.expire have dirv " << lp->second.dirv
	       << " on " << *dir << endl;
    }
  }

  // commit
  assert(!commit.empty());

  if (ncommit == 1) {
    mds->mdstore->commit_dir(commit.front(), c);
  } else {
    C_Gather *gather = new C_Gather(c);
    for (list<CDir*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p)
      mds->mdstore->commit_dir(*p, gather->new_sub());
    for (list<CDir*>::iterator p = waitfor_export.begin();
	 p != waitfor_export.end();
	 ++p) 
      mds->mdcache->migrator->add_export_finish_waiter(*p, gather->new_sub());
  }
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
  dout(10) << "EImportMap.replay -- reconstructing import/export spanning tree" << endl;
  assert(mds->mdcache->imports.empty());

  // first, stick the spanning tree in my cache
  metablob.replay(mds);

  // restore import/export maps
  for (set<inodeno_t>::iterator p = imports.begin();
       p != imports.end();
       ++p) {
    CInode *imi = mds->mdcache->get_inode(*p);
    assert(imi);
    CDir *im = imi->get_or_open_dir(mds->mdcache);
    assert(im);

    im->set_dir_auth(mds->get_nodeid());
    mds->mdcache->add_import(im);

    // nested exports
    for (set<inodeno_t>::iterator q = nested_exports[*p].begin();
	 q != nested_exports[*p].end();
	 ++q) {
      CInode *exi = mds->mdcache->get_inode(*q);
      assert(exi);
      CDir *ex = exi->get_or_open_dir(mds->mdcache);
      assert(ex);

      ex->set_dir_auth(CDIR_AUTH_UNKNOWN);
      ex->state_set(CDIR_STATE_EXPORT);
      ex->get(CDir::PIN_EXPORT);
      mds->mdcache->exports.insert(ex);
      mds->mdcache->nested_exports[im].insert(ex);
    }
  }

  mds->mdcache->show_imports();

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




// -----------------------
// EExportStart

bool EExportStart::has_expired(MDS *mds)
{
  CInode *diri = mds->mdcache->get_inode(dirino);
  if (!diri) return true;
  CDir *dir = diri->dir;
  if (!dir) return true;
  if (!mds->mdcache->migrator->is_exporting(dir))
    return true;
  dout(10) << "EExportStart.has_expired still exporting " << *dir << endl;
  return false;
}

void EExportStart::expire(MDS *mds, Context *c)
{
  CInode *diri = mds->mdcache->get_inode(dirino);
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);
  assert(mds->mdcache->migrator->is_exporting(dir));

  dout(10) << "EExportStart.expire waiting for export of " << *dir << endl;
  mds->mdcache->migrator->add_export_finish_waiter(dir, c);
}

void EExportStart::replay(MDS *mds)
{
  dout(10) << "EExportStart.replay " << dirino << " -> " << dest << endl;
  metablob.replay(mds);
  
  // put in pending_exports lists
  CInode *diri = mds->mdcache->get_inode(dirino);
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  mds->mdlog->pending_exports[dirino] = bounds;
}

// -----------------------
// EExportFinish

bool EExportFinish::has_expired(MDS *mds)
{
  // we can always expire.
  return true;
}

void EExportFinish::expire(MDS *mds, Context *c)
{
  assert(0);  // should never happen.
}

void EExportFinish::replay(MDS *mds)
{
  dout(10) << "EExportFinish.replay " << dirino << " success=" << success << endl;

  CInode *diri = mds->mdcache->get_inode(dirino);
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  set<inodeno_t> bounds;
  bounds.swap( mds->mdlog->pending_exports[dirino] );
  mds->mdlog->pending_exports.erase(dirino);

  if (success) {
    // adjust dir_auth
    CDir *im = mds->mdcache->get_auth_container(dir);
    if (dir->get_inode()->authority() == CDIR_AUTH_UNKNOWN) { 
      // was an import, hose it
      assert(im == dir);
      assert(mds->mdcache->imports.count(dir));
      mds->mdcache->imports.erase(dir);
      dir->set_dir_auth( CDIR_AUTH_PARENT );
    } else {
      // i'm now an export
      mds->mdcache->exports.insert(dir);
      mds->mdcache->nested_exports[im].insert(dir);
      dir->set_dir_auth( CDIR_AUTH_UNKNOWN );  // not me
    }
    
    // bounds (there were exports, before)
    for (set<inodeno_t>::iterator p = bounds.begin();
	 p != bounds.end();
	 ++p) {
      CInode *bi = mds->mdcache->get_inode(*p);
      assert(bi);
      CDir *bd = bi->dir;
      assert(bd);

      // hose export
      assert(mds->mdcache->exports.count(bd));
      mds->mdcache->exports.erase(bd);
      mds->mdcache->nested_exports[im].erase(bd);
      
      // fix dir_auth
      assert(bd->get_dir_auth() != CDIR_AUTH_PARENT);
      bd->set_dir_auth( CDIR_AUTH_PARENT );  // not me
    }
  }
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
  for (list<inodeno_t>::iterator p = bounds.begin(); p != bounds.end(); ++p)
    mds->mdcache->my_ambiguous_imports[dirino].insert(*p);
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
  mds->mdcache->my_ambiguous_imports.erase(dirino);

  CInode *diri = mds->mdcache->get_inode(dirino);
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  set<inodeno_t> bounds;
  bounds.swap( mds->mdcache->my_ambiguous_imports[dirino] );
  mds->mdcache->my_ambiguous_imports.erase(dirino);
  
  if (success) {
    // adjust dir_auth
    CDir *im = dir;
    if (dir->get_inode()->authority() == mds->get_nodeid()) {
      // parent is already me.  adding to existing import.
      im = mds->mdcache->get_auth_container(dir);
      mds->mdcache->nested_exports[im].erase(dir);
      dir->set_dir_auth( CDIR_AUTH_PARENT );     
    } else {
      // parent isn't me.  new import.
      mds->mdcache->imports.insert(dir);
      dir->set_dir_auth( mds->get_nodeid() );               
    }

    // bounds (exports, before)
    for (set<inodeno_t>::iterator p = bounds.begin();
	 p != bounds.end();
	 ++p) {
      CInode *bi = mds->mdcache->get_inode(*p);
      assert(bi);
      CDir *bd = bi->dir;
      assert(bd);

      if (bd->get_dir_auth() == mds->get_nodeid()) {
	// still me.  was an import.  move nested exports.
	mds->mdcache->imports.erase(bd);
	for (set<CDir*>::iterator q = mds->mdcache->nested_exports[bd].begin();
	     q != mds->mdcache->nested_exports[bd].end();
	     ++q) 
	  mds->mdcache->nested_exports[im].insert(*q);
	mds->mdcache->nested_exports.erase(bd);	
	bd->set_dir_auth( CDIR_AUTH_PARENT );   
      } else {
	// not me anymore.  now an export.
	mds->mdcache->exports.insert(bd);
	mds->mdcache->nested_exports[im].insert(bd);
	bd->set_dir_auth( CDIR_AUTH_UNKNOWN );
      }
    }
  }
}





