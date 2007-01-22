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
      diri = new CInode(mds->mdcache);
      diri->inode.ino = *lp;
      diri->inode.mode = INODE_MODE_DIR;
      mds->mdcache->add_inode(diri);

      dir = diri->get_or_open_dir(mds->mdcache);

      if (dir->ino() == 1)
	mds->mdcache->set_root(diri);

      dout(10) << "EMetaBlob.replay added dir " << *dir << endl;
    } else {
      dir = diri->get_or_open_dir(mds->mdcache);
      dout(20) << "EMetaBlob.replay had dir " << *dir << endl;
    }
    dir->set_version( lump.dirv-1 );
    dir->mark_dirty( lump.dirv );
    if (lump.is_complete())
      dir->mark_complete();
    
    // decode bits
    lump._decode_bits();

    // full dentry+inode pairs
    for (list<fullbit>::iterator p = lump.dfull.begin();
	 p != lump.dfull.end();
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
    for (list<remotebit>::iterator p = lump.dremote.begin();
	 p != lump.dremote.end();
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
    for (list<nullbit>::iterator p = lump.dnull.begin();
	 p != lump.dnull.end();
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
  assert(imports.empty());

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

      ex->set_dir_auth(mds->get_nodeid() + 1);  // anything that's not me, for now!
      ex->state_set(CDIR_STATE_EXPORT);
      ex->get(CDir::PIN_EXPORT);
      mds->mdcache->exports.insert(ex);
      mds->mdcache->nested_exports[im].insert(ex);
    }
  }

  // twiddle all dir and inode auth bits
  for (hash_map<inodeno_t,CInode*>::iterator p = mds->mdcache->inode_map.begin();
       p != mds->mdcache->inode_map.end();
       ++p) {
    CInode *in = p->second;
    if (in->authority() == mds->get_nodeid())
      in->state_set(CInode::STATE_AUTH);
    else
      in->state_clear(CInode::STATE_AUTH);

    if (in->dir) {
      if (in->dir->authority() == mds->get_nodeid())
	in->dir->state_set(CDIR_STATE_AUTH);
      else
	in->dir->state_clear(CDIR_STATE_AUTH);
    }
  }

  mds->mdcache->show_imports();
  mds->mdcache->show_cache();

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






bool EExportStart::has_expired(MDS *mds)
{
  return true;
}
void EExportStart::expire(MDS *mds, Context *c)
{
}
void EExportStart::replay(MDS *mds)
{
}


bool EExportFinish::has_expired(MDS *mds)
{
  return true;
}
void EExportFinish::expire(MDS *mds, Context *c)
{
}
void EExportFinish::replay(MDS *mds)
{
}


bool EImportStart::has_expired(MDS *mds)
{
  return metablob.has_expired(mds);
}

void EImportStart::expire(MDS *mds, Context *c)
{
  metablob.expire(mds, c);
}

void EImportStart::replay(MDS *mds)
{
}


bool EImportFinish::has_expired(MDS *mds)
{
  return true;
}
void EImportFinish::expire(MDS *mds, Context *c)
{
}
void EImportFinish::replay(MDS *mds)
{
}





