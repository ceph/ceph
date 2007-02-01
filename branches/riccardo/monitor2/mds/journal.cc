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

#include "events/ETrace.h"
#include "events/EMknod.h"
#include "events/EMkdir.h"
#include "events/EInodeUpdate.h"
#include "events/EPurgeFinish.h"
#include "events/EUnlink.h"

#include "MDS.h"
#include "MDCache.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".journal "


// -----------------------
// ETrace

CInode *ETrace::restore_trace(MDS *mds) 
{
  CInode *in = 0;
  for (list<bit>::iterator p = trace.begin();
       p != trace.end();
       ++p) {
    // the dir 
    CInode *diri = mds->mdcache->get_inode(p->dirino);
    if (!diri) {
      dout(10) << "ETrace.restore_trace adding dir " << p->dirino << endl;
      diri = new CInode(mds->mdcache);
      diri->inode.ino = p->dirino;
      diri->inode.mode = INODE_MODE_DIR;
      mds->mdcache->add_inode(diri);

      CDir *dir = diri->get_or_open_dir(mds);

      // root?  import?
      if (p == trace.begin()) {
	mds->mdcache->add_import(dir);
	if (dir->ino() == 1) 
	  mds->mdcache->set_root(diri);
      }
    } else {
      dout(20) << "ETrace.restore_trace had dir " << p->dirino << endl;
      diri->get_or_open_dir(mds);
    }
    assert(diri->dir);
    dout(20) << "ETrace.restore_trace dir is " << *diri->dir << endl;
    
    // the inode
    in = mds->mdcache->get_inode(p->inode.ino);
    if (!in) {
      dout(10) << "ETrace.restore_trace adding dn '" << p->dn << "' inode " << p->inode.ino << endl;
      in = new CInode(mds->mdcache);
      in->inode = p->inode;
      mds->mdcache->add_inode(in);
      
      // the dentry
      CDentry *dn = diri->dir->add_dentry( p->dn, in );
      dn->mark_dirty();
      assert(dn);
    } else {
      dout(20) << "ETrace.restore_trace had dn '" << p->dn << "' inode " << p->inode.ino << endl;
      in->inode = p->inode;
    }
    dout(20) << "ETrace.restore_trace in is " << *in << endl;
  }
  return in;
}


// -----------------------
// EMkdir
// - trace goes to new dir's inode.

bool EMkdir::can_expire(MDS *mds) 
{
  // am i obsolete?
  CInode *in = mds->mdcache->get_inode( trace.back().inode.ino );
  if (!in) return true;
  CDir *dir = in->dir;
  if (!dir) return true;
  CDir *pdir = in->get_parent_dir();
  assert(pdir);
  
  dout(10) << "EMkdir.can_expire  in is " << *in << endl;
  dout(10) << "EMkdir.can_expire inv is " << trace.back().inode.version << endl;
  dout(10) << "EMkdir.can_expire dir is " << *dir << endl;
  bool commitparent = in->get_last_committed_version() < trace.back().inode.version;
  bool commitnew = dir->get_last_committed_version() == 0;

  if (commitparent || commitnew) return false;
  return true;
}

void EMkdir::retire(MDS *mds, Context *c) 
{
  // commit parent dir AND my dir
  CInode *in = mds->mdcache->get_inode( trace.back().inode.ino );
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  CDir *pdir = in->get_parent_dir();
  assert(pdir);
  
  dout(10) << "EMkdir.retire  in is " << *in << endl;
  dout(10) << "EMkdir.retire inv is " << trace.back().inode.version << endl;
  dout(10) << "EMkdir.retire dir is " << *dir << endl;
  bool commitparent = in->get_last_committed_version() < trace.back().inode.version;
  bool commitnew = dir->get_last_committed_version() == 0;
  
  if (commitparent && commitnew) {
    // both
    dout(10) << "EMkdir.retire committing parent+new dir " << *dir << endl;
    C_Gather *gather = new C_Gather(c);
    mds->mdstore->commit_dir(pdir, gather->new_sub());
    mds->mdstore->commit_dir(dir, gather->new_sub());
  } else if (commitparent) {
    // just parent
    dout(10) << "EMkdir.retire committing parent dir " << *dir << endl;
    mds->mdstore->commit_dir(pdir, c);
  } else {
    // just new dir
    dout(10) << "EMkdir.retire committing new dir " << *dir << endl;
    mds->mdstore->commit_dir(dir, c);
  }
}

bool EMkdir::has_happened(MDS *mds) 
{
  return false;     
}
  
void EMkdir::replay(MDS *mds) 
{
  dout(10) << "EMkdir.replay " << *this << endl;
  CInode *in = trace.restore_trace(mds);

  // mark dir inode dirty
  in->mark_dirty();

  // mark parent dir dirty, and set version.  
  // this may end up being below water when dir is fetched from disk.
  CDir *pdir = in->get_parent_dir();
  if (!pdir->is_dirty()) pdir->mark_dirty();
  pdir->set_version(trace.back().dirv);
 
  // mark new dir dirty + complete
  CDir *dir = in->get_or_open_dir(mds);
  dir->mark_dirty();
  dir->mark_complete();
}



// -----------------------
// EMknod
  
bool EMknod::can_expire(MDS *mds) 
{
  // am i obsolete?
  CInode *in = mds->mdcache->get_inode( trace.back().inode.ino );
  if (!in) return true;

  if (!in->is_auth()) return true;  // not my inode anymore!
  if (in->get_version() != trace.back().inode.version)
    return true;  // i'm obsolete!  (another log entry follows)

  if (in->get_last_committed_version() >= trace.back().inode.version)
    return true;

  return false;
}

void EMknod::retire(MDS *mds, Context *c) 
{
  // commit parent directory
  CInode *diri = mds->mdcache->get_inode( trace.back().dirino );
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  dout(10) << "EMknod.retire committing parent dir " << *dir << endl;
  mds->mdstore->commit_dir(dir, c);
}

bool EMknod::has_happened(MDS *mds) 
{
  return false;
}
  
void EMknod::replay(MDS *mds) 
{
  dout(10) << "EMknod.replay " << *this << endl;
  CInode *in = trace.restore_trace(mds);
  in->mark_dirty();

  // mark parent dir dirty, and set version.  
  // this may end up being below water when dir is fetched from disk.
  CDir *pdir = in->get_parent_dir();
  if (!pdir->is_dirty()) pdir->mark_dirty();
  pdir->set_version(trace.back().dirv);
}



// -----------------------
// EInodeUpdate

bool EInodeUpdate::can_expire(MDS *mds) 
{
  CInode *in = mds->mdcache->get_inode( trace.back().inode.ino );
  if (!in) return true;

  if (!in->is_auth()) return true;  // not my inode anymore!
  if (in->get_version() != trace.back().inode.version)
    return true;  // i'm obsolete!  (another log entry follows)

  /*
  // frozen -> exporting -> obsolete    (FOR NOW?)
  if (in->is_frozen())
  return true; 
  */

  if (in->get_last_committed_version() >= trace.back().inode.version)
    return true;

  return false;
}

void EInodeUpdate::retire(MDS *mds, Context *c) 
{
   // commit parent directory
  CInode *diri = mds->mdcache->get_inode( trace.back().dirino );
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);

  dout(10) << "EMknod.retire committing parent dir " << *dir << endl;
  mds->mdstore->commit_dir(dir, c);
}
  
bool EInodeUpdate::has_happened(MDS *mds)
{
  return false;
}

void EInodeUpdate::replay(MDS *mds) 
{
  dout(10) << "EInodeUpdate.replay " << *this << endl;
  CInode *in = trace.restore_trace(mds);
  in->mark_dirty();

  // mark parent dir dirty, and set version.  
  // this may end up being below water when dir is fetched from disk.
  CDir *pdir = in->get_parent_dir();
  if (!pdir->is_dirty()) pdir->mark_dirty();
  pdir->set_version(trace.back().dirv);
}



// -----------------------
// EUnlink

bool EUnlink::can_expire(MDS *mds)
{
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

  return true;
}

void EUnlink::retire(MDS *mds, Context *c)
{
  CInode *diri = mds->mdcache->get_inode( diritrace.back().inode.ino );
  CDir *dir = diri->dir;
  assert(dir);
  
  // okay!
  dout(7) << "commiting dirty (from unlink) dir " << *dir << endl;
  mds->mdstore->commit_dir(dir, dirv, c);
}

bool EUnlink::has_happened(MDS *mds)
{
  return true;
}

void EUnlink::replay(MDS *mds)
{
}




// -----------------------
// EPurgeFinish


bool EPurgeFinish::can_expire(MDS *mds)
{
  return true;
}

void EPurgeFinish::retire(MDS *mds, Context *c)
{
}

bool EPurgeFinish::has_happened(MDS *mds)
{
  return true;
}

void EPurgeFinish::replay(MDS *mds)
{
}




