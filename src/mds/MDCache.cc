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



#include "MDCache.h"
#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "Migrator.h"

#include "AnchorClient.h"
#include "SnapClient.h"

#include "MDSMap.h"

#include "CInode.h"
#include "CDir.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "common/Logger.h"
#include "common/MemoryModel.h"

#include "osdc/Filer.h"

#include "events/ESubtreeMap.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EString.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"
#include "events/ECommitted.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSCacheRejoin.h"

#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"
#include "messages/MDentryLink.h"
#include "messages/MDentryUnlink.h"

#include "messages/MClientRequest.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MMDSFragmentNotify.h"


#include "InoTable.h"

#include "common/Timer.h"

#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

#include "config.h"

#define DOUT_SUBSYS mds
#undef dout_prefix
#define dout_prefix _prefix(mds)
static ostream& _prefix(MDS *mds) {
  return *_dout << dbeginl << "mds" << mds->get_nodeid() << ".cache ";
}

long g_num_ino = 0;
long g_num_dir = 0;
long g_num_dn = 0;
long g_num_cap = 0;

long g_num_inoa = 0;
long g_num_dira = 0;
long g_num_dna = 0;
long g_num_capa = 0;

long g_num_inos = 0;
long g_num_dirs = 0;
long g_num_dns = 0;
long g_num_caps = 0;

set<int> SimpleLock::empty_gather_set;


MDCache::MDCache(MDS *m)
{
  mds = m;
  migrator = new Migrator(mds, this);
  root = NULL;
  myin = NULL;
  stray = NULL;

  num_inodes_with_caps = 0;
  num_caps = 0;

  discover_last_tid = 0;

  last_cap_id = 0;

  client_lease_durations[0] = 5.0;
  client_lease_durations[1] = 30.0;
  client_lease_durations[2] = 300.0;

  opening_root = open = false;
  lru.lru_set_max(g_conf.mds_cache_size);
  lru.lru_set_midpoint(g_conf.mds_cache_mid);

  decayrate.set_halflife(g_conf.mds_decay_halflife);

  did_shutdown_log_cap = false;
}

MDCache::~MDCache() 
{
  delete migrator;
  //delete renamer;
}



void MDCache::log_stat()
{
  mds->logger->set(l_mds_imax, g_conf.mds_cache_size);
  mds->logger->set(l_mds_i, lru.lru_get_size());
  mds->logger->set(l_mds_ipin, lru.lru_get_num_pinned());
  mds->logger->set(l_mds_itop, lru.lru_get_top());
  mds->logger->set(l_mds_ibot, lru.lru_get_bot());
  mds->logger->set(l_mds_iptail, lru.lru_get_pintail());
  mds->logger->set(l_mds_icap, num_inodes_with_caps);
  mds->logger->set(l_mds_cap, num_caps);
}


// 

bool MDCache::shutdown()
{
  if (lru.lru_get_size() > 0) {
    dout(7) << "WARNING: mdcache shutdown with non-empty cache" << dendl;
    //show_cache();
    show_subtrees();
    //dump();
  }
  return true;
}


// ====================================================================
// some inode functions

void MDCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  assert(inode_map.count(in->vino()) == 0);  // should be no dup inos!
  inode_map[ in->vino() ] = in;

  if (in->ino() < MDS_INO_SYSTEM_BASE) {
    if (in->ino() == MDS_INO_ROOT)
      root = in;
    else if (in->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
      myin = in;
    else if (in->ino() == MDS_INO_STRAY(mds->get_nodeid()))
      stray = in;
    if (in->is_base())
      base_inodes.insert(in);
  }
}

void MDCache::remove_inode(CInode *o) 
{ 
  dout(14) << "remove_inode " << *o << dendl;

  if (o->get_parent_dn()) {
    // FIXME: multiple parents?
    CDentry *dn = o->get_parent_dn();
    assert(!dn->is_dirty());
    dn->dir->unlink_inode(dn);   // leave dentry ... FIXME?
  }

  if (o->is_dirty())
    o->mark_clean();

  o->filelock.clear_dirty();
  o->nestlock.clear_dirty();
  o->dirfragtreelock.clear_dirty();

  o->item_open_file.remove_myself();

  // remove from inode map
  inode_map.erase(o->vino());    

  if (o->ino() < MDS_INO_SYSTEM_BASE) {
    if (o == root) root = 0;
    if (o == myin) myin = 0;
    if (o == stray) stray = 0;
    if (o->is_base())
      base_inodes.erase(o);
  }

  // delete it
  assert(o->get_num_ref() == 0);
  delete o; 
}



void MDCache::init_layouts()
{
  default_file_layout = g_default_file_layout;
  default_file_layout.fl_pg_preferred = -1;
  default_file_layout.fl_pg_pool = mds->mdsmap->get_data_pg_pool();

  default_log_layout = g_default_file_layout;
  default_log_layout.fl_pg_preferred = -1;
  default_log_layout.fl_pg_pool = mds->mdsmap->get_metadata_pg_pool();
}

CInode *MDCache::create_system_inode(inodeno_t ino, int mode)
{
  dout(0) << "creating system inode with ino:" << ino << dendl;
  CInode *in = new CInode(this);
  in->inode.ino = ino;
  in->inode.version = 1;
  in->inode.mode = 0500 | mode;
  in->inode.size = 0;
  in->inode.ctime = 
    in->inode.mtime = g_clock.now();
  in->inode.nlink = 1;
  in->inode.truncate_size = -1ull;

  memset(&in->inode.dir_layout, 0, sizeof(in->inode.dir_layout));
  if (in->inode.is_dir()) {
    memset(&in->inode.layout, 0, sizeof(in->inode.layout));
    in->inode.dir_layout.dl_dir_hash = g_conf.mds_default_dir_hash;
  } else {
    in->inode.layout = default_file_layout;
  }

  if (in->is_base()) {
    if (in->is_root())
      in->inode_auth = pair<int,int>(mds->whoami, CDIR_AUTH_UNKNOWN);
    else
      in->inode_auth = pair<int,int>(in->ino() - MDS_INO_MDSDIR_OFFSET, CDIR_AUTH_UNKNOWN);
    in->open_snaprealm();  // empty snaprealm
    in->snaprealm->srnode.seq = 1;
  }
  
  add_inode(in);
  return in;
}

CInode *MDCache::create_root_inode()
{
  CInode *i = create_system_inode(MDS_INO_ROOT, S_IFDIR|0755);
  i->default_layout = new struct default_file_layout;
  i->default_layout->layout = default_file_layout;
  i->default_layout->layout.fl_pg_pool = mds->mdsmap->get_data_pg_pool();
  return i;
}

void MDCache::create_empty_hierarchy(C_Gather *gather)
{
  // create root dir
  CInode *root = create_root_inode();

  // force empty root dir
  CDir *rootdir = root->get_or_open_dirfrag(this, frag_t());
  adjust_subtree_auth(rootdir, mds->whoami);   
  rootdir->dir_rep = CDir::REP_ALL;   //NONE;

  // create ceph dir
  CInode *ceph = create_system_inode(MDS_INO_CEPH, S_IFDIR);
  rootdir->add_primary_dentry(".ceph", ceph);

  CDir *cephdir = ceph->get_or_open_dirfrag(this, frag_t());
  cephdir->dir_rep = CDir::REP_ALL;   //NONE;

  ceph->inode.dirstat = cephdir->fnode.fragstat;
  ceph->inode.rstat = cephdir->fnode.rstat;
  ceph->inode.accounted_rstat = ceph->inode.rstat;

  rootdir->fnode.fragstat.nsubdirs = 1;
  rootdir->fnode.rstat = ceph->inode.rstat;
  rootdir->fnode.rstat.rsubdirs++;
  rootdir->fnode.accounted_fragstat = rootdir->fnode.fragstat;
  rootdir->fnode.accounted_rstat = rootdir->fnode.rstat;

  root->inode.dirstat = rootdir->fnode.fragstat;
  root->inode.rstat = rootdir->fnode.rstat;
  root->inode.accounted_rstat = root->inode.rstat;

  cephdir->mark_complete();
  cephdir->mark_dirty(cephdir->pre_dirty(), mds->mdlog->get_current_segment());
  cephdir->commit(0, gather->new_sub());

  rootdir->mark_complete();
  rootdir->mark_dirty(rootdir->pre_dirty(), mds->mdlog->get_current_segment());
  rootdir->commit(0, gather->new_sub());

  root->store(gather->new_sub());
}

void MDCache::create_mydir_hierarchy(C_Gather *gather)
{
  // create mds dir
  char myname[10];
  snprintf(myname, sizeof(myname), "mds%d", mds->whoami);
  CInode *my = create_system_inode(MDS_INO_MDSDIR(mds->whoami), S_IFDIR);
  //cephdir->add_remote_dentry(myname, MDS_INO_MDSDIR(mds->whoami), S_IFDIR);

  CDir *mydir = my->get_or_open_dirfrag(this, frag_t());
  adjust_subtree_auth(mydir, mds->whoami);   

  // stray dir
  CInode *stray = create_system_inode(MDS_INO_STRAY(mds->whoami), S_IFDIR);
  CDir *straydir = stray->get_or_open_dirfrag(this, frag_t());
  string name("stray");
  mydir->add_primary_dentry(name, stray);
  stray->inode.dirstat = straydir->fnode.fragstat;
  stray->inode.accounted_rstat = stray->inode.rstat;

  CInode *journal = create_system_inode(MDS_INO_LOG_OFFSET + mds->whoami, S_IFREG);
  name = "journal";
  mydir->add_primary_dentry(name, journal);

  mydir->fnode.fragstat.nsubdirs = 1;
  mydir->fnode.fragstat.nfiles = 1;
  mydir->fnode.rstat = stray->inode.rstat;
  mydir->fnode.rstat.rsubdirs++;
  mydir->fnode.rstat.rfiles++;
  mydir->fnode.accounted_fragstat = mydir->fnode.fragstat;
  mydir->fnode.accounted_rstat = mydir->fnode.rstat;

  myin->inode.dirstat = mydir->fnode.fragstat;
  myin->inode.rstat = mydir->fnode.rstat;
  myin->inode.accounted_rstat = myin->inode.rstat;

  // save them
  straydir->mark_complete();
  straydir->mark_dirty(straydir->pre_dirty(), mds->mdlog->get_current_segment());
  straydir->commit(0, gather->new_sub());

  mydir->mark_complete();
  mydir->mark_dirty(mydir->pre_dirty(), mds->mdlog->get_current_segment());
  mydir->commit(0, gather->new_sub());

  myin->store(gather->new_sub());
}

struct C_MDC_CreateSystemFile : public Context {
  MDCache *cache;
  Mutation *mut;
  CDentry *dn;
  Context *fin;
  C_MDC_CreateSystemFile(MDCache *c, Mutation *mu, CDentry *d, Context *f) : cache(c), mut(mu), dn(d), fin(f) {}
  void finish(int r) {
    cache->_create_system_file_finish(mut, dn, fin);
  }
};

void MDCache::_create_system_file(CDir *dir, const char *name, CInode *in, Context *fin)
{
  dout(10) << "_create_system_file " << name << " in " << *dir << dendl;
  CDentry *dn = dir->add_null_dentry(name);

  dn->push_projected_linkage(in);

  CDir *mdir = 0;
  if (in->inode.is_dir()) {
    in->inode.rstat.rsubdirs = 1;

    mdir = in->get_or_open_dirfrag(this, frag_t());
    mdir->mark_complete();
    mdir->pre_dirty();
  } else
    in->inode.rstat.rfiles = 1;
  in->inode.version = dn->pre_dirty();
  
  SnapRealm *realm = dir->get_inode()->find_snaprealm();
  dn->first = in->first = realm->get_newest_seq() + 1;

  Mutation *mut = new Mutation;

  // force some locks.  hacky.
  mds->locker->wrlock_force(&dir->inode->filelock, mut);
  mds->locker->wrlock_force(&dir->inode->nestlock, mut);

  mut->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, "create system file");
  mds->mdlog->start_entry(le);

  if (!in->is_mdsdir()) {
    predirty_journal_parents(mut, &le->metablob, in, dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
    le->metablob.add_primary_dentry(dn, true, in);
  } else {
    predirty_journal_parents(mut, &le->metablob, in, dir, PREDIRTY_DIR, 1);
    journal_dirty_inode(mut, &le->metablob, in);
    dn->push_projected_linkage(in->ino(), in->d_type());
    le->metablob.add_remote_dentry(dn, true, in->ino(), in->d_type());
    le->metablob.add_root(true, in);
  }
  if (mdir)
    le->metablob.add_dir(mdir, true, true, true); // dirty AND complete AND new

  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(new C_MDC_CreateSystemFile(this, mut, dn, fin));
  mds->mdlog->flush();
}

void MDCache::_create_system_file_finish(Mutation *mut, CDentry *dn, Context *fin)
{
  dout(10) << "_create_system_file_finish " << *dn << dendl;
  
  dn->pop_projected_linkage();

  CInode *in = dn->get_linkage()->get_inode();
  in->inode.version--;
  in->mark_dirty(in->inode.version + 1, mut->ls);

  CDir *dir = 0;
  if (in->inode.is_dir()) {
    dir = in->get_dirfrag(frag_t());
    dir->mark_dirty(1, mut->ls);
    dir->mark_new(mut->ls);
  }

  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  fin->finish(0);
  delete fin;

  //if (dir && MDS_INO_IS_MDSDIR(in->ino()))
  //migrator->export_dir(dir, (int)in->ino() - MDS_INO_MDSDIR_OFFSET);
}



struct C_MDS_RetryOpenRoot : public Context {
  MDCache *cache;
  C_MDS_RetryOpenRoot(MDCache *c) : cache(c) {}
  void finish(int r) {
    if (r < 0)
      cache->mds->suicide();
    else
      cache->open_root();
  }
};

void MDCache::open_root_inode(Context *c)
{
  CInode *in;
  if (mds->whoami == mds->mdsmap->get_root()) {
    in = create_system_inode(MDS_INO_ROOT, S_IFDIR|0755);  // initially inaccurate!
    in->fetch(c);
  } else {
    discover_base_ino(MDS_INO_ROOT, c, mds->mdsmap->get_root());
  }
}

void MDCache::open_root()
{
  dout(10) << "open_root" << dendl;

  if (!root) {
    open_root_inode(new C_MDS_RetryOpenRoot(this));
    return;
  }
  if (mds->whoami == mds->mdsmap->get_root()) {
    assert(root->is_auth());  
    CDir *rootdir = root->get_or_open_dirfrag(this, frag_t());
    assert(rootdir);
    if (!rootdir->is_subtree_root())
      adjust_subtree_auth(rootdir, mds->whoami);   
    if (!rootdir->is_complete()) {
      rootdir->fetch(new C_MDS_RetryOpenRoot(this));
      return;
    }
  } else {
    assert(!root->is_auth());
    CDir *rootdir = root->get_dirfrag(frag_t());
    if (!rootdir) {
      discover_dir_frag(root, frag_t(), new C_MDS_RetryOpenRoot(this));
      return;
    }    
  }

  if (!myin) {
    CInode *in = create_system_inode(MDS_INO_MDSDIR(mds->whoami), S_IFDIR|0755);  // initially inaccurate!
    in->fetch(new C_MDS_RetryOpenRoot(this));
    return;
  }
  CDir *mydir = myin->get_or_open_dirfrag(this, frag_t());
  assert(mydir);

  populate_mydir();
}

void MDCache::populate_mydir()
{
  assert(myin);
  CDir *mydir = myin->get_dirfrag(frag_t());
  assert(mydir);

  dout(10) << "populate_mydir " << *mydir << dendl;

  if (!mydir->is_complete()) {
    mydir->fetch(new C_MDS_RetryOpenRoot(this));
    return;
  }    

  // open or create stray
  string strayname("stray");
  CDentry *straydn = mydir->lookup(strayname);
  if (!straydn || !straydn->get_linkage()->get_inode()) {
    _create_system_file(mydir, strayname.c_str(), create_system_inode(MDS_INO_STRAY(mds->whoami), S_IFDIR),
			new C_MDS_RetryOpenRoot(this));
    return;
  }
  assert(straydn);
  assert(stray);

  // open or create journal file
  string jname("journal");
  CDentry *jdn = mydir->lookup(jname);
  if (!jdn || !jdn->get_linkage()->get_inode()) {
    _create_system_file(mydir, jname.c_str(), create_system_inode(MDS_INO_LOG_OFFSET + mds->whoami, S_IFREG),
			new C_MDS_RetryOpenRoot(this));
    return;
  }

  // okay!
  dout(10) << "populate_mydir done" << dendl;
  assert(!open);    
  open = true;
  stray->get(CInode::PIN_STRAY);
  dout(20) << " stray is " << *stray << dendl;

  mds->queue_waiters(waiting_for_open);
}

void MDCache::open_foreign_mdsdir(inodeno_t ino, Context *fin)
{
  discover_base_ino(ino, fin, ino & (MAX_MDS-1));
}

CDentry *MDCache::get_or_create_stray_dentry(CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);
  
  assert(stray);

  frag_t fg = stray->pick_dirfrag(straydname);

  CDir *straydir = stray->get_or_open_dirfrag(this, fg);
  
  CDentry *straydn = straydir->lookup(straydname);
  if (!straydn) {
    straydn = straydir->add_null_dentry(straydname);
    straydn->mark_new();
  } else 
    assert(straydn->get_projected_linkage()->is_null());

  return straydn;
}



MDSCacheObject *MDCache::get_object(MDSCacheObjectInfo &info) 
{
  // inode?
  if (info.ino) 
    return get_inode(info.ino, info.snapid);

  // dir or dentry.
  CDir *dir = get_dirfrag(info.dirfrag);
  if (!dir) return 0;
    
  if (info.dname.length()) 
    return dir->lookup(info.dname, info.snapid);
  else
    return dir;
}




// ====================================================================
// subtree management

void MDCache::list_subtrees(list<CDir*>& ls)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p)
    ls.push_back(p->first);
}

/*
 * adjust the dir_auth of a subtree.
 * merge with parent and/or child subtrees, if is it appropriate.
 * merge can ONLY happen if both parent and child have unambiguous auth.
 */
void MDCache::adjust_subtree_auth(CDir *dir, pair<int,int> auth, bool do_eval)
{
  dout(7) << "adjust_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir << dendl;

  show_subtrees();

  CDir *root;
  if (dir->inode->is_base()) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) {
      subtrees[root].clear();
      root->get(CDir::PIN_SUBTREE);
    }
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  assert(root);
  assert(subtrees.count(root));
  dout(7) << " current root is " << *root << dendl;

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << dendl;
    assert(subtrees.count(dir) == 0);
    subtrees[dir].clear();      // create empty subtree bounds list for me.
    dir->get(CDir::PIN_SUBTREE);

    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      next++;
      if (get_subtree_root((*p)->get_parent_dir()) == dir) {
	// move under me
	dout(10) << "  claiming child bound " << **p << dendl;
	subtrees[dir].insert(*p); 
	subtrees[root].erase(p);
      }
      p = next;
    }
    
    // i am a bound of the parent subtree.
    subtrees[root].insert(dir); 

    // i am now the subtree root.
    root = dir;

    // adjust recursive pop counters
    if (dir->is_auth()) {
      utime_t now = g_clock.now();
      CDir *p = dir->get_parent_dir();
      while (p) {
	p->pop_auth_subtree.sub(now, decayrate, dir->pop_auth_subtree);
	if (p->is_subtree_root()) break;
	p = p->inode->get_parent_dir();
      }
    }

    if (do_eval)
      eval_subtree_root(dir->get_inode());
  }

  show_subtrees();
}


void MDCache::try_subtree_merge(CDir *dir)
{
  dout(7) << "try_subtree_merge " << *dir << dendl;
  assert(subtrees.count(dir));
  set<CDir*> oldbounds = subtrees[dir];

  // try merge at my root
  try_subtree_merge_at(dir);

  // try merge at my old bounds
  for (set<CDir*>::iterator p = oldbounds.begin();
       p != oldbounds.end();
       ++p) 
    try_subtree_merge_at(*p);
}

class C_MDC_SubtreeMergeWB : public Context {
  MDCache *mdcache;
  CInode *in;
  Mutation *mut;
public:
  C_MDC_SubtreeMergeWB(MDCache *mdc, CInode *i, Mutation *m) : mdcache(mdc), in(i), mut(m) {}
  void finish(int r) { 
    mdcache->subtree_merge_writebehind_finish(in, mut);
  }
};

void MDCache::try_subtree_merge_at(CDir *dir)
{
  dout(10) << "try_subtree_merge_at " << *dir << dendl;
  assert(subtrees.count(dir));

  // merge with parent?
  CDir *parent = dir;  
  if (!dir->inode->is_base())
    parent = get_subtree_root(dir->get_parent_dir());
  
  if (parent != dir &&                              // we have a parent,
      parent->dir_auth == dir->dir_auth &&          // auth matches,
      dir->dir_auth.second == CDIR_AUTH_UNKNOWN &&  // auth is unambiguous,
      !dir->state_test(CDir::STATE_EXPORTBOUND)) {  // not an exportbound,
    // merge with parent.
    dout(10) << "  subtree merge at " << *dir << dendl;
    dir->set_dir_auth(CDIR_AUTH_DEFAULT);
    
    // move our bounds under the parent
    for (set<CDir*>::iterator p = subtrees[dir].begin();
	 p != subtrees[dir].end();
	 ++p) 
      subtrees[parent].insert(*p);
    
    // we are no longer a subtree or bound
    dir->put(CDir::PIN_SUBTREE);
    subtrees.erase(dir);
    subtrees[parent].erase(dir);

    // adjust popularity?
    if (dir->is_auth()) {
      utime_t now = g_clock.now();
      CDir *p = dir->get_parent_dir();
      while (p) {
	p->pop_auth_subtree.add(now, decayrate, dir->pop_auth_subtree);
	if (p->is_subtree_root()) break;
	p = p->inode->get_parent_dir();
      }
    }

    eval_subtree_root(dir->get_inode());

    // journal inode? 
    //  (this is a large hammer to ensure that dirfragtree updates will
    //   hit the disk before the relevant dirfrags ever close)
    if (dir->inode->is_auth() &&
	dir->inode->can_auth_pin() &&
	(mds->is_clientreplay() || mds->is_active() || mds->is_stopping())) {
      CInode *in = dir->inode;
      dout(10) << "try_subtree_merge_at journaling merged bound " << *in << dendl;
      
      in->auth_pin(this);

      // journal write-behind.
      inode_t *pi = in->project_inode();
      pi->version = in->pre_dirty();
      
      Mutation *mut = new Mutation;
      mut->ls = mds->mdlog->get_current_segment();
      EUpdate *le = new EUpdate(mds->mdlog, "subtree merge writebehind");
      mds->mdlog->start_entry(le);

      le->metablob.add_dir_context(in->get_parent_dn()->get_dir());
      journal_dirty_inode(mut, &le->metablob, in);
      
      mds->mdlog->submit_entry(le);
      mds->mdlog->wait_for_sync(new C_MDC_SubtreeMergeWB(this, in, mut));
      mds->mdlog->flush();
    }
  } 

  show_subtrees(15);
}

void MDCache::subtree_merge_writebehind_finish(CInode *in, Mutation *mut)
{
  dout(10) << "subtree_merge_writebehind_finish on " << in << dendl;
  in->pop_and_dirty_projected_inode(mut->ls);

  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  in->auth_unpin(this);
}

void MDCache::eval_subtree_root(CInode *diri)
{
  // evaluate subtree inode filelock?
  //  (we should scatter the filelock on subtree bounds)
  if (diri->is_auth())
    mds->locker->try_eval(diri, CEPH_LOCK_IFILE | CEPH_LOCK_INEST);
}


void MDCache::adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, pair<int,int> auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir
	  << " bounds " << bounds
	  << dendl;

  show_subtrees();

  CDir *root;
  if (dir->ino() == MDS_INO_ROOT) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) {
      subtrees[root].clear();
      root->get(CDir::PIN_SUBTREE);
    }
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  assert(root);
  assert(subtrees.count(root));
  dout(7) << " current root is " << *root << dendl;

  pair<int,int> oldauth = dir->authority();

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << dendl;
    assert(subtrees.count(dir) == 0);
    subtrees[dir].clear();      // create empty subtree bounds list for me.
    dir->get(CDir::PIN_SUBTREE);
    
    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      next++;
      if (get_subtree_root((*p)->get_parent_dir()) == dir) {
	// move under me
	dout(10) << "  claiming child bound " << **p << dendl;
	subtrees[dir].insert(*p); 
	subtrees[root].erase(p);
      }
      p = next;
    }
    
    // i am a bound of the parent subtree.
    subtrees[root].insert(dir); 

    // i am now the subtree root.
    root = dir;
  }

  // verify/adjust bounds.
  // - these may be new, or
  // - beneath existing ambiguous bounds (which will be collapsed),
  // - but NOT beneath unambiguous bounds.
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bound = *p;
    
    // new bound?
    if (subtrees[dir].count(bound) == 0) {
      if (get_subtree_root(bound) == dir) {
	dout(10) << "  new bound " << *bound << ", adjusting auth back to old " << oldauth << dendl;
	adjust_subtree_auth(bound, oldauth);       // otherwise, adjust at bound.
      }
      else {
	dout(10) << "  want bound " << *bound << dendl;
	// make sure it's nested beneath ambiguous subtree(s)
	while (1) {
	  CDir *t = get_subtree_root(bound->get_parent_dir());
	  if (t == dir) break;
	  while (subtrees[dir].count(t) == 0)
	    t = get_subtree_root(t->get_parent_dir());
	  dout(10) << "  swallowing intervening subtree at " << *t << dendl;
	  adjust_subtree_auth(t, auth);
	  try_subtree_merge_at(t);
	}
      }
    }
    else {
      dout(10) << "  already have bound " << *bound << dendl;
    }
  }
  // merge stray bounds?
  set<CDir*>::iterator p = subtrees[dir].begin();
  while (p != subtrees[dir].end()) {
    set<CDir*>::iterator n = p;
    n++;
    if (bounds.count(*p) == 0) {
      CDir *stray = *p;
      dout(10) << "  swallowing extra subtree at " << *stray << dendl;
      adjust_subtree_auth(stray, auth);
      try_subtree_merge_at(stray);
    }
    p = n;
  }

  // bound should now match.
  verify_subtree_bounds(dir, bounds);

  show_subtrees();
}


void MDCache::adjust_bounded_subtree_auth(CDir *dir, vector<dirfrag_t>& bound_dfs, pair<int,int> auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir 
	  << " bound_dfs " << bound_dfs
	  << dendl;
  
  // make bounds list
  set<CDir*> bounds;
  for (vector<dirfrag_t>::iterator p = bound_dfs.begin();
       p != bound_dfs.end();
       ++p) {
    CDir *bd = get_dirfrag(*p);
    if (bd) 
      bounds.insert(bd);
  }
  
  adjust_bounded_subtree_auth(dir, bounds, auth);
}

void MDCache::map_dirfrag_set(list<dirfrag_t>& dfs, set<CDir*>& result)
{
  // group by inode
  map<inodeno_t, fragset_t> ino_fragset;
  for (list<dirfrag_t>::iterator p = dfs.begin(); p != dfs.end(); ++p)
    ino_fragset[p->ino].insert(p->frag);

  // get frags
  for (map<inodeno_t, fragset_t>::iterator p = ino_fragset.begin();
       p != ino_fragset.end();
       ++p) {
    CInode *in = get_inode(p->first);
    if (!in) continue;

    list<frag_t> fglist;
    for (set<frag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
      in->dirfragtree.get_leaves_under(*q, fglist);

    dout(15) << "map_dirfrag_set " << p->second << " -> " << fglist
	     << " on " << *in << dendl;

    for (list<frag_t>::iterator q = fglist.begin(); q != fglist.end(); ++q) {
      CDir *dir = in->get_dirfrag(*q);
      if (dir) result.insert(dir);
    }
  }
}



CDir *MDCache::get_subtree_root(CDir *dir)
{
  // find the underlying dir that delegates (or is about to delegate) auth
  while (true) {
    if (dir->is_subtree_root()) 
      return dir;
    dir = dir->get_inode()->get_parent_dir();
    if (!dir) 
      return 0;             // none
  }
}

void MDCache::remove_subtree(CDir *dir)
{
  dout(10) << "remove_subtree " << *dir << dendl;
  assert(subtrees.count(dir));
  assert(subtrees[dir].empty());
  subtrees.erase(dir);
  dir->put(CDir::PIN_SUBTREE);
  if (dir->get_parent_dir()) {
    CDir *p = get_subtree_root(dir->get_parent_dir());
    assert(subtrees[p].count(dir));
    subtrees[p].erase(dir);
  }
}

void MDCache::get_subtree_bounds(CDir *dir, set<CDir*>& bounds)
{
  assert(subtrees.count(dir));
  bounds = subtrees[dir];
}

void MDCache::get_wouldbe_subtree_bounds(CDir *dir, set<CDir*>& bounds)
{
  if (subtrees.count(dir)) {
    // just copy them, dir is a subtree.
    get_subtree_bounds(dir, bounds);
  } else {
    // find them
    CDir *root = get_subtree_root(dir);
    for (set<CDir*>::iterator p = subtrees[root].begin();
	 p != subtrees[root].end();
	 ++p) {
      CDir *t = *p;
      while (t != root) {
	t = t->get_parent_dir();
	assert(t);
	if (t == dir) {
	  bounds.insert(*p);
	  continue;
	}
      }
    }
  }
}

void MDCache::verify_subtree_bounds(CDir *dir, const set<CDir*>& bounds)
{
  // for debugging only.
  assert(subtrees.count(dir));
  if (bounds != subtrees[dir]) {
    dout(0) << "verify_subtree_bounds failed" << dendl;
    set<CDir*> b = bounds;
    for (set<CDir*>::iterator p = subtrees[dir].begin();
	 p != subtrees[dir].end();
	 ++p) {
      if (bounds.count(*p)) {
	b.erase(*p);
	continue;
      }
      dout(0) << "  missing bound " << **p << dendl;
    }
    for (set<CDir*>::iterator p = b.begin();
	 p != b.end();
	 ++p) 
      dout(0) << "    extra bound " << **p << dendl;
  }
  assert(bounds == subtrees[dir]);
}

void MDCache::verify_subtree_bounds(CDir *dir, const list<dirfrag_t>& bounds)
{
  // for debugging only.
  assert(subtrees.count(dir));

  // make sure that any bounds i do have are properly noted as such.
  int failed = 0;
  for (list<dirfrag_t>::const_iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CDir *bd = get_dirfrag(*p);
    if (!bd) continue;
    if (subtrees[dir].count(bd) == 0) {
      dout(0) << "verify_subtree_bounds failed: extra bound " << *bd << dendl;
      failed++;
    }
  }
  assert(failed == 0);
}

void MDCache::adjust_subtree_after_rename(CInode *diri, CDir *olddir)
{
  dout(10) << "adjust_subtree_after_rename " << *diri << " from " << *olddir << dendl;

  //show_subtrees();

  // adjust subtree
  list<CDir*> dfls;
  diri->get_dirfrags(dfls);
  for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) {
    CDir *dir = *p;

    dout(10) << "dirfrag " << *dir << dendl;
    CDir *oldparent = get_subtree_root(olddir);
    dout(10) << " old parent " << *oldparent << dendl;
    CDir *newparent = get_subtree_root(diri->get_parent_dir());
    dout(10) << " new parent " << *newparent << dendl;

    if (oldparent == newparent) {
      dout(10) << "parent unchanged for " << *dir << " at " << *oldparent << dendl;
      continue;
    }

    if (dir->is_subtree_root()) {
      // children are fine.  change parent.
      dout(10) << "moving " << *dir << " from " << *oldparent << " to " << *newparent << dendl;
      assert(subtrees[oldparent].count(dir));
      subtrees[oldparent].erase(dir);
      assert(subtrees.count(newparent));
      subtrees[newparent].insert(dir);
    } else {
      // mid-subtree.

      // see if any old bounds move to the new parent.
      list<CDir*> tomove;
      for (set<CDir*>::iterator p = subtrees[oldparent].begin();
	   p != subtrees[oldparent].end();
	   ++p) {
	CDir *bound = *p;
	CDir *broot = get_subtree_root(bound->get_parent_dir());
	if (broot != oldparent) {
	  assert(broot == newparent);
	  tomove.push_back(bound);
	}
      }
      for (list<CDir*>::iterator p = tomove.begin(); p != tomove.end(); ++p) {
	CDir *bound = *p;
	dout(10) << "moving bound " << *bound << " from " << *oldparent << " to " << *newparent << dendl;
	subtrees[oldparent].erase(bound);
	subtrees[newparent].insert(bound);
      }	   

      // did auth change?
      if (oldparent->authority() != newparent->authority()) 
	adjust_subtree_auth(dir, oldparent->authority());  // caller is responsible for *diri.
    }
  }

  for (list<CDir*>::iterator p = dfls.begin(); p != dfls.end(); ++p) {
    CDir *dir = *p;

    // un-force dir to subtree root
    if (dir->dir_auth == pair<int,int>(dir->dir_auth.first, dir->dir_auth.first)) {
      adjust_subtree_auth(dir, dir->dir_auth.first);
      try_subtree_merge_at(dir);
    }
  }

  show_subtrees();
}


void MDCache::get_fullauth_subtrees(set<CDir*>& s)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_full_dir_auth())
      s.insert(root);
  }
}
void MDCache::get_auth_subtrees(set<CDir*>& s)
{
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_auth())
      s.insert(root);
  }
}


// count.

int MDCache::num_subtrees()
{
  return subtrees.size();
}

int MDCache::num_subtrees_fullauth()
{
  int n = 0;
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_full_dir_auth())
      n++;
  }
  return n;
}

int MDCache::num_subtrees_fullnonauth()
{
  int n = 0;
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *root = p->first;
    if (root->is_full_dir_nonauth())
      n++;
  }
  return n;
}



// ===================================
// journal and snap/cow helpers


/*
 * find first inode in cache that follows given snapid.  otherwise, return current.
 */
CInode *MDCache::pick_inode_snap(CInode *in, snapid_t follows)
{
  dout(10) << "pick_inode_snap follows " << follows << " on " << *in << dendl;
  assert(in->last == CEPH_NOSNAP);

  SnapRealm *realm = in->find_snaprealm();
  const set<snapid_t>& snaps = realm->get_snaps();
  dout(10) << " realm " << *realm << " " << *realm->inode << dendl;
  dout(10) << " snaps " << snaps << dendl;

  if (snaps.empty())
    return in;

  for (set<snapid_t>::const_iterator p = snaps.upper_bound(follows);  // first item > follows
       p != snaps.end();
       p++) {
    CInode *t = get_inode(in->ino(), *p);
    if (t) {
      in = t;
      dout(10) << "pick_inode_snap snap " << *p << " found " << *in << dendl;
      break;
    }
  }
  return in;
}


/*
 * note: i'm currently cheating wrt dirty and inode.version on cow
 * items.  instead of doing a full dir predirty, i just take the
 * original item's version, and set the dirty flag (via
 * mutation::add_cow_{inode,dentry}() and mutation::apply().  that
 * means a special case in the dir commit clean sweep assertions.
 * bah.
 */
CInode *MDCache::cow_inode(CInode *in, snapid_t last)
{
  assert(last >= in->first);

  CInode *oldin = new CInode(this, true, in->first, last);
  oldin->inode = *in->get_previous_projected_inode();
  oldin->symlink = in->symlink;
  oldin->xattrs = *in->get_previous_projected_xattrs();

  oldin->inode.trim_client_ranges(last);

  in->first = last+1;

  dout(10) << "cow_inode " << *in << " to " << *oldin << dendl;
  add_inode(oldin);
  
  SnapRealm *realm = in->find_snaprealm();
  const set<snapid_t>& snaps = realm->get_snaps();

  // clone caps?
  for (map<client_t,Capability*>::iterator p = in->client_caps.begin();
      p != in->client_caps.end();
      p++) {
    client_t client = p->first;
    Capability *cap = p->second;
    int issued = cap->issued();
    if ((issued & CEPH_CAP_ANY_WR) &&
	cap->client_follows <= oldin->first) {
      // note in oldin
      for (int i = 0; i < num_cinode_locks; i++) {
	if (issued & cinode_lock_info[i].wr_caps) {
	  int lockid = cinode_lock_info[i].lock;
	  SimpleLock *lock = oldin->get_lock(lockid);
	  assert(lock);
	  oldin->client_snap_caps[lockid].insert(client);
	  oldin->auth_pin(lock);
	  lock->set_state(LOCK_SNAP_SYNC);  // gathering
	  lock->get_wrlock(true);
	  dout(10) << " client" << client << " cap " << ccap_string(issued & cinode_lock_info[i].wr_caps)
		   << " wrlock lock " << *lock << " on " << *oldin << dendl;
	}
      }
      cap->client_follows = last;
      
      // we need snapflushes for any intervening snaps
      dout(10) << "  snaps " << snaps << dendl;
      for (set<snapid_t>::const_iterator q = snaps.lower_bound(oldin->first);
	   q != snaps.end() && *q <= last;
	   q++) {
	in->add_need_snapflush(oldin, *q, client);
      }
    } else {
      dout(10) << " ignoring client" << client << " cap follows " << cap->client_follows << dendl;
    }
  }

  return oldin;
}

void MDCache::journal_cow_dentry(Mutation *mut, EMetaBlob *metablob, CDentry *dn, snapid_t follows,
				 CInode **pcow_inode, CDentry::linkage_t *dnl)
{
  if (!dn) {
    dout(10) << "journal_cow_dentry got null CDentry, returning" << dendl;
    return;
  }
  dout(10) << "journal_cow_dentry follows " << follows << " on " << *dn << dendl;

  // nothing to cow on a null dentry, fix caller
  if (!dnl)
    dnl = dn->get_projected_linkage();
  assert(!dnl->is_null());

  if (dnl->is_primary() && dnl->get_inode()->is_multiversion()) {
    // multiversion inode.
    CInode *in = dnl->get_inode();

    if (follows == CEPH_NOSNAP)
      follows = in->find_snaprealm()->get_newest_seq();

    if (in->get_projected_parent_dn() != dn &&
	follows+1 > dn->first) {
      snapid_t oldfirst = dn->first;
      dn->first = follows+1;
      CDentry *olddn = dn->dir->add_remote_dentry(dn->name, in->ino(),  in->d_type(),
						  oldfirst, follows);
      olddn->pre_dirty();
      dout(10) << " olddn " << *olddn << dendl;
      metablob->add_remote_dentry(olddn, true);
      mut->add_cow_dentry(olddn);

      // FIXME: adjust link count here?  hmm.
    }

    // already cloned?
    if (follows < in->first) {
      dout(10) << "journal_cow_dentry follows " << follows << " < first on " << *in << dendl;
      return;
    }

    in->cow_old_inode(follows, false);

  } else {
    if (follows == CEPH_NOSNAP)
      follows = dn->dir->inode->find_snaprealm()->get_newest_seq();
    
    // already cloned?
    if (follows < dn->first) {
      dout(10) << "journal_cow_dentry follows " << follows << " < first on " << *dn << dendl;
      return;
    }
       
    // update dn.first before adding old dentry to cdir's map
    snapid_t oldfirst = dn->first;
    dn->first = follows+1;
    
    dout(10) << "    dn " << *dn << dendl;
    if (dnl->is_primary()) {
      assert(oldfirst == dnl->get_inode()->first);
      CInode *oldin = cow_inode(dnl->get_inode(), follows);
      mut->add_cow_inode(oldin);
      if (pcow_inode)
	*pcow_inode = oldin;
      CDentry *olddn = dn->dir->add_primary_dentry(dn->name, oldin, oldfirst, follows);
      oldin->inode.version = olddn->pre_dirty();
      dout(10) << " olddn " << *olddn << dendl;
      bufferlist snapbl;
      if (dnl->get_inode()->projected_nodes.back()->snapnode)
        dnl->get_inode()->projected_nodes.back()->snapnode->encode(snapbl);
      metablob->add_primary_dentry(olddn, true, 0, 0, (snapbl.length() ? &snapbl : NULL));
      mut->add_cow_dentry(olddn);
    } else {
      assert(dnl->is_remote());
      CDentry *olddn = dn->dir->add_remote_dentry(dn->name, dnl->get_remote_ino(), dnl->get_remote_d_type(),
						  oldfirst, follows);
      olddn->pre_dirty();
      dout(10) << " olddn " << *olddn << dendl;
      metablob->add_remote_dentry(olddn, true);
      mut->add_cow_dentry(olddn);
    }
  }
}


void MDCache::journal_cow_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows,
				CInode **pcow_inode)
{
  dout(10) << "journal_cow_inode follows " << follows << " on " << *in << dendl;
  CDentry *dn = in->get_projected_parent_dn();
  journal_cow_dentry(mut, metablob, dn, follows, pcow_inode);
}

inode_t *MDCache::journal_dirty_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows)
{
  if (in->is_base()) {
    return metablob->add_root(true, in, in->get_projected_inode());
  } else {
    if (follows == CEPH_NOSNAP && in->last != CEPH_NOSNAP)
      follows = in->first - 1;
    CDentry *dn = in->get_projected_parent_dn();
    if (!dn->get_projected_linkage()->is_null())  // no need to cow a null dentry
      journal_cow_dentry(mut, metablob, dn, follows);
    return metablob->add_primary_dentry(dn, true, in);
  }
}



// nested ---------------------------------------------------------------

void MDCache::project_rstat_inode_to_frag(CInode *cur, CDir *parent, snapid_t first, int linkunlink)
{
  CDentry *parentdn = cur->get_projected_parent_dn();
  inode_t *curi = cur->get_projected_inode();

  if (cur->first > first)
    first = cur->first;

  dout(10) << "projected_rstat_inode_to_frag first " << first << " linkunlink " << linkunlink
	   << " " << *cur << dendl;
  dout(20) << "    frag head is [" << parent->first << ",head] " << dendl;
  dout(20) << " inode update is [" << first << "," << cur->last << "]" << dendl;

  /*
   * FIXME.  this incompletely propagates rstats to _old_ parents
   * (i.e. shortly after a directory rename).  but we need full
   * blown hard link backpointers to make this work properly...
   */
  snapid_t floor = parentdn->first;
  dout(20) << " floor of " << floor << " from parent dn " << *parentdn << dendl;

  if (cur->last >= floor)
    _project_rstat_inode_to_frag(*curi, MAX(first, floor), cur->last, parent, linkunlink);
      
  for (set<snapid_t>::iterator p = cur->dirty_old_rstats.begin();
       p != cur->dirty_old_rstats.end();
       p++) {
    old_inode_t& old = cur->old_inodes[*p];
    if (*p >= floor)
      _project_rstat_inode_to_frag(old.inode, MAX(old.first, floor), *p, parent);
  }
  cur->dirty_old_rstats.clear();
}


void MDCache::_project_rstat_inode_to_frag(inode_t& inode, snapid_t ofirst, snapid_t last,
					  CDir *parent, int linkunlink)
{
  dout(10) << "_project_rstat_inode_to_frag [" << ofirst << "," << last << "]" << dendl;
  dout(20) << "  inode           rstat " << inode.rstat << dendl;
  dout(20) << "  inode accounted_rstat " << inode.accounted_rstat << dendl;
  nest_info_t delta;
  if (linkunlink == 0) {
    delta.add(inode.rstat);
    delta.sub(inode.accounted_rstat);
  } else if (linkunlink < 0) {
    delta.sub(inode.accounted_rstat);
  } else {
    delta.add(inode.rstat);
  }
  dout(20) << "                  delta " << delta << dendl;

  inode.accounted_rstat = inode.rstat;

  while (last >= ofirst) {
    /*
     * pick fnode version to update.  at each iteration, we want to
     * pick a segment ending in 'last' to update.  split as necessary
     * to make that work.  then, adjust first up so that we only
     * update one segment at a time.  then loop to cover the whole
     * [ofirst,last] interval.
     */    
    nest_info_t *prstat;
    snapid_t first;
    fnode_t *pf = parent->get_projected_fnode();
    if (last == CEPH_NOSNAP) {
      first = MAX(ofirst, parent->first);
      prstat = &pf->rstat;
      dout(20) << " projecting to head [" << first << "," << last << "] " << *prstat << dendl;

      if (first > parent->first &&
	  !(pf->rstat == pf->accounted_rstat)) {
	dout(10) << "  target snapped and not fully accounted, cow to dirty_old_rstat ["
		 << parent->first << "," << (first-1) << "] "
		 << " " << *prstat << "/" << pf->accounted_rstat
		 << dendl;
	parent->dirty_old_rstat[first-1].first = parent->first;
	parent->dirty_old_rstat[first-1].rstat = pf->rstat;
	parent->dirty_old_rstat[first-1].accounted_rstat = pf->accounted_rstat;
      }
      parent->first = first;
    } else if (last >= parent->first) {
      first = parent->first;
      parent->dirty_old_rstat[last].first = first;
      parent->dirty_old_rstat[last].rstat = pf->rstat;
      parent->dirty_old_rstat[last].accounted_rstat = pf->accounted_rstat;
      prstat = &parent->dirty_old_rstat[last].rstat;
      dout(10) << " projecting to newly split dirty_old_fnode [" << first << "," << last << "] "
	       << " " << *prstat << "/" << pf->accounted_rstat << dendl;
    } else {
      // be careful, dirty_old_rstat is a _sparse_ map.
      // sorry, this is ugly.
      first = ofirst;

      // find any intersection with last
      map<snapid_t,old_rstat_t>::iterator p = parent->dirty_old_rstat.lower_bound(last);
      if (p == parent->dirty_old_rstat.end()) {
	dout(20) << "  no dirty_old_rstat with last >= last " << last << dendl;
	if (!parent->dirty_old_rstat.empty() && parent->dirty_old_rstat.rbegin()->first >= first) {
	  dout(20) << "  last dirty_old_rstat ends at " << parent->dirty_old_rstat.rbegin()->first << dendl;
	  first = parent->dirty_old_rstat.rbegin()->first+1;
	}
      } else {
	// *p last is >= last
	if (p->second.first <= last) {
	  // *p intersects [first,last]
	  if (p->second.first < first) {
	    dout(10) << " splitting off left bit [" << p->second.first << "," << first-1 << "]" << dendl;
	    parent->dirty_old_rstat[first-1] = p->second;
	    p->second.first = first;
	  }
	  if (p->second.first > first)
	    first = p->second.first;
	  if (last < p->first) {
	    dout(10) << " splitting off right bit [" << last+1 << "," << p->first << "]" << dendl;
	    parent->dirty_old_rstat[last] = p->second;
	    p->second.first = last+1;
	  }
	} else {
	  // *p is to the _right_ of [first,last]
	  p = parent->dirty_old_rstat.lower_bound(first);
	  // new *p last is >= first
	  if (p->second.first <= last &&  // new *p isn't also to the right, and
	      p->first >= first) {        // it intersects our first bit,
	    dout(10) << " staying to the right of [" << p->second.first << "," << p->first << "]..." << dendl;
	    first = p->first+1;
	  }
	  dout(10) << " projecting to new dirty_old_rstat [" << first << "," << last << "]" << dendl;
	}
      }
      dout(20) << " projecting to dirty_old_rstat [" << first << "," << last << "]" << dendl;
      parent->dirty_old_rstat[last].first = first;
      prstat = &parent->dirty_old_rstat[last].rstat;
    }
    
    // apply
    dout(20) << "  project to [" << first << "," << last << "] " << *prstat << dendl;
    assert(last >= first);
    prstat->add(delta);
    inode.accounted_rstat = inode.rstat;
    dout(20) << "      result [" << first << "," << last << "] " << *prstat << " " << *parent << dendl;

    last = first-1;
  }
}

void MDCache::project_rstat_frag_to_inode(nest_info_t& rstat, nest_info_t& accounted_rstat,
					  snapid_t ofirst, snapid_t last, 
					  CInode *pin, bool cow_head)
{
  dout(10) << "project_rstat_frag_to_inode [" << ofirst << "," << last << "]" << dendl;
  dout(20) << "  frag           rstat " << rstat << dendl;
  dout(20) << "  frag accounted_rstat " << accounted_rstat << dendl;
  nest_info_t delta = rstat;
  delta.sub(accounted_rstat);
  dout(20) << "                 delta " << delta << dendl;

  while (last >= ofirst) {
    inode_t *pi;
    snapid_t first;
    if (last == pin->last) {
      pi = pin->get_projected_inode();
      first = MAX(ofirst, pin->first);
      if (first > pin->first) {
	old_inode_t& old = pin->cow_old_inode(first-1, cow_head);
	dout(20) << "   cloned old_inode rstat is " << old.inode.rstat << dendl;
      }
    } else {
      if (last >= pin->first) {
	first = pin->first;
	pin->cow_old_inode(last, cow_head);
      } else {
	// our life is easier here because old_inodes is not sparse
	// (although it may not begin at snapid 1)
	map<snapid_t,old_inode_t>::iterator p = pin->old_inodes.lower_bound(last);
	if (p == pin->old_inodes.end()) {
	  dout(10) << " no old_inode <= " << last << ", done." << dendl;
	  break;
	}
	first = p->second.first;
	if (first > last) {
	  dout(10) << " oldest old_inode is [" << first << "," << p->first << "], done." << dendl;
	  assert(p == pin->old_inodes.begin());
	  break;
	}
	if (p->first > last) {
	  dout(10) << " splitting right old_inode [" << first << "," << p->first << "] to ["
		   << (last+1) << "," << p->first << "]" << dendl;
	  pin->old_inodes[last] = p->second;
	  p->second.first = last+1;
	  pin->dirty_old_rstats.insert(p->first);
	}
      }
      if (first < ofirst) {
	dout(10) << " splitting left old_inode [" << first << "," << last << "] to ["
		 << first << "," << ofirst-1 << "]" << dendl;
	pin->old_inodes[ofirst-1] = pin->old_inodes[last];
	pin->dirty_old_rstats.insert(ofirst-1);
	pin->old_inodes[last].first = first = ofirst;
      }
      pi = &pin->old_inodes[last].inode;
      pin->dirty_old_rstats.insert(last);
    }
    dout(20) << " projecting to [" << first << "," << last << "] " << pi->rstat << dendl;
    pi->rstat.add(delta);
    dout(20) << "        result [" << first << "," << last << "] " << pi->rstat << dendl;

    if (pi->rstat.rbytes < 0)
      assert(!"negative rstat rbytes" == g_conf.mds_verify_scatter);

    last = first-1;
  }
}


/*
 * NOTE: we _have_ to delay the scatter if we are called during a
 * rejoin, because we can't twiddle locks between when the
 * rejoin_(weak|strong) is received and when we send the rejoin_ack.
 * normally, this isn't a problem: a recover mds doesn't twiddle locks
 * (no requests), and a survivor acks immediately.  _except_ that
 * during rejoin_(weak|strong) processing, we may complete a lock
 * gather, and do a scatter_writebehind.. and we _can't_ twiddle the
 * scatterlock state in that case or the lock states will get out of
 * sync between the auth and replica.
 *
 * the simple solution is to never do the scatter here.  instead, put
 * the scatterlock on a list if it isn't already wrlockable.  this is
 * probably the best plan anyway, since we avoid too many
 * scatters/locks under normal usage.
 */
/*
 * some notes on dirlock/nestlock scatterlock semantics:
 *
 * the fragstat (dirlock) will never be updated without
 * dirlock+nestlock wrlock held by the caller.
 *
 * the rstat (nestlock) _may_ get updated without a wrlock when nested
 * data is pushed up the tree.  this could be changed with some
 * restructuring here, but in its current form we ensure that the
 * fragstat+rstat _always_ reflect an accurrate summation over the dir
 * frag, which is nice.  and, we only need to track frags that need to
 * be nudged (and not inodes with pending rstat changes that need to
 * be pushed into the frag).  a consequence of this is that the
 * accounted_rstat on scatterlock sync may not match our current
 * rstat.  this is normal and expected.
 */
void MDCache::predirty_journal_parents(Mutation *mut, EMetaBlob *blob,
				       CInode *in, CDir *parent,
				       int flags, int linkunlink,
				       snapid_t cfollows)
{
  bool primary_dn = flags & PREDIRTY_PRIMARY;
  bool do_parent_mtime = flags & PREDIRTY_DIR;
  bool shallow = flags & PREDIRTY_SHALLOW;

  assert(mds->mdlog->entry_is_open());

  // declare now?
  if (mut->now == utime_t())
    mut->now = g_clock.real_now();

  if (in->is_base())
    return;

  dout(10) << "predirty_journal_parents"
	   << (do_parent_mtime ? " do_parent_mtime":"")
	   << " linkunlink=" <<  linkunlink
	   << (primary_dn ? " primary_dn":" remote_dn")
	   << (shallow ? " SHALLOW":"")
	   << " follows " << cfollows
	   << " " << *in << dendl;

  if (!parent) {
    assert(primary_dn);
    parent = in->get_projected_parent_dn()->get_dir();
  }

  if (flags == 0 && linkunlink == 0) {
    dout(10) << " no flags/linkunlink, just adding dir context to blob(s)" << dendl;
    blob->add_dir_context(parent);
    return;
  }

  inode_t *curi = in->get_projected_inode();

  // build list of inodes to wrlock, dirty, and update
  list<CInode*> lsi;
  CInode *cur = in;
  CDentry *parentdn = cur->get_projected_parent_dn();
  bool first = true;
  while (parent) {
    //assert(cur->is_auth() || !primary_dn);  // this breaks the rename auth twiddle hack
    assert(parent->is_auth());
    
    // opportunistically adjust parent dirfrag
    CInode *pin = parent->get_inode();

    // inode -> dirfrag
    mut->auth_pin(parent);
    mut->add_projected_fnode(parent);

    fnode_t *pf = parent->project_fnode();
    pf->version = parent->pre_dirty();

    if (do_parent_mtime || linkunlink) {
      assert(mut->wrlocks.count(&pin->filelock) ||
	     mut->is_slave());   // we are slave.  master will have wrlocked the dir.
      assert(cfollows == CEPH_NOSNAP);
      
      // update stale fragstat?
      parent->resync_accounted_fragstat();

      if (do_parent_mtime) {
	pf->fragstat.mtime = mut->now;
	if (mut->now > pf->rstat.rctime) {
	  dout(10) << "predirty_journal_parents updating mtime on " << *parent << dendl;
	  pf->rstat.rctime = mut->now;
	} else {
	  dout(10) << "predirty_journal_parents updating mtime UNDERWATER on " << *parent << dendl;
	}
      }
      if (linkunlink) {
	dout(10) << "predirty_journal_parents updating size on " << *parent << dendl;
	if (in->is_dir()) {
	  pf->fragstat.nsubdirs += linkunlink;
	  //pf->rstat.rsubdirs += linkunlink;
	} else {
 	  pf->fragstat.nfiles += linkunlink;
 	  //pf->rstat.rfiles += linkunlink;
	}
      }
    }

    
    // rstat
    if (!primary_dn) {
      // don't update parent this pass
    } else if (!parent->inode->nestlock.can_wrlock(-1)) {
      dout(20) << " unwritable parent nestlock " << parent->inode->nestlock
	       << ", marking dirty rstat on " << *cur << dendl;
      cur->mark_dirty_rstat();      
   } else {
      // if we don't hold a wrlock reference on this nestlock, take one,
      // because we are about to write into the dirfrag fnode and that needs
      // to commit before the lock can cycle.
      if (mut->wrlocks.count(&parent->inode->nestlock) == 0) {
	dout(10) << " taking wrlock on " << parent->inode->nestlock << " on " << *parent->inode << dendl;
	mds->locker->wrlock_force(&parent->inode->nestlock, mut);
      }

      // now we can project the inode rstat diff the dirfrag
      SnapRealm *prealm = parent->inode->find_snaprealm();
      
      snapid_t follows = cfollows;
      if (follows == CEPH_NOSNAP)
	follows = prealm->get_newest_seq();
      
      snapid_t first = follows+1;

      // first, if the frag is stale, bring it back in sync.
      parent->resync_accounted_rstat();

      // now push inode rstats into frag
      project_rstat_inode_to_frag(cur, parent, first, linkunlink);
      cur->clear_dirty_rstat();
    }

    bool stop = false;
    if (!pin->is_auth() || pin->is_ambiguous_auth()) {
      dout(10) << "predirty_journal_parents !auth or ambig on " << *pin << dendl;
      stop = true;
    }

    // delay propagating until later?
    if (!stop && !first &&
	g_conf.mds_dirstat_min_interval > 0) {
      if (pin->last_dirstat_prop.sec() > 0) {
	double since_last_prop = mut->now - pin->last_dirstat_prop;
	if (since_last_prop < g_conf.mds_dirstat_min_interval) {
	  dout(10) << "predirty_journal_parents last prop " << since_last_prop
		   << " < " << g_conf.mds_dirstat_min_interval
		   << ", stopping" << dendl;
	  stop = true;
	} else {
	  dout(10) << "predirty_journal_parents last prop " << since_last_prop << " ago, continuing" << dendl;
	}
      } else {
	dout(10) << "predirty_journal_parents last prop never, stopping" << dendl;
	stop = true;
      }
    }

    if (!stop &&
	mut->wrlocks.count(&pin->nestlock) == 0 &&
	(!pin->can_auth_pin() ||
	 !pin->versionlock.can_wrlock() ||                   // make sure we can take versionlock, too
	 //true
	 !mds->locker->wrlock_start(&pin->nestlock, (MDRequest*)mut, true) // can cast only because i'm passing nowait=true
	 )) {  // ** do not initiate.. see above comment **
      dout(10) << "predirty_journal_parents can't wrlock one of " << pin->versionlock << " or " << pin->nestlock
	       << " on " << *pin << dendl;
      stop = true;
    }
    if (stop) {
      dout(10) << "predirty_journal_parents stop.  marking nestlock on " << *pin << dendl;
      mds->locker->mark_updated_scatterlock(&pin->nestlock);
      mut->ls->dirty_dirfrag_nest.push_back(&pin->item_dirty_dirfrag_nest);
      mut->add_updated_lock(&pin->nestlock);
      break;
    }
    if (!mut->wrlocks.count(&pin->versionlock))
      mds->locker->local_wrlock_grab(&pin->versionlock, mut);

    assert(mut->wrlocks.count(&pin->nestlock) ||
	   mut->is_slave());
    
    pin->last_dirstat_prop = mut->now;

    // dirfrag -> diri
    mut->auth_pin(pin);
    mut->add_projected_inode(pin);
    lsi.push_front(pin);

    pin->pre_cow_old_inode();  // avoid cow mayhem!

    inode_t *pi = pin->project_inode();
    pi->version = pin->pre_dirty();

    // dirstat
    if (do_parent_mtime || linkunlink) {
      dout(20) << "predirty_journal_parents add_delta " << pf->fragstat << dendl;
      dout(20) << "predirty_journal_parents         - " << pf->accounted_fragstat << dendl;
      bool touched_mtime = false;
      pi->dirstat.add_delta(pf->fragstat, pf->accounted_fragstat, touched_mtime);
      pf->accounted_fragstat = pf->fragstat;
      if (touched_mtime)
	pi->mtime = pi->ctime = pi->dirstat.mtime;
      dout(20) << "predirty_journal_parents     gives " << pi->dirstat << " on " << *pin << dendl;

      if (pi->dirstat.size() < 0)
	assert(!"negative dirstat size" == g_conf.mds_verify_scatter);
      if (parent->get_frag() == frag_t()) { // i.e., we are the only frag
	if (pi->dirstat.size() != pf->fragstat.size()) {
	  stringstream ss;
	  ss << "unmatched fragstat size on single dirfrag " << parent->dirfrag()
	     << ", inode has " << pi->dirstat << ", dirfrag has " << pf->fragstat;
	  mds->logclient.log(LOG_ERROR, ss);
	  
	  // trust the dirfrag for now
	  pi->dirstat = pf->fragstat;

	  assert(!"unmatched fragstat size" == g_conf.mds_verify_scatter);
	}
      }
    }

    /* 
     * the rule here is to follow the _oldest_ parent with dirty rstat
     * data.  if we don't propagate all data, we add ourselves to the
     * nudge list.  that way all rstat data will (eventually) get
     * pushed up the tree.
     *
     * actually, no.  for now, silently drop rstats for old parents.  we need 
     * hard link backpointers to do the above properly.
     */

    // stop?
    if (pin->is_base())
      break;
    parentdn = pin->get_projected_parent_dn();
    assert(parentdn);

    // rstat
    if (primary_dn) {

      dout(10) << "predirty_journal_parents frag->inode on " << *parent << dendl;

      // first, if the frag is stale, bring it back in sync.
      parent->resync_accounted_rstat();

      for (map<snapid_t,old_rstat_t>::iterator p = parent->dirty_old_rstat.begin();
	   p != parent->dirty_old_rstat.end();
	   p++)
	project_rstat_frag_to_inode(p->second.rstat, p->second.accounted_rstat, p->second.first, p->first, pin, true);//false);
      parent->dirty_old_rstat.clear();
      project_rstat_frag_to_inode(pf->rstat, pf->accounted_rstat, parent->first, CEPH_NOSNAP, pin, true);//false);

      pf->accounted_rstat = pf->rstat;

      if (parent->get_frag() == frag_t()) { // i.e., we are the only frag
	if (pi->rstat.rbytes != pf->rstat.rbytes) { 
	  stringstream ss;
	  ss << "unmatched rstat rbytes on single dirfrag " << parent->dirfrag()
	     << ", inode has " << pi->rstat << ", dirfrag has " << pf->rstat;
	  mds->logclient.log(LOG_ERROR, ss);
	  
	  // trust the dirfrag for now
	  pi->rstat = pf->rstat;

	  assert(!"unmatched rstat rbytes" == g_conf.mds_verify_scatter);
	}
      }
    }

    // next parent!
    cur = pin;
    curi = pi;
    parent = parentdn->get_dir();
    linkunlink = 0;
    do_parent_mtime = false;
    primary_dn = true;
    first = false;
  }

  // now, stick it in the blob
  assert(parent->is_auth());
  blob->add_dir_context(parent);
  blob->add_dir(parent, true);
  for (list<CInode*>::iterator p = lsi.begin();
       p != lsi.end();
       p++) {
    CInode *cur = *p;
    journal_dirty_inode(mut, blob, cur);
  }
 
}





// ===================================
// slave requests


/*
 * some handlers for master requests with slaves.  we need to make 
 * sure slaves journal commits before we forget we mastered them and
 * remove them from the uncommitted_masters map (used during recovery
 * to commit|abort slaves).
 */
struct C_MDC_CommittedMaster : public Context {
  MDCache *cache;
  metareqid_t reqid;
  LogSegment *ls;
  list<Context*> waiters;
  C_MDC_CommittedMaster(MDCache *s, metareqid_t r, LogSegment *l, list<Context*> &w) :
    cache(s), reqid(r), ls(l) {
    waiters.swap(w);
  }
  void finish(int r) {
    cache->_logged_master_commit(reqid, ls, waiters);
  }
};

void MDCache::log_master_commit(metareqid_t reqid)
{
  dout(10) << "log_master_commit " << reqid << dendl;
  mds->mdlog->start_submit_entry(new ECommitted(reqid), 
				 new C_MDC_CommittedMaster(this, reqid, 
							   uncommitted_masters[reqid].ls,
							   uncommitted_masters[reqid].waiters));
  mds->mdcache->uncommitted_masters.erase(reqid);
}

void MDCache::_logged_master_commit(metareqid_t reqid, LogSegment *ls, list<Context*> &waiters)
{
  dout(10) << "_logged_master_commit " << reqid << dendl;
  ls->uncommitted_masters.erase(reqid);
  mds->queue_waiters(waiters);
}

// while active...

void MDCache::committed_master_slave(metareqid_t r, int from)
{
  dout(10) << "committed_master_slave mds" << from << " on " << r << dendl;
  assert(uncommitted_masters.count(r));
  uncommitted_masters[r].slaves.erase(from);
  if (uncommitted_masters[r].slaves.empty())
    log_master_commit(r);
}



/*
 * at end of resolve... we must journal a commit|abort for all slave
 * updates, before moving on.
 * 
 * this is so that the master can safely journal ECommitted on ops it
 * masters when it reaches up:active (all other recovering nodes must
 * complete resolve before that happens).
 */
struct C_MDC_SlaveCommit : public Context {
  MDCache *cache;
  int from;
  metareqid_t reqid;
  C_MDC_SlaveCommit(MDCache *c, int f, metareqid_t r) : cache(c), from(f), reqid(r) {}
  void finish(int r) {
    cache->_logged_slave_commit(from, reqid);
  }
};

void MDCache::_logged_slave_commit(int from, metareqid_t reqid)
{
  dout(10) << "_logged_slave_commit from mds" << from << " " << reqid << dendl;
  
  // send a message
  MMDSSlaveRequest *req = new MMDSSlaveRequest(reqid, MMDSSlaveRequest::OP_COMMITTED);
  mds->send_message_mds(req, from);
}






// ====================================================================
// import map, recovery


ESubtreeMap *MDCache::create_subtree_map() 
{
  dout(10) << "create_subtree_map " << num_subtrees() << " subtrees, " 
	   << num_subtrees_fullauth() << " fullauth"
	   << dendl;
  
  ESubtreeMap *le = new ESubtreeMap();
  mds->mdlog->start_entry(le);

  
  CDir *mydir = 0;
  if (myin) {
    mydir = myin->get_dirfrag(frag_t());
    assert(mydir);
  }

  // include all auth subtrees, and their bounds.
  // and a spanning tree to tie it to the root.
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    if (!dir->is_auth()) continue;

    dout(15) << " subtree " << *dir << dendl;
    le->subtrees[dir->dirfrag()].clear();
    le->metablob.add_dir_context(dir, EMetaBlob::TO_ROOT);
    le->metablob.add_dir(dir, false);

    if (mydir == dir)
      mydir = NULL;

    // bounds
    for (set<CDir*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDir *bound = *q;
      dout(15) << " subtree bound " << *bound << dendl;
      le->subtrees[dir->dirfrag()].push_back(bound->dirfrag());
      le->metablob.add_dir_context(bound, EMetaBlob::TO_ROOT);
      le->metablob.add_dir(bound, false);
    }
  }

  if (mydir) {
    // include my dir
    le->metablob.add_dir_context(mydir, EMetaBlob::TO_ROOT);
    le->metablob.add_dir(mydir, false);
  }

  //le->metablob.print(cout);
  return le;
}


void MDCache::resolve_start()
{
  dout(10) << "resolve_start" << dendl;

  if (mds->mdsmap->get_root() != mds->whoami) {
    // if we don't have the root dir, adjust it to UNKNOWN.  during
    // resolve we want mds0 to explicit claim the portion of it that
    // it owns, so that anything beyond its bounds get left as
    // unknown.
    CDir *rootdir = root->get_dirfrag(frag_t());
    if (rootdir)
      adjust_subtree_auth(rootdir, CDIR_AUTH_UNKNOWN);
  }

  for (set<int>::iterator p = recovery_set.begin(); p != recovery_set.end(); ++p) {
    if (*p == mds->whoami)
      continue;
    send_resolve(*p);  // now.
  }
}

void MDCache::send_resolve(int who)
{
  if (migrator->is_importing() || 
      migrator->is_exporting())
    send_resolve_later(who);
  else
    send_resolve_now(who);
}

void MDCache::send_resolve_later(int who)
{
  dout(10) << "send_resolve_later to mds" << who << dendl;
  wants_resolve.insert(who);
}

void MDCache::maybe_send_pending_resolves()
{
  if (wants_resolve.empty())
    return;  // nothing to send.

  // only if it's appropriate!
  if (migrator->is_exporting() ||
      migrator->is_importing()) {
    dout(7) << "maybe_send_pending_resolves waiting, imports/exports still in progress" << dendl;
    migrator->show_importing();
    migrator->show_exporting();
    return;  // not now
  }
  
  // ok, send them.
  for (set<int>::iterator p = wants_resolve.begin();
       p != wants_resolve.end();
       p++) 
    send_resolve_now(*p);
  wants_resolve.clear();
}


class C_MDC_SendResolve : public Context {
  MDCache *mdc;
  int who;
public:
  C_MDC_SendResolve(MDCache *c, int w) : mdc(c), who(w) { }
  void finish(int r) {
    mdc->send_resolve_now(who);
  }
};

void MDCache::send_resolve_now(int who)
{
  dout(10) << "send_resolve_now to mds" << who << dendl;
  MMDSResolve *m = new MMDSResolve;

  show_subtrees();

  // known
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) {
    CDir *dir = p->first;

    // only our subtrees
    if (dir->authority().first != mds->get_nodeid()) 
      continue;
    
    if (migrator->is_importing(dir->dirfrag())) {
      // ambiguous (mid-import)
      //  NOTE: because we are first authority, import state is at least IMPORT_LOGGINSTART.
      assert(migrator->get_import_state(dir->dirfrag()) >= Migrator::IMPORT_LOGGINGSTART);
      set<CDir*> bounds;
      get_subtree_bounds(dir, bounds);
      vector<dirfrag_t> dfls;
      for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p)
	dfls.push_back((*p)->dirfrag());
      m->add_ambiguous_import(dir->dirfrag(), dfls);
    } else {
      // not ambiguous.
      m->add_subtree(dir->dirfrag());
      
      // bounds too
      for (set<CDir*>::iterator q = subtrees[dir].begin();
	   q != subtrees[dir].end();
	   ++q) {
	CDir *bound = *q;
	m->add_subtree_bound(dir->dirfrag(), bound->dirfrag());
      }
    }
  }

  // ambiguous
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
       p != my_ambiguous_imports.end();
       ++p) 
    m->add_ambiguous_import(p->first, p->second);
  

  // list prepare requests lacking a commit
  // [active survivor]
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    if (p->second->is_slave() && p->second->slave_to_mds == who) {
      dout(10) << " including uncommitted " << *p->second << dendl;
      m->add_slave_request(p->first);
    }
  }
  // [resolving]
  if (uncommitted_slave_updates.count(who) &&
      !uncommitted_slave_updates[who].empty()) {
    for (map<metareqid_t, MDSlaveUpdate*>::iterator p = uncommitted_slave_updates[who].begin();
	 p != uncommitted_slave_updates[who].end();
	 ++p) {
      dout(10) << " including uncommitted " << p->first << dendl;
      m->add_slave_request(p->first);
    }
    dout(10) << " will need resolve ack from mds" << who << dendl;
    need_resolve_ack.insert(who);
  }


  // send
  mds->send_message_mds(m, who);
}


void MDCache::handle_mds_failure(int who)
{
  dout(7) << "handle_mds_failure mds" << who << dendl;
  
  // make note of recovery set
  mds->mdsmap->get_recovery_mds_set(recovery_set);
  recovery_set.erase(mds->get_nodeid());
  dout(1) << "handle_mds_failure mds" << who << " : recovery peers are " << recovery_set << dendl;

  // adjust my recovery lists
  wants_resolve.erase(who);   // MDS will ask again
  got_resolve.erase(who);     // i'll get another.

  rejoin_sent.erase(who);        // i need to send another
  rejoin_ack_gather.erase(who);  // i'll need/get another.

  dout(10) << " wants_resolve " << wants_resolve << dendl;
  dout(10) << " got_resolve " << got_resolve << dendl;
  dout(10) << " rejoin_sent " << rejoin_sent << dendl;
  dout(10) << " rejoin_gather " << rejoin_gather << dendl;
  dout(10) << " rejoin_ack_gather " << rejoin_ack_gather << dendl;

 
  // tell the migrator too.
  migrator->handle_mds_failure_or_stop(who);

  // kick any discovers that are waiting
  kick_discovers(who);
  
  // clean up any requests slave to/from this node
  list<MDRequest*> finish;
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    // slave to the failed node?
    if (p->second->slave_to_mds == who) {
      if (p->second->slave_did_prepare()) {
	dout(10) << " slave request " << *p->second << " uncommitted, will resolve shortly" << dendl;
      } else {
	dout(10) << " slave request " << *p->second << " has no prepare, finishing up" << dendl;
	if (p->second->slave_request)
	  p->second->aborted = true;
	else
	  finish.push_back(p->second);
      }
    }
    
    // failed node is slave?
    if (!p->second->committing) {
      if (p->second->more()->witnessed.count(who)) {
	dout(10) << " master request " << *p->second << " no longer witnessed by slave mds" << who
		 << dendl;
	// discard this peer's prepare (if any)
	p->second->more()->witnessed.erase(who);
      }
      
      if (p->second->more()->waiting_on_slave.count(who)) {
	dout(10) << " master request " << *p->second << " waiting for slave mds" << who
		 << " to recover" << dendl;
	// retry request when peer recovers
	p->second->more()->waiting_on_slave.erase(who);
	mds->wait_for_active_peer(who, new C_MDS_RetryRequest(this, p->second));
      }
    }
  }

  while (!finish.empty()) {
    dout(10) << "cleaning up slave request " << *finish.front() << dendl;
    request_finish(finish.front());
    finish.pop_front();
  }

  show_subtrees();  
}

/*
 * handle_mds_recovery - called on another node's transition 
 * from resolve -> active.
 */
void MDCache::handle_mds_recovery(int who)
{
  dout(7) << "handle_mds_recovery mds" << who << dendl;

  list<Context*> waiters;

  // wake up any waiters in their subtrees
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;

    if (dir->authority().first != who) continue;
    assert(!dir->is_auth());
   
    // wake any waiters
    list<CDir*> q;
    q.push_back(dir);

    while (!q.empty()) {
      CDir *d = q.front();
      q.pop_front();
      d->take_waiting(CDir::WAIT_ANY_MASK, waiters);

      // inode waiters too
      for (CDir::map_t::iterator p = d->items.begin();
	   p != d->items.end();
	   ++p) {
	CDentry *dn = p->second;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (dnl->is_primary()) {
	  dnl->get_inode()->take_waiting(CInode::WAIT_ANY_MASK, waiters);
	  
	  // recurse?
	  list<CDir*> ls;
	  dnl->get_inode()->get_dirfrags(ls);
	  for (list<CDir*>::iterator p = ls.begin();
	       p != ls.end();
	       ++p) {
	    CDir *subdir = *p;
	    if (!subdir->is_subtree_root())
	      q.push_back(subdir);
	  }
	}
      }
    }
  }

  kick_discovers(who);

  // queue them up.
  mds->queue_waiters(waiters);
}

void MDCache::set_recovery_set(set<int>& s) 
{
  dout(7) << "set_recovery_set " << s << dendl;
  recovery_set = s;
}


/*
 * during resolve state, we share resolves to determine who
 * is authoritative for which trees.  we expect to get an resolve
 * from _everyone_ in the recovery_set (the mds cluster at the time of
 * the first failure).
 *
 * This functions puts the passed message before returning
 */
void MDCache::handle_resolve(MMDSResolve *m)
{
  dout(7) << "handle_resolve from " << m->get_source() << dendl;
  int from = m->get_source().num();

  // ambiguous slave requests?
  if (!m->slave_requests.empty()) {
    MMDSResolveAck *ack = new MMDSResolveAck;
    for (vector<metareqid_t>::iterator p = m->slave_requests.begin();
	 p != m->slave_requests.end();
	 ++p) {
      if (uncommitted_masters.count(*p)) {  //mds->sessionmap.have_completed_request(*p)) {
	// COMMIT
	dout(10) << " ambiguous slave request " << *p << " will COMMIT" << dendl;
	ack->add_commit(*p);
	uncommitted_masters[*p].slaves.insert(from);   // wait for slave OP_COMMITTED before we log ECommitted
      } else {
	// ABORT
	dout(10) << " ambiguous slave request " << *p << " will ABORT" << dendl;
	ack->add_abort(*p);      
      }
    }
    mds->send_message(ack, m->get_connection());
  }

  // am i a surviving ambiguous importer?
  if (mds->is_clientreplay() || mds->is_active() || mds->is_stopping()) {
    // check for any import success/failure (from this node)
    map<dirfrag_t, vector<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
    while (p != my_ambiguous_imports.end()) {
      map<dirfrag_t, vector<dirfrag_t> >::iterator next = p;
      next++;
      CDir *dir = get_dirfrag(p->first);
      assert(dir);
      dout(10) << "checking ambiguous import " << *dir << dendl;
      if (migrator->is_importing(dir->dirfrag()) &&
	  migrator->get_import_peer(dir->dirfrag()) == from) {
	assert(migrator->get_import_state(dir->dirfrag()) == Migrator::IMPORT_ACKING);
	
	// check if sender claims the subtree
	bool claimed_by_sender = false;
	for (map<dirfrag_t, vector<dirfrag_t> >::iterator q = m->subtrees.begin();
	     q != m->subtrees.end();
	     ++q) {
	  CDir *base = get_dirfrag(q->first);
	  if (!base || !base->contains(dir)) 
	    continue;  // base not dir or an ancestor of dir, clearly doesn't claim dir.

	  bool inside = true;
	  for (vector<dirfrag_t>::iterator r = q->second.begin();
	       r != q->second.end();
	       ++r) {
	    CDir *bound = get_dirfrag(*r);
	    if (bound && bound->contains(dir)) {
	      inside = false;  // nope, bound is dir or parent of dir, not inside.
	      break;
	    }
	  }
	  if (inside)
	    claimed_by_sender = true;
	}

	if (claimed_by_sender) {
	  dout(7) << "ambiguous import failed on " << *dir << dendl;
	  migrator->import_reverse(dir);
	} else {
	  dout(7) << "ambiguous import succeeded on " << *dir << dendl;
	  migrator->import_finish(dir);
	}
	my_ambiguous_imports.erase(p);  // no longer ambiguous.
      }
      p = next;
    }
  }    

  // update my dir_auth values
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator pi = m->subtrees.begin();
       pi != m->subtrees.end();
       ++pi) {
    CInode *diri = get_inode(pi->first.ino);
    if (!diri) continue;
    bool forced = diri->dirfragtree.force_to_leaf(pi->first.frag);
    if (forced) {
      dout(10) << " forced frag " << pi->first.frag << " to leaf in " 
	       << diri->dirfragtree 
	       << " on " << pi->first << dendl;
    }

    CDir *dir = diri->get_dirfrag(pi->first.frag);
    if (!dir) continue;

    adjust_bounded_subtree_auth(dir, pi->second, from);
    try_subtree_merge(dir);
  }

  show_subtrees();

  if (mds->is_resolve()) {
    // note ambiguous imports too
    for (map<dirfrag_t, vector<dirfrag_t> >::iterator pi = m->ambiguous_imports.begin();
	 pi != m->ambiguous_imports.end();
	 ++pi) {
      dout(10) << "noting ambiguous import on " << pi->first << " bounds " << pi->second << dendl;
      other_ambiguous_imports[from][pi->first].swap( pi->second );
    }
    
    // did i get them all?
    got_resolve.insert(from);
  
    maybe_resolve_finish();
  }

  m->put();
}

void MDCache::maybe_resolve_finish()
{
  if (got_resolve != recovery_set) {
    dout(10) << "maybe_resolve_finish still waiting for more resolves, got (" 
	     << got_resolve << "), need (" << recovery_set << ")" << dendl;
  } 
  else if (!need_resolve_ack.empty()) {
    dout(10) << "maybe_resolve_finish still waiting for resolve_ack from (" 
	     << need_resolve_ack << ")" << dendl;
  }
  else if (!need_resolve_rollback.empty()) {
    dout(10) << "maybe_resolve_finish still waiting for rollback to commit on (" 
	     << need_resolve_rollback << ")" << dendl;
  } 
  else {
    dout(10) << "maybe_resolve_finish got all resolves+resolve_acks, done." << dendl;
    disambiguate_imports();
    if (mds->is_resolve()) {
      trim_unlinked_inodes();
      recalc_auth_bits();
      trim_non_auth(); 
      mds->resolve_done();
    }
  } 
}

/* This functions puts the passed message before returning */
void MDCache::handle_resolve_ack(MMDSResolveAck *ack)
{
  dout(10) << "handle_resolve_ack " << *ack << " from " << ack->get_source() << dendl;
  int from = ack->get_source().num();

  for (vector<metareqid_t>::iterator p = ack->commit.begin();
       p != ack->commit.end();
       ++p) {
    dout(10) << " commit on slave " << *p << dendl;
    
    if (mds->is_resolve()) {
      // replay
      assert(uncommitted_slave_updates[from].count(*p));
      // log commit
      mds->mdlog->start_submit_entry(new ESlaveUpdate(mds->mdlog, "unknown", *p, from,
						      ESlaveUpdate::OP_COMMIT,
						      uncommitted_slave_updates[from][*p]->origop));

      delete uncommitted_slave_updates[from][*p];
      uncommitted_slave_updates[from].erase(*p);
      if (uncommitted_slave_updates[from].empty())
	uncommitted_slave_updates.erase(from);

      mds->mdlog->wait_for_sync(new C_MDC_SlaveCommit(this, from, *p));
      mds->mdlog->flush();
    } else {
      MDRequest *mdr = request_get(*p);
      assert(mdr->slave_request == 0);  // shouldn't be doing anything!
      request_finish(mdr);
    }
  }

  for (vector<metareqid_t>::iterator p = ack->abort.begin();
       p != ack->abort.end();
       ++p) {
    dout(10) << " abort on slave " << *p << dendl;

    if (mds->is_resolve()) {
      assert(uncommitted_slave_updates[from].count(*p));

      // perform rollback (and journal a rollback entry)
      // note: this will hold up the resolve a bit, until the rollback entries journal.
      if (uncommitted_slave_updates[from][*p]->origop == ESlaveUpdate::LINK)
	mds->server->do_link_rollback(uncommitted_slave_updates[from][*p]->rollback, from, 0);
      else if (uncommitted_slave_updates[from][*p]->origop == ESlaveUpdate::RENAME)  
	mds->server->do_rename_rollback(uncommitted_slave_updates[from][*p]->rollback, from, 0);    
      else
	assert(0);

      delete uncommitted_slave_updates[from][*p];
      uncommitted_slave_updates[from].erase(*p);
      if (uncommitted_slave_updates[from].empty())
	uncommitted_slave_updates.erase(from);
    } else {
      MDRequest *mdr = request_get(*p);
      if (mdr->more()->slave_commit) {
	Context *fin = mdr->more()->slave_commit;
	mdr->more()->slave_commit = 0;
	fin->finish(-1);
	delete fin;
      } else {
	if (mdr->slave_request) 
	  mdr->aborted = true;
	else
	  request_finish(mdr);
      }
    }
  }

  need_resolve_ack.erase(from);

  if (mds->is_resolve()) 
    maybe_resolve_finish();

  ack->put();
}



void MDCache::disambiguate_imports()
{
  dout(10) << "disambiguate_imports" << dendl;

  // other nodes' ambiguous imports
  for (map<int, map<dirfrag_t, vector<dirfrag_t> > >::iterator p = other_ambiguous_imports.begin();
       p != other_ambiguous_imports.end();
       ++p) {
    int who = p->first;
    dout(10) << "ambiguous imports for mds" << who << dendl;

    for (map<dirfrag_t, vector<dirfrag_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << " ambiguous import " << q->first << " bounds " << q->second << dendl;
      CDir *dir = get_dirfrag(q->first);
      if (!dir) continue;
      
      if (dir->authority().first == CDIR_AUTH_UNKNOWN ||   // if i am resolving
	  dir->is_ambiguous_auth()) {                      // if i am a surviving bystander
	dout(10) << "  mds" << who << " did import " << *dir << dendl;
	adjust_bounded_subtree_auth(dir, q->second, who);
	try_subtree_merge(dir);
      } else {
	dout(10) << "  mds" << who << " did not import " << *dir << dendl;
      }
    }
  }
  other_ambiguous_imports.clear();

  // my ambiguous imports
  while (!my_ambiguous_imports.empty()) {
    map<dirfrag_t, vector<dirfrag_t> >::iterator q = my_ambiguous_imports.begin();

    CDir *dir = get_dirfrag(q->first);
    if (!dir) continue;
    
    if (dir->authority().first != CDIR_AUTH_UNKNOWN) {
      dout(10) << "ambiguous import auth known, must not be me " << *dir << dendl;
      cancel_ambiguous_import(q->first);
      mds->mdlog->start_submit_entry(new EImportFinish(dir, false));
    } else {
      dout(10) << "ambiguous import auth unknown, must be me " << *dir << dendl;
      finish_ambiguous_import(q->first);
      mds->mdlog->start_submit_entry(new EImportFinish(dir, true));
    }
  }
  assert(my_ambiguous_imports.empty());
  mds->mdlog->flush();

  if (mds->is_resolve()) {
    // verify all my subtrees are unambiguous!
    for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      CDir *dir = p->first;
      if (dir->is_ambiguous_dir_auth()) {
	dout(0) << "disambiguate_imports uh oh, dir_auth is still ambiguous for " << *dir << dendl;
	show_subtrees();
      }
      assert(!dir->is_ambiguous_dir_auth());
    }
  }

  show_subtrees();
}


void MDCache::add_ambiguous_import(dirfrag_t base, vector<dirfrag_t>& bounds) 
{
  assert(my_ambiguous_imports.count(base) == 0);
  my_ambiguous_imports[base].swap( bounds );
}


void MDCache::add_ambiguous_import(CDir *base, const set<CDir*>& bounds)
{
  // make a list
  vector<dirfrag_t> binos;
  for (set<CDir*>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) 
    binos.push_back((*p)->dirfrag());
  
  // note: this can get called twice if the exporter fails during recovery
  if (my_ambiguous_imports.count(base->dirfrag()))
    my_ambiguous_imports.erase(base->dirfrag());

  add_ambiguous_import(base->dirfrag(), binos);
}

void MDCache::cancel_ambiguous_import(dirfrag_t df)
{
  assert(my_ambiguous_imports.count(df));
  dout(10) << "cancel_ambiguous_import " << df
	   << " bounds " << my_ambiguous_imports[df]
	   << dendl;
  my_ambiguous_imports.erase(df);
}

void MDCache::finish_ambiguous_import(dirfrag_t df)
{
  assert(my_ambiguous_imports.count(df));
  vector<dirfrag_t> bounds;
  bounds.swap(my_ambiguous_imports[df]);
  my_ambiguous_imports.erase(df);
  
  dout(10) << "finish_ambiguous_import " << df
	   << " bounds " << bounds
	   << dendl;
  CDir *dir = get_dirfrag(df);
  assert(dir);
  
  // adjust dir_auth, import maps
  adjust_bounded_subtree_auth(dir, bounds, mds->get_nodeid());
  try_subtree_merge(dir);
}

void MDCache::remove_inode_recursive(CInode *in)
{
  dout(10) << "remove_inode_recursive " << *in << dendl;
  list<CDir*> ls;
  in->get_dirfrags(ls);
  list<CDir*>::iterator p = ls.begin();
  while (p != ls.end()) {
    CDir *subdir = *p++;

    dout(10) << " removing dirfrag " << subdir << dendl;
    CDir::map_t::iterator q = subdir->items.begin();
    while (q != subdir->items.end()) {
      CDentry *dn = q->second;
      ++q;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	subdir->unlink_inode(dn);
	remove_inode_recursive(in);
      }
      subdir->remove_dentry(dn);
    }
    
    if (subdir->is_subtree_root()) 
      remove_subtree(subdir);
    in->close_dirfrag(subdir->dirfrag().frag);
  }
  remove_inode(in);
}

void MDCache::trim_unlinked_inodes()
{
  dout(7) << "trim_unlinked_inodes" << dendl;
  list<CInode*> q;
  for (hash_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       p++) {
    CInode *in = p->second;
    if (in->get_parent_dn() == NULL && !in->is_base()) {
      dout(7) << " will trim from " << *in << dendl;
      q.push_back(in);
    }
  }
  for (list<CInode*>::iterator p = q.begin(); p != q.end(); p++)
    remove_inode_recursive(*p);
}

/** recalc_auth_bits()
 * once subtree auth is disambiguated, we need to adjust all the 
 * auth and dirty bits in our cache before moving on.
 */
void MDCache::recalc_auth_bits()
{
  dout(7) << "recalc_auth_bits" << dendl;

  if (root) {
    root->inode_auth.first = mds->mdsmap->get_root();
    if (mds->whoami != root->inode_auth.first)
      root->state_clear(CInode::STATE_AUTH);
  }

  set<CInode*> subtree_inodes;
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p)
    subtree_inodes.insert(p->first->inode);      

  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    list<CDir*> dfq;  // dirfrag queue
    dfq.push_back(p->first);

    bool auth = p->first->authority().first == mds->get_nodeid();
    dout(10) << " subtree auth=" << auth << " for " << *p->first << dendl;

    while (!dfq.empty()) {
      CDir *dir = dfq.front();
      dfq.pop_front();

      // dir
      if (auth) 
	dir->state_set(CDir::STATE_AUTH);
      else {
	dir->state_set(CDir::STATE_REJOINING);
	dir->state_clear(CDir::STATE_AUTH);
	if (dir->is_dirty()) 
	  dir->mark_clean();
      }

      // dentries in this dir
      for (CDir::map_t::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	// dn
	CDentry *dn = q->second;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (auth)
	  dn->state_set(CDentry::STATE_AUTH);
	else {
	  dn->state_set(CDentry::STATE_REJOINING);
	  dn->state_clear(CDentry::STATE_AUTH);
	  if (dn->is_dirty()) 
	    dn->mark_clean();
	}

	if (dnl->is_primary()) {
	  // inode
	  if (auth) 
	    dnl->get_inode()->state_set(CInode::STATE_AUTH);
	  else {
	    dnl->get_inode()->state_set(CInode::STATE_REJOINING);
	    dnl->get_inode()->state_clear(CInode::STATE_AUTH);
	    if (dnl->get_inode()->is_dirty())
	      dnl->get_inode()->mark_clean();
	    // avoid touching scatterlocks for our subtree roots!
	    if (subtree_inodes.count(dnl->get_inode()) == 0) {
	      dnl->get_inode()->filelock.clear_dirty();
	      dnl->get_inode()->nestlock.clear_dirty();
	      dnl->get_inode()->dirfragtreelock.clear_dirty();
	    }
	  }

	  // recurse?
	  if (dnl->get_inode()->is_dir()) 
	    dnl->get_inode()->get_nested_dirfrags(dfq);
	}
      }
    }
  }
  
  show_subtrees();
  show_cache();
}



// ===========================================================================
// REJOIN

/*
 * notes on scatterlock recovery:
 *
 * - recovering inode replica sends scatterlock data for any subtree
 *   roots (the only ones that are possibly dirty).
 *
 * - surviving auth incorporates any provided scatterlock data.  any
 *   pending gathers are then finished, as with the other lock types.
 *
 * that takes care of surviving auth + (recovering replica)*.
 *
 * - surviving replica sends strong_inode, which includes current
 *   scatterlock state, AND any dirty scatterlock data.  this
 *   provides the recovering auth with everything it might need.
 * 
 * - recovering auth must pick initial scatterlock state based on
 *   (weak|strong) rejoins.
 *   - always assimilate scatterlock data (it can't hurt)
 *   - any surviving replica in SCATTER state -> SCATTER.  otherwise, SYNC.
 *   - include base inode in ack for all inodes that saw scatterlock content
 *
 * also, for scatter gather,
 *
 * - auth increments {frag,r}stat.version on completion of any gather.
 *
 * - auth incorporates changes in a gather _only_ if the version
 *   matches.
 *
 * - replica discards changes any time the scatterlock syncs, and
 *   after recovery.
 */


/*
 * rejoin phase!
 *
 * this initiates rejoin.  it shoudl be called before we get any
 * rejoin or rejoin_ack messages (or else mdsmap distribution is broken).
 *
 * we start out by sending rejoins to everyone in the recovery set.
 *
 * if we are rejoin, send for all regions in our cache.
 * if we are active|stopping, send only to nodes that are are rejoining.
 */
void MDCache::rejoin_send_rejoins()
{
  dout(10) << "rejoin_send_rejoins with recovery_set " << recovery_set << dendl;

  map<int, MMDSCacheRejoin*> rejoins;

  // encode cap list once.
  bufferlist cap_export_bl;
  if (mds->is_rejoin()) {
    ::encode(cap_exports, cap_export_bl);
    ::encode(cap_export_paths, cap_export_bl);
  }

  // if i am rejoining, send a rejoin to everyone.
  // otherwise, just send to others who are rejoining.
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (*p == mds->get_nodeid())  continue;  // nothing to myself!
    if (rejoin_sent.count(*p)) continue;     // already sent a rejoin to this node!
    if (mds->is_rejoin()) {
      rejoin_gather.insert(*p);
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_WEAK);
      rejoins[*p]->copy_cap_exports(cap_export_bl);
    } else if (mds->mdsmap->is_rejoin(*p))
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_STRONG);
  }	
  
  assert(!migrator->is_importing());
  assert(!migrator->is_exporting());
  
  // check all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    assert(dir->is_subtree_root());
    assert(!dir->is_ambiguous_dir_auth());

    // my subtree?
    if (dir->is_auth()) {
      // include scatterlock state with parent inode's subtree?
      int inauth = dir->inode->authority().first;
      if (rejoins.count(inauth)) {
	dout(10) << " sending scatterlock state to mds" << inauth << " for " << *dir << dendl;
	rejoins[inauth]->add_scatterlock_state(dir->inode);
      }
      continue;  // skip my own regions!
    }

    int auth = dir->get_dir_auth().first;
    assert(auth >= 0);
    if (rejoins.count(auth) == 0)
      continue;   // don't care about this node's subtrees

    rejoin_walk(dir, rejoins[auth]);
  }
  
  // rejoin root inodes, too
  for (map<int, MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    if (mds->is_rejoin()) {
      // weak
      if (p->first == 0 && root) 
	p->second->add_weak_inode(root->vino());
      CInode *s = get_inode(MDS_INO_STRAY(p->first)); 
      if (s)
	p->second->add_weak_inode(s->vino());
    } else {
      // strong
      if (p->first == 0 && root) {
	p->second->add_weak_inode(root->vino());
	p->second->add_strong_inode(root->vino(),
				    root->get_caps_wanted(),
				    root->filelock.get_state(),
				    root->nestlock.get_state(),
				    root->dirfragtreelock.get_state());
      }
      if (CInode *in = get_inode(MDS_INO_STRAY(p->first))) {
    	p->second->add_weak_inode(in->vino());
  	p->second->add_strong_inode(in->vino(),
				    in->get_caps_wanted(),
				    in->filelock.get_state(),
				    in->nestlock.get_state(),
				    in->dirfragtreelock.get_state());
      }
    }
  }  

  if (!mds->is_rejoin()) {
    // i am survivor.  send strong rejoin.
    // note request remote_auth_pins, xlocks
    for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
	 p != active_requests.end();
	 ++p) {
      // auth pins
      for (set<MDSCacheObject*>::iterator q = p->second->remote_auth_pins.begin();
	   q != p->second->remote_auth_pins.end();
	   ++q) {
	if (!(*q)->is_auth()) {
	  int who = (*q)->authority().first;
	  if (rejoins.count(who) == 0) continue;
	  MMDSCacheRejoin *rejoin = rejoins[who];
	  
	  dout(15) << " " << *p->second << " authpin on " << **q << dendl;
	  MDSCacheObjectInfo i;
	  (*q)->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_authpin(vinodeno_t(i.ino, i.snapid), p->second->reqid);
	  else
	    rejoin->add_dentry_authpin(i.dirfrag, i.dname, i.snapid, p->second->reqid);
	}
      }
      // xlocks
      for (set<SimpleLock*>::iterator q = p->second->xlocks.begin();
	   q != p->second->xlocks.end();
	   ++q) {
	if (!(*q)->get_parent()->is_auth()) {
	  int who = (*q)->get_parent()->authority().first;
	  if (rejoins.count(who) == 0) continue;
	  MMDSCacheRejoin *rejoin = rejoins[who];
	  
	  dout(15) << " " << *p->second << " xlock on " << **q << " " << *(*q)->get_parent() << dendl;
	  MDSCacheObjectInfo i;
	  (*q)->get_parent()->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_xlock(vinodeno_t(i.ino, i.snapid), (*q)->get_type(), p->second->reqid);
	  else
	    rejoin->add_dentry_xlock(i.dirfrag, i.dname, i.snapid, p->second->reqid);
	}
      }
    }
  }

  // send the messages
  for (map<int,MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    assert(rejoin_sent.count(p->first) == 0);
    assert(rejoin_ack_gather.count(p->first) == 0);
    rejoin_sent.insert(p->first);
    rejoin_ack_gather.insert(p->first);
    mds->send_message_mds(p->second, p->first);
  }
  rejoin_ack_gather.insert(mds->whoami);   // we need to complete rejoin_gather_finish, too

  // nothing?
  if (mds->is_rejoin() && rejoins.empty()) {
    dout(10) << "nothing to rejoin" << dendl;
    rejoin_gather_finish();
  }
}


/** 
 * rejoin_walk - build rejoin declarations for a subtree
 * 
 * @dir subtree root
 * @rejoin rejoin message
 *
 * from a rejoining node:
 *  weak dirfrag
 *  weak dentries (w/ connectivity)
 *
 * from a surviving node:
 *  strong dirfrag
 *  strong dentries (no connectivity!)
 *  strong inodes
 */
void MDCache::rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin)
{
  dout(10) << "rejoin_walk " << *dir << dendl;

  list<CDir*> nested;  // finish this dir, then do nested items
  
  if (mds->is_rejoin()) {
    // WEAK
    dout(15) << " add_weak_dirfrag " << *dir << dendl;
    rejoin->add_weak_dirfrag(dir->dirfrag());

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) {
      CDentry *dn = p->second;
      CDentry::linkage_t *dnl = dn->get_linkage();
      dout(15) << " add_weak_primary_dentry " << *dn << dendl;
      assert(dnl->is_primary());
      assert(dnl->get_inode()->is_dir());
      rejoin->add_weak_primary_dentry(dir->dirfrag(), dn->name.c_str(), dn->first, dn->last, dnl->get_inode()->ino());
      dnl->get_inode()->get_nested_dirfrags(nested);
    }
  } else {
    // STRONG
    dout(15) << " add_strong_dirfrag " << *dir << dendl;
    rejoin->add_strong_dirfrag(dir->dirfrag(), dir->get_replica_nonce(), dir->get_dir_rep());

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) {
      CDentry *dn = p->second;
      CDentry::linkage_t *dnl = dn->get_linkage();
      dout(15) << " add_strong_dentry " << *dn << dendl;
      rejoin->add_strong_dentry(dir->dirfrag(), dn->name, dn->first, dn->last,
				dnl->is_primary() ? dnl->get_inode()->ino():inodeno_t(0),
				dnl->is_remote() ? dnl->get_remote_ino():inodeno_t(0),
				dnl->is_remote() ? dnl->get_remote_d_type():0, 
				dn->get_replica_nonce(),
				dn->lock.get_state());
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	dout(15) << " add_strong_inode " << *in << dendl;
	rejoin->add_strong_inode(in->vino(),
				 in->get_caps_wanted(),
				 in->filelock.get_state(),
				 in->nestlock.get_state(),
				 in->dirfragtreelock.get_state());
	in->get_nested_dirfrags(nested);
      }
    }
  }

  // recurse into nested dirs
  for (list<CDir*>::iterator p = nested.begin(); 
       p != nested.end();
       ++p)
    rejoin_walk(*p, rejoin);
}


/*
 * i got a rejoin.
 *  - reply with the lockstate
 *
 * if i am active|stopping, 
 *  - remove source from replica list for everything not referenced here.
 * This function puts the passed message before returning.
 */
void MDCache::handle_cache_rejoin(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin " << *m << " from " << m->get_source() 
	  << " (" << m->get_payload().length() << " bytes)"
	  << dendl;

  switch (m->op) {
  case MMDSCacheRejoin::OP_WEAK:
    handle_cache_rejoin_weak(m);
    break;
  case MMDSCacheRejoin::OP_STRONG:
    handle_cache_rejoin_strong(m);
    break;

  case MMDSCacheRejoin::OP_ACK:
    handle_cache_rejoin_ack(m);
    break;
  case MMDSCacheRejoin::OP_MISSING:
    handle_cache_rejoin_missing(m);
    break;

  case MMDSCacheRejoin::OP_FULL:
    handle_cache_rejoin_full(m);
    break;

  default: 
    assert(0);
  }
  m->put();
}


/*
 * handle_cache_rejoin_weak
 *
 * the sender 
 *  - is recovering from their journal.
 *  - may have incorrect (out of date) inode contents
 *  - will include weak dirfrag if sender is dirfrag auth and parent inode auth is recipient
 *
 * if the sender didn't trim_non_auth(), they
 *  - may have incorrect (out of date) dentry/inode linkage
 *  - may have deleted/purged inodes
 * and i may have to go to disk to get accurate inode contents.  yuck.
 * This functions DOES NOT put the passed message before returning
 */
void MDCache::handle_cache_rejoin_weak(MMDSCacheRejoin *weak)
{
  int from = weak->get_source().num();

  // possible response(s)
  MMDSCacheRejoin *ack = 0;      // if survivor
  bool survivor = false;  // am i a survivor?
  
  if (mds->is_clientreplay() || mds->is_active() || mds->is_stopping()) {
    survivor = true;
    dout(10) << "i am a surivivor, and will ack immediately" << dendl;
    ack = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);

    // check cap exports
    for (map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> >::iterator p = weak->cap_exports.begin();
	 p != weak->cap_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      if (!in || !in->is_auth()) continue;
      for (map<client_t,ceph_mds_cap_reconnect>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " claiming cap import " << p->first << " client" << q->first << " on " << *in << dendl;
	rejoin_import_cap(in, q->first, q->second, from);
      }
    }
  } else {
    assert(mds->is_rejoin());

    // check cap exports.
    for (map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> >::iterator p = weak->cap_exports.begin();
	 p != weak->cap_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      if (in && !in->is_auth()) continue;
      filepath& path = weak->cap_export_paths[p->first];
      if (!in) {
	if (!path_is_mine(path))
	  continue;
	cap_import_paths[p->first] = path;
	dout(10) << " noting cap import " << p->first << " path " << path << dendl;
      }
      
      // note
      for (map<client_t,ceph_mds_cap_reconnect>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " claiming cap import " << p->first << " client" << q->first << dendl;
	cap_imports[p->first][q->first][from] = q->second;
      }
    }
  }

  // assimilate any potentially dirty scatterlock state
  for (map<inodeno_t,MMDSCacheRejoin::lock_bls>::iterator p = weak->inode_scatterlocks.begin();
       p != weak->inode_scatterlocks.end();
       p++) {
    CInode *in = get_inode(p->first);
    assert(in);
    in->decode_lock_state(CEPH_LOCK_IFILE, p->second.file);
    in->decode_lock_state(CEPH_LOCK_INEST, p->second.nest);
    in->decode_lock_state(CEPH_LOCK_IDFT, p->second.dft);
    if (!survivor)
      rejoin_potential_updated_scatterlocks.insert(in);
  }
  
  // walk weak map
  for (map<dirfrag_t, map<string_snap_t, MMDSCacheRejoin::dn_weak> >::iterator p = weak->weak.begin();
       p != weak->weak.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) dout(0) << " missing dirfrag " << p->first << dendl;
    assert(dir);

    int nonce = dir->add_replica(from);
    dout(10) << " have " << *dir << dendl;
    if (ack) 
      ack->add_strong_dirfrag(p->first, nonce, dir->dir_rep);
    
    // weak dentries
    for (map<string_snap_t,MMDSCacheRejoin::dn_weak>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first.name, q->first.snapid);
      assert(dn);
      CDentry::linkage_t *dnl = dn->get_linkage();
      assert(dnl->is_primary());
      
      if (survivor && dn->is_replica(from)) 
	dentry_remove_replica(dn, from);  // this induces a lock gather completion
      int dnonce = dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      if (ack) 
	ack->add_strong_dentry(p->first, dn->name, dn->first, dn->last,
			       dnl->get_inode()->ino(), inodeno_t(0), 0, 
			       dnonce, dn->lock.get_replica_state());

      // inode
      CInode *in = dnl->get_inode();
      assert(in);

      if (survivor && in->is_replica(from)) 
	inode_remove_replica(in, from);
      int inonce = in->add_replica(from);
      dout(10) << " have " << *in << dendl;

      // scatter the dirlock, just in case?
      if (!survivor && in->is_dir() && in->has_subtree_root_dirfrag())
	in->filelock.set_state(LOCK_MIX);

      if (ack) {
	ack->add_inode_base(in);
	ack->add_inode_locks(in, inonce);
      }
    }
  }
  
  // weak base inodes?  (root, stray, etc.)
  for (set<vinodeno_t>::iterator p = weak->weak_inodes.begin();
       p != weak->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    assert(in);   // hmm fixme wrt stray?
    if (survivor && in->is_replica(from)) 
      inode_remove_replica(in, from);    // this induces a lock gather completion
    int inonce = in->add_replica(from);
    dout(10) << " have base " << *in << dendl;
    
    if (ack) {
      ack->add_inode_base(in);
      ack->add_inode_locks(in, inonce);
    }
  }

  if (survivor) {
    // survivor.  do everything now.
    for (map<inodeno_t,MMDSCacheRejoin::lock_bls>::iterator p = weak->inode_scatterlocks.begin();
	 p != weak->inode_scatterlocks.end();
	 p++) {
      CInode *in = get_inode(p->first);
      dout(10) << " including base inode (due to potential scatterlock update) " << *in << dendl;
      ack->add_inode_base(in);
    }

    rejoin_scour_survivor_replicas(from, ack);
    mds->send_message(ack, weak->get_connection());
  } else {
    // done?
    assert(rejoin_gather.count(from));
    rejoin_gather.erase(from);
    if (rejoin_gather.empty()) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}


/**
 * parallel_fetch -- make a pass at fetching a bunch of paths in parallel
 *
 * @pathmap - map of inodeno to full pathnames.  we remove items from this map 
 *            as we discover we have them.
 *
 * returns a C_Gather* if there is work to do.  caller is responsible for setting
 * the C_Gather completer.
 */

C_Gather *MDCache::parallel_fetch(map<inodeno_t,filepath>& pathmap, set<inodeno_t>& missing)
{
  dout(10) << "parallel_fetch on " << pathmap.size() << " paths" << dendl;

  C_Gather *gather = new C_Gather;

  // scan list
  set<CDir*> fetch_queue;
  map<inodeno_t,filepath>::iterator p = pathmap.begin();
  while (p != pathmap.end()) {
    // do we have the target already?
    CInode *cur = get_inode(p->first);
    if (cur) {
      dout(15) << " have " << *cur << dendl;
      pathmap.erase(p++);
      continue;
    }

    // traverse
    dout(17) << " missing " << p->first << " at " << p->second << dendl;
    if (parallel_fetch_traverse_dir(p->first, p->second, fetch_queue, missing, gather))
      pathmap.erase(p++);
    else
      p++;
  }

  if (pathmap.empty() && gather->empty()) {
    dout(10) << "parallel_fetch done" << dendl;
    assert(fetch_queue.empty());
    delete gather;
    return false;
  }

  // do a parallel fetch
  for (set<CDir*>::iterator p = fetch_queue.begin();
       p != fetch_queue.end();
       ++p) {
    dout(10) << "parallel_fetch fetching " << **p << dendl;
    (*p)->fetch(gather->new_sub());
  }
  
  return gather;
}

// true if we're done with this path
bool MDCache::parallel_fetch_traverse_dir(inodeno_t ino, filepath& path,
					  set<CDir*>& fetch_queue, set<inodeno_t>& missing, C_Gather *gather)
{
  CInode *cur = get_inode(path.get_ino());
  if (!cur) {
    dout(5) << " missing " << path << " base ino " << path.get_ino() << dendl;
    missing.insert(ino);
    return true;
  }

  for (unsigned i=0; i<path.depth(); i++) {
    dout(20) << " path " << path << " seg " << i << "/" << path.depth() << ": " << path[i]
	     << " under " << *cur << dendl;
    if (!cur->is_dir()) {
      dout(5) << " bad path " << path << " ENOTDIR at " << path[i] << dendl;
      missing.insert(ino);
      return true;
    }
      
    frag_t fg = cur->pick_dirfrag(path[i]);
    CDir *dir = cur->get_or_open_dirfrag(this, fg);
    CDentry *dn = dir->lookup(path[i]);
    CDentry::linkage_t *dnl = dn->get_linkage();
    if (!dn || dnl->is_null()) {
      if (!dir->is_complete()) {
	// fetch dir
	fetch_queue.insert(dir);
	return false;
      } else {
	// probably because the client created it and held a cap but it never committed
	// to the journal, and the op hasn't replayed yet.
	dout(5) << " dne (not created yet?) " << ino << " at " << path << dendl;
	missing.insert(ino);
	return true;
      }
    }
    cur = dnl->get_inode();
    if (!cur) {
      assert(dnl->is_remote());
      cur = get_inode(dnl->get_remote_ino());
      if (cur) {
	dn->link_remote(dnl, cur);
      } else {
	// open remote ino
	open_remote_ino(dnl->get_remote_ino(), gather->new_sub());
	return false;
      }
    }
  }

  dout(5) << " ino not found " << ino << " at " << path << dendl;
  missing.insert(ino);
  return true;
}




/*
 * rejoin_scour_survivor_replica - remove source from replica list on unmentioned objects
 *
 * all validated replicas are acked with a strong nonce, etc.  if that isn't in the
 * ack, the replica dne, and we can remove it from our replica maps.
 */
void MDCache::rejoin_scour_survivor_replicas(int from, MMDSCacheRejoin *ack)
{
  dout(10) << "rejoin_scour_survivor_replicas from mds" << from << dendl;

  // FIXME: what about root and stray inodes.
  
  for (hash_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    CInode *in = p->second;
    
    // inode?
    if (in->is_auth() &&
	in->is_replica(from) &&
	ack->strong_inodes.count(p->second->vino()) == 0) {
      inode_remove_replica(in, from);
      dout(10) << " rem " << *in << dendl;
    }

    if (!in->is_dir()) continue;
    
    list<CDir*> dfs;
    in->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin();
	 p != dfs.end();
	 ++p) {
      CDir *dir = *p;
      
      if (dir->is_auth() &&
	  dir->is_replica(from) &&
	  ack->strong_dirfrags.count(dir->dirfrag()) == 0) {
	dir->remove_replica(from);
	dout(10) << " rem " << *dir << dendl;
      } 
      
      // dentries
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	
	if (dn->is_replica(from) &&
	    (ack->strong_dentries.count(dir->dirfrag()) == 0 ||
	     ack->strong_dentries[dir->dirfrag()].count(string_snap_t(dn->name, dn->last)) == 0)) {
	  dentry_remove_replica(dn, from);
	  dout(10) << " rem " << *dn << dendl;
	}
      }
    }
  }
}


CInode *MDCache::rejoin_invent_inode(inodeno_t ino, snapid_t last)
{
  CInode *in = new CInode(this, true, 1, last);
  in->inode.ino = ino;
  in->state_set(CInode::STATE_REJOINUNDEF);
  add_inode(in);
  rejoin_undef_inodes.insert(in);
  dout(10) << " invented " << *in << dendl;
  return in;
}

/* This functions DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_strong(MMDSCacheRejoin *strong)
{
  int from = strong->get_source().num();

  // only a recovering node will get a strong rejoin.
  assert(mds->is_rejoin());      

  MMDSCacheRejoin *missing = 0;  // if i'm missing something..
  
  // assimilate any potentially dirty scatterlock state
  for (map<inodeno_t,MMDSCacheRejoin::lock_bls>::iterator p = strong->inode_scatterlocks.begin();
       p != strong->inode_scatterlocks.end();
       p++) {
    CInode *in = get_inode(p->first);
    assert(in);
    in->decode_lock_state(CEPH_LOCK_IFILE, p->second.file);
    in->decode_lock_state(CEPH_LOCK_INEST, p->second.nest);
    in->decode_lock_state(CEPH_LOCK_IDFT, p->second.dft);
    rejoin_potential_updated_scatterlocks.insert(in);
  }

  // strong dirfrags/dentries.
  //  also process auth_pins, xlocks.
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = strong->strong_dirfrags.begin();
       p != strong->strong_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) {
      CInode *in = get_inode(p->first.ino);
      if (!in)
	in = rejoin_invent_inode(p->first.ino, CEPH_NOSNAP);
      if (!in->is_dir()) {
	assert(in->state_test(CInode::STATE_REJOINUNDEF));
	in->inode.mode = S_IFDIR;
      }
      dir = in->get_or_open_dirfrag(this, p->first.frag);
      dout(10) << " invented " << *dir << dendl;
    } else {
      dout(10) << " have " << *dir << dendl;
    }
    dir->add_replica(from);
    dir->dir_rep = p->second.dir_rep;

    for (map<string_snap_t,MMDSCacheRejoin::dn_strong>::iterator q = strong->strong_dentries[p->first].begin();
	 q != strong->strong_dentries[p->first].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first.name, q->first.snapid);
      if (!dn) {
	if (q->second.is_remote()) {
	  dn = dir->add_remote_dentry(q->first.name, q->second.remote_ino, q->second.remote_d_type, q->second.first, q->first.snapid);
	} else if (q->second.is_null()) {
	  dn = dir->add_null_dentry(q->first.name, q->second.first, q->first.snapid);
	} else {
	  CInode *in = get_inode(q->second.ino, q->first.snapid);
	  if (!in) in = rejoin_invent_inode(q->second.ino, q->first.snapid);
	  dn = dir->add_primary_dentry(q->first.name, in, q->second.first, q->first.snapid);

	  dout(10) << " missing " << q->second.ino << "." << q->first.snapid << dendl;
	  if (!missing) missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
	  missing->add_weak_inode(vinodeno_t(q->second.ino, q->first.snapid));  // we want it back!
	}
	dout(10) << " invented " << *dn << dendl;
      }
      CDentry::linkage_t *dnl = dn->get_linkage();

      // dn auth_pin?
      if (strong->authpinned_dentries.count(p->first) &&
	  strong->authpinned_dentries[p->first].count(q->first)) {
	metareqid_t ri = strong->authpinned_dentries[p->first][q->first];
	dout(10) << " dn authpin by " << ri << " on " << *dn << dendl;
	
	// get/create slave mdrequest
	MDRequest *mdr;
	if (have_request(ri))
	  mdr = request_get(ri);
	else
	  mdr = request_start_slave(ri, from);
	mdr->auth_pin(dn);
      }

      // dn xlock?
      if (strong->xlocked_dentries.count(p->first) &&
	  strong->xlocked_dentries[p->first].count(q->first)) {
	metareqid_t ri = strong->xlocked_dentries[p->first][q->first];
	dout(10) << " dn xlock by " << ri << " on " << *dn << dendl;
	MDRequest *mdr = request_get(ri);  // should have this from auth_pin above.
	assert(mdr->is_auth_pinned(dn));
	dn->lock.set_state(LOCK_LOCK);
	dn->lock.get_xlock(mdr, mdr->get_client());
	mdr->xlocks.insert(&dn->lock);
	mdr->locks.insert(&dn->lock);
      }

      dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      
      // inode?
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	assert(in);

	if (strong->strong_inodes.count(in->vino())) {
	  MMDSCacheRejoin::inode_strong &is = strong->strong_inodes[in->vino()];

	  // caps_wanted
	  if (is.caps_wanted) {
	    in->mds_caps_wanted[from] = is.caps_wanted;
	    dout(15) << " inode caps_wanted " << ccap_string(is.caps_wanted)
		     << " on " << *in << dendl;
	  } 
	  
	  // scatterlocks?
	  //  infer state from replica state:
	  //   * go to MIX if they might have wrlocks
	  //   * go to LOCK if they are LOCK (just bc identify_files_to_recover might start twiddling filelock)
	  in->filelock.infer_state_from_strong_rejoin(is.filelock, true);  // maybe also go to LOCK
	  in->nestlock.infer_state_from_strong_rejoin(is.nestlock, false);
	  in->dirfragtreelock.infer_state_from_strong_rejoin(is.dftlock, false);
	  
	  // auth pin?
	  if (strong->authpinned_inodes.count(in->vino())) {
	    metareqid_t ri = strong->authpinned_inodes[in->vino()];
	    dout(10) << " inode authpin by " << ri << " on " << *in << dendl;
	    
	    // get/create slave mdrequest
	    MDRequest *mdr;
	    if (have_request(ri))
	      mdr = request_get(ri);
	    else
	      mdr = request_start_slave(ri, from);
	    mdr->auth_pin(in);
	  }
	  
	  // xlock(s)?
	  if (strong->xlocked_inodes.count(in->vino())) {
	    for (map<int,metareqid_t>::iterator r = strong->xlocked_inodes[in->vino()].begin();
		 r != strong->xlocked_inodes[in->vino()].end();
		 ++r) {
	      SimpleLock *lock = in->get_lock(r->first);
	      dout(10) << " inode xlock by " << r->second << " on " << *lock << " on " << *in << dendl;
	      MDRequest *mdr = request_get(r->second);  // should have this from auth_pin above.
	      assert(mdr->is_auth_pinned(in));
	      lock->set_state(LOCK_LOCK);
	      if (lock == &in->filelock)
		in->loner_cap = -1;
	      lock->get_xlock(mdr, mdr->get_client());
	      mdr->xlocks.insert(lock);
	      mdr->locks.insert(lock);
	    }
	  }
	} else {
	  dout(10) << " sender has dentry but not inode, adding them as a replica" << dendl;
	}
	
	in->add_replica(from);
	dout(10) << " have " << *in << dendl;
      }
    }
  }

  // base inodes?  (root, stray, etc.)
  for (set<vinodeno_t>::iterator p = strong->weak_inodes.begin();
       p != strong->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    dout(10) << " have base " << *in << dendl;
    in->add_replica(from);
  }

  // send missing?
  if (missing) {
    // we expect a FULL soon.
    mds->send_message(missing, strong->get_connection());
  } else {
    // done?
    assert(rejoin_gather.count(from));
    rejoin_gather.erase(from);
    if (rejoin_gather.empty()) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}

/* This functions DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_ack(MMDSCacheRejoin *ack)
{
  dout(7) << "handle_cache_rejoin_ack from " << ack->get_source() << dendl;
  int from = ack->get_source().num();

  // dirs
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = ack->strong_dirfrags.begin();
       p != ack->strong_dirfrags.end();
       ++p) {
    CDir *dir = get_dirfrag(p->first);
    if (!dir) continue;  // must have trimmed?

    dir->set_replica_nonce(p->second.nonce);
    dir->state_clear(CDir::STATE_REJOINING);
    dout(10) << " got " << *dir << dendl;

    // dentries
    for (map<string_snap_t,MMDSCacheRejoin::dn_strong>::iterator q = ack->strong_dentries[p->first].begin();
	 q != ack->strong_dentries[p->first].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first.name, q->first.snapid);
      if (!dn) continue;  // must have trimmed?
      CDentry::linkage_t *dnl = dn->get_linkage();

      assert(dn->last == q->first.snapid);
      if (dn->first != q->second.first) {
	dout(10) << " adjust dn.first " << dn->first << " -> " << q->second.first << " on " << *dn << dendl;
	dn->first = q->second.first;
      }

      // hmm, did we have the proper linkage here?
      if (dnl->is_null() &&
	  !q->second.is_null()) {
	dout(10) << " had bad (missing) linkage for " << *dn << dendl;
	if (q->second.is_remote()) {
	  dn->dir->link_remote_inode(dn, q->second.remote_ino, q->second.remote_d_type);
	} else {
	  CInode *in = get_inode(q->second.ino, q->first.snapid);
	  assert(in == 0);  // a rename would have been caught be the resolve stage.
	  // barebones inode; the full inode loop below will clean up.
	  in = new CInode(this, false, q->second.first, q->first.snapid);
	  in->inode.ino = q->second.ino;
	  add_inode(in);
	  dn->dir->link_primary_inode(dn, in);
	}
      }
      else if (!dnl->is_null() &&
	       q->second.is_null()) {
	dout(-10) << " had bad linkage for " << *dn << dendl;
	/* 
	 * this should happen:
	 *  if we're a survivor, any unlink should commit or rollback during
	 * the resolve stage.
	 *  if we failed, we shouldn't have non-auth leaf dentries at all
	 */
	assert(0);  // uh oh.	
      }
      dn->set_replica_nonce(q->second.nonce);
      dn->lock.set_state_rejoin(q->second.lock, rejoin_waiters);
      dn->state_clear(CDentry::STATE_REJOINING);
      dout(10) << " got " << *dn << dendl;
    }
  }

  // full inodes
  bufferlist::iterator p = ack->inode_base.begin();
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    bufferlist basebl;
    ::decode(ino, p);
    ::decode(last, p);
    ::decode(basebl, p);
    CInode *in = get_inode(ino, last);
    if (!in) continue;
    bufferlist::iterator q = basebl.begin();
    in->_decode_base(q);
    dout(10) << " got inode base " << *in << dendl;
  }

  // inodes
  p = ack->inode_locks.begin();
  //dout(10) << "inode_locks len " << ack->inode_locks.length() << " is " << ack->inode_locks << dendl;
  while (!p.end()) {
    dout(10) << " p pos is " << p.get_off() << dendl;
    inodeno_t ino;
    snapid_t last;
    __u32 nonce;
    bufferlist lockbl;
    ::decode(ino, p);
    ::decode(last, p);
    ::decode(nonce, p);
    ::decode(lockbl, p);
    
    CInode *in = get_inode(ino, last);
    if (!in) continue;
    in->set_replica_nonce(nonce);
    bufferlist::iterator q = lockbl.begin();
    in->_decode_locks_rejoin(q, rejoin_waiters);
    in->state_clear(CInode::STATE_REJOINING);
    dout(10) << " got inode locks " << *in << dendl;
  }

  // done?
  assert(rejoin_ack_gather.count(from));
  rejoin_ack_gather.erase(from);
  if (mds->is_rejoin() && 
      rejoin_gather.empty() &&     // make sure we've gotten our FULL inodes, too.
      rejoin_ack_gather.empty()) {
    // finally, kickstart past snap parent opens
    open_snap_parents();
  } else {
    dout(7) << "still need rejoin from (" << rejoin_gather << ")"
	    << ", rejoin_ack from (" << rejoin_ack_gather << ")" << dendl;
  }
}



/* This functions DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_missing(MMDSCacheRejoin *missing)
{
  dout(7) << "handle_cache_rejoin_missing from " << missing->get_source() << dendl;

  MMDSCacheRejoin *full = new MMDSCacheRejoin(MMDSCacheRejoin::OP_FULL);
  
  // inodes
  for (set<vinodeno_t>::iterator p = missing->weak_inodes.begin();
       p != missing->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    if (!in) {
      dout(10) << " don't have inode " << *p << dendl;
      continue; // we must have trimmed it after the originalo rejoin
    }
    
    dout(10) << " sending " << *in << dendl;
    full->add_inode_base(in);
  }

  mds->send_message(full, missing->get_connection());
}

/* This function DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_full(MMDSCacheRejoin *full)
{
  dout(7) << "handle_cache_rejoin_full from " << full->get_source() << dendl;
  int from = full->get_source().num();
  
  // integrate full inodes
  bufferlist::iterator p = full->inode_base.begin();
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    bufferlist basebl;
    ::decode(ino, p);
    ::decode(last, p);
    ::decode(basebl, p);

    CInode *in = get_inode(ino);
    assert(in);
    bufferlist::iterator pp = basebl.begin();
    in->_decode_base(pp);

    set<CInode*>::iterator q = rejoin_undef_inodes.find(in);
    if (q != rejoin_undef_inodes.end()) {
      CInode *in = *q;
      in->state_clear(CInode::STATE_REJOINUNDEF);
      dout(10) << " got full " << *in << dendl;
      rejoin_undef_inodes.erase(q);
    } else {
      dout(10) << " had full " << *in << dendl;
    }
  }

  // done?
  assert(rejoin_gather.count(from));
  rejoin_gather.erase(from);
  if (rejoin_gather.empty()) {
    rejoin_gather_finish();
  } else {
    dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
  }
}



/**
 * rejoin_trim_undef_inodes() -- remove REJOINUNDEF flagged inodes
 *
 * FIXME: wait, can this actually happen?  a survivor should generate cache trim
 * messages that clean these guys up...
 */
void MDCache::rejoin_trim_undef_inodes()
{
  dout(10) << "rejoin_trim_undef_inodes" << dendl;

  while (!rejoin_undef_inodes.empty()) {
    set<CInode*>::iterator p = rejoin_undef_inodes.begin();
    CInode *in = *p;
    rejoin_undef_inodes.erase(p);

    in->clear_replica_map();
    
    // close out dirfrags
    if (in->is_dir()) {
      list<CDir*> dfls;
      in->get_dirfrags(dfls);
      for (list<CDir*>::iterator p = dfls.begin();
	   p != dfls.end();
	   ++p) {
	CDir *dir = *p;
	dir->clear_replica_map();

	for (CDir::map_t::iterator p = dir->items.begin();
	     p != dir->items.end();
	     ++p) {
	  CDentry *dn = p->second;
	  dn->clear_replica_map();

	  dout(10) << " trimming " << *dn << dendl;
	  dir->remove_dentry(dn);
	}

	dout(10) << " trimming " << *dir << dendl;
	in->close_dirfrag(dir->dirfrag().frag);
      }
    }
    
    CDentry *dn = in->get_parent_dn();
    if (dn) {
      dn->clear_replica_map();
      dout(10) << " trimming " << *dn << dendl;
      dn->dir->remove_dentry(dn);
    } else {
      dout(10) << " trimming " << *in << dendl;
      remove_inode(in);
    }
  }

  assert(rejoin_undef_inodes.empty());
}

class C_MDC_RejoinGatherFinish : public Context {
  MDCache *cache;
public:
  C_MDC_RejoinGatherFinish(MDCache *c) : cache(c) {}
  void finish(int r) {
    cache->rejoin_gather_finish();
  }
};



void MDCache::rejoin_gather_finish() 
{
  dout(10) << "rejoin_gather_finish" << dendl;
  assert(mds->is_rejoin());

  rejoin_trim_undef_inodes();

  // fetch paths?
  //  do this before ack, since some inodes we may have already gotten
  //  from surviving MDSs.
  if (!cap_import_paths.empty()) {
    C_Gather *gather = parallel_fetch(cap_import_paths, cap_imports_missing);
    if (gather) {
      gather->set_finisher(new C_MDC_RejoinGatherFinish(this));
      return;
    }
  }
  
  process_imported_caps();
  choose_lock_states_and_reconnect_caps();

  vector<CInode*> recover_q, check_q;
  identify_files_to_recover(rejoin_recover_q, rejoin_check_q);
  rejoin_send_acks();
  
  // signal completion of fetches, rejoin_gather_finish, etc.
  assert(rejoin_ack_gather.count(mds->whoami));
  rejoin_ack_gather.erase(mds->whoami);

  // did we already get our acks too?
  // this happens when the rejoin_gather has to wait on a MISSING/FULL exchange.
  if (rejoin_ack_gather.empty()) {
    // finally, kickstart past snap parent opens
    open_snap_parents();
  }
}

void MDCache::process_imported_caps()
{
  dout(10) << "process_imported_caps" << dendl;

  // process cap imports
  //  ino -> client -> frommds -> capex
  map<inodeno_t,map<client_t, map<int,ceph_mds_cap_reconnect> > >::iterator p = cap_imports.begin();
  while (p != cap_imports.end()) {
    CInode *in = get_inode(p->first);
    if (!in) {
      dout(10) << "process_imported_caps still missing " << p->first
	       << ", will try again after replayed client requests"
	       << dendl;
      p++;
      continue;
    }
    for (map<client_t, map<int,ceph_mds_cap_reconnect> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) 
      for (map<int,ceph_mds_cap_reconnect>::iterator r = q->second.begin();
	   r != q->second.end();
	   ++r) {
	dout(20) << " add_reconnected_cap " << in->ino() << " client" << q->first << dendl;
	add_reconnected_cap(in, q->first, inodeno_t(r->second.snaprealm));
	rejoin_import_cap(in, q->first, r->second, r->first);
      }
    cap_imports.erase(p++);  // remove and move on
  }
}

void MDCache::check_realm_past_parents(SnapRealm *realm)
{
  // are this realm's parents fully open?
  if (realm->have_past_parents_open()) {
    dout(10) << " have past snap parents for realm " << *realm 
	     << " on " << *realm->inode << dendl;
  } else {
    if (!missing_snap_parents.count(realm->inode)) {
      dout(10) << " MISSING past snap parents for realm " << *realm
	       << " on " << *realm->inode << dendl;
      realm->inode->get(CInode::PIN_OPENINGSNAPPARENTS);
      missing_snap_parents[realm->inode].size();   // just to get it into the map!
    } else {
      dout(10) << " (already) MISSING past snap parents for realm " << *realm 
	       << " on " << *realm->inode << dendl;
    }
  }
}

/*
 * choose lock states based on reconnected caps
 */
void MDCache::choose_lock_states_and_reconnect_caps()
{
  dout(10) << "choose_lock_states_and_reconnect_caps" << dendl;

  map<client_t,MClientSnap*> splits;

  for (hash_map<vinodeno_t,CInode*>::iterator i = inode_map.begin();
       i != inode_map.end();
       ++i) {
    CInode *in = i->second;
 
    if (in->is_auth() && !in->is_base() && in->inode.is_dirty_rstat())
      in->mark_dirty_rstat();

    in->choose_lock_states();
    dout(15) << " chose lock states on " << *in << dendl;

    SnapRealm *realm = in->find_snaprealm();

    check_realm_past_parents(realm);

    map<CInode*,map<client_t,inodeno_t> >::iterator p = reconnected_caps.find(in);
    if (p != reconnected_caps.end()) {

      // also, make sure client's cap is in the correct snaprealm.
      for (map<client_t,inodeno_t>::iterator q = p->second.begin();
	   q != p->second.end();
	   q++) {
	if (q->second == realm->inode->ino()) {
	  dout(15) << "  client" << q->first << " has correct realm " << q->second << dendl;
	} else {
	  dout(15) << "  client" << q->first << " has wrong realm " << q->second
		   << " != " << realm->inode->ino() << dendl;
	  if (realm->have_past_parents_open()) {
	    // ok, include in a split message _now_.
	    prepare_realm_split(realm, q->first, in->ino(), splits);
	  } else {
	    // send the split later.
	    missing_snap_parents[realm->inode][q->first].insert(in->ino());
	  }
	}
      }
    }
  }    
  reconnected_caps.clear();

  send_snaps(splits);
}

void MDCache::prepare_realm_split(SnapRealm *realm, client_t client, inodeno_t ino,
				  map<client_t,MClientSnap*>& splits)
{
  MClientSnap *snap;
  if (splits.count(client) == 0) {
    splits[client] = snap = new MClientSnap(CEPH_SNAP_OP_SPLIT);
    snap->head.split = realm->inode->ino();
    realm->build_snap_trace(snap->bl);

    for (set<SnapRealm*>::iterator p = realm->open_children.begin();
	 p != realm->open_children.end();
	 p++)
      snap->split_realms.push_back((*p)->inode->ino());
    
  } else 
    snap = splits[client]; 
  snap->split_inos.push_back(ino);	
}

void MDCache::send_snaps(map<client_t,MClientSnap*>& splits)
{
  dout(10) << "send_snaps" << dendl;
  
  for (map<client_t,MClientSnap*>::iterator p = splits.begin();
       p != splits.end();
       p++) {
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p->first.v));
    if (session) {
      dout(10) << " client" << p->first
	       << " split " << p->second->head.split
	       << " inos " << p->second->split_inos
	       << dendl;
      mds->send_message_client_counted(p->second, session);
    } else {
      dout(10) << " no session for client" << p->first << dendl;
      p->second->put();
    }
  }
  splits.clear();
}


/*
 * remove any items from logsegment open_file lists that don't have
 * any caps
 */
void MDCache::clean_open_file_lists()
{
  dout(10) << "clean_open_file_lists" << dendl;
  
  for (map<loff_t,LogSegment*>::iterator p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       p++) {
    LogSegment *ls = p->second;
    
    elist<CInode*>::iterator q = ls->open_files.begin(member_offset(CInode, item_open_file));
    while (!q.end()) {
      CInode *in = *q;
      ++q;
      if (!in->is_any_caps_wanted()) {
	dout(10) << " unlisting unwanted/capless inode " << *in << dendl;
	in->item_open_file.remove_myself();
      }
    }
  }
}



void MDCache::rejoin_import_cap(CInode *in, client_t client, ceph_mds_cap_reconnect& icr, int frommds)
{
  dout(10) << "rejoin_import_cap for client" << client << " from mds" << frommds
	   << " on " << *in << dendl;
  Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(client.v));
  assert(session);

  Capability *cap = in->reconnect_cap(client, icr, session);

  if (frommds >= 0)
    do_cap_import(session, in, cap);
}

void MDCache::try_reconnect_cap(CInode *in, Session *session)
{
  client_t client = session->get_client();
  ceph_mds_cap_reconnect *rc = get_replay_cap_reconnect(in->ino(), client);
  if (rc) {
    in->reconnect_cap(client, *rc, session);
    dout(10) << "try_reconnect_cap client" << client
	     << " reconnect wanted " << ccap_string(rc->wanted)
	     << " issue " << ccap_string(rc->issued)
	     << " on " << *in << dendl;
    remove_replay_cap_reconnect(in->ino(), client);

    if (in->is_replicated()) {
      mds->locker->try_eval(in, CEPH_CAP_LOCKS);
    } else {
      in->choose_lock_states();
      dout(15) << " chose lock states on " << *in << dendl;
    }
  }
}



// -------
// cap imports and delayed snap parent opens

void MDCache::do_cap_import(Session *session, CInode *in, Capability *cap)
{
  client_t client = session->inst.name.num();
  SnapRealm *realm = in->find_snaprealm();
  if (realm->have_past_parents_open()) {
    dout(10) << "do_cap_import " << session->inst.name << " mseq " << cap->get_mseq() << " on " << *in << dendl;
    cap->set_last_issue();
    MClientCaps *reap = new MClientCaps(CEPH_CAP_OP_IMPORT,
					in->ino(),
					realm->inode->ino(),
					cap->get_cap_id(), cap->get_last_seq(),
					cap->pending(), cap->wanted(), 0,
					cap->get_mseq());
    in->encode_cap_message(reap, cap);
    realm->build_snap_trace(reap->snapbl);
    mds->send_message_client_counted(reap, session);
  } else {
    dout(10) << "do_cap_import missing past snap parents, delaying " << session->inst.name << " mseq "
	     << cap->get_mseq() << " on " << *in << dendl;
    in->auth_pin(this);
    cap->inc_suppress();
    delayed_imported_caps[client].insert(in);
    missing_snap_parents[in].size();
  }
}

void MDCache::do_delayed_cap_imports()
{
  dout(10) << "do_delayed_cap_imports" << dendl;

  map<client_t,set<CInode*> > d;
  d.swap(delayed_imported_caps);

  for (map<client_t,set<CInode*> >::iterator p = d.begin();
       p != d.end();
       p++) {
    for (set<CInode*>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      CInode *in = *q;
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p->first.v));
      if (session) {
	Capability *cap = in->get_client_cap(p->first);
	if (cap) {
	  do_cap_import(session, in, cap);  // note: this may fail and requeue!
	  cap->dec_suppress();
	}
      }
      in->auth_unpin(this);

      if (in->is_head())
	mds->locker->issue_caps(in);
    }
  }    
}

struct C_MDC_OpenSnapParents : public Context {
  MDCache *mdcache;
  C_MDC_OpenSnapParents(MDCache *c) : mdcache(c) {}
  void finish(int r) {
    mdcache->open_snap_parents();
  }
};

void MDCache::open_snap_parents()
{
  dout(10) << "open_snap_parents" << dendl;
  
  map<client_t,MClientSnap*> splits;
  C_Gather *gather = new C_Gather;

  map<CInode*,map<client_t,set<inodeno_t> > >::iterator p = missing_snap_parents.begin();
  while (p != missing_snap_parents.end()) {
    CInode *in = p->first;
    assert(in->snaprealm);
    if (in->snaprealm->open_parents(gather->new_sub())) {
      dout(10) << " past parents now open on " << *in << dendl;
      
      // include in a (now safe) snap split?
      for (map<client_t,set<inodeno_t> >::iterator q = p->second.begin();
	   q != p->second.end();
	   q++)
	for (set<inodeno_t>::iterator r = q->second.begin();
	     r != q->second.end();
	     r++)
	  prepare_realm_split(in->snaprealm, q->first, *r, splits);

      missing_snap_parents.erase(p++);

      in->put(CInode::PIN_OPENINGSNAPPARENTS);

      // finish off client snaprealm reconnects?
      map<inodeno_t,map<client_t,snapid_t> >::iterator q = reconnected_snaprealms.find(in->ino());
      if (q != reconnected_snaprealms.end()) {
	for (map<client_t,snapid_t>::iterator r = q->second.begin();
	     r != q->second.end();
	     r++)
	  finish_snaprealm_reconnect(r->first, in->snaprealm, r->second);
      	reconnected_snaprealms.erase(q);
      }
    } else {
      dout(10) << " opening past parents on " << *in << dendl;
      p++;
    }
  }

  send_snaps(splits);

  if (gather->get_num()) {
    dout(10) << "open_snap_parents - waiting for " << gather->get_num() << dendl;
    gather->set_finisher(new C_MDC_OpenSnapParents(this));
  } else {
    assert(missing_snap_parents.empty());
    assert(reconnected_snaprealms.empty());
    dout(10) << "open_snap_parents - all open" << dendl;
    do_delayed_cap_imports();
    start_files_to_recover(rejoin_recover_q, rejoin_check_q);

    mds->queue_waiters(rejoin_waiters);

    mds->rejoin_done();
  }
}

void MDCache::finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq)
{
  if (seq < realm->get_newest_seq()) {
    dout(10) << "finish_snaprealm_reconnect client" << client << " has old seq " << seq << " < " 
	     << realm->get_newest_seq()
    	     << " on " << *realm << dendl;
    // send an update
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(client.v));
    if (session) {
      MClientSnap *snap = new MClientSnap(CEPH_SNAP_OP_UPDATE);
      realm->build_snap_trace(snap->bl);
      mds->send_message_client_counted(snap, session);
    } else {
      dout(10) << " ...or not, no session for this client!" << dendl;
    }
  } else {
    dout(10) << "finish_snaprealm_reconnect client" << client << " up to date"
	     << " on " << *realm << dendl;
  }
}



void MDCache::rejoin_send_acks()
{
  dout(7) << "rejoin_send_acks" << dendl;
  
  // send acks to everyone in the recovery set
  map<int,MMDSCacheRejoin*> ack;
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) 
    ack[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
  
  // walk subtrees
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin(); 
       p != subtrees.end();
       p++) {
    CDir *dir = p->first;
    if (!dir->is_auth()) continue;
    dout(10) << "subtree " << *dir << dendl;
    
    // auth items in this subtree
    list<CDir*> dq;
    dq.push_back(dir);

    while (!dq.empty()) {
      CDir *dir = dq.front();
      dq.pop_front();
      
      // dir
      for (map<int,int>::iterator r = dir->replicas_begin();
	   r != dir->replicas_end();
	   ++r) 
	ack[r->first]->add_strong_dirfrag(dir->dirfrag(), r->second, dir->dir_rep);
	   
      for (CDir::map_t::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	CDentry *dn = q->second;
	CDentry::linkage_t *dnl = dn->get_linkage();

	// dentry
	for (map<int,int>::iterator r = dn->replicas_begin();
	     r != dn->replicas_end();
	     ++r) 
	  ack[r->first]->add_strong_dentry(dir->dirfrag(), dn->name, dn->first, dn->last,
					   dnl->is_primary() ? dnl->get_inode()->ino():inodeno_t(0),
					   dnl->is_remote() ? dnl->get_remote_ino():inodeno_t(0),
					   dnl->is_remote() ? dnl->get_remote_d_type():0,
					   r->second,
					   dn->lock.get_replica_state());
	
	if (!dnl->is_primary()) continue;

	// inode
	CInode *in = dnl->get_inode();

	for (map<int,int>::iterator r = in->replicas_begin();
	     r != in->replicas_end();
	     ++r) {
	  ack[r->first]->add_inode_base(in);
	  ack[r->first]->add_inode_locks(in, r->second);
	}
	
	// subdirs in this subtree?
	in->get_nested_dirfrags(dq);
      }
    }
  }

  // base inodes too
  if (root && root->is_auth()) 
    for (map<int,int>::iterator r = root->replicas_begin();
	 r != root->replicas_end();
	 ++r) {
      ack[r->first]->add_inode_base(root);
      ack[r->first]->add_inode_locks(root, r->second);
    }
  if (myin)
    for (map<int,int>::iterator r = myin->replicas_begin();
	 r != myin->replicas_end();
	 ++r) {
      ack[r->first]->add_inode_base(myin);
      ack[r->first]->add_inode_locks(myin, r->second);
    }

  // include inode base for any inodes whose scatterlocks may have updated
  for (set<CInode*>::iterator p = rejoin_potential_updated_scatterlocks.begin();
       p != rejoin_potential_updated_scatterlocks.end();
       p++) {
    CInode *in = *p;
    for (map<int,int>::iterator r = in->replicas_begin();
	 r != in->replicas_end();
	 ++r)
      ack[r->first]->add_inode_base(in);
  }

  // send acks
  for (map<int,MMDSCacheRejoin*>::iterator p = ack.begin();
       p != ack.end();
       ++p) 
    mds->send_message_mds(p->second, p->first);
  
}


void MDCache::reissue_all_caps()
{
  dout(10) << "reissue_all_caps" << dendl;

  for (hash_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    CInode *in = p->second;
    if (in->is_head() && in->is_any_caps()) {
      if (!mds->locker->eval(in, CEPH_CAP_LOCKS))
	mds->locker->issue_caps(in);
    }
  }
}


// ===============================================================================

struct C_MDC_QueuedCow : public Context {
  MDCache *mdcache;
  CInode *in;
  Mutation *mut;
  C_MDC_QueuedCow(MDCache *mdc, CInode *i, Mutation *m) : mdcache(mdc), in(i), mut(m) {}
  void finish(int r) {
    mdcache->_queued_file_recover_cow(in, mut);
  }
};

void MDCache::queue_file_recover(CInode *in)
{
  dout(10) << "queue_file_recover " << *in << dendl;
  assert(in->is_auth());

  // cow?
  SnapRealm *realm = in->find_snaprealm();
  set<snapid_t> s = realm->get_snaps();
  while (!s.empty() && *s.begin() < in->first)
    s.erase(s.begin());
  while (!s.empty() && *s.rbegin() > in->last)
    s.erase(*s.rbegin());
  dout(10) << " snaps in [" << in->first << "," << in->last << "] are " << s << dendl;
  if (s.size() > 1) {
    inode_t *pi = in->project_inode();
    pi->version = in->pre_dirty();

    Mutation *mut = new Mutation;
    mut->ls = mds->mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mds->mdlog, "queue_file_recover cow");
    mds->mdlog->start_entry(le);
    predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);

    s.erase(*s.begin());
    while (s.size()) {
      snapid_t snapid = *s.begin();
      CInode *cow_inode = 0;
      journal_cow_inode(mut, &le->metablob, in, snapid-1, &cow_inode);
      assert(cow_inode);
      _queue_file_recover(cow_inode);
      s.erase(*s.begin());
    }
    
    in->parent->first = in->first;
    le->metablob.add_primary_dentry(in->parent, true, in);
    mds->mdlog->submit_entry(le, new C_MDC_QueuedCow(this, in, mut));
    mds->mdlog->flush();
  }

  _queue_file_recover(in);
}

void MDCache::_queued_file_recover_cow(CInode *in, Mutation *mut)
{
  in->pop_and_dirty_projected_inode(mut->ls);
  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;
}

void MDCache::_queue_file_recover(CInode *in)
{
  dout(15) << "_queue_file_recover " << *in << dendl;
  assert(in->is_auth());
  in->state_clear(CInode::STATE_NEEDSRECOVER);
  in->state_set(CInode::STATE_RECOVERING);
  in->auth_pin(this);
  file_recover_queue.insert(in);
}

void MDCache::unqueue_file_recover(CInode *in)
{
  dout(15) << "unqueue_file_recover " << *in << dendl;
  in->state_clear(CInode::STATE_RECOVERING);
  in->auth_unpin(this);
  file_recover_queue.erase(in);
}

/*
 * called after recovery to recover file sizes for previously opened (for write)
 * files.  that is, those where max_size > size.
 */
void MDCache::identify_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q)
{
  dout(10) << "identify_files_to_recover" << dendl;
  for (hash_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    CInode *in = p->second;
    if (!in->is_auth())
      continue;
    
    bool recover = false;
    for (map<client_t,client_writeable_range_t>::iterator p = in->inode.client_ranges.begin();
	 p != in->inode.client_ranges.end();
	 p++) {
      Capability *cap = in->get_client_cap(p->first);
      if (!cap) {
	dout(10) << " client" << p->first << " has range " << p->second << " but no cap on " << *in << dendl;
	recover = true;
	break;
      }
    }

    if (recover) {
      in->filelock.set_state(LOCK_PRE_SCAN);
      recover_q.push_back(in);
      
      // make sure past parents are open/get opened
      SnapRealm *realm = in->find_snaprealm();
      check_realm_past_parents(realm);
    } else {
      check_q.push_back(in);
    }
  }
}

void MDCache::start_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q)
{
  for (vector<CInode*>::iterator p = check_q.begin(); p != check_q.end(); p++) {
    CInode *in = *p;
    mds->locker->check_inode_max_size(in);
  }
  for (vector<CInode*>::iterator p = recover_q.begin(); p != recover_q.end(); p++) {
    CInode *in = *p;
    mds->locker->file_recover(&in->filelock);
  }
}

struct C_MDC_Recover : public Context {
  MDCache *mdc;
  CInode *in;
  uint64_t size;
  utime_t mtime;
  C_MDC_Recover(MDCache *m, CInode *i) : mdc(m), in(i), size(0) {}
  void finish(int r) {
    mdc->_recovered(in, r, size, mtime);
  }
};

void MDCache::do_file_recover()
{
  dout(10) << "do_file_recover " << file_recover_queue.size() << " queued, "
	   << file_recovering.size() << " recovering" << dendl;

  while (file_recovering.size() < 5 &&
	 !file_recover_queue.empty()) {
    CInode *in = *file_recover_queue.begin();
    file_recover_queue.erase(in);

    inode_t *pi = in->get_projected_inode();

    // blech
    if (pi->client_ranges.size() && !pi->get_max_size()) {
      stringstream ss;
      ss << "bad client_range " << pi->client_ranges << " on ino " << pi->ino;
      mds->logclient.log(LOG_WARN, ss);
    }

    if (pi->client_ranges.size() && pi->get_max_size()) {
      dout(10) << "do_file_recover starting " << in->inode.size << " " << pi->client_ranges
	       << " " << *in << dendl;
      file_recovering.insert(in);
      
      C_MDC_Recover *fin = new C_MDC_Recover(this, in);
      mds->filer->probe(in->inode.ino, &in->inode.layout, in->last,
			pi->get_max_size(), &fin->size, &fin->mtime, false,
			0, fin);    
    } else {
      dout(10) << "do_file_recover skipping " << in->inode.size
	       << " " << *in << dendl;
      in->state_clear(CInode::STATE_NEEDSRECOVER);
      in->auth_unpin(this);
      if (in->filelock.is_stable())
	mds->locker->eval(&in->filelock);
      else
	mds->locker->eval_gather(&in->filelock);
    }
  }
}

void MDCache::_recovered(CInode *in, int r, uint64_t size, utime_t mtime)
{
  dout(10) << "_recovered r=" << r << " size=" << in->inode.size << " mtime=" << in->inode.mtime
	   << " for " << *in << dendl;

  file_recovering.erase(in);
  in->state_clear(CInode::STATE_RECOVERING);

  if (!in->get_parent_dn() && !in->get_projected_parent_dn()) {
    dout(10) << " inode has no parents, killing it off" << dendl;
    in->auth_unpin(this);
    remove_inode(in);
  } else {
    // journal
    mds->locker->check_inode_max_size(in, true, true, size, mtime);
    in->auth_unpin(this);
  }

  do_file_recover();
}

void MDCache::purge_prealloc_ino(inodeno_t ino, Context *fin)
{
  char n[30];
  snprintf(n, sizeof(n), "%llx.%08llx", (long long unsigned)ino, 0ull);
  object_t oid(n);
  object_locator_t oloc(mds->mdsmap->get_metadata_pg_pool());

  dout(10) << "purge_prealloc_ino " << ino << " oid " << oid << dendl;
  SnapContext snapc;
  mds->objecter->remove(oid, oloc, snapc, g_clock.now(), 0, 0, fin);
}  




// ===============================================================================



// ----------------------------
// truncate

void MDCache::truncate_inode(CInode *in, LogSegment *ls)
{
  inode_t *pi = in->get_projected_inode();
  dout(10) << "truncate_inode "
	   << pi->truncate_from << " -> " << pi->truncate_size
	   << " on " << *in
	   << dendl;

  ls->truncating_inodes.insert(in);
  
  _truncate_inode(in, ls);
}

struct C_MDC_TruncateFinish : public Context {
  MDCache *mdc;
  CInode *in;
  LogSegment *ls;
  C_MDC_TruncateFinish(MDCache *c, CInode *i, LogSegment *l) :
    mdc(c), in(i), ls(l) {}
  void finish(int r) {
    mdc->truncate_inode_finish(in, ls);
  }
};

void MDCache::_truncate_inode(CInode *in, LogSegment *ls)
{
  inode_t *pi = &in->inode;
  dout(10) << "_truncate_inode "
	   << pi->truncate_from << " -> " << pi->truncate_size
	   << " on " << *in << dendl;

  in->get(CInode::PIN_TRUNCATING);
  in->auth_pin(this);

  SnapRealm *realm = in->find_snaprealm();
  SnapContext nullsnap;
  const SnapContext *snapc;
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnap;
    assert(in->last == CEPH_NOSNAP);
  }
  dout(10) << "_truncate_inode  snapc " << snapc << " on " << *in << dendl;
  mds->filer->truncate(in->inode.ino, &in->inode.layout, *snapc,
		       pi->truncate_size, pi->truncate_from-pi->truncate_size, pi->truncate_seq, utime_t(), 0,
		       0, new C_MDC_TruncateFinish(this, in, ls));
}

struct C_MDC_TruncateLogged : public Context {
  MDCache *mdc;
  CInode *in;
  Mutation *mut;
  C_MDC_TruncateLogged(MDCache *m, CInode *i, Mutation *mu) : mdc(m), in(i), mut(mu) {}
  void finish(int r) {
    mdc->truncate_inode_logged(in, mut);
  }
};

void MDCache::truncate_inode_finish(CInode *in, LogSegment *ls)
{
  dout(10) << "truncate_inode_finish " << *in << dendl;

  ls->truncating_inodes.erase(in);

  // update
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->truncate_from = 0;
  pi->truncate_pending--;

  Mutation *mut = new Mutation;
  mut->ls = mds->mdlog->get_current_segment();
  mut->add_projected_inode(in);

  EUpdate *le = new EUpdate(mds->mdlog, "truncate finish");
  mds->mdlog->start_entry(le);
  le->metablob.add_dir_context(in->get_parent_dir());
  le->metablob.add_primary_dentry(in->get_projected_parent_dn(), true, in);
  le->metablob.add_truncate_finish(in->ino(), ls->offset);

  journal_dirty_inode(mut, &le->metablob, in);
  mds->mdlog->submit_entry(le, new C_MDC_TruncateLogged(this, in, mut));

  // flush immediately if there are readers/writers waiting
  if (in->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR))
    mds->mdlog->flush();
}

void MDCache::truncate_inode_logged(CInode *in, Mutation *mut)
{
  dout(10) << "truncate_inode_logged " << *in << dendl;
  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  in->put(CInode::PIN_TRUNCATING);
  in->auth_unpin(this);

  list<Context*> waiters;
  in->take_waiting(CInode::WAIT_TRUNC, waiters);
  mds->queue_waiters(waiters);
}


void MDCache::add_recovered_truncate(CInode *in, LogSegment *ls)
{
  ls->truncating_inodes.insert(in);
}

void MDCache::start_recovered_truncates()
{
  dout(10) << "start_recovered_truncates" << dendl;
  for (map<loff_t,LogSegment*>::iterator p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       p++) {
    LogSegment *ls = p->second;
    for (set<CInode*>::iterator q = ls->truncating_inodes.begin();
	 q != ls->truncating_inodes.end();
	 q++)
      _truncate_inode(*q, ls);
  }
}






// ================================================================================
// cache trimming


/*
 * note: only called while MDS is active or stopping... NOT during recovery.
 * however, we may expire a replica whose authority is recovering.
 * 
 */
bool MDCache::trim(int max) 
{
  // trim LRU
  if (max < 0) {
    max = g_conf.mds_cache_size;
    if (!max) return false;
  }
  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << dendl;

  map<int, MCacheExpire*> expiremap;

  // trim dentries from the LRU
  while (lru.lru_get_size() > (unsigned)max) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    trim_dentry(dn, expiremap);
  }

  // trim root?
  if (max == 0 && root) {
    list<CDir*> ls;
    root->get_dirfrags(ls);
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CDir *dir = *p;
      if (dir->get_num_ref() == 1)  // subtree pin
	trim_dirfrag(dir, 0, expiremap);
    }
    if (root->get_num_ref() == 0)
      trim_inode(0, root, 0, expiremap);
  }

  // send any expire messages
  send_expire_messages(expiremap);

  return true;
}

void MDCache::send_expire_messages(map<int, MCacheExpire*>& expiremap)
{
  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << dendl;
    mds->send_message_mds(it->second, it->first);
  }
}


void MDCache::trim_dentry(CDentry *dn, map<int, MCacheExpire*>& expiremap)
{
  dout(12) << "trim_dentry " << *dn << dendl;
  
  CDentry::linkage_t *dnl = dn->get_linkage();

  CDir *dir = dn->get_dir();
  assert(dir);
  
  CDir *con = get_subtree_root(dir);
  if (con)
    dout(12) << " in container " << *con << dendl;
  else {
    dout(12) << " no container; under a not-yet-linked dir" << dendl;
    assert(dn->is_auth());
  }

  // notify dentry authority?
  if (!dn->is_auth()) {
    pair<int,int> auth = dn->authority();
    
    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.
      
      dout(12) << "  sending expire to mds" << a << " on " << *dn << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dentry(con->dirfrag(), dir->dirfrag(), dn->name, dn->last, dn->get_replica_nonce());
    }
  }

  // adjust the dir state
  // NOTE: we can safely remove a clean, null dentry without effecting
  //       directory completeness.
  // (do this _before_ we unlink the inode, below!)
  if (!(dnl->is_null() && dn->is_clean())) 
    dir->state_clear(CDir::STATE_COMPLETE); 
  
  // unlink the dentry
  if (dnl->is_remote()) {
    // just unlink.
    dir->unlink_inode(dn);
  } 
  else if (dnl->is_primary()) {
    // expire the inode, too.
    CInode *in = dnl->get_inode();
    assert(in);
    trim_inode(dn, in, con, expiremap);
  } 
  else {
    assert(dnl->is_null());
  }
    
  // remove dentry
  dir->add_to_bloom(dn);
  dir->remove_dentry(dn);
  
  // reexport?
  if (dir->get_num_head_items() == 0 && dir->is_subtree_root())
    migrator->export_empty_import(dir);
  
  if (mds->logger) mds->logger->inc(l_mds_iex);
}


void MDCache::trim_dirfrag(CDir *dir, CDir *con, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_dirfrag " << *dir << dendl;

  if (dir->is_subtree_root()) {
    assert(!dir->is_auth() ||
	   (!dir->is_replicated() && dir->inode->is_base()));
    remove_subtree(dir);	// remove from subtree map
  }
  assert(dir->get_num_ref() == 0);

  CInode *in = dir->get_inode();

  if (!dir->is_auth()) {
    pair<int,int> auth = dir->authority();
    
    // was this an auth delegation?  (if so, slightly modified container)
    dirfrag_t condf;
    if (dir->is_subtree_root()) {
      dout(12) << " subtree root, container is " << *dir << dendl;
      con = dir;
      condf = dir->dirfrag();
    } else {
      condf = con->dirfrag();
    }
      
    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds" << a << " on   " << *dir << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dir(condf, dir->dirfrag(), dir->replica_nonce);
    }
  }
  
  in->close_dirfrag(dir->dirfrag().frag);
}

void MDCache::trim_inode(CDentry *dn, CInode *in, CDir *con, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_inode " << *in << dendl;
  assert(in->get_num_ref() == 0);
    
  // DIR
  list<CDir*> dfls;
  in->get_dirfrags(dfls);
  for (list<CDir*>::iterator p = dfls.begin();
       p != dfls.end();
       ++p) 
    trim_dirfrag(*p, con ? con:*p, expiremap);  // if no container (e.g. root dirfrag), use *p
  
  // INODE
  if (!in->is_auth()) {
    pair<int,int> auth = in->authority();
    
    dirfrag_t df;
    if (con)
      df = con->dirfrag();
    else
      df = dirfrag_t(0,frag_t());   // must be a root or stray inode.

    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (con && mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds" << a << " on " << *in << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_inode(df, in->vino(), in->get_replica_nonce());
    }
  }

  /*
  if (in->is_auth()) {
    if (in->hack_accessed)
      mds->logger->inc("outt");
    else {
      mds->logger->inc("outut");
      mds->logger->favg("oututl", g_clock.now() - in->hack_load_stamp);
    }
  }
  */
    
  // unlink
  if (dn)
    dn->get_dir()->unlink_inode(dn);
  remove_inode(in);
}


/**
 * trim_non_auth - remove any non-auth items from our cache
 *
 * this reduces the amount of non-auth metadata in our cache, reducing the 
 * load incurred by the rejoin phase.
 *
 * the only non-auth items that remain are those that are needed to 
 * attach our own subtrees to the root.  
 *
 * when we are done, all dentries will be in the top bit of the lru.
 *
 * why we have to do this:
 *  we may not have accurate linkage for non-auth items.  which means we will 
 *  know which subtree it falls into, and can not be sure to declare it to the
 *  correct authority.  
 */
void MDCache::trim_non_auth()
{
  dout(7) << "trim_non_auth" << dendl;
  stringstream warn_string_dirs;
  
  // temporarily pin all subtree roots
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) 
    p->first->get(CDir::PIN_SUBTREETEMP);

  // note first auth item we see.  
  // when we see it the second time, stop.
  CDentry *first_auth = 0;
  
  // trim non-auth items from the lru
  while (lru.lru_get_size() > 0) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    CDentry::linkage_t *dnl = dn->get_linkage();

    if (dn->is_auth()) {
      // add back into lru (at the top)
      lru.lru_insert_top(dn);

      if (!first_auth) {
	first_auth = dn;
      } else {
	if (first_auth == dn) 
	  break;
      }
    } else {
      // non-auth.  expire.
      CDir *dir = dn->get_dir();
      assert(dir);

      // unlink the dentry
      dout(15) << "trim_non_auth removing " << *dn << dendl;
      if (dnl->is_remote()) {
	dir->unlink_inode(dn);
      } 
      else if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	list<CDir*> ls;
        warn_string_dirs << in->get_parent_dn()->get_name() << std::endl;
	in->get_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
	  CDir *subdir = *p;
	  warn_string_dirs << subdir->get_inode()->get_parent_dn()->get_name()
	                   << std::endl;
	  if (subdir->is_subtree_root()) 
	    remove_subtree(subdir);
	  in->close_dirfrag(subdir->dirfrag().frag);
	}
	dir->unlink_inode(dn);
	remove_inode(in);
      } 
      else {
	assert(dnl->is_null());
      }
      dir->add_to_bloom(dn);
      dir->remove_dentry(dn);
      
      // adjust the dir state
      dir->state_clear(CDir::STATE_COMPLETE);  // dir incomplete!
    }
  }

  // move everything in the pintail to the top bit of the lru.
  lru.lru_touch_entire_pintail();

  // unpin all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) 
    p->first->put(CDir::PIN_SUBTREETEMP);

  if (lru.lru_get_size() == 0) {
    // root, stray, etc.?
    hash_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
    while (p != inode_map.end()) {
      hash_map<vinodeno_t,CInode*>::iterator next = p;
      ++next;
      CInode *in = p->second;
      if (!in->is_auth()) {
	list<CDir*> ls;
	in->get_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin();
	     p != ls.end();
	     ++p) {
	  dout(0) << " ... " << **p << dendl;
	  warn_string_dirs << (*p)->get_inode()->get_parent_dn()->get_name()
	                   << std::endl;
	  assert((*p)->get_num_ref() == 1);  // SUBTREE
	  remove_subtree((*p));
	  in->close_dirfrag((*p)->dirfrag().frag);
	}
	dout(0) << " ... " << *in << dendl;
	warn_string_dirs << in->get_parent_dn()->get_name() << std::endl;
	assert(in->get_num_ref() == 0);
	remove_inode(in);
      }
      p = next;
    }
  }

  show_subtrees();
  if (warn_string_dirs.peek() != EOF) {
    stringstream warn_string;
    warn_string << "trim_non_auth has deleted paths: " << std::endl;
    warn_string << warn_string_dirs;
    mds->logclient.log(LOG_INFO, warn_string);
  }
}

/**
 * Recursively trim the subtree rooted at directory to remove all
 * CInodes/CDentrys/CDirs that aren't links to remote MDSes, or ancestors
 * of those links. This is used to clear invalid data out of the cache.
 * Note that it doesn't clear the passed-in directory, since that's not
 * always safe.
 */
bool MDCache::trim_non_auth_subtree(CDir *directory)
{
  dout(10) << "trim_non_auth_subtree " << directory << dendl;
  bool keep_directory = false;
  CDir::map_t::iterator j = directory->begin();
  CDir::map_t::iterator i = j;
  while (j != directory->end()) {
    i = j++;
    CDentry *dn = i->second;
    dout(10) << "Checking dentry " << dn << dendl;
    CDentry::linkage_t *dnl = dn->get_linkage();
    if (dnl->is_primary()) { // check for subdirectories, etc
      CInode *in = dnl->get_inode();
      bool keep_inode = false;
      if (in->is_dir()) {
        list<CDir*> subdirs;
        in->get_dirfrags(subdirs);
        for (list<CDir*>::iterator subdir = subdirs.begin();
            subdir != subdirs.end();
            ++subdir) {
          if ((*subdir)->is_subtree_root()) {
            keep_inode = true;
            dout(10) << "subdir " << *subdir << "is kept!" << dendl;
          }
          else {
            if (trim_non_auth_subtree(*subdir))
              keep_inode = true;
            else {
              in->close_dirfrag((*subdir)->get_frag());
              directory->state_clear(CDir::STATE_COMPLETE);  // now incomplete!
            }
          }
        }

      }
      if (!keep_inode) { // remove it!
        dout(20) << "removing inode " << in << " with dentry" << dn << dendl;
        directory->unlink_inode(dn);
        remove_inode(in);
        directory->add_to_bloom(dn);
        directory->remove_dentry(dn);
      } else {
        dout(20) << "keeping inode " << in << "with dentry " << dn <<dendl;
        keep_directory = true;
      }
    } else { // just remove it
      dout(20) << "removing dentry " << dn << dendl;
      if (dnl->is_remote())
        directory->unlink_inode(dn);
      directory->remove_dentry(dn);
    }
  }
  /**
   * We've now checked all our children and deleted those that need it.
   * Now return to caller, and tell them if *we're* a keeper.
   */
  return keep_directory;
}

/* This function DOES put the passed message before returning */
void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  
  dout(7) << "cache_expire from mds" << from << dendl;

  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    m->put();
    return;
  }

  // loop over realms
  for (map<dirfrag_t,MCacheExpire::realm>::iterator p = m->realms.begin();
       p != m->realms.end();
       ++p) {
    // check container?
    if (p->first.ino > 0) {
      CInode *coni = get_inode(p->first.ino);
      assert(coni);  // we had better have this.
      CDir *con = coni->get_approx_dirfrag(p->first.frag);
      assert(con);
      
      if (!con->is_auth() ||
	  (con->is_auth() && con->is_exporting() &&
	   migrator->get_export_state(con) == Migrator::EXPORT_WARNING &&
	   migrator->export_has_warned(con,from))) {
	// not auth.
	dout(7) << "delaying nonauth|warned expires for " << *con << dendl;
	assert(con->is_frozen_tree_root());
	
	// make a message container
	if (delayed_expire[con].count(from) == 0) 
	  delayed_expire[con][from] = new MCacheExpire(from);
	
	// merge these expires into it
	delayed_expire[con][from]->add_realm(p->first, p->second);
	continue;
      }
      dout(7) << "expires for " << *con << dendl;
    } else {
      dout(7) << "containerless expires (root, stray inodes)" << dendl;
    }

    // INODES
    for (map<vinodeno_t,int>::iterator it = p->second.inodes.begin();
	 it != p->second.inodes.end();
	 it++) {
      CInode *in = get_inode(it->first);
      int nonce = it->second;
      
      if (!in) {
	dout(0) << " inode expire on " << it->first << " from " << from 
		<< ", don't have it" << dendl;
	assert(in);
      }        
      assert(in->is_auth());
      
      // check nonce
      if (nonce == in->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " inode expire on " << *in << " from mds" << from 
		<< " cached_by was " << in->get_replicas() << dendl;
	inode_remove_replica(in, from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " inode expire on " << *in << " from mds" << from
		<< " with old nonce " << nonce
		<< " (current " << in->get_replica_nonce(from) << "), dropping" 
		<< dendl;
      }
    }
    
    // DIRS
    for (map<dirfrag_t,int>::iterator it = p->second.dirs.begin();
	 it != p->second.dirs.end();
	 it++) {
      CDir *dir = get_dirfrag(it->first);
      int nonce = it->second;
      
      if (!dir) {
	dout(0) << " dir expire on " << it->first << " from " << from 
		<< ", don't have it" << dendl;
	assert(dir);
      }  
      assert(dir->is_auth());
      
      // check nonce
      if (nonce == dir->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " dir expire on " << *dir << " from mds" << from
		<< " replicas was " << dir->replica_map << dendl;
	dir->remove_replica(from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " dir expire on " << *dir << " from mds" << from 
		<< " with old nonce " << nonce << " (current " << dir->get_replica_nonce(from)
		<< "), dropping" << dendl;
      }
    }
    
    // DENTRIES
    for (map<dirfrag_t, map<pair<string,snapid_t>,int> >::iterator pd = p->second.dentries.begin();
	 pd != p->second.dentries.end();
	 ++pd) {
      dout(10) << " dn expires in dir " << pd->first << dendl;
      CInode *diri = get_inode(pd->first.ino);
      assert(diri);
      CDir *dir = diri->get_dirfrag(pd->first.frag);
      
      if (!dir) {
	dout(0) << " dn expires on " << pd->first << " from " << from
		<< ", must have refragmented" << dendl;
      } else {
	assert(dir->is_auth());
      }
      
      for (map<pair<string,snapid_t>,int>::iterator p = pd->second.begin();
	   p != pd->second.end();
	   ++p) {
	int nonce = p->second;
	CDentry *dn;
	
	if (dir) {
	  dn = dir->lookup(p->first.first, p->first.second);
	} else {
	  // which dirfrag for this dentry?
	  CDir *dir = diri->get_dirfrag(diri->pick_dirfrag(p->first.first));
	  assert(dir->is_auth());
	  dn = dir->lookup(p->first.first, p->first.second);
	} 

	if (!dn) 
	  dout(0) << "  missing dentry for " << p->first.first << " snap " << p->first.second << " in " << *dir << dendl;
	assert(dn);
	
	if (nonce == dn->get_replica_nonce(from)) {
	  dout(7) << "  dentry_expire on " << *dn << " from mds" << from << dendl;
	  dentry_remove_replica(dn, from);
	} 
	else {
	  dout(7) << "  dentry_expire on " << *dn << " from mds" << from
		  << " with old nonce " << nonce << " (current " << dn->get_replica_nonce(from)
		  << "), dropping" << dendl;
	}
      }
    }
  }


  // done
  m->put();
}

void MDCache::process_delayed_expire(CDir *dir)
{
  dout(7) << "process_delayed_expire on " << *dir << dendl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[dir].begin();
       p != delayed_expire[dir].end();
       ++p) 
    handle_cache_expire(p->second);
  delayed_expire.erase(dir);  
}

void MDCache::discard_delayed_expire(CDir *dir)
{
  dout(7) << "discard_delayed_expire on " << *dir << dendl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[dir].begin();
       p != delayed_expire[dir].end();
       ++p) 
    p->second->put();
  delayed_expire.erase(dir);  
}

void MDCache::inode_remove_replica(CInode *in, int from)
{
  in->remove_replica(from);
  in->mds_caps_wanted.erase(from);
  
  // note: this code calls _eval more often than it needs to!
  // fix lock
  if (in->authlock.remove_replica(from)) mds->locker->eval_gather(&in->authlock);
  if (in->linklock.remove_replica(from)) mds->locker->eval_gather(&in->linklock);
  if (in->dirfragtreelock.remove_replica(from)) mds->locker->eval_gather(&in->dirfragtreelock);
  if (in->filelock.remove_replica(from)) mds->locker->eval_gather(&in->filelock);
  if (in->snaplock.remove_replica(from)) mds->locker->eval_gather(&in->snaplock);
  if (in->xattrlock.remove_replica(from)) mds->locker->eval_gather(&in->xattrlock);

  if (in->nestlock.remove_replica(from)) mds->locker->eval_gather(&in->nestlock);
  if (in->flocklock.remove_replica(from)) mds->locker->eval_gather(&in->flocklock);
  if (in->policylock.remove_replica(from)) mds->locker->eval_gather(&in->policylock);

  // trim?
  maybe_eval_stray(in);
}

void MDCache::dentry_remove_replica(CDentry *dn, int from)
{
  dn->remove_replica(from);

  // fix lock
  if (dn->lock.remove_replica(from))
    mds->locker->eval_gather(&dn->lock);
}



void MDCache::trim_client_leases()
{
  utime_t now = g_clock.now();
  
  dout(10) << "trim_client_leases" << dendl;

  for (int pool=0; pool<client_lease_pools; pool++) {
    int before = client_leases[pool].size();
    if (client_leases[pool].empty()) 
      continue;

    while (!client_leases[pool].empty()) {
      ClientLease *r = client_leases[pool].front();
      if (r->ttl > now) break;
      CDentry *dn = (CDentry*)r->parent;
      dout(10) << " expiring client" << r->client << " lease of " << *dn << dendl;
      dn->remove_client_lease(r, mds->locker);
    }
    int after = client_leases[pool].size();
    dout(10) << "trim_client_leases pool " << pool << " trimmed "
	     << (before-after) << " leases, " << after << " left" << dendl;
  }
}


void MDCache::check_memory_usage()
{
  static MemoryModel mm;
  static MemoryModel::snap last;
  mm.sample(&last);
  static MemoryModel::snap baseline = last;

  // check client caps
  int num_inodes = inode_map.size();
  float caps_per_inode = (float)num_caps / (float)num_inodes;
  //float cap_rate = (float)num_inodes_with_caps / (float)inode_map.size();

  dout(2) << "check_memory_usage"
	   << " total " << last.get_total()
	   << ", rss " << last.get_rss()
	   << ", heap " << last.get_heap()
	   << ", malloc " << last.malloc << " mmap " << last.mmap
	   << ", baseline " << baseline.get_heap()
	   << ", buffers " << (buffer_total_alloc.read() >> 10)
	   << ", max " << g_conf.mds_mem_max
	   << ", " << num_inodes_with_caps << " / " << inode_map.size() << " inodes have caps"
	   << ", " << num_caps << " caps, " << caps_per_inode << " caps per inode"
	   << dendl;

  mds->mlogger->set(l_mdm_rss, last.get_rss());
  mds->mlogger->set(l_mdm_heap, last.get_heap());
  mds->mlogger->set(l_mdm_malloc, last.malloc);

  /*int size = last.get_total();
  if (size > g_conf.mds_mem_max * .9) {
    float ratio = (float)g_conf.mds_mem_max * .9 / (float)size;
    if (ratio < 1.0)
      mds->server->recall_client_state(ratio);
  } else 
    */
  if (num_inodes_with_caps > g_conf.mds_cache_size) {
    float ratio = (float)g_conf.mds_cache_size * .9 / (float)num_inodes_with_caps;
    if (ratio < 1.0)
      mds->server->recall_client_state(ratio);
  }

}



// =========================================================================================
// shutdown

class C_MDC_ShutdownCheck : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCheck(MDCache *m) : mdc(m) {}
  void finish(int) {
    mdc->shutdown_check();
  }
};

void MDCache::shutdown_check()
{
  dout(0) << "shutdown_check at " << g_clock.now() << dendl;

  // cache
  int o = g_conf.debug_mds;
  g_conf.debug_mds = 10;
  show_cache();
  g_conf.debug_mds = o;
  mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << dendl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << dendl;


  if (mds->objecter->is_active()) {
    dout(0) << "objecter still active" << dendl;
    mds->objecter->dump_active();
  }
}


void MDCache::shutdown_start()
{
  dout(2) << "shutdown_start" << dendl;

  if (g_conf.mds_shutdown_check)
    mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  //  g_conf.debug_mds = 10;
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << dendl;

  if (mds->is_stopped()) {
    dout(7) << " already shut down" << dendl;
    show_cache();
    show_subtrees();
    return true;
  }

  // close out any sessions (and open files!) before we try to trim the log, etc.
  if (!mds->server->terminating_sessions &&
      mds->sessionmap.have_unclosed_sessions()) {
    mds->server->terminate_sessions();
    return false;
  }


  // flush what we can from the log
  mds->mdlog->trim(0);

  if (mds->mdlog->get_num_segments() > 1) {
    dout(7) << "still >1 segments, waiting for log to trim" << dendl;
    return false;
  }

  // empty stray dir
  if (!shutdown_export_strays()) {
    dout(7) << "waiting for strays to migrate" << dendl;
    return false;
  }
  
  // drop our reference to our stray dir inode
  static bool did_stray_put = false;
  if (!did_stray_put && stray) {
    did_stray_put = true;
    stray->put(CInode::PIN_STRAY);
  }

  // trim cache
  trim(0);
  dout(5) << "lru size now " << lru.lru_get_size() << dendl;

  // SUBTREES
  if (!subtrees.empty() &&
      mds->get_nodeid() != 0 && 
      !migrator->is_exporting() //&&
      //!migrator->is_importing()
      ) {
    dout(7) << "looking for subtrees to export to mds0" << dendl;
    list<CDir*> ls;
    for (map<CDir*, set<CDir*> >::iterator it = subtrees.begin();
         it != subtrees.end();
         it++) {
      CDir *dir = it->first;
      if (dir->get_inode()->is_mdsdir())
	continue;
      if (dir->is_frozen() || dir->is_freezing())
	continue;
      if (!dir->is_full_dir_auth())
	continue;
      ls.push_back(dir);
    }
    int max = 5; // throttle shutdown exports.. hack!
    for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CDir *dir = *p;
      int dest = dir->get_inode()->authority().first;
      if (dest > 0 && !mds->mdsmap->is_active(dest))
	dest = 0;
      dout(7) << "sending " << *dir << " back to mds" << dest << dendl;
      migrator->export_dir(dir, dest);
      if (--max == 0)
	break;
    }
  }

  if (!shutdown_export_caps()) {
    dout(7) << "waiting for residual caps to export" << dendl;
    return false;
  }

  // make mydir subtree go away
  if (myin) {
    CDir *mydir = myin->get_dirfrag(frag_t());
    if (mydir && mydir->is_subtree_root()) {
      adjust_subtree_auth(mydir, CDIR_AUTH_UNKNOWN);
      remove_subtree(mydir);
    }
  }
  
  // subtrees map not empty yet?
  if (!subtrees.empty()) {
    dout(7) << "still have " << num_subtrees() << " subtrees" << dendl;
    show_subtrees();
    migrator->show_importing();
    migrator->show_exporting();
    if (!migrator->is_importing() && !migrator->is_exporting())
      show_cache();
    return false;
  }
  assert(subtrees.empty());
  assert(!migrator->is_exporting());
  assert(!migrator->is_importing());

  // (only do this once!)
  if (!mds->mdlog->is_capped()) {
    dout(7) << "capping the log" << dendl;
    mds->mdlog->cap();
    mds->mdlog->trim();
  }
  
  if (!mds->mdlog->empty()) {
    dout(7) << "waiting for log to flush.. " << mds->mdlog->get_num_events() 
	    << " in " << mds->mdlog->get_num_segments() << " segments" << dendl;
    return false;
  }
  
  if (!did_shutdown_log_cap) {
    // flush journal header
    dout(7) << "writing header for (now-empty) journal" << dendl;
    assert(mds->mdlog->empty());
    mds->mdlog->write_head(0);  
    // NOTE: filer active checker below will block us until this completes.
    did_shutdown_log_cap = true;
    return false;
  }

  // filer active?
  if (mds->objecter->is_active()) {
    dout(7) << "objecter still active" << dendl;
    mds->objecter->dump_active();
    return false;
  }

  // trim what we can from the cache
  if (lru.lru_get_size() > 0) {
    dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << dendl;
    show_cache();
    //dump();
    return false;
  } 
  
  // done!
  dout(2) << "shutdown done." << dendl;
  return true;
}

bool MDCache::shutdown_export_strays()
{
  if (mds->get_nodeid() == 0) return true;
  if (!stray) return true;
  
  dout(10) << "shutdown_export_strays" << dendl;

  bool done = true;
  static set<inodeno_t> exported_strays;
  list<CDir*> dfs;

  stray->get_dirfrags(dfs);
  while (!dfs.empty()) {
    CDir *dir = dfs.front();
    dfs.pop_front();

    if (!dir->is_complete()) {
      dir->fetch(0);
      done = false;
    }
    
    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 p++) {
      CDentry *dn = p->second;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_null()) continue;
      done = false;
      
      // FIXME: we'll deadlock if a rename fails.
      if (exported_strays.count(dnl->get_inode()->ino()) == 0) {
	exported_strays.insert(dnl->get_inode()->ino());
	migrate_stray(dn, mds->get_nodeid(), 0);  // send to root!
      } else {
	dout(10) << "already exporting " << *dn << dendl;
      }
    }
  }

  return done;
}

bool MDCache::shutdown_export_caps()
{
  // export caps?
  //  note: this runs more often than it should.
  static bool exported_caps = false;
  static set<CDir*> exported_caps_in;
  if (!exported_caps) {
    dout(7) << "searching for caps to export" << dendl;
    exported_caps = true;

    list<CDir*> dirq;
    for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      if (exported_caps_in.count(p->first)) continue;
      if (p->first->is_auth() ||
	  p->first->is_ambiguous_auth())
	exported_caps = false; // we'll have to try again
      else {
	dirq.push_back(p->first);
	exported_caps_in.insert(p->first);
      }
    }
    while (!dirq.empty()) {
      CDir *dir = dirq.front();
      dirq.pop_front();
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (!dnl->is_primary()) continue;
	CInode *in = dnl->get_inode();
	if (in->is_dir())
	  in->get_nested_dirfrags(dirq);
	if (in->is_any_caps() && !in->state_test(CInode::STATE_EXPORTINGCAPS))
	  migrator->export_caps(in);
      }
    }
  }

  return true;
}





// ========= messaging ==============

/* This function DOES put the passed message before returning */
void MDCache::dispatch(Message *m)
{
  switch (m->get_type()) {

    // RESOLVE
  case MSG_MDS_RESOLVE:
    handle_resolve((MMDSResolve*)m);
    break;
  case MSG_MDS_RESOLVEACK:
    handle_resolve_ack((MMDSResolveAck*)m);
    break;

    // REJOIN
  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin((MMDSCacheRejoin*)m);
    break;

  case MSG_MDS_DISCOVER:
    handle_discover((MDiscover*)m);
    break;
  case MSG_MDS_DISCOVERREPLY:
    handle_discover_reply((MDiscoverReply*)m);
    break;

  case MSG_MDS_DIRUPDATE:
    handle_dir_update((MDirUpdate*)m);
    break;

  case MSG_MDS_CACHEEXPIRE:
    handle_cache_expire((MCacheExpire*)m);
    break;



  case MSG_MDS_DENTRYLINK:
    handle_dentry_link((MDentryLink*)m);
    break;
  case MSG_MDS_DENTRYUNLINK:
    handle_dentry_unlink((MDentryUnlink*)m);
    break;


  case MSG_MDS_FRAGMENTNOTIFY:
    handle_fragment_notify((MMDSFragmentNotify*)m);
    break;
    

    
  default:
    dout(7) << "cache unknown message " << m->get_type() << dendl;
    assert(0);
    m->put();
    break;
  }
}


/* path_traverse
 *
 * return values:
 *   <0 : traverse error (ENOTDIR, ENOENT, etc.)
 *    0 : success
 *   >0 : delayed or forwarded
 *
 * onfail values:
 *
 *  MDS_TRAVERSE_FORWARD       - forward to auth (or best guess)
 *  MDS_TRAVERSE_DISCOVER      - discover missing items.  skip permission checks.
 *  MDS_TRAVERSE_DISCOVERXLOCK - discover XLOCKED items too (be careful!).
 *  MDS_TRAVERSE_FAIL          - return an error
 */

Context *MDCache::_get_waiter(MDRequest *mdr, Message *req)
{
  if (mdr) {
    dout(20) << "_get_waiter retryrequest" << dendl;
    return new C_MDS_RetryRequest(this, mdr);
  } else {
    dout(20) << "_get_waiter retrymessage" << dendl;
    return new C_MDS_RetryMessage(mds, req);
  }
}

/*
 * Returns 0 on success, >0 if request has been put on hold or otherwise dealt with,
 * <0 if there's been a failure the caller needs to clean up from.
 *
 * on succes, @pdnvec points to a vector of dentries we traverse.  
 * on failure, @pdnvec it is either the full trace, up to and
 *             including the final null dn, or empty.
 */
int MDCache::path_traverse(MDRequest *mdr, Message *req,     // who
			   const filepath& path,                   // what
                           vector<CDentry*> *pdnvec,         // result
			   CInode **pin,
                           int onfail)
{
  assert(mdr || req);
  bool null_okay = (onfail == MDS_TRAVERSE_DISCOVERXLOCK);
  bool forward = (onfail == MDS_TRAVERSE_FORWARD);

  snapid_t snapid = CEPH_NOSNAP;
  if (mdr)
    mdr->snapid = snapid;

  client_t client = (mdr && mdr->reqid.name.is_client()) ? mdr->reqid.name.num() : -1;

  if (mds->logger) mds->logger->inc(l_mds_t);

  dout(7) << "traverse: opening base ino " << path.get_ino() << " snap " << snapid << dendl;
  CInode *cur = get_inode(path.get_ino());
  if (cur == NULL) {
    if (MDS_INO_IS_MDSDIR(path.get_ino())) 
      open_foreign_mdsdir(path.get_ino(), _get_waiter(mdr, req));
    else {
      //assert(0);  // hrm.. broken
      return -ESTALE;
    }
    return 1;
  }
  if (cur->state_test(CInode::STATE_PURGING))
    return -ESTALE;

  // start trace
  if (pdnvec)
    pdnvec->clear();
  if (pin)
    *pin = cur;

  unsigned depth = 0;
  while (depth < path.depth()) {
    dout(12) << "traverse: path seg depth " << depth << " '" << path[depth]
	     << "' snapid " << snapid << dendl;
    
    if (!cur->is_dir()) {
      dout(7) << "traverse: " << *cur << " not a dir " << dendl;
      return -ENOTDIR;
    }

    // walk into snapdir?
    if (path[depth].length() == 0) {
      dout(10) << "traverse: snapdir" << dendl;
      if (!mdr)
	return -EINVAL;
      snapid = CEPH_SNAPDIR;
      mdr->snapid = snapid;
      depth++;
      continue;
    }
    // walk thru snapdir?
    if (snapid == CEPH_SNAPDIR) {
      if (!mdr)
	return -EINVAL;
      SnapRealm *realm = cur->find_snaprealm();
      snapid = realm->resolve_snapname(path[depth], cur->ino());
      dout(10) << "traverse: snap " << path[depth] << " -> " << snapid << dendl;
      if (!snapid)
	return -ENOENT;
      mdr->snapid = snapid;
      depth++;
      continue;
    }

    // open dir
    frag_t fg = cur->pick_dirfrag(path[depth]);
    CDir *curdir = cur->get_dirfrag(fg);
    if (!curdir) {
      if (cur->is_auth()) {
        // parent dir frozen_dir?
        if (cur->is_frozen_dir()) {
          dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << dendl;
          cur->get_parent_dn()->get_dir()->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req));
          return 1;
        }
        curdir = cur->get_or_open_dirfrag(this, fg);
      } else {
        // discover?
	dout(10) << "traverse: need dirfrag " << fg << ", doing discover from " << *cur << dendl;
	discover_path(cur, snapid, path.postfixpath(depth), _get_waiter(mdr, req),
		      onfail == MDS_TRAVERSE_DISCOVERXLOCK);
	if (mds->logger) mds->logger->inc(l_mds_tdis);
        return 1;
      }
    }
    assert(curdir);

#ifdef MDS_VERIFY_FRAGSTAT
    if (curdir->is_complete())
      curdir->verify_fragstat();
#endif

    // frozen?
    /*
    if (curdir->is_frozen()) {
    // doh!
      // FIXME: traverse is allowed?
      dout(7) << "traverse: " << *curdir << " is frozen, waiting" << dendl;
      curdir->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req));
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
#if 0    
    if (!noperm && 
	!mds->locker->rdlock_try(&cur->authlock, client, 0)) {
      dout(7) << "traverse: waiting on authlock rdlock on " << *cur << dendl;
      cur->authlock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req));
      return 1;
    }
#endif
    

    // make sure snaprealm parents are open...
    if (cur->snaprealm && !cur->snaprealm->open && mdr &&
	!cur->snaprealm->open_parents(new C_MDS_RetryRequest(this, mdr)))
      return 1;


    // dentry
    CDentry *dn = curdir->lookup(path[depth], snapid);
    CDentry::linkage_t *dnl = dn ? dn->get_projected_linkage() : 0;

    // null and last_bit and xlocked by me?
    if (dnl && dnl->is_null() && null_okay) {
      dout(10) << "traverse: hit null dentry at tail of traverse, succeeding" << dendl;
      if (pdnvec)
	pdnvec->push_back(dn);
      if (pin)
	*pin = 0;
      break; // done!
    }

    if (dnl &&
	dn->lock.is_xlocked() &&
	dn->lock.get_xlock_by() != mdr &&
	!dn->lock.can_read(client) &&
	(dnl->is_null() || forward)) {
      dout(10) << "traverse: xlocked dentry at " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req));
      if (mds->logger) mds->logger->inc(l_mds_tlock);
      return 1;
    }
    
    if (dnl && !dnl->is_null()) {
      CInode *in = dnl->get_inode();
      
      // do we have inode?
      if (!in) {
        assert(dnl->is_remote());
        // do i have it?
        in = get_inode(dnl->get_remote_ino());
        if (in) {
	  dout(7) << "linking in remote in " << *in << dendl;
	  dn->link_remote(dnl, in);
	} else {
          dout(7) << "remote link to " << dnl->get_remote_ino() << ", which i don't have" << dendl;
	  assert(mdr);  // we shouldn't hit non-primary dentries doing a non-mdr traversal!
          open_remote_ino(dnl->get_remote_ino(), _get_waiter(mdr, req));
	  if (mds->logger) mds->logger->inc(l_mds_trino);
          return 1;
        }        
      }

      // forwarder wants replicas?
#if 0
      if (mdr && mdr->client_request && 
	  mdr->client_request->get_mds_wants_replica_in_dirino()) {
	dout(30) << "traverse: REP is here, " 
		 << mdr->client_request->get_mds_wants_replica_in_dirino() 
		 << " vs " << curdir->dirfrag() << dendl;
	
	if (mdr->client_request->get_mds_wants_replica_in_dirino() == curdir->ino() &&
	    curdir->is_auth() && 
	    curdir->is_rep() &&
	    curdir->is_replica(req->get_source().num()) &&
	    dn->is_auth()
	    ) {
	  assert(req->get_source().is_mds());
	  int from = req->get_source().num();
	  
	  if (dn->is_replica(from)) {
	    dout(15) << "traverse: REP would replicate to mds" << from << ", but already cached_by " 
		     << req->get_source() << " dn " << *dn << dendl; 
	  } else {
	    dout(10) << "traverse: REP replicating to " << req->get_source() << " dn " << *dn << dendl;
	    MDiscoverReply *reply = new MDiscoverReply(curdir->dirfrag());
	    reply->mark_unsolicited();
	    reply->starts_with = MDiscoverReply::DENTRY;
	    replicate_dentry(dn, from, reply->trace);
	    if (dnl->is_primary())
	      replicate_inode(in, from, reply->trace);
	    if (req->get_source() != req->get_orig_source())
	      mds->send_message_mds(reply, req->get_source().num());
	    else mds->send_message(reply->req->get_connnection());
	  }
	}
      }
#endif
      
      // add to trace, continue.
      cur = in;
      touch_inode(cur);
      if (pdnvec)
	pdnvec->push_back(dn);
      if (pin)
	*pin = cur;
      depth++;
      continue;
    }
    

    // MISS.  dentry doesn't exist.
    dout(12) << "traverse: miss on dentry " << path[depth] << " in " << *curdir << dendl;

    if (curdir->is_auth()) {
      // dentry is mine.
      if (curdir->is_complete() || (curdir->has_bloom() &&
          !curdir->is_in_bloom(path[depth]))){
        // file not found
	if (pdnvec) {
	  // instantiate a null dn?
	  if (depth < path.depth()-1){
	    dout(20) << " didn't traverse full path; not returning pdnvec" << dendl;
	    dn = NULL;
	  } else if (dn) {
	    dout(20) << " had null " << *dn << dendl;
	    assert(dnl->is_null());
	  } else if (curdir->is_frozen()) {
	    dout(20) << " not adding null to frozen dir " << dendl;
	  } else if (snapid < CEPH_MAXSNAP) {
	    dout(20) << " not adding null for snapid " << snapid << dendl;
	  } else {
	    // create a null dentry
	    dn = curdir->add_null_dentry(path[depth]);
	    dout(20) << " added null " << *dn << dendl;
	  }
	  if (dn)
	    pdnvec->push_back(dn);
	  else
	    pdnvec->clear();   // do not confuse likes of rdlock_path_pin_ref();
	}
        return -ENOENT;
      } else {
	// directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << dendl;
        touch_inode(cur);
        curdir->fetch(_get_waiter(mdr, req), path[depth]);
	if (mds->logger) mds->logger->inc(l_mds_tdirf);
        return 1;
      }
    } else {
      // dirfrag/dentry is not mine.
      pair<int,int> dauth = curdir->authority();

      if (onfail == MDS_TRAVERSE_FORWARD &&
	  snapid && mdr->client_request &&
	  (int)depth < mdr->client_request->get_retry_attempt()) {
	dout(7) << "traverse: snap " << snapid << " and depth " << depth
		<< " < retry " << mdr->client_request->get_retry_attempt()
		<< ", discovering instead of forwarding" << dendl;
	onfail = MDS_TRAVERSE_DISCOVER;
      }

      if ((onfail == MDS_TRAVERSE_DISCOVER ||
           onfail == MDS_TRAVERSE_DISCOVERXLOCK)) {
	dout(7) << "traverse: discover from " << path[depth] << " from " << *curdir << dendl;
	discover_path(curdir, snapid, path.postfixpath(depth), _get_waiter(mdr, req),
		      onfail == MDS_TRAVERSE_DISCOVERXLOCK);
	if (mds->logger) mds->logger->inc(l_mds_tdis);
        return 1;
      } 
      if (onfail == MDS_TRAVERSE_FORWARD) {
        // forward
        dout(7) << "traverse: not auth for " << path << " in " << *curdir << dendl;
	
	if (curdir->is_ambiguous_auth()) {
	  // wait
	  dout(7) << "traverse: waiting for single auth in " << *curdir << dendl;
	  curdir->add_waiter(CDir::WAIT_SINGLEAUTH, _get_waiter(mdr, req));
	  return 1;
	} 

	dout(7) << "traverse: forwarding, not auth for " << *curdir << dendl;
	
#if 0
	// request replication?
	if (mdr && mdr->client_request && curdir->is_rep()) {
	  dout(15) << "traverse: REP fw to mds" << dauth << ", requesting rep under "
		   << *curdir << " req " << *(MClientRequest*)req << dendl;
	  mdr->client_request->set_mds_wants_replica_in_dirino(curdir->ino());
	  req->clear_payload();  // reencode!
	}
#endif
	
	if (mdr) 
	  request_forward(mdr, dauth.first);
	else
	  mds->forward_message_mds(req, dauth.first);
	
	if (mds->logger) mds->logger->inc(l_mds_tfw);
	return 2;
      }    
      if (onfail == MDS_TRAVERSE_FAIL)
        return -ENOENT;  // not necessarily exactly true....
    }
    
    assert(0);  // i shouldn't get here
  }
  
  // success.
  if (mds->logger) mds->logger->inc(l_mds_thit);
  dout(10) << "path_traverse finish on snapid " << snapid << dendl;
  if (mdr) 
    assert(mdr->snapid == snapid);
  return 0;
}

bool MDCache::path_is_mine(filepath& path)
{
  dout(15) << "path_is_mine " << path.get_ino() << " " << path << dendl;
  
  CInode *cur = get_inode(path.get_ino());
  if (!cur)
    return false;  // who knows!

  for (unsigned i=0; i<path.depth(); i++) {
    dout(15) << "path_is_mine seg " << i << ": " << path[i] << " under " << *cur << dendl;
    frag_t fg = cur->pick_dirfrag(path[i]);
    CDir *dir = cur->get_dirfrag(fg);
    if (!dir)
      return cur->is_auth();
    CDentry *dn = dir->lookup(path[i]);
    CDentry::linkage_t *dnl = dn->get_linkage();
    if (!dn || dnl->is_null())
      return dir->is_auth();
    assert(dnl->is_primary());
    cur = dnl->get_inode();
  }

  return cur->is_auth();
}

CInode *MDCache::cache_traverse(const filepath& fp)
{
  dout(10) << "cache_traverse " << fp << dendl;

  CInode *in;
  if (fp.get_ino())
    in = get_inode(fp.get_ino());
  else
    in = root;
  if (!in)
    return NULL;

  for (unsigned i = 0; i < fp.depth(); i++) {
    const string& dname = fp[i];
    frag_t fg = in->pick_dirfrag(dname);
    dout(20) << " " << i << " " << dname << " frag " << fg << " from " << *in << dendl;
    CDir *curdir = in->get_dirfrag(fg);
    if (!curdir)
      return NULL;
    CDentry *dn = curdir->lookup(dname, CEPH_NOSNAP);
    if (!dn)
      return NULL;
    in = dn->get_linkage()->get_inode();
    if (!in)
      return NULL;
  }
  dout(10) << " got " << *in << dendl;
  return in;
}


/**
 * open_remote_dir -- open up a remote dirfrag
 *
 * @diri - base inode
 * @approxfg - approximate fragment.
 * @fin - completion callback
 */
void MDCache::open_remote_dirfrag(CInode *diri, frag_t approxfg, Context *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << dendl;
  
  assert(diri->is_dir());
  assert(!diri->is_auth());
  assert(diri->get_dirfrag(approxfg) == 0);

  int auth = diri->authority().first;

  if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
    discover_dir_frag(diri, approxfg, fin);
  } else {
    // mds is down or recovering.  forge a replica!
    forge_replica_dir(diri, approxfg, auth);
    if (fin)
      mds->queue_waiter(fin);
  }
}


/** 
 * get_dentry_inode - get or open inode
 *
 * @dn the dentry
 * @mdr current request
 *
 * will return inode for primary, or link up/open up remote link's inode as necessary.
 * If it's not available right now, puts mdr on wait list and returns null.
 */
CInode *MDCache::get_dentry_inode(CDentry *dn, MDRequest *mdr, bool projected)
{
  CDentry::linkage_t *dnl;
  if (projected)
    dnl = dn->get_projected_linkage();
  else
    dnl = dn->get_linkage();

  assert(!dnl->is_null());
  
  if (dnl->is_primary())
    return dnl->inode;

  assert(dnl->is_remote());
  CInode *in = get_inode(dnl->get_remote_ino());
  if (in) {
    dout(7) << "get_dentry_inode linking in remote in " << *in << dendl;
    dn->link_remote(dnl, in);
    return in;
  } else {
    dout(10) << "get_dentry_inode on remote dn, opening inode for " << *dn << dendl;
    open_remote_ino(dnl->remote_ino, new C_MDS_RetryRequest(this, mdr));
    return 0;
  }
}

class C_MDC_RetryOpenRemoteIno : public Context {
  MDCache *mdcache;
  inodeno_t ino;
  Context *onfinish;
public:
  C_MDC_RetryOpenRemoteIno(MDCache *mdc, inodeno_t i, Context *c) :
    mdcache(mdc), ino(i), onfinish(c) {}
  void finish(int r) {
    mdcache->open_remote_ino(ino, onfinish);
  }
};


class C_MDC_OpenRemoteIno : public Context {
  MDCache *mdcache;
  inodeno_t ino;
  inodeno_t hadino;
  version_t hadv;
  Context *onfinish;
public:
  vector<Anchor> anchortrace;

  C_MDC_OpenRemoteIno(MDCache *mdc, inodeno_t i, inodeno_t hi, version_t hv, Context *c) :
    mdcache(mdc), ino(i), hadino(hi), hadv(hv), onfinish(c) {}
  C_MDC_OpenRemoteIno(MDCache *mdc, inodeno_t i, vector<Anchor>& at, Context *c) :
    mdcache(mdc), ino(i), hadino(0), hadv(0), onfinish(c), anchortrace(at) {}

  void finish(int r) {
    assert(r == 0);
    if (r == 0)
      mdcache->open_remote_ino_2(ino, anchortrace, hadino, hadv, onfinish);
    else {
      onfinish->finish(r);
      delete onfinish;
    }
  }
};

void MDCache::open_remote_ino(inodeno_t ino, Context *onfinish, inodeno_t hadino, version_t hadv)
{
  dout(7) << "open_remote_ino on " << ino << dendl;
  
  C_MDC_OpenRemoteIno *c = new C_MDC_OpenRemoteIno(this, ino, hadino, hadv, onfinish);
  mds->anchorclient->lookup(ino, c->anchortrace, c);
}

void MDCache::open_remote_ino_2(inodeno_t ino,
                                vector<Anchor>& anchortrace,
				inodeno_t hadino, version_t hadv,
                                Context *onfinish)
{
  dout(7) << "open_remote_ino_2 on " << ino
	  << ", trace depth is " << anchortrace.size() << dendl;
  
  // find deepest cached inode in prefix
  unsigned i = anchortrace.size();  // i := array index + 1
  CInode *in = 0;
  while (1) {
    // inode?
    dout(10) << " " << i << ": " << anchortrace[i-1] << dendl;
    in = get_inode(anchortrace[i-1].ino);
    if (in)
      break;
    i--;
    if (!i) {
      in = get_inode(anchortrace[i].dirino);
      if (!in) {
	dout(0) << "open_remote_ino_2 don't have dir inode " << anchortrace[i].dirino << dendl;
	if (MDS_INO_IS_MDSDIR(anchortrace[i].dirino)) {
	  open_foreign_mdsdir(anchortrace[i].dirino, onfinish);
	  return;
	}
	assert(in);  // hrm!
      }
      break;
    }
  }
  dout(10) << "deepest cached inode at " << i << " is " << *in << dendl;

  if (in->ino() == ino) {
    // success
    dout(10) << "open_remote_ino_2 have " << *in << dendl;
    onfinish->finish(0);
    delete onfinish;
    return;
  } 

  // open dirfrag beneath *in
  frag_t frag = in->dirfragtree[anchortrace[i].dn_hash];

  if (!in->dirfragtree.contains(frag)) {
    dout(10) << "frag " << frag << " not valid, requerying anchortable" << dendl;
    open_remote_ino(ino, onfinish);
    return;
  }

  CDir *dir = in->get_dirfrag(frag);

  if (!dir && !in->is_auth()) {
    dout(10) << "opening remote dirfrag " << frag << " under " << *in << dendl;
    /* we re-query the anchortable just to avoid a fragtree update race */
    open_remote_dirfrag(in, frag,
			new C_MDC_RetryOpenRemoteIno(this, ino, onfinish));
    return;
  }

  if (!dir && in->is_auth()) {
    if (in->is_frozen_dir()) {
      dout(7) << "traverse: " << *in << " is frozen_dir, waiting" << dendl;
      in->parent->dir->add_waiter(CDir::WAIT_UNFREEZE, onfinish);
      return;
    }
    dir = in->get_or_open_dirfrag(this, frag);
  }
  assert(dir);

  if (dir->is_auth()) {
    if (dir->is_complete()) {
      // make sure we didn't get to the same version anchor 2x in a row
      if (hadv && hadino == anchortrace[i].ino && hadv == anchortrace[i].updated) {
	dout(10) << "expected ino " << anchortrace[i].ino
		 << " in complete dir " << *dir
		 << ", got same anchor " << anchortrace[i] << " 2x in a row" << dendl;
	onfinish->finish(-ENOENT);
	delete onfinish;
      } else {
	// hrm.  requery anchor table.
	dout(10) << "expected ino " << anchortrace[i].ino
		 << " in complete dir " << *dir
		 << ", requerying anchortable"
		 << dendl;
	open_remote_ino(ino, onfinish, anchortrace[i].ino, anchortrace[i].updated);
      }
    } else {
      dout(10) << "need ino " << anchortrace[i].ino
	       << ", fetching incomplete dir " << *dir
	       << dendl;
      dir->fetch(new C_MDC_OpenRemoteIno(this, ino, anchortrace, onfinish));
    }
  } else {
    // hmm, discover.
    dout(10) << "have remote dirfrag " << *dir << ", discovering " 
	     << anchortrace[i].ino << dendl;
    discover_ino(dir, anchortrace[i].ino, 
		 new C_MDC_OpenRemoteIno(this, ino, anchortrace, onfinish));
  }
}


struct C_MDC_OpenRemoteDentry : public Context {
  MDCache *mdc;
  CDentry *dn;
  bool projected;
  Context *onfinish;
  C_MDC_OpenRemoteDentry(MDCache *m, CDentry *d, bool p, Context *f) :
    mdc(m), dn(d), projected(p), onfinish(f) {}
  void finish(int r) {
    mdc->_open_remote_dentry_finish(r, dn, projected, onfinish);
  }
};

void MDCache::open_remote_dentry(CDentry *dn, bool projected, Context *fin)
{
  dout(10) << "open_remote_dentry " << *dn << dendl;
  CDentry::linkage_t *dnl = projected ? dn->get_projected_linkage() : dn->get_linkage();
  open_remote_ino(dnl->get_remote_ino(), 
		  new C_MDC_OpenRemoteDentry(this, dn, projected, fin));
}

void MDCache::_open_remote_dentry_finish(int r, CDentry *dn, bool projected, Context *fin)
{
  if (r == -ENOENT) {
    dout(0) << "open_remote_dentry_finish bad remote dentry " << *dn << dendl;
    dn->state_set(CDentry::STATE_BADREMOTEINO);
  } else if (r != 0)
    assert(0);
  fin->finish(r);
  delete fin;
}



void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  // empty trace if we're a base inode
  if (in->is_base())
    return;

  CInode *parent = in->get_parent_inode();
  assert(parent);
  make_trace(trace, parent);

  CDentry *dn = in->get_parent_dn();
  dout(15) << "make_trace adding " << *dn << dendl;
  trace.push_back(dn);
}

/* This function takes over the reference to the passed Message */
MDRequest *MDCache::request_start(MClientRequest *req)
{
  // did we win a forward race against a slave?
  if (active_requests.count(req->get_reqid())) {
    MDRequest *mdr = active_requests[req->get_reqid()];
    if (mdr->is_slave()) {
      dout(10) << "request_start already had " << *mdr << ", cleaning up" << dendl;
      request_cleanup(mdr);
    } else {
      dout(10) << "request_start already processing " << *mdr << ", dropping new msg" << dendl;
      req->put();
      return 0;
    }
  }

  // register new client request
  MDRequest *mdr = new MDRequest(req->get_reqid(), req);
  active_requests[req->get_reqid()] = mdr;
  dout(7) << "request_start " << *mdr << dendl;
  return mdr;
}

MDRequest *MDCache::request_start_slave(metareqid_t ri, int by)
{
  MDRequest *mdr = new MDRequest(ri, by);
  assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_slave " << *mdr << " by mds" << by << dendl;
  return mdr;
}

MDRequest *MDCache::request_start_internal(int op)
{
  MDRequest *mdr = new MDRequest;
  mdr->reqid.name = entity_name_t::MDS(mds->get_nodeid());
  mdr->reqid.tid = mds->issue_tid();
  mdr->internal_op = op;

  assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_internal " << *mdr << " op " << op << dendl;
  return mdr;
}


MDRequest *MDCache::request_get(metareqid_t rid)
{
  assert(active_requests.count(rid));
  dout(7) << "request_get " << rid << " " << *active_requests[rid] << dendl;
  return active_requests[rid];
}

void MDCache::request_finish(MDRequest *mdr)
{
  dout(7) << "request_finish " << *mdr << dendl;

  // slave finisher?
  if (mdr->more()->slave_commit) {
    Context *fin = mdr->more()->slave_commit;
    mdr->more()->slave_commit = 0;
    fin->finish(0);   // this must re-call request_finish.
    delete fin;
    return; 
  }

  request_cleanup(mdr);
}


void MDCache::request_forward(MDRequest *mdr, int who, int port)
{
  dout(7) << "request_forward " << *mdr << " to mds" << who << " req " << *mdr << dendl;
  
  mds->forward_message_mds(mdr->client_request, who);  
  mdr->client_request = 0;
  request_cleanup(mdr);

  if (mds->logger) mds->logger->inc(l_mds_fw);
}


void MDCache::dispatch_request(MDRequest *mdr)
{
  if (mdr->client_request) {
    if (!mdr->item_session_request.is_on_list()) {
      dout(10) << "request " << *mdr << " is canceled" << dendl;
      return;
    }
    mds->server->dispatch_client_request(mdr);
  } else if (mdr->slave_request) {
    mds->server->dispatch_slave_request(mdr);
  } else {
    switch (mdr->internal_op) {
      
      // ...
      
    default:
      assert(0);
    }
  }
}


void MDCache::request_forget_foreign_locks(MDRequest *mdr)
{
  // xlocks
  set<SimpleLock*>::iterator p = mdr->xlocks.begin();
  while (p != mdr->xlocks.end()) {
    if ((*p)->get_parent()->is_auth()) 
      p++;
    else {
      dout(10) << "request_forget_foreign_locks " << **p
	       << " on " << *(*p)->get_parent() << dendl;
      (*p)->put_xlock();
      mdr->locks.erase(*p);
      mdr->xlocks.erase(p++);
    }
  }
}


void MDCache::request_drop_locks(MDRequest *mdr)
{
  // clean up slaves
  //  (will implicitly drop remote dn pins)
  for (set<int>::iterator p = mdr->more()->slaves.begin();
       p != mdr->more()->slaves.end();
       ++p) {
    MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_FINISH);
    mds->send_message_mds(r, *p);
  }

  // strip foreign xlocks out of lock lists, since the OP_FINISH drops them implicitly.
  request_forget_foreign_locks(mdr);


  // drop locks
  mds->locker->drop_locks(mdr);
}

void MDCache::request_cleanup(MDRequest *mdr)
{
  dout(15) << "request_cleanup " << *mdr << dendl;
  metareqid_t ri = mdr->reqid;

  request_drop_locks(mdr);

  // drop (local) auth pins
  mdr->drop_local_auth_pins();

  // drop stickydirs
  for (set<CInode*>::iterator p = mdr->stickydirs.begin();
       p != mdr->stickydirs.end();
       ++p) 
    (*p)->put_stickydirs();

  mds->locker->kick_cap_releases(mdr);

  // drop cache pins
  mdr->drop_pins();

  // remove from session
  mdr->item_session_request.remove_myself();

  bool was_replay = mdr->client_request && mdr->client_request->is_replay();

  // remove from map
  active_requests.erase(mdr->reqid);
  mdr->put();

  // fail-safe!
  if (was_replay && active_requests.empty()) {
    dout(10) << " fail-safe queueing next replay op" << dendl;
    mds->queue_one_replay();
  }

  if (mds->logger)
    log_stat();
}

void MDCache::request_kill(MDRequest *mdr)
{
  dout(10) << "request_kill " << *mdr << dendl;
  request_cleanup(mdr);
}


// --------------------------------------------------------------------
// ANCHORS

class C_MDC_AnchorPrepared : public Context {
  MDCache *cache;
  CInode *in;
  bool add;
public:
  version_t atid;
  C_MDC_AnchorPrepared(MDCache *c, CInode *i, bool a) : cache(c), in(i), add(a) {}
  void finish(int r) {
    cache->_anchor_prepared(in, atid, add);
  }
};

void MDCache::anchor_create_prep_locks(MDRequest *mdr, CInode *in,
				       set<SimpleLock*>& rdlocks, set<SimpleLock*>& xlocks)
{
  dout(10) << "anchor_create_prep_locks " << *in << dendl;

  if (in->is_anchored()) {
    // caller may have already xlocked it.. if so, that will suffice!
    if (xlocks.count(&in->linklock) == 0)
      rdlocks.insert(&in->linklock);
  } else {
    xlocks.insert(&in->linklock);

    // path components too!
    CDentry *dn = in->get_projected_parent_dn();
    while (dn) {
      rdlocks.insert(&dn->lock);
      dn = dn->get_dir()->get_inode()->get_parent_dn();
    }
  }
}

void MDCache::anchor_create(MDRequest *mdr, CInode *in, Context *onfinish)
{
  assert(in->is_auth());
  dout(10) << "anchor_create " << *in << dendl;

  // auth pin
  if (!in->can_auth_pin() &&
      !mdr->is_auth_pinned(in)) {
    dout(7) << "anchor_create not authpinnable, waiting on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE, onfinish);
    return;
  }

  // wait
  in->add_waiter(CInode::WAIT_ANCHORED, onfinish);

  // already anchoring?
  if (in->state_test(CInode::STATE_ANCHORING)) {
    dout(7) << "anchor_create already anchoring " << *in << dendl;
    return;
  }

  dout(7) << "anchor_create " << *in << dendl;

  // auth: do it
  in->state_set(CInode::STATE_ANCHORING);
  in->get(CInode::PIN_ANCHORING);
  in->auth_pin(this);
  
  // make trace
  vector<Anchor> trace;
  in->make_anchor_trace(trace);
  if (!trace.size()) {
    assert(MDS_INO_IS_BASE(in->ino()));
    trace.push_back(Anchor(in->ino(), in->ino(), 0, 0, 0));
  }
  
  // do it
  C_MDC_AnchorPrepared *fin = new C_MDC_AnchorPrepared(this, in, true);
  mds->anchorclient->prepare_create(in->ino(), trace, &fin->atid, fin);
}

void MDCache::anchor_destroy(CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // auth pin
  if (!in->can_auth_pin()/* &&
			    !mdr->is_auth_pinned(in)*/) {
    dout(7) << "anchor_destroy not authpinnable, waiting on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE, onfinish);
    return;
  }

  dout(7) << "anchor_destroy " << *in << dendl;

  // wait
  if (onfinish)
    in->add_waiter(CInode::WAIT_UNANCHORED, onfinish);

  // already anchoring?
  if (in->state_test(CInode::STATE_UNANCHORING)) {
    dout(7) << "anchor_destroy already unanchoring " << *in << dendl;
    return;
  }

  // auth: do it
  in->state_set(CInode::STATE_UNANCHORING);
  in->get(CInode::PIN_UNANCHORING);
  in->auth_pin(this);
  
  // do it
  C_MDC_AnchorPrepared *fin = new C_MDC_AnchorPrepared(this, in, false);
  mds->anchorclient->prepare_destroy(in->ino(), &fin->atid, fin);
}

class C_MDC_AnchorLogged : public Context {
  MDCache *cache;
  CInode *in;
  version_t atid;
  Mutation *mut;
public:
  C_MDC_AnchorLogged(MDCache *c, CInode *i, version_t t, Mutation *m) : 
    cache(c), in(i), atid(t), mut(m) {}
  void finish(int r) {
    cache->_anchor_logged(in, atid, mut);
  }
};

void MDCache::_anchor_prepared(CInode *in, version_t atid, bool add)
{
  dout(10) << "_anchor_prepared " << *in << " atid " << atid 
	   << " " << (add ? "create":"destroy") << dendl;
  assert(in->inode.anchored == !add);

  // update the logged inode copy
  inode_t *pi = in->project_inode();
  if (add) {
    pi->anchored = true;
    pi->rstat.ranchors++;
  } else {
    pi->anchored = false;
    pi->rstat.ranchors--;
  }
  pi->version = in->pre_dirty();

  Mutation *mut = new Mutation;
  mut->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, add ? "anchor_create":"anchor_destroy");
  mds->mdlog->start_entry(le);
  predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  journal_dirty_inode(mut, &le->metablob, in);
  le->metablob.add_table_transaction(TABLE_ANCHOR, atid);
  mds->mdlog->submit_entry(le, new C_MDC_AnchorLogged(this, in, atid, mut));
  mds->mdlog->flush();
}


void MDCache::_anchor_logged(CInode *in, version_t atid, Mutation *mut)
{
  dout(10) << "_anchor_logged on " << *in << dendl;

  // unpin
  if (in->state_test(CInode::STATE_ANCHORING)) {
    in->state_clear(CInode::STATE_ANCHORING);
    in->put(CInode::PIN_ANCHORING);
    if (in->parent)
      in->parent->adjust_nested_anchors(1);
  } else if (in->state_test(CInode::STATE_UNANCHORING)) {
    in->state_clear(CInode::STATE_UNANCHORING);
    in->put(CInode::PIN_UNANCHORING);
    if (in->parent)
      in->parent->adjust_nested_anchors(-1);
  }
  in->auth_unpin(this);
  
  // apply update to cache
  in->pop_and_dirty_projected_inode(mut->ls);
  mut->apply();
  
  // tell the anchortable we've committed
  mds->anchorclient->commit(atid, mut->ls);

  // drop locks and finish
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  // trigger waiters
  in->finish_waiting(CInode::WAIT_ANCHORED|CInode::WAIT_UNANCHORED, 0);
}


// -------------------------------------------------------------------------------
// SNAPREALMS

struct C_MDC_snaprealm_create_finish : public Context {
  MDCache *cache;
  MDRequest *mdr;
  Mutation *mut;
  CInode *in;
  C_MDC_snaprealm_create_finish(MDCache *c, MDRequest *m, Mutation *mu, CInode *i) : 
    cache(c), mdr(m), mut(mu), in(i) {}
  void finish(int r) {
    cache->_snaprealm_create_finish(mdr, mut, in);
  }
};

void MDCache::snaprealm_create(MDRequest *mdr, CInode *in)
{
  dout(10) << "snaprealm_create " << *in << dendl;
  assert(!in->snaprealm);

  if (!in->inode.anchored) {
    mds->mdcache->anchor_create(mdr, in, new C_MDS_RetryRequest(mds->mdcache, mdr));
    return;
  }

  // allocate an id..
  if (!mdr->more()->stid) {
    mds->snapclient->prepare_create_realm(in->ino(), &mdr->more()->stid, &mdr->more()->snapidbl,
					  new C_MDS_RetryRequest(this, mdr));
    return;
  }

  Mutation *mut = new Mutation;
  mut->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, "snaprealm_create");
  mds->mdlog->start_entry(le);

  le->metablob.add_table_transaction(TABLE_SNAP, mdr->more()->stid);

  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->rstat.rsnaprealms++;

  bufferlist::iterator p = mdr->more()->snapidbl.begin();
  snapid_t seq;
  ::decode(seq, p);

  SnapRealm t(this, in);
  t.srnode.created = seq;
  bufferlist snapbl;
  ::encode(t, snapbl);
  
  predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  journal_cow_inode(mut, &le->metablob, in);
  le->metablob.add_primary_dentry(in->get_projected_parent_dn(), true, in, 0, &snapbl);

  mds->mdlog->submit_entry(le, new C_MDC_snaprealm_create_finish(this, mdr, mut, in));
  mds->mdlog->flush();
}


void MDCache::do_realm_invalidate_and_update_notify(CInode *in, int snapop, bool nosend)
{
  dout(10) << "do_realm_invalidate_and_update_notify " << *in->snaprealm << " " << *in << dendl;

  vector<inodeno_t> split_inos;
  vector<inodeno_t> split_realms;

  if (snapop == CEPH_SNAP_OP_SPLIT) {
    // notify clients of update|split
    for (elist<CInode*>::iterator p = in->snaprealm->inodes_with_caps.begin(member_offset(CInode, item_caps));
	 !p.end(); ++p)
      split_inos.push_back((*p)->ino());
    
    for (set<SnapRealm*>::iterator p = in->snaprealm->open_children.begin();
	 p != in->snaprealm->open_children.end();
	 p++)
      split_realms.push_back((*p)->inode->ino());
  }

  bufferlist snapbl;
  in->snaprealm->build_snap_trace(snapbl);

  map<client_t, MClientSnap*> updates;
  list<SnapRealm*> q;
  q.push_back(in->snaprealm);
  while (!q.empty()) {
    SnapRealm *realm = q.front();
    q.pop_front();

    dout(10) << " realm " << *realm << " on " << *realm->inode << dendl;
    realm->invalidate_cached_snaps();

    for (map<client_t, xlist<Capability*> >::iterator p = realm->client_caps.begin();
	 p != realm->client_caps.end();
	 p++) {
      assert(!p->second.empty());
      if (!nosend && updates.count(p->first) == 0) {
	MClientSnap *update = new MClientSnap(snapop);
	update->head.split = in->ino();
	update->split_inos = split_inos;
	update->split_realms = split_realms;
	update->bl = snapbl;
	updates[p->first] = update;
      }
    }

    // notify for active children, too.
    dout(10) << " " << realm << " open_children are " << realm->open_children << dendl;
    for (set<SnapRealm*>::iterator p = realm->open_children.begin();
	 p != realm->open_children.end();
	 p++)
      q.push_back(*p);
  }

  if (!nosend)
    send_snaps(updates);
}

void MDCache::_snaprealm_create_finish(MDRequest *mdr, Mutation *mut, CInode *in)
{
  dout(10) << "_snaprealm_create_finish " << *in << dendl;

  // apply
  in->pop_and_dirty_projected_inode(mut->ls);
  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();

  // tell table we've committed
  mds->snapclient->commit(mdr->more()->stid, mut->ls);

  delete mut;

  // create
  bufferlist::iterator p = mdr->more()->snapidbl.begin();
  snapid_t seq;
  ::decode(seq, p);

  in->open_snaprealm();
  in->snaprealm->srnode.seq = seq;
  in->snaprealm->srnode.created = seq;
  in->snaprealm->srnode.current_parent_since = seq;

  do_realm_invalidate_and_update_notify(in, CEPH_SNAP_OP_SPLIT);

  /*
  static int count = 5;
  if (--count == 0)
    assert(0);  // hack test test **********
  */

  // done.
  mdr->more()->stid = 0;  // caller will likely need to reuse this
  dispatch_request(mdr);
}


// -------------------------------------------------------------------------------
// STRAYS

void MDCache::scan_stray_dir()
{
  dout(10) << "scan_stray_dir" << dendl;
  
  list<CDir*> ls;
  for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); p++) {
    CDir *dir = *p;
    for (CDir::map_t::iterator q = dir->items.begin(); q != dir->items.end(); q++) {
      CDentry *dn = q->second;
      CDentry::linkage_t *dnl = dn->get_projected_linkage();
      if (dnl->is_primary())
	maybe_eval_stray(dnl->get_inode());
    }
  }
}

struct C_MDC_EvalStray : public Context {
  MDCache *mdcache;
  CDentry *dn;
  C_MDC_EvalStray(MDCache *c, CDentry *d) : mdcache(c), dn(d) {}
  void finish(int r) {
    mdcache->eval_stray(dn);
  }
};

void MDCache::eval_stray(CDentry *dn)
{
  dout(10) << "eval_stray " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  dout(10) << " inode is " << *dnl->get_inode() << dendl;
  assert(dnl->is_primary());
  CInode *in = dnl->get_inode();
  assert(in);

  assert(dn->get_dir()->get_inode()->is_stray());

  if (!dn->is_auth()) {
    // has to be mine
    // move to bottom of lru so that we trim quickly!
    touch_dentry_bottom(dn);
    return;
  }

  // purge?
  if (in->inode.nlink == 0) {
    if (in->is_dir()) {
      // past snaprealm parents imply snapped dentry remote links.
      // only important for directories.  normal file data snaps are handled
      // by the object store.
      if (in->snaprealm && in->snaprealm->has_past_parents()) {
	if (!in->snaprealm->have_past_parents_open() &&
	    !in->snaprealm->open_parents(new C_MDC_EvalStray(this, dn)))
	  return;
	in->snaprealm->prune_past_parents();
	if (in->snaprealm->has_past_parents()) {
	  dout(20) << "  has past parents " << in->snaprealm->srnode.past_parents << dendl;
	  return;  // not until some snaps are deleted.
	}
      }
    }
    if (dn->is_replicated()) {
      dout(20) << " replicated" << dendl;
      return;
    }
    if (dn->is_any_leases() || in->is_any_caps()) {
      dout(20) << " caps | leases" << dendl;
      return;  // wait
    }
    if (!in->dirfrags.empty()) {
      dout(20) << " open dirfrags" << dendl;
      return;  // wait for dirs to close/trim
    }
    if (dn->state_test(CDentry::STATE_PURGING)) {
      dout(20) << " already purging" << dendl;
      return;  // already purging
    }
    if (in->state_test(CInode::STATE_NEEDSRECOVER) ||
	in->state_test(CInode::STATE_RECOVERING)) {
      dout(20) << " pending recovery" << dendl;
      return;  // don't mess with file size probing
    }
    if (in->get_num_ref() > (int)in->is_dirty()) {
      dout(20) << " too many inode refs" << dendl;
      return;
    }
    if (dn->get_num_ref() > (int)dn->is_dirty() + !!in->get_num_ref()) {
      dout(20) << " too many dn refs" << dendl;
      return;
    }
    purge_stray(dn);
  }
  else if (in->inode.nlink == 1) {
    // trivial reintegrate?
    if (!in->remote_parents.empty()) {
      CDentry *rlink = *in->remote_parents.begin();
      
      // don't do anything if the remote parent is projected, or we may
      // break user-visible semantics!
      // NOTE: we repeat this check in _rename(), since our submission path is racey.
      if (!rlink->is_projected()) {
	if (rlink->is_auth() && rlink->dir->can_auth_pin())
	  reintegrate_stray(dn, rlink);
	
	if (!rlink->is_auth() && dn->is_auth())
	  migrate_stray(dn, mds->get_nodeid(), rlink->authority().first);
      }
    }
  } else {
    // wait for next use.
  }
}

void MDCache::eval_remote(CDentry *dn)
{
  dout(10) << "eval_remote " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  assert(dnl->is_remote());
  CInode *in = dnl->get_inode();
  if (!in) return;

  // refers to stray?
  if (in->get_parent_dn()->get_dir()->get_inode()->is_stray()) {
    if (in->is_auth())
      eval_stray(in->get_parent_dn());
    else
      migrate_stray(in->get_parent_dn(), in->authority().first, mds->get_nodeid());
  }
}

class C_MDC_PurgeStrayPurged : public Context {
  MDCache *cache;
  CDentry *dn;
public:
  C_MDC_PurgeStrayPurged(MDCache *c, CDentry *d) : 
    cache(c), dn(d) { }
  void finish(int r) {
    cache->_purge_stray_purged(dn);
  }
};

void MDCache::purge_stray(CDentry *dn)
{
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();
  dout(10) << "purge_stray " << *dn << " " << *in << dendl;
  assert(!dn->is_replicated());

  // anchored?
  if (in->inode.anchored) {
    anchor_destroy(in, new C_MDC_EvalStray(this, dn));
    return;
  }

  dn->state_set(CDentry::STATE_PURGING);
  dn->get(CDentry::PIN_PURGING);
  in->state_set(CInode::STATE_PURGING);

  
  // CHEAT.  there's no real need to journal our intent to purge, since
  // that is implicit in the dentry's presence and non-use in the stray
  // dir.  on recovery, we'll need to re-eval all strays anyway.
  
  SnapRealm *realm = in->find_snaprealm();
  SnapContext nullsnap;
  const SnapContext *snapc;
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnap;
    assert(in->last == CEPH_NOSNAP);
  }

  uint64_t period = in->inode.layout.fl_object_size * in->inode.layout.fl_stripe_count;
  uint64_t cur_max_size = in->inode.get_max_size();
  uint64_t to = MAX(in->inode.size, cur_max_size);
  uint64_t num = (to + period - 1) / period;
  dout(10) << "purge_stray 0~" << to << " objects 0~" << num << " snapc " << snapc << " on " << *in << dendl;
  if (to)
    mds->filer->purge_range(in->inode.ino, &in->inode.layout, *snapc,
			    0, num, g_clock.now(), 0,
			    new C_MDC_PurgeStrayPurged(this, dn));
  else
    _purge_stray_purged(dn);
}

class C_MDC_PurgeStrayLogged : public Context {
  MDCache *cache;
  CDentry *dn;
  version_t pdv;
  LogSegment *ls;
public:
  C_MDC_PurgeStrayLogged(MDCache *c, CDentry *d, version_t v, LogSegment *s) : 
    cache(c), dn(d), pdv(v), ls(s) { }
  void finish(int r) {
    cache->_purge_stray_logged(dn, pdv, ls);
  }
};
class C_MDC_PurgeStrayLoggedTruncate : public Context {
  MDCache *cache;
  CDentry *dn;
  LogSegment *ls;
public:
  C_MDC_PurgeStrayLoggedTruncate(MDCache *c, CDentry *d, LogSegment *s) : 
    cache(c), dn(d), ls(s) { }
  void finish(int r) {
    cache->_purge_stray_logged_truncate(dn, ls);
  }
};

void MDCache::_purge_stray_purged(CDentry *dn)
{
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_purged " << *dn << " " << *in << dendl;

  if (in->get_num_ref() == (int)in->is_dirty() &&
      dn->get_num_ref() == (int)dn->is_dirty() + !!in->get_num_ref() + 1/*PIN_PURGING*/) {
    // kill dentry.
    version_t pdv = dn->pre_dirty();
    dn->push_projected_linkage(); // NULL

    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray");
    mds->mdlog->start_entry(le);
    
    le->metablob.add_dir_context(dn->dir);
    le->metablob.add_null_dentry(dn, true);
    le->metablob.add_destroyed_inode(in->ino());

    mds->mdlog->submit_entry(le, new C_MDC_PurgeStrayLogged(this, dn, pdv, mds->mdlog->get_current_segment()));
  } else {
    // new refs.. just truncate to 0
    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray truncate");
    mds->mdlog->start_entry(le);
    
    inode_t *pi = in->project_inode();
    pi->size = 0;
    pi->client_ranges.clear();
    pi->truncate_size = 0;
    pi->truncate_from = 0;
    pi->version = in->pre_dirty();

    le->metablob.add_dir_context(dn->dir);
    le->metablob.add_primary_dentry(dn, true, in);

    mds->mdlog->submit_entry(le, new C_MDC_PurgeStrayLoggedTruncate(this, dn, mds->mdlog->get_current_segment()));
  }
}

void MDCache::_purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls)
{
  CInode *in = dn->get_linkage()->get_inode();
  dout(10) << "_purge_stray_logged " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  assert(!in->state_test(CInode::STATE_RECOVERING));

  // unlink
  assert(dn->get_projected_linkage()->is_null());
  dn->dir->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(pdv, ls);

  // drop inode
  if (in->is_dirty())
    in->mark_clean();
  remove_inode(in);

  // drop dentry?
  if (dn->is_new()) {
    dout(20) << " dn is new, removing" << dendl;
    dn->mark_clean();
    dn->dir->remove_dentry(dn);
  } else
    touch_dentry_bottom(dn);  // drop dn as quickly as possible.
}

void MDCache::_purge_stray_logged_truncate(CDentry *dn, LogSegment *ls)
{
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_logged_truncate " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  in->pop_and_dirty_projected_inode(ls);

  eval_stray(dn);
}



void MDCache::reintegrate_stray(CDentry *straydn, CDentry *rdn)
{
  dout(10) << "reintegrate_stray " << *straydn << " into " << *rdn << dendl;
  
  // rename it to another mds.
  filepath src;
  straydn->make_path(src);
  filepath dst;
  rdn->make_path(dst);

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, rdn->authority().first);
}
 

void MDCache::migrate_stray(CDentry *dn, int from, int to)
{
  dout(10) << "migrate_stray from mds" << from << " to mds" << to 
	   << " " << *dn << " " << *dn->get_projected_linkage()->get_inode() << dendl;

  // rename it to another mds.
  string dname;
  dn->get_projected_linkage()->get_inode()->name_stray_dentry(dname);
  filepath src(dname, MDS_INO_STRAY(from));
  filepath dst(dname, MDS_INO_STRAY(to));

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, to);
}




// ========================================================================================
// DISCOVER
/*

  - for all discovers (except base_inos, e.g. root, stray), waiters are attached
  to the parent metadata object in the cache (pinning it).

  - all discovers are tracked by tid, so that we can ignore potentially dup replies.

*/

void MDCache::_send_discover(discover_info_t& d)
{
  MDiscover *dis = new MDiscover(d.ino, d.frag, d.snap,
				 d.want_path, d.want_ino, d.want_base_dir, d.want_xlocked);
  dis->set_tid(d.tid);
  mds->send_message_mds(dis, d.mds);
}

void MDCache::discover_base_ino(inodeno_t want_ino,
				Context *onfinish,
				int from) 
{
  dout(7) << "discover_base_ino " << want_ino << " from mds" << from << dendl;
  if (waiting_for_base_ino[from].count(want_ino) == 0) {
    discover_info_t& d = _create_discover(from);
    d.ino = want_ino;
    _send_discover(d);
    waiting_for_base_ino[from][want_ino].push_back(onfinish);
  }
}


void MDCache::discover_dir_frag(CInode *base,
				frag_t approx_fg,
				Context *onfinish,
				int from)
{
  if (from < 0)
    from = base->authority().first;

  dirfrag_t df(base->ino(), approx_fg);
  dout(7) << "discover_dir_frag " << df
	  << " from mds" << from << dendl;

  if (!base->is_waiter_for(CInode::WAIT_DIR) || !onfinish) {  // FIXME: this is kind of weak!
    discover_info_t& d = _create_discover(from);
    d.ino = base->ino();
    d.frag = approx_fg;
    d.want_base_dir = true;
    _send_discover(d);
  }

  if (onfinish) 
    base->add_waiter(CInode::WAIT_DIR, onfinish);
}

struct C_MDC_RetryDiscoverPath : public Context {
  MDCache *mdc;
  CInode *base;
  snapid_t snapid;
  filepath path;
  int from;
  C_MDC_RetryDiscoverPath(MDCache *c, CInode *b, snapid_t s, filepath &p, int f) :
    mdc(c), base(b), snapid(s), path(p), from(f)  {}
  void finish(int r) {
    mdc->discover_path(base, snapid, path, 0, from);
  }
};

void MDCache::discover_path(CInode *base,
			    snapid_t snap,
			    filepath want_path,
			    Context *onfinish,
			    bool want_xlocked,
			    int from)
{
  if (from < 0)
    from = base->authority().first;

  dout(7) << "discover_path " << base->ino() << " " << want_path << " snap " << snap << " from mds" << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(10) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverPath(this, base, snap, want_path, from);
    base->add_waiter(CInode::WAIT_SINGLEAUTH, onfinish);
    return;
  } 

  if (!base->is_waiter_for(CInode::WAIT_DIR) || !onfinish) { // FIXME: weak!
    discover_info_t& d = _create_discover(from);
    d.ino = base->ino();
    d.snap = snap;
    d.want_path = want_path;
    d.want_base_dir = true;
    d.want_xlocked = want_xlocked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_waiter(CInode::WAIT_DIR, onfinish);
}

struct C_MDC_RetryDiscoverPath2 : public Context {
  MDCache *mdc;
  CDir *base;
  snapid_t snapid;
  filepath path;
  C_MDC_RetryDiscoverPath2(MDCache *c, CDir *b, snapid_t s, filepath &p) :
    mdc(c), base(b), snapid(s), path(p) {}
  void finish(int r) {
    mdc->discover_path(base, snapid, path, 0);
  }
};

void MDCache::discover_path(CDir *base,
			    snapid_t snap,
			    filepath want_path,
			    Context *onfinish,
			    bool want_xlocked)
{
  int from = base->authority().first;

  dout(7) << "discover_path " << base->dirfrag() << " " << want_path << " snap " << snap << " from mds" << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(7) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverPath2(this, base, snap, want_path);
    base->add_waiter(CDir::WAIT_SINGLEAUTH, onfinish);
    return;
  }

  if (!base->is_waiting_for_dentry(want_path[0].c_str(), snap) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.ino = base->ino();
    d.frag = base->get_frag();
    d.snap = snap;
    d.want_path = want_path;
    d.want_base_dir = true;
    d.want_xlocked = want_xlocked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_dentry_waiter(want_path[0], snap, onfinish);
}

struct C_MDC_RetryDiscoverIno : public Context {
  MDCache *mdc;
  CDir *base;
  inodeno_t want_ino;
  C_MDC_RetryDiscoverIno(MDCache *c, CDir *b, inodeno_t i) :
    mdc(c), base(b), want_ino(i) {}
  void finish(int r) {
    mdc->discover_ino(base, want_ino, 0);
  }
};

void MDCache::discover_ino(CDir *base,
			   inodeno_t want_ino,
			   Context *onfinish,
			   bool want_xlocked)
{
  int from = base->authority().first;

  dout(7) << "discover_ino " << base->dirfrag() << " " << want_ino << " from mds" << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;
  
  if (base->is_ambiguous_auth()) {
    dout(10) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverIno(this, base, want_ino);
    base->add_waiter(CDir::WAIT_SINGLEAUTH, onfinish);
    return;
  } 

  if (!base->is_waiting_for_ino(want_ino)) {
    discover_info_t& d = _create_discover(from);
    d.ino = base->ino();
    d.frag = base->get_frag();
    d.want_ino = want_ino;
    d.want_base_dir = true;
    d.want_xlocked = want_xlocked;
    _send_discover(d);
  }
  
  // register + wait
  base->add_ino_waiter(want_ino, onfinish);
}



void MDCache::kick_discovers(int who)
{
  for (map<tid_t,discover_info_t>::iterator p = discovers.begin();
       p != discovers.end();
       p++)
    _send_discover(p->second);
}


/* This function DOES put the passed message before returning */
void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();
  int from = dis->get_source_inst().name._num;

  assert(from != whoami);

  if (mds->get_state() < MDSMap::STATE_CLIENTREPLAY) {
    int from = dis->get_source().num();
    if (mds->get_state() < MDSMap::STATE_REJOIN ||
	rejoin_ack_gather.count(from)) {
      dout(-7) << "discover_reply not yet active(|still rejoining), delaying" << dendl;
      mds->wait_for_active(new C_MDS_RetryMessage(mds, dis));
      return;
    }
  }


  CInode *cur = 0;
  MDiscoverReply *reply = new MDiscoverReply(dis);

  snapid_t snapid = dis->get_snapid();

  // get started.
  if (MDS_INO_IS_BASE(dis->get_base_ino())) {
    // wants root
    dout(7) << "handle_discover from mds" << from
	    << " wants base + " << dis->get_want().get_path()
	    << " snap " << snapid
	    << dendl;

    cur = get_inode(dis->get_base_ino());

    // add root
    reply->starts_with = MDiscoverReply::INODE;
    replicate_inode(cur, from, reply->trace);
    dout(10) << "added base " << *cur << dendl;
  }
  else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino(), snapid);
    if (!cur && snapid != CEPH_NOSNAP) {
      cur = get_inode(dis->get_base_ino());
      if (!cur->is_multiversion())
	cur = NULL;  // nope!
    }
    
    if (!cur) {
      dout(7) << "handle_discover mds" << from 
	      << " don't have base ino " << dis->get_base_ino() << "." << snapid
	      << dendl;
      reply->set_flag_error_dir();
    } else if (dis->wants_base_dir()) {
      dout(7) << "handle_discover mds" << from 
	      << " wants basedir+" << dis->get_want().get_path() 
	      << " has " << *cur 
	      << dendl;
    } else {
      dout(7) << "handle_discover mds" << from 
	      << " wants " << dis->get_want().get_path()
	      << " has " << *cur
	      << dendl;
    }
  }

  assert(reply);
  
  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; 
       cur && (i < dis->get_want().depth() || dis->get_want().depth() == 0); 
       i++) {

    // -- figure out the dir

    // is *cur even a dir at all?
    if (!cur->is_dir()) {
      dout(7) << *cur << " not a dir" << dendl;
      reply->set_flag_error_dir();
      break;
    }

    // pick frag
    frag_t fg;
    if (dis->get_want().depth()) {
      // dentry specifies
      fg = cur->pick_dirfrag(dis->get_dentry(i));
    } else {
      // requester explicity specified the frag
      fg = dis->get_base_dir_frag();
      assert(dis->wants_base_dir() || dis->get_want_ino() || MDS_INO_IS_BASE(dis->get_base_ino()));
    }
    CDir *curdir = cur->get_dirfrag(fg);

    if ((!curdir && !cur->is_auth()) ||
	(curdir && !curdir->is_auth())) {

	/* before:
	 * ONLY set flag if empty!!
	 * otherwise requester will wake up waiter(s) _and_ continue with discover,
	 * resulting in duplicate discovers in flight,
	 * which can wreak havoc when discovering rename srcdn (which may move)
	 */

      if (reply->is_empty()) {
	// only hint if empty.
	//  someday this could be better, but right now the waiter logic isn't smart enough.
	
	// hint
	if (curdir) {
	  dout(7) << " not dirfrag auth, setting dir_auth_hint for " << *curdir << dendl;
	  reply->set_dir_auth_hint(curdir->authority().first);
	} else {
	  dout(7) << " dirfrag not open, not inode auth, setting dir_auth_hint for " 
		  << *cur << dendl;
	  reply->set_dir_auth_hint(cur->authority().first);
	}
	
	// note error dentry, if any
	//  NOTE: important, as it allows requester to issue an equivalent discover
	//        to whomever we hint at.
	if (dis->get_want().depth() > i)
	  reply->set_error_dentry(dis->get_dentry(i));
      }

      break;
    }

    // open dir?
    if (!curdir) 
      curdir = cur->get_or_open_dirfrag(this, fg);
    assert(curdir);
    assert(curdir->is_auth());
    
    // is dir frozen?
    if (curdir->is_frozen()) {
      if (reply->is_empty()) {
	dout(7) << *curdir << " is frozen, empty reply, waiting" << dendl;
	curdir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      } else {
	dout(7) << *curdir << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }
    
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "handle_discover not adding unwanted base dir " << *curdir << dendl;
    } else {
      assert(!curdir->is_ambiguous_auth()); // would be frozen.
      if (!reply->trace.length())
	reply->starts_with = MDiscoverReply::DIR;
      replicate_dir(curdir, from, reply->trace);
      dout(7) << "handle_discover added dir " << *curdir << dendl;
    }

    // lookup
    CDentry *dn = 0;
    if (dis->get_want_ino()) {
      // lookup by ino
      CInode *in = get_inode(dis->get_want_ino(), snapid);
      if (in && in->is_auth() && in->get_parent_dn()->get_dir() == curdir)
	dn = in->get_parent_dn();
    } else if (dis->get_want().depth() > 0) {
      // lookup dentry
      dn = curdir->lookup(dis->get_dentry(i), snapid);
    } else 
      break; // done!
          
    // incomplete dir?
    if (!dn) {
      if (!curdir->is_complete()) {
	// readdir
	dout(7) << "incomplete dir contents for " << *curdir << ", fetching" << dendl;
	if (reply->is_empty()) {
	  // fetch and wait
	  curdir->fetch(new C_MDS_RetryMessage(mds, dis));
	  reply->put();
	  return;
	} else {
	  // initiate fetch, but send what we have so far
	  curdir->fetch(0);
	  break;
	}
      }
      
      // don't have wanted ino in this dir?
      if (dis->get_want_ino()) {
	// set error flag in reply
	dout(7) << "ino " << dis->get_want_ino() << " in this dir, flagging error in "
		<< *curdir << dendl;
	reply->set_flag_error_ino();
	break;
      }
      
      // is this a new mds dir?
      /*
      if (curdir->ino() == MDS_INO_CEPH) {
	char t[10];
	snprintf(t, sizeof(t), "mds%d", from);
	if (t == dis->get_dentry(i)) {
	  // yes.
	  _create_mdsdir_dentry(curdir, from, t, new C_MDS_RetryMessage(mds, dis));
	  //_create_system_file(curdir, t, create_system_inode(MDS_INO_MDSDIR(from), S_IFDIR),
	  //new C_MDS_RetryMessage(mds, dis));
	  reply->put();
	  return;
	}
      }	
      */
      
      // send null dentry
      dout(7) << "dentry " << dis->get_dentry(i) << " dne, returning null in "
	      << *curdir << dendl;
      dn = curdir->add_null_dentry(dis->get_dentry(i));
    }
    assert(dn);

    CDentry::linkage_t *dnl = dn->get_linkage();

    // xlocked dentry?
    //  ...always block on non-tail items (they are unrelated)
    //  ...allow xlocked tail disocvery _only_ if explicitly requested
    bool tailitem = (dis->get_want().depth() == 0) || (i == dis->get_want().depth() - 1);
    if (dn->lock.is_xlocked()) {
      // is this the last (tail) item in the discover traversal?
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "handle_discover allowing discovery of xlocked tail " << *dn << dendl;
      } else {
	dout(7) << "handle_discover blocking on xlocked " << *dn << dendl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      }
    }

    // frozen inode?
    if (dnl->is_primary() && dnl->get_inode()->is_frozen()) {
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "handle_discover allowing discovery of frozen tail " << *dnl->get_inode() << dendl;
      } else if (reply->is_empty()) {
	dout(7) << *dnl->get_inode() << " is frozen, empty reply, waiting" << dendl;
	dnl->get_inode()->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      } else {
	dout(7) << *dnl->get_inode() << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }

    // add dentry
    if (!reply->trace.length())
      reply->starts_with = MDiscoverReply::DENTRY;
    replicate_dentry(dn, from, reply->trace);
    dout(7) << "handle_discover added dentry " << *dn << dendl;
    
    if (!dnl->is_primary()) break;  // stop on null or remote link.
    
    // add inode
    CInode *next = dnl->get_inode();
    assert(next->is_auth());
    
    replicate_inode(next, from, reply->trace);
    dout(7) << "handle_discover added inode " << *next << dendl;
    
    // descend, keep going.
    cur = next;
    continue;
  }

  // how did we do?
  assert(!reply->is_empty());
  dout(7) << "handle_discover sending result back to asker mds" << from << dendl;
  mds->send_message(reply, dis->get_connection());

  dis->put();
}

/* This function DOES put the passed message before returning */
void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  /*
  if (mds->get_state() < MDSMap::STATE_ACTIVE) {
    dout(-7) << "discover_reply NOT ACTIVE YET" << dendl;
    m->put();
    return;
  }
  */
  dout(7) << "discover_reply " << *m << dendl;
  if (m->is_flag_error_dir()) 
    dout(7) << " flag error, dir" << dendl;
  if (m->is_flag_error_dn()) 
    dout(7) << " flag error, dentry = " << m->get_error_dentry() << dendl;
  if (m->is_flag_error_ino()) 
    dout(7) << " flag error, ino = " << m->get_wanted_ino() << dendl;

  list<Context*> finished, error;
  int from = m->get_source().num();

  // starting point
  CInode *cur = get_inode(m->get_base_ino());
  bufferlist::iterator p = m->trace.begin();

  int next = m->starts_with;

  // decrement discover counters
  if (m->get_tid()) {
    map<tid_t,discover_info_t>::iterator p = discovers.find(m->get_tid());
    if (p != discovers.end()) {
      dout(10) << " found tid " << m->get_tid() << dendl;
      discovers.erase(p);
    } else {
      dout(10) << " tid " << m->get_tid() << " not found, must be dup reply" << dendl;
    }
  }

  // discover may start with an inode
  if (!p.end() && next == MDiscoverReply::INODE) {
    cur = add_replica_inode(p, NULL, finished);
    dout(7) << "discover_reply got base inode " << *cur << dendl;
    assert(cur->is_base());
    
    next = MDiscoverReply::DIR;
    
    // take waiters?
    if (cur->is_base() &&
	waiting_for_base_ino[from].count(cur->ino())) {
      finished.swap(waiting_for_base_ino[from][cur->ino()]);
      waiting_for_base_ino[from].erase(cur->ino());
    }
  }
  assert(cur);
  
  // loop over discover results.
  // indexes follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  while (!p.end()) {
    // dir
    frag_t fg;
    CDir *curdir = 0;
    if (next == MDiscoverReply::DIR)
      curdir = add_replica_dir(p, cur, m->get_source().num(), finished);
    else {
      // note: this can only happen our first way around this loop.
      if (p.end() && m->is_flag_error_dn()) {
	fg = cur->pick_dirfrag(m->get_error_dentry());
	curdir = cur->get_dirfrag(fg);
      } else
	curdir = cur->get_dirfrag(m->get_base_dir_frag());
    }

    // dentry error?
    if (p.end() && (m->is_flag_error_dn() || m->is_flag_error_ino())) {
      // error!
      assert(cur->is_dir());
      if (curdir) {
	if (m->get_error_dentry().length()) {
	  dout(7) << " flag_error on dentry " << m->get_error_dentry()
		  << ", triggering dentry" << dendl;
	  curdir->take_dentry_waiting(m->get_error_dentry(), 
				      m->get_wanted_snapid(), m->get_wanted_snapid(), error);
	} else {
	  dout(7) << " flag_error on ino " << m->get_wanted_ino()
		  << ", triggering ino" << dendl;
	  curdir->take_ino_waiting(m->get_wanted_ino(), error);
	}
      } else {
	dout(7) << " flag_error on dentry " << m->get_error_dentry() 
		<< ", triggering dir?" << dendl;
	cur->take_waiting(CInode::WAIT_DIR, error);
      }
      break;
    }
    assert(curdir);

    if (p.end())
      break;
    
    // dentry
    CDentry *dn = add_replica_dentry(p, curdir, finished);
    
    if (p.end())
      break;

    // inode
    cur = add_replica_inode(p, dn, finished);

    next = MDiscoverReply::DIR;
  }

  // dir error?
  // or dir_auth hint?
  if (m->is_flag_error_dir() && !cur->is_dir()) {
    // not a dir.
    cur->take_waiting(CInode::WAIT_DIR, error);
  } else if (m->is_flag_error_dir() ||
	     (m->get_dir_auth_hint() != CDIR_AUTH_UNKNOWN &&
	      m->get_dir_auth_hint() != mds->get_nodeid())) {
    int who = m->get_dir_auth_hint();
    if (who == mds->get_nodeid()) who = -1;
    if (who >= 0)
      dout(7) << " dir_auth_hint is " << m->get_dir_auth_hint() << dendl;

    // try again?
    if (m->get_error_dentry().length()) {
      // wanted a dentry
      frag_t fg = cur->pick_dirfrag(m->get_error_dentry());
      CDir *dir = cur->get_dirfrag(fg);
      filepath relpath(m->get_error_dentry(), 0);
      if (dir) {
	// don't actaully need the hint, now
	if (dir->lookup(m->get_error_dentry()) == 0 &&
	    dir->is_waiting_for_dentry(m->get_error_dentry().c_str(), m->get_wanted_snapid())) 
	  discover_path(dir, m->get_wanted_snapid(), relpath, 0, m->get_wanted_xlocked()); 
	else 
	  dout(7) << " doing nothing, have dir but nobody is waiting on dentry " 
		  << m->get_error_dentry() << dendl;
      } else {
	if (cur->is_waiter_for(CInode::WAIT_DIR)) 
	  discover_path(cur, m->get_wanted_snapid(), relpath, 0, m->get_wanted_xlocked(), who);
	else
	  dout(7) << " doing nothing, nobody is waiting for dir" << dendl;
      }
    } else {
      // wanted just the dir
      frag_t fg = m->get_base_dir_frag();
      if (cur->get_dirfrag(fg) == 0 && cur->is_waiter_for(CInode::WAIT_DIR))
	discover_dir_frag(cur, fg, 0, who);
      else
	dout(7) << " doing nothing, nobody is waiting for dir" << dendl;	  
    }
  }

  // waiters
  finish_contexts(error, -ENOENT);  // finish errors directly
  mds->queue_waiters(finished);

  // done
  m->put();
}



// ----------------------------
// REPLICAS

CDir *MDCache::add_replica_dir(bufferlist::iterator& p, CInode *diri, int from,
			       list<Context*>& finished)
{
  dirfrag_t df;
  ::decode(df, p);

  assert(diri->ino() == df.ino);

  // add it (_replica_)
  CDir *dir = diri->get_dirfrag(df.frag);

  if (dir) {
    // had replica. update w/ new nonce.
    dir->decode_replica(p);
    dout(7) << "add_replica_dir had " << *dir << " nonce " << dir->replica_nonce << dendl;
  } else {
    // force frag to leaf in the diri tree
    if (!diri->dirfragtree.is_leaf(df.frag)) {
      dout(7) << "add_replica_dir forcing frag " << df.frag << " to leaf in the fragtree "
	      << diri->dirfragtree << dendl;
      diri->dirfragtree.force_to_leaf(df.frag);
    }

    // add replica.
    dir = diri->add_dirfrag( new CDir(diri, df.frag, this, false) );
    dir->decode_replica(p);

    // is this a dir_auth delegation boundary?
    if (from != diri->authority().first ||
	diri->is_ambiguous_auth() ||
	diri->is_base())
      adjust_subtree_auth(dir, from);
    
    dout(7) << "add_replica_dir added " << *dir << " nonce " << dir->replica_nonce << dendl;
    
    // get waiters
    diri->take_waiting(CInode::WAIT_DIR, finished);
  }

  return dir;
}

CDir *MDCache::forge_replica_dir(CInode *diri, frag_t fg, int from)
{
  assert(mds->mdsmap->get_state(from) < MDSMap::STATE_REJOIN);
  
  // forge a replica.
  CDir *dir = diri->add_dirfrag( new CDir(diri, fg, this, false) );
  
  // i'm assuming this is a subtree root. 
  adjust_subtree_auth(dir, from);

  dout(7) << "forge_replica_dir added " << *dir << " while mds" << from << " is down" << dendl;

  return dir;
}

CDentry *MDCache::add_replica_dentry(bufferlist::iterator& p, CDir *dir, list<Context*>& finished)
{
  string name;
  snapid_t last;
  ::decode(name, p);
  ::decode(last, p);

  CDentry *dn = dir->lookup(name, last);
  
  // have it?
  if (dn) {
    dn->decode_replica(p, false);
    dout(7) << "add_replica_dentry had " << *dn << dendl;
  } else {
    dn = dir->add_null_dentry(name, 1 /* this will get updated below */, last);
    dn->decode_replica(p, true);
    dout(7) << "add_replica_dentry added " << *dn << dendl;
  }

  dir->take_dentry_waiting(name, dn->first, dn->last, finished);

  return dn;
}

CInode *MDCache::add_replica_inode(bufferlist::iterator& p, CDentry *dn, list<Context*>& finished)
{
  inodeno_t ino;
  snapid_t last;
  ::decode(ino, p);
  ::decode(last, p);
  CInode *in = get_inode(ino, last);
  if (!in) {
    in = new CInode(this, false, 1, last);
    in->decode_replica(p, true);
    add_inode(in);
    if (in->ino() == MDS_INO_ROOT)
      in->inode_auth.first = 0;
    else if (in->is_mdsdir())
      in->inode_auth.first = in->ino() - MDS_INO_MDSDIR_OFFSET;
    dout(10) << "add_replica_inode added " << *in << dendl;
    if (dn) {
      assert(dn->get_linkage()->is_null());
      dn->dir->link_primary_inode(dn, in);
    }
  } else {
    in->decode_replica(p, false);
    dout(10) << "add_replica_inode had " << *in << dendl;
    assert(!dn || dn->get_linkage()->get_inode() == in);
  }

  if (dn) {
    assert(dn->get_linkage()->is_primary());
    assert(dn->get_linkage()->get_inode() == in);
    
    dn->get_dir()->take_ino_waiting(in->ino(), finished);
  }
  
  return in;
}

 
void MDCache::replicate_stray(CDentry *straydn, int who, bufferlist& bl)
{
  replicate_inode(get_myin(), who, bl);
  replicate_dir(straydn->get_dir()->inode->get_parent_dn()->get_dir(), who, bl);
  replicate_dentry(straydn->get_dir()->inode->get_parent_dn(), who, bl);
  replicate_inode(straydn->get_dir()->inode, who, bl);
  replicate_dir(straydn->get_dir(), who, bl);
  replicate_dentry(straydn, who, bl);
}
   
CDentry *MDCache::add_replica_stray(bufferlist &bl, int from)
{
  list<Context*> finished;
  bufferlist::iterator p = bl.begin();

  CInode *mdsin = add_replica_inode(p, NULL, finished);
  CDir *mdsdir = add_replica_dir(p, mdsin, from, finished);
  CDentry *straydirdn = add_replica_dentry(p, mdsdir, finished);
  CInode *strayin = add_replica_inode(p, straydirdn, finished);
  CDir *straydir = add_replica_dir(p, strayin, from, finished);
  CDentry *straydn = add_replica_dentry(p, straydir, finished);
  if (!finished.empty())
    mds->queue_waiters(finished);

  return straydn;
}


int MDCache::send_dir_updates(CDir *dir, bool bcast)
{
  // this is an FYI, re: replication

  set<int> who;
  if (bcast) {
    mds->get_mds_map()->get_active_mds_set(who);
  } else {
    for (map<int,int>::iterator p = dir->replicas_begin();
	 p != dir->replicas_end();
	 ++p)
      who.insert(p->first);
  }
  
  dout(7) << "sending dir_update on " << *dir << " bcast " << bcast << " to " << who << dendl;

  filepath path;
  dir->inode->make_path(path);

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = who.begin();
       it != who.end();
       it++) {
    if (*it == whoami) continue;
    //if (*it == except) continue;
    dout(7) << "sending dir_update on " << *dir << " to " << *it << dendl;

    mds->send_message_mds(new MDirUpdate(mds->get_nodeid(),
					 dir->dirfrag(),
					 dir->dir_rep,
					 dir->dir_rep_by,
					 path,
					 bcast),
			  *it);
  }

  return 0;
}

/* This function DOES put the passed message before returning */
void MDCache::handle_dir_update(MDirUpdate *m)
{
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(5) << "dir_update on " << m->get_dirfrag() << ", don't have it" << dendl;

    // discover it?
    if (m->should_discover()) {
      // only try once! 
      // this is key to avoid a fragtree update race, among other things.
      m->tried_discover();        
      vector<CDentry*> trace;
      CInode *in;
      filepath path = m->get_path();
      dout(5) << "trying discover on dir_update for " << path << dendl;
      int r = path_traverse(0, m, path, &trace, &in, MDS_TRAVERSE_DISCOVER);
      if (r > 0)
        return;
      assert(r == 0);
      open_remote_dirfrag(in, m->get_dirfrag().frag, 
			  new C_MDS_RetryMessage(mds, m));
      return;
    }

    m->put();
    return;
  }

  // update
  dout(5) << "dir_update on " << *dir << dendl;
  dir->dir_rep = m->get_dir_rep();
  dir->dir_rep_by = m->get_dir_rep_by();
  
  // done
  m->put();
}





// LINK

void MDCache::send_dentry_link(CDentry *dn)
{
  dout(7) << "send_dentry_link " << *dn << dendl;

  for (map<int,int>::iterator p = dn->replicas_begin(); 
       p != dn->replicas_end(); 
       p++) {
    if (mds->mdsmap->get_state(p->first) < MDSMap::STATE_REJOIN) 
      continue;
    CDentry::linkage_t *dnl = dn->get_linkage();
    MDentryLink *m = new MDentryLink(dn->get_dir()->dirfrag(), dn->name,
				     dnl->is_primary());
    if (dnl->is_primary()) {
      dout(10) << "  primary " << *dnl->get_inode() << dendl;
      replicate_inode(dnl->get_inode(), p->first, m->bl);
    } else if (dnl->is_remote()) {
      inodeno_t ino = dnl->get_remote_ino();
      __u8 d_type = dnl->get_remote_d_type();
      dout(10) << "  remote " << ino << " " << d_type << dendl;
      ::encode(ino, m->bl);
      ::encode(d_type, m->bl);
    } else
      assert(0);   // aie, bad caller!
    mds->send_message_mds(m, p->first);
  }
}

/* This function DOES put the passed message before returning */
void MDCache::handle_dentry_link(MDentryLink *m)
{
  CDir *dir = get_dirfrag(m->get_dirfrag());
  assert(dir);
  CDentry *dn = dir->lookup(m->get_dn());
  assert(dn);

  dout(7) << "handle_dentry_link on " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_linkage();

  assert(!dn->is_auth());
  assert(dnl->is_null());

  bufferlist::iterator p = m->bl.begin();
  list<Context*> finished;
  
  if (m->get_is_primary()) {
    // primary link.
    add_replica_inode(p, dn, finished);
  } else {
    // remote link, easy enough.
    inodeno_t ino;
    __u8 d_type;
    ::decode(ino, p);
    ::decode(d_type, p);
    dir->link_remote_inode(dn, ino, d_type);
  }
  
  if (!finished.empty())
    mds->queue_waiters(finished);

  m->put();
  return;
}


// UNLINK

void MDCache::send_dentry_unlink(CDentry *dn, CDentry *straydn)
{
  dout(10) << "send_dentry_unlink " << *dn << dendl;
  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    MDentryUnlink *unlink = new MDentryUnlink(dn->get_dir()->dirfrag(), dn->name);
    if (straydn)
      replicate_stray(straydn, it->first, unlink->straybl);
    mds->send_message_mds(unlink, it->first);
  }
}

/* This function DOES put the passed message before returning */
void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(7) << "handle_dentry_unlink don't have dirfrag " << m->get_dirfrag() << dendl;
  } else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << dendl;
    } else {
      dout(7) << "handle_dentry_unlink on " << *dn << dendl;
      CDentry::linkage_t *dnl = dn->get_linkage();

      // straydn
      CDentry *straydn = NULL;
      if (m->straybl.length()) {
	int from = m->get_source().num();
	straydn = add_replica_stray(m->straybl, from);
      }

      // open inode?
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	dn->dir->unlink_inode(dn);
	assert(straydn);
	straydn->dir->link_primary_inode(straydn, in);

	// in->first is lazily updated on replica; drag it forward so
	// that we always keep it in sync with the dnq
	assert(straydn->first >= in->first);
	in->first = straydn->first;

	// update subtree map?
	if (in->is_dir()) 
	  adjust_subtree_after_rename(in, dir);

	// send caps to auth (if we're not already)
	if (in->is_any_caps() &&
	    !in->state_test(CInode::STATE_EXPORTINGCAPS))
	  migrator->export_caps(in);
	
	lru.lru_bottouch(straydn);  // move stray to end of lru

      } else {
	assert(dnl->is_remote());
	dn->dir->unlink_inode(dn);
      }
      assert(dnl->is_null());
      
      // move to bottom of lru
      lru.lru_bottouch(dn);
    }
  }

  m->put();
  return;
}






// ===================================================================



// ===================================================================
// FRAGMENT


/** 
 * adjust_dir_fragments -- adjust fragmentation for a directory
 *
 * @diri - directory inode
 * @basefrag - base fragment
 * @bits - bit adjustment.  positive for split, negative for merge.
 */
void MDCache::adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
				   list<CDir*>& resultfrags, 
				   list<Context*>& waiters,
				   bool replay)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " " << bits 
	   << " on " << *diri << dendl;

  list<CDir*> srcfrags;
  diri->get_dirfrags_under(basefrag, srcfrags);

  adjust_dir_fragments(diri, srcfrags, basefrag, bits, resultfrags, waiters, replay);
}

void MDCache::adjust_dir_fragments(CInode *diri,
				   list<CDir*>& srcfrags,
				   frag_t basefrag, int bits,
				   list<CDir*>& resultfrags, 
				   list<Context*>& waiters,
				   bool replay)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " bits " << bits
	   << " srcfrags " << srcfrags
	   << " on " << *diri << dendl;

  // adjust fragtree
  // yuck.  we may have discovered the inode while it was being fragmented.
  if (!diri->dirfragtree.is_leaf(basefrag))
    diri->dirfragtree.force_to_leaf(basefrag);

  if (bits > 0)
    diri->dirfragtree.split(basefrag, bits);
  dout(10) << " new fragtree is " << diri->dirfragtree << dendl;

  // split
  CDir *parent_dir = diri->get_parent_dir();
  CDir *parent_subtree = 0;
  if (parent_dir)
    parent_subtree = get_subtree_root(parent_dir);

  if (bits > 0) {
    // SPLIT
    assert(srcfrags.size() == 1);
    CDir *dir = srcfrags.front();

    dir->split(bits, resultfrags, waiters, replay);

    // did i change the subtree map?
    if (dir->is_subtree_root()) {
      // new frags are now separate subtrees
      for (list<CDir*>::iterator p = resultfrags.begin();
	   p != resultfrags.end();
	   ++p)
	subtrees[*p].clear();   // new frag is now its own subtree
      
      // was i a bound?
      if (parent_subtree) {
	assert(subtrees[parent_subtree].count(dir));
	subtrees[parent_subtree].erase(dir);
	for (list<CDir*>::iterator p = resultfrags.begin();
	     p != resultfrags.end();
	     ++p)
	  subtrees[parent_subtree].insert(*p);
      }
      
      // adjust my bounds.
      set<CDir*> bounds;
      bounds.swap(subtrees[dir]);
      subtrees.erase(dir);
      for (set<CDir*>::iterator p = bounds.begin();
	   p != bounds.end();
	   ++p) {
	CDir *frag = get_subtree_root((*p)->get_parent_dir());
	subtrees[frag].insert(*p);
      }

      show_subtrees(10);

      // dir has no PIN_SUBTREE; CDir::purge_stolen() drops it.
      dir->dir_auth = CDIR_AUTH_DEFAULT;
    }
    
    diri->close_dirfrag(dir->get_frag());
    
  } else {
    // MERGE

    // are my constituent bits subtrees?  if so, i will be too.
    // (it's all or none, actually.)
    bool was_subtree = false;
    set<CDir*> new_bounds;
    for (list<CDir*>::iterator p = srcfrags.begin(); p != srcfrags.end(); p++) {
      CDir *dir = *p;
      if (dir->is_subtree_root()) {
	dout(10) << " taking srcfrag subtree bounds from " << *dir << dendl;
	was_subtree = true;
	map<CDir*, set<CDir*> >::iterator q = subtrees.find(dir);
	set<CDir*>::iterator r = q->second.begin();
	while (r != subtrees[dir].end()) {
	  new_bounds.insert(*r);
	  subtrees[dir].erase(r++);
	}
	subtrees.erase(q);
	
	// remove myself as my parent's bound
	if (parent_subtree)
	  subtrees[parent_subtree].erase(dir);
      }
    }
    
    // merge
    CDir *f = new CDir(diri, basefrag, this, srcfrags.front()->is_auth());
    f->merge(srcfrags, waiters, replay);
    diri->add_dirfrag(f);

    if (was_subtree) {
      subtrees[f].swap(new_bounds);
      if (parent_subtree)
	subtrees[parent_subtree].insert(f);
      
      show_subtrees(10);
    }

    resultfrags.push_back(f);
  }
}


class C_MDC_FragmentFrozen : public Context {
  MDCache *mdcache;
  list<CDir*> dirs;
  frag_t basefrag;
  int by;
public:
  C_MDC_FragmentFrozen(MDCache *m, list<CDir*> d, frag_t bf, int b) : mdcache(m), dirs(d), basefrag(bf), by(b) {}
  virtual void finish(int r) {
    mdcache->fragment_frozen(dirs, basefrag, by);
  }
};


bool MDCache::can_fragment_lock(CInode *diri)
{
  if (!diri->dirfragtreelock.can_wrlock(-1)) {
    dout(7) << "can_fragment: can't wrlock dftlock" << dendl;
    mds->locker->scatter_nudge(&diri->dirfragtreelock, NULL);
    return false;
  }
  return true;
}

bool MDCache::can_fragment(CInode *diri, list<CDir*>& dirs)
{
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "can_fragment: cluster degraded, no fragmenting for now" << dendl;
    return false;
  }
  if (diri->get_parent_dir() &&
      diri->get_parent_dir()->get_inode()->is_stray()) {
    dout(7) << "can_fragment: i won't merge|split anything in stray" << dendl;
    return false;
  }
  if (diri->is_mdsdir() || diri->ino() == MDS_INO_CEPH) {
    dout(7) << "can_fragment: i won't fragment the mdsdir or .ceph" << dendl;
    return false;
  }

  for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
    CDir *dir = *p;
    if (dir->state_test(CDir::STATE_FRAGMENTING)) {
      dout(7) << "can_fragment: already fragmenting " << *dir << dendl;
      return false;
    }
    if (!dir->is_auth()) {
      dout(7) << "can_fragment: not auth on " << *dir << dendl;
      return false;
    }
    if (dir->is_frozen() ||
	dir->is_freezing()) {
      dout(7) << "can_fragment: can't merge, freezing|frozen.  wait for other exports to finish first." << dendl;
      return false;
    }
  }

  return true;
}

void MDCache::split_dir(CDir *dir, int bits)
{
  dout(7) << "split_dir " << *dir << " bits " << bits << dendl;
  assert(dir->is_auth());
  CInode *diri = dir->inode;

  list<CDir*> dirs;
  dirs.push_back(dir);

  if (!can_fragment(diri, dirs))
    return;
  if (!can_fragment_lock(diri)) {
    dout(10) << " requeuing dir " << dir->dirfrag() << dendl;
    mds->balancer->queue_split(dir);
    return;
  }

  C_Gather *gather = new C_Gather(new C_MDC_FragmentFrozen(this, dirs, dir->get_frag(), bits));
  fragment_freeze_dirs(dirs, gather);

  // initial mark+complete pass
  fragment_mark_and_complete(dirs);
}

void MDCache::merge_dir(CInode *diri, frag_t frag)
{
  dout(7) << "merge_dir to " << frag << " on " << *diri << dendl;

  list<CDir*> dirs;
  if (!diri->get_dirfrags_under(frag, dirs)) {
    dout(7) << "don't have all frags under " << frag << " for " << *diri << dendl;
    return;
  }

  if (diri->dirfragtree.is_leaf(frag)) {
    dout(10) << " " << frag << " already a leaf for " << *diri << dendl;
    return;
  }

  if (!can_fragment(diri, dirs))
    return;
  if (!can_fragment_lock(diri)) {
    //dout(10) << " requeuing dir " << dir->dirfrag() << dendl;
    //mds->mdbalancer->split_queue.insert(dir->dirfrag());
    return;
  }

  C_Gather *gather = new C_Gather(new C_MDC_FragmentFrozen(this, dirs, frag, 0));
  fragment_freeze_dirs(dirs, gather);

  // initial mark+complete pass
  fragment_mark_and_complete(dirs);
}

void MDCache::fragment_freeze_dirs(list<CDir*>& dirs, C_Gather *gather)
{
  for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
    CDir *dir = *p;
    dir->auth_pin(dir);  // until we mark and complete them
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->freeze_dir();
    assert(dir->is_freezing_dir());
    dir->add_waiter(CDir::WAIT_FROZEN, gather->new_sub());
  }
}

class C_MDC_FragmentMarking : public Context {
  MDCache *mdcache;
  list<CDir*>& dirs;
public:
  C_MDC_FragmentMarking(MDCache *m, list<CDir*>& d) : mdcache(m), dirs(d) {}
  virtual void finish(int r) {
    mdcache->fragment_mark_and_complete(dirs);
  }
};

void MDCache::fragment_mark_and_complete(list<CDir*>& dirs)
{
  CInode *diri = dirs.front()->get_inode();
  dout(10) << "fragment_mark_and_complete " << dirs << " on " << *diri << dendl;

  C_Gather *gather = 0;
  
  for (list<CDir*>::iterator p = dirs.begin();
       p != dirs.end();
       ++p) {
    CDir *dir = *p;
    
    if (!dir->is_complete()) {
      dout(15) << " fetching incomplete " << *dir << dendl;
      if (!gather)
	gather = new C_Gather(new C_MDC_FragmentMarking(this, dirs));
      dir->fetch(gather->new_sub(), 
		 true);  // ignore authpinnability
    } 
    else if (!dir->state_test(CDir::STATE_DNPINNEDFRAG)) {
      dout(15) << " marking " << *dir << dendl;
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	p->second->get(CDentry::PIN_FRAGMENTING);
	p->second->state_set(CDentry::STATE_FRAGMENTING);
      }
      dir->state_set(CDir::STATE_DNPINNEDFRAG);
      dir->auth_unpin(dir);
    }
    else {
      dout(15) << " already marked " << *dir << dendl;
    }
  }

  // flush log so that request auth_pins are retired
  mds->mdlog->flush();
}

void MDCache::fragment_unmark_unfreeze_dirs(list<CDir*>& dirs)
{
  dout(10) << "fragment_unmark_unfreeze_dirs " << dirs << dendl;
  for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
    CDir *dir = *p;
    dout(10) << " frag " << *dir << dendl;

    assert(dir->state_test(CDir::STATE_DNPINNEDFRAG));
    dir->state_clear(CDir::STATE_DNPINNEDFRAG);

    assert(dir->state_test(CDir::STATE_FRAGMENTING));
    dir->state_clear(CDir::STATE_FRAGMENTING);

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) {
      p->second->state_clear(CDentry::STATE_FRAGMENTING);
      p->second->put(CDentry::PIN_FRAGMENTING);
    }

    dir->unfreeze_dir();
  }
}

class C_MDC_FragmentStored : public Context {
  MDCache *mdcache;
  list<CDir*> dirs;
  frag_t basefrag;
  int bits;
public:
  C_MDC_FragmentStored(MDCache *m, list<CDir*>& d, frag_t bf, int b) :
    mdcache(m), dirs(d), basefrag(bf), bits(b) {}
  virtual void finish(int r) {
    mdcache->fragment_stored(dirs, basefrag, bits);
  }
};

void MDCache::fragment_frozen(list<CDir*>& dirs, frag_t basefrag, int bits)
{
  CInode *diri = dirs.front()->get_inode();

  if (bits > 0) {
    assert(dirs.size() == 1);
  } else {
    assert(bits == 0);
  }

  dout(10) << "fragment_frozen " << dirs << " " << basefrag << " by " << bits 
	   << " on " << *diri << dendl;

  // wrlock dirfragtreelock
  if (!diri->dirfragtreelock.can_wrlock(-1)) {
    dout(10) << " can't wrlock " << diri->dirfragtreelock << " on " << *diri << dendl;
    fragment_unmark_unfreeze_dirs(dirs);
    return;
  }
  diri->dirfragtreelock.get_wrlock(true);

  // prevent a racing gather on any other scatterlocks too
  diri->nestlock.get_wrlock(true);
  diri->filelock.get_wrlock(true);

  // refragment
  list<CDir*> resultfrags;
  list<Context*> waiters;
  adjust_dir_fragments(diri, dirs, basefrag, bits, resultfrags, waiters, false);
  mds->queue_waiters(waiters);

  C_Gather *gather = new C_Gather(new C_MDC_FragmentStored(this, resultfrags, basefrag, bits));

  // freeze, store resulting frags
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->commit(0, gather->new_sub());
    dir->_freeze_dir();
  }  
}

class C_MDC_FragmentLogged : public Context {
  MDCache *mdcache;
  Mutation *mut;
  list<CDir*> resultfrags;
  frag_t basefrag;
  int bits;
public:
  C_MDC_FragmentLogged(MDCache *m, Mutation *mu, list<CDir*>& r, frag_t bf, int bi) : 
    mdcache(m), mut(mu), resultfrags(r), basefrag(bf), bits(bi) {}
  virtual void finish(int r) {
    mdcache->fragment_logged(mut, resultfrags, basefrag, bits);
  }
};

void MDCache::fragment_stored(list<CDir*>& resultfrags, frag_t basefrag, int bits)
{
  CInode *diri = resultfrags.front()->get_inode();
  dout(10) << "fragment_stored " << resultfrags << " " << basefrag << " by " << bits 
	   << " on " << *diri << dendl;

  Mutation *mut = new Mutation;

  mut->ls = mds->mdlog->get_current_segment();
  EFragment *le = new EFragment(mds->mdlog, diri->ino(), basefrag, bits);
  mds->mdlog->start_entry(le);

  le->metablob.add_dir_context(*resultfrags.begin());

  // dft lock
  mds->locker->mark_updated_scatterlock(&diri->dirfragtreelock);
  mut->ls->dirty_dirfrag_dirfragtree.push_back(&diri->item_dirty_dirfrag_dirfragtree);
  mut->add_updated_lock(&diri->dirfragtreelock);

  /*
  // filelock
  mds->locker->mark_updated_scatterlock(&diri->filelock);
  mut->ls->dirty_dirfrag_dir.push_back(&diri->item_dirty_dirfrag_dir);
  mut->add_updated_lock(&diri->filelock);

  // dirlock
  mds->locker->mark_updated_scatterlock(&diri->nestlock);
  mut->ls->dirty_dirfrag_nest.push_back(&diri->item_dirty_dirfrag_nest);
  mut->add_updated_lock(&diri->nestlock);
  */

  // journal new dirfrag fragstats for each new fragment.
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    le->metablob.add_dir(dir, false);
  }
  
  mds->mdlog->submit_entry(le,
			   new C_MDC_FragmentLogged(this, mut, resultfrags, basefrag, bits));
  mds->mdlog->flush();
}

void MDCache::fragment_logged(Mutation *mut, list<CDir*>& resultfrags, frag_t basefrag, int bits)
{
  CInode *diri = resultfrags.front()->get_inode();

  dout(10) << "fragment_logged " << resultfrags << " " << basefrag << " bits " << bits 
	   << " on " << *diri << dendl;
  
  // tell peers
  CDir *first = *resultfrags.begin();
  for (map<int,int>::iterator p = first->replica_map.begin();
       p != first->replica_map.end();
       p++) {
    MMDSFragmentNotify *notify = new MMDSFragmentNotify(diri->ino(), basefrag, bits);

    /*
    // freshly replicate new dirs to peers
    for (list<CDir*>::iterator q = resultfrags.begin(); q != resultfrags.end(); q++)
      replicate_dir(*q, p->first, notify->basebl);
    */

    mds->send_message_mds(notify, p->first);
  } 
  
  mut->apply();  // mark scatterlock
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  // drop dft wrlock
  mds->locker->wrlock_finish(&diri->dirfragtreelock, NULL);
  mds->locker->wrlock_finish(&diri->nestlock, NULL);
  mds->locker->wrlock_finish(&diri->filelock, NULL);

  // unfreeze resulting frags
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    
    // unmark, unfreeze
    dir->state_clear(CDir::STATE_FRAGMENTING);  

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) { 
      CDentry *dn = p->second;
      if (dn->state_test(CDentry::STATE_FRAGMENTING)) 
	dn->put(CDentry::PIN_FRAGMENTING);
    }

    dir->unfreeze_dir();
  }
}


/* This function DOES put the passed message before returning */
void MDCache::handle_fragment_notify(MMDSFragmentNotify *notify)
{
  dout(10) << "handle_fragment_notify " << *notify << " from " << notify->get_source() << dendl;

  CInode *diri = get_inode(notify->get_ino());
  if (diri) {
    list<Context*> waiters;

    // refragment
    list<CDir*> resultfrags;
    adjust_dir_fragments(diri, notify->get_basefrag(), notify->get_bits(), 
			 resultfrags, waiters, false);

    /*
    // add new replica dirs values
    bufferlist::iterator p = notify->basebl.begin();
    while (!p.end()) {
      add_replica_dir(p, diri, notify->get_source().num(), waiters);
    */

    mds->queue_waiters(waiters);
  }

  notify->put();
}





// ==============================================================
// debug crap

void MDCache::show_subtrees(int dbl)
{
  //dout(10) << "show_subtrees" << dendl;

  if (dbl > g_conf.debug && dbl > g_conf.debug_mds) 
    return;  // i won't print anything.

  if (subtrees.empty()) {
    dout(dbl) << "show_subtrees - no subtrees" << dendl;
    return;
  }

  // root frags
  list<CDir*> basefrags;
  for (set<CInode*>::iterator p = base_inodes.begin();
       p != base_inodes.end();
       p++) 
    (*p)->get_dirfrags(basefrags);
  //dout(15) << "show_subtrees, base dirfrags " << basefrags << dendl;
  dout(15) << "show_subtrees" << dendl;

  // queue stuff
  list<pair<CDir*,int> > q;
  string indent;
  set<CDir*> seen;

  // calc max depth
  for (list<CDir*>::iterator p = basefrags.begin(); p != basefrags.end(); ++p) 
    q.push_back(pair<CDir*,int>(*p, 0));

  set<CDir*> subtrees_seen;

  int depth = 0;
  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (subtrees.count(dir) == 0) continue;

    subtrees_seen.insert(dir);

    if (d > depth) depth = d;

    // sanity check
    //dout(25) << "saw depth " << d << " " << *dir << dendl;
    if (seen.count(dir)) dout(0) << "aah, already seen " << *dir << dendl;
    assert(seen.count(dir) == 0);
    seen.insert(dir);

    // nested items?
    if (!subtrees[dir].empty()) {
      for (set<CDir*>::iterator p = subtrees[dir].begin();
	   p != subtrees[dir].end();
	   ++p) {
	//dout(25) << " saw sub " << **p << dendl;
	q.push_front(pair<CDir*,int>(*p, d+1));
      }
    }
  }


  // print tree
  for (list<CDir*>::iterator p = basefrags.begin(); p != basefrags.end(); ++p) 
    q.push_back(pair<CDir*,int>(*p, 0));

  while (!q.empty()) {
    CDir *dir = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (subtrees.count(dir) == 0) continue;

    // adjust indenter
    while ((unsigned)d < indent.size()) 
      indent.resize(d);
    
    // pad
    string pad = "______________________________________";
    pad.resize(depth*2+1-indent.size());
    if (!subtrees[dir].empty()) 
      pad[0] = '.'; // parent


    string auth;
    if (dir->is_auth())
      auth = "auth ";
    else
      auth = " rep ";

    char s[10];
    if (dir->get_dir_auth().second == CDIR_AUTH_UNKNOWN)
      snprintf(s, sizeof(s), "%2d   ", dir->get_dir_auth().first);
    else
      snprintf(s, sizeof(s), "%2d,%2d", dir->get_dir_auth().first, dir->get_dir_auth().second);
    
    // print
    dout(dbl) << indent << "|_" << pad << s << " " << auth << *dir << dendl;

    if (dir->ino() == MDS_INO_ROOT)
      assert(dir->inode == root);
    if (dir->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
      assert(dir->inode == myin);
    if (dir->ino() == MDS_INO_STRAY(mds->get_nodeid()))
      assert(dir->inode == stray);

    // nested items?
    if (!subtrees[dir].empty()) {
      // more at my level?
      if (!q.empty() && q.front().second == d)
	indent += "| ";
      else
	indent += "  ";

      for (set<CDir*>::iterator p = subtrees[dir].begin();
	   p != subtrees[dir].end();
	   ++p) 
	q.push_front(pair<CDir*,int>(*p, d+2));
    }
  }

  // verify there isn't stray crap in subtree map
  int lost = 0;
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    if (subtrees_seen.count(p->first)) continue;
    dout(10) << "*** stray/lost entry in subtree map: " << *p->first << dendl;
    lost++;
  }
  assert(lost == 0);
}


void MDCache::show_cache()
{
  dout(7) << "show_cache" << dendl;
  
  for (hash_map<vinodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    // unlinked?
    if (!it->second->parent)
      dout(7) << " unlinked " << *it->second << dendl;
    
    // dirfrags?
    list<CDir*> dfs;
    it->second->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
      dout(7) << "  dirfrag " << *dir << dendl;
	    
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	dout(7) << "   dentry " << *dn << dendl;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (dnl->is_primary() && dnl->get_inode()) 
	  dout(7) << "    inode " << *dnl->get_inode() << dendl;
      }
    }
  }
}


void MDCache::dump_cache(const char *fn)
{
  char deffn[200];
  if (!fn) {
    snprintf(deffn, sizeof(deffn), "cachedump.%d.mds%d", (int)mds->mdsmap->get_epoch(), mds->get_nodeid());
    fn = deffn;
  }

  dout(1) << "dump_cache to " << fn << dendl;

  ofstream myfile;
  myfile.open(fn);
  
  for (hash_map<vinodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    CInode *in = it->second;
    myfile << *in << std::endl;

    list<CDir*> dfs;
    in->get_dirfrags(dfs);
    for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
      CDir *dir = *p;
      myfile << " " << *dir << std::endl;
      
      for (CDir::map_t::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	CDentry *dn = q->second;
	myfile << "  " << *dn << std::endl;
      }
    }
  }

  myfile.close();
}
