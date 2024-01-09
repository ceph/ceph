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

#include <errno.h>
#include <ostream>
#include <string>
#include <string_view>
#include <map>

#include "MDCache.h"
#include "MDSRank.h"
#include "Server.h"
#include "Locker.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "Migrator.h"
#include "ScrubStack.h"

#include "SnapClient.h"

#include "MDSMap.h"

#include "CInode.h"
#include "CDir.h"

#include "Mutation.h"

#include "include/ceph_fs.h"
#include "include/filepath.h"
#include "include/util.h"

#include "messages/MClientCaps.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "common/MemoryModel.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/safe_io.h"

#include "osdc/Journaler.h"
#include "osdc/Filer.h"

#include "events/ESubtreeMap.h"
#include "events/ELid.h"
#include "events/EUpdate.h"
#include "events/EPeerUpdate.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"
#include "events/ECommitted.h"
#include "events/EPurged.h"
#include "events/ESessions.h"

#include "InoTable.h"
#include "fscrypt.h"

#include "common/Timer.h"

#include "perfglue/heap_profiler.h"


#include "common/config.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)

using namespace std;

static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".cache ";
}

set<int> SimpleLock::empty_gather_set;


/**
 * All non-I/O contexts that require a reference
 * to an MDCache instance descend from this.
 */
class MDCacheContext : public virtual MDSContext {
protected:
  MDCache *mdcache;
  MDSRank *get_mds() override
  {
    ceph_assert(mdcache != NULL);
    return mdcache->mds;
  }
public:
  explicit MDCacheContext(MDCache *mdc_) : mdcache(mdc_) {}
};

class MDCacheLogContext : public virtual MDSLogContextBase {
protected:
  MDCache *mdcache;
  MDSRank *get_mds() override
  {
    ceph_assert(mdcache != NULL);
    return mdcache->mds;
  }
public:
  explicit MDCacheLogContext(MDCache *mdc_) : mdcache(mdc_) {}
};

MDCache::MDCache(MDSRank *m, PurgeQueue &purge_queue_) :
  mds(m),
  open_file_table(m),
  filer(m->objecter, m->finisher),
  stray_manager(m, purge_queue_),
  recovery_queue(m),
  trim_counter(g_conf().get_val<double>("mds_cache_trim_decay_rate"))
{
  migrator.reset(new Migrator(mds, this));

  max_dir_commit_size = g_conf()->mds_dir_max_commit_size ?
                        (g_conf()->mds_dir_max_commit_size << 20) :
                        (0.9 *(g_conf()->osd_max_write_size << 20));

  cache_memory_limit = g_conf().get_val<Option::size_t>("mds_cache_memory_limit");
  cache_reservation = g_conf().get_val<double>("mds_cache_reservation");
  cache_health_threshold = g_conf().get_val<double>("mds_health_cache_threshold");

  export_ephemeral_distributed_config =  g_conf().get_val<bool>("mds_export_ephemeral_distributed");
  export_ephemeral_random_config =  g_conf().get_val<bool>("mds_export_ephemeral_random");
  export_ephemeral_random_max = g_conf().get_val<double>("mds_export_ephemeral_random_max");

  symlink_recovery = g_conf().get_val<bool>("mds_symlink_recovery");

  kill_shutdown_at = g_conf().get_val<uint64_t>("mds_kill_shutdown_at");

  lru.lru_set_midpoint(g_conf().get_val<double>("mds_cache_mid"));

  bottom_lru.lru_set_midpoint(0);

  decayrate.set_halflife(g_conf()->mds_decay_halflife);

  upkeeper = std::thread(&MDCache::upkeep_main, this);
}

MDCache::~MDCache() 
{
  if (logger) {
    g_ceph_context->get_perfcounters_collection()->remove(logger.get());
  }
  if (upkeeper.joinable())
    upkeeper.join();
}

void MDCache::handle_conf_change(const std::set<std::string>& changed, const MDSMap& mdsmap)
{
  dout(20) << "config changes: " << changed << dendl;
  if (changed.count("mds_cache_memory_limit"))
    cache_memory_limit = g_conf().get_val<Option::size_t>("mds_cache_memory_limit");
  if (changed.count("mds_cache_reservation"))
    cache_reservation = g_conf().get_val<double>("mds_cache_reservation");

  bool ephemeral_pin_config_changed = false;
  if (changed.count("mds_export_ephemeral_distributed")) {
    export_ephemeral_distributed_config = g_conf().get_val<bool>("mds_export_ephemeral_distributed");
    dout(10) << "Migrating any ephemeral distributed pinned inodes" << dendl;
    /* copy to vector to avoid removals during iteration */
    ephemeral_pin_config_changed = true;
  }
  if (changed.count("mds_export_ephemeral_random")) {
    export_ephemeral_random_config = g_conf().get_val<bool>("mds_export_ephemeral_random");
    dout(10) << "Migrating any ephemeral random pinned inodes" << dendl;
    /* copy to vector to avoid removals during iteration */
    ephemeral_pin_config_changed = true;
  }
  if (ephemeral_pin_config_changed) {
    std::vector<CInode*> migrate;
    migrate.assign(export_ephemeral_pins.begin(), export_ephemeral_pins.end());
    for (auto& in : migrate) {
      in->maybe_export_pin(true);
    }
  }
  if (changed.count("mds_export_ephemeral_random_max")) {
    export_ephemeral_random_max = g_conf().get_val<double>("mds_export_ephemeral_random_max");
  }
  if (changed.count("mds_health_cache_threshold"))
    cache_health_threshold = g_conf().get_val<double>("mds_health_cache_threshold");
  if (changed.count("mds_cache_mid"))
    lru.lru_set_midpoint(g_conf().get_val<double>("mds_cache_mid"));
  if (changed.count("mds_cache_trim_decay_rate")) {
    trim_counter = DecayCounter(g_conf().get_val<double>("mds_cache_trim_decay_rate"));
  }
  if (changed.count("mds_symlink_recovery")) {
    symlink_recovery = g_conf().get_val<bool>("mds_symlink_recovery");
    dout(10) << "Storing symlink targets on file object's head " << symlink_recovery << dendl;
  }
  if (changed.count("mds_kill_shutdown_at")) {
    kill_shutdown_at = g_conf().get_val<uint64_t>("mds_kill_shutdown_at");
  }

  migrator->handle_conf_change(changed, mdsmap);
  mds->balancer->handle_conf_change(changed, mdsmap);
}

void MDCache::log_stat()
{
  mds->logger->set(l_mds_inodes, lru.lru_get_size());
  mds->logger->set(l_mds_inodes_pinned, lru.lru_get_num_pinned());
  mds->logger->set(l_mds_inodes_top, lru.lru_get_top());
  mds->logger->set(l_mds_inodes_bottom, lru.lru_get_bot());
  mds->logger->set(l_mds_inodes_pin_tail, lru.lru_get_pintail());
  mds->logger->set(l_mds_inodes_with_caps, num_inodes_with_caps);
  mds->logger->set(l_mds_caps, Capability::count());
  if (root) {
    mds->logger->set(l_mds_root_rfiles, root->get_inode()->rstat.rfiles);
    mds->logger->set(l_mds_root_rbytes, root->get_inode()->rstat.rbytes);
    mds->logger->set(l_mds_root_rsnaps, root->get_inode()->rstat.rsnaps);
  }
}


//

bool MDCache::shutdown()
{
  {
    std::scoped_lock lock(upkeep_mutex);
    upkeep_trim_shutdown = true;
    upkeep_cvar.notify_one();
  }
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
  // add to inode map
  if (in->last == CEPH_NOSNAP) {
    auto &p = inode_map[in->ino()];
    ceph_assert(!p); // should be no dup inos!
    p = in;
  } else {
    auto &p = snap_inode_map[in->vino()];
    ceph_assert(!p); // should be no dup inos!
    p = in;
  }

  if (in->ino() < MDS_INO_SYSTEM_BASE) {
    if (in->ino() == CEPH_INO_ROOT)
      root = in;
    else if (in->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
      myin = in;
    else if (in->is_stray()) {
      if (MDS_INO_STRAY_OWNER(in->ino()) == mds->get_nodeid()) {
	strays[MDS_INO_STRAY_INDEX(in->ino())] = in;
      }
    }
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
    ceph_assert(!dn->is_dirty());
    dn->dir->unlink_inode(dn);   // leave dentry ... FIXME?
  }

  if (o->is_dirty())
    o->mark_clean();
  if (o->is_dirty_parent())
    o->clear_dirty_parent();

  o->clear_scatter_dirty();

  o->clear_clientwriteable();

  o->item_open_file.remove_myself();

  if (o->state_test(CInode::STATE_QUEUEDEXPORTPIN))
    export_pin_queue.erase(o);

  if (o->state_test(CInode::STATE_DELAYEDEXPORTPIN))
    export_pin_delayed_queue.erase(o);

  o->clear_ephemeral_pin(true, true);

  // remove from inode map
  if (o->last == CEPH_NOSNAP) {
    inode_map.erase(o->ino());
  } else {
    o->item_caps.remove_myself();
    snap_inode_map.erase(o->vino());
  }
  o->item_realm.remove_myself();

  clear_taken_inos(o->ino());

  if (o->ino() < MDS_INO_SYSTEM_BASE) {
    if (o == root) root = 0;
    if (o == myin) myin = 0;
    if (o->is_stray()) {
      if (MDS_INO_STRAY_OWNER(o->ino()) == mds->get_nodeid()) {
	strays[MDS_INO_STRAY_INDEX(o->ino())] = 0;
      }
    }
    if (o->is_base())
      base_inodes.erase(o);
  }

  // delete it
  ceph_assert(o->get_num_ref() == 0);
  delete o; 
}

file_layout_t MDCache::gen_default_file_layout(const MDSMap &mdsmap)
{
  file_layout_t result = file_layout_t::get_default();
  result.pool_id = mdsmap.get_first_data_pool();
  return result;
}

file_layout_t MDCache::gen_default_log_layout(const MDSMap &mdsmap)
{
  file_layout_t result = file_layout_t::get_default();
  result.pool_id = mdsmap.get_metadata_pool();
  if (g_conf()->mds_log_segment_size > 0) {
    result.object_size = g_conf()->mds_log_segment_size;
    result.stripe_unit = g_conf()->mds_log_segment_size;
  }
  return result;
}

void MDCache::init_layouts()
{
  default_file_layout = gen_default_file_layout(*(mds->mdsmap));
  default_log_layout = gen_default_log_layout(*(mds->mdsmap));
}

void MDCache::create_unlinked_system_inode(CInode *in, inodeno_t ino, int mode) const
{
  auto _inode = in->_get_inode();
  _inode->ino = ino;
  _inode->version = 1;
  _inode->xattr_version = 1;
  _inode->mode = 0500 | mode;
  _inode->size = 0;
  _inode->ctime = _inode->mtime = _inode->btime = ceph_clock_now();
  _inode->nlink = 1;
  _inode->truncate_size = -1ull;
  _inode->change_attr = 0;
  _inode->export_pin = MDS_RANK_NONE;

  // FIPS zeroization audit 20191117: this memset is not security related.
  memset(&_inode->dir_layout, 0, sizeof(_inode->dir_layout));
  if (_inode->is_dir()) {
    _inode->dir_layout.dl_dir_hash = g_conf()->mds_default_dir_hash;
    _inode->rstat.rsubdirs = 1; /* itself */
    _inode->rstat.rctime = in->get_inode()->ctime;
  } else {
    _inode->layout = default_file_layout;
    ++_inode->rstat.rfiles;
  }
  _inode->accounted_rstat = _inode->rstat;

  if (in->is_base()) {
    if (in->is_root())
      in->inode_auth = mds_authority_t(mds->get_nodeid(), CDIR_AUTH_UNKNOWN);
    else
      in->inode_auth = mds_authority_t(mds_rank_t(in->ino() - MDS_INO_MDSDIR_OFFSET), CDIR_AUTH_UNKNOWN);
    in->open_snaprealm();  // empty snaprealm
    ceph_assert(!in->snaprealm->parent); // created its own
    in->snaprealm->srnode.seq = 1;
  }
}

CInode *MDCache::create_system_inode(inodeno_t ino, int mode)
{
  dout(0) << "creating system inode with ino:" << ino << dendl;
  CInode *in = new CInode(this);
  create_unlinked_system_inode(in, ino, mode);
  add_inode(in);
  return in;
}

CInode *MDCache::create_root_inode()
{
  CInode *in = create_system_inode(CEPH_INO_ROOT, S_IFDIR|0755);
  auto _inode = in->_get_inode();
  _inode->uid = g_conf()->mds_root_ino_uid;
  _inode->gid = g_conf()->mds_root_ino_gid;
  _inode->layout = default_file_layout;
  _inode->layout.pool_id = mds->mdsmap->get_first_data_pool();
  return in;
}

void MDCache::create_empty_hierarchy(MDSGather *gather)
{
  // create root dir
  CInode *root = create_root_inode();

  // force empty root dir
  CDir *rootdir = root->get_or_open_dirfrag(this, frag_t());
  adjust_subtree_auth(rootdir, mds->get_nodeid());   
  rootdir->dir_rep = CDir::REP_ALL;   //NONE;

  ceph_assert(rootdir->get_fnode()->accounted_fragstat == rootdir->get_fnode()->fragstat);
  ceph_assert(rootdir->get_fnode()->fragstat == root->get_inode()->dirstat);
  ceph_assert(rootdir->get_fnode()->accounted_rstat == rootdir->get_fnode()->rstat);
  /* Do no update rootdir rstat information of the fragment, rstat upkeep magic
   * assume version 0 is stale/invalid.
   */

  rootdir->mark_complete();
  rootdir->_get_fnode()->version = rootdir->pre_dirty();
  rootdir->mark_dirty(mds->mdlog->get_current_segment());
  rootdir->commit(0, gather->new_sub());

  root->store(gather->new_sub());
  root->mark_dirty_parent(mds->mdlog->get_current_segment(), true);
  root->store_backtrace(gather->new_sub());
}

void MDCache::create_mydir_hierarchy(MDSGather *gather)
{
  // create mds dir
  CInode *my = create_system_inode(MDS_INO_MDSDIR(mds->get_nodeid()), S_IFDIR);

  CDir *mydir = my->get_or_open_dirfrag(this, frag_t());
  auto mydir_fnode = mydir->_get_fnode();

  adjust_subtree_auth(mydir, mds->get_nodeid());   

  LogSegment *ls = mds->mdlog->get_current_segment();

  // stray dir
  for (int i = 0; i < NUM_STRAY; ++i) {
    CInode *stray = create_system_inode(MDS_INO_STRAY(mds->get_nodeid(), i), S_IFDIR);
    CDir *straydir = stray->get_or_open_dirfrag(this, frag_t());
    CachedStackStringStream css;
    *css << "stray" << i;
    CDentry *sdn = mydir->add_primary_dentry(css->str(), stray, "");
    sdn->_mark_dirty(mds->mdlog->get_current_segment());

    stray->_get_inode()->dirstat = straydir->get_fnode()->fragstat;

    mydir_fnode->rstat.add(stray->get_inode()->rstat);
    mydir_fnode->fragstat.nsubdirs++;
    // save them
    straydir->mark_complete();
    straydir->_get_fnode()->version = straydir->pre_dirty();
    straydir->mark_dirty(ls);
    straydir->commit(0, gather->new_sub());
    stray->mark_dirty_parent(ls, true);
    stray->store_backtrace(gather->new_sub());
  }

  mydir_fnode->accounted_fragstat = mydir->get_fnode()->fragstat;
  mydir_fnode->accounted_rstat = mydir->get_fnode()->rstat;

  auto inode = myin->_get_inode();
  inode->dirstat = mydir->get_fnode()->fragstat;
  inode->rstat = mydir->get_fnode()->rstat;
  ++inode->rstat.rsubdirs;
  inode->accounted_rstat = inode->rstat;

  mydir->mark_complete();
  mydir_fnode->version = mydir->pre_dirty();
  mydir->mark_dirty(ls);
  mydir->commit(0, gather->new_sub());

  myin->store(gather->new_sub());
}

struct C_MDC_CreateSystemFile : public MDCacheLogContext {
  MutationRef mut;
  CDentry *dn;
  version_t dpv;
  MDSContext *fin;
  C_MDC_CreateSystemFile(MDCache *c, MutationRef& mu, CDentry *d, version_t v, MDSContext *f) :
    MDCacheLogContext(c), mut(mu), dn(d), dpv(v), fin(f) {}
  void finish(int r) override {
    mdcache->_create_system_file_finish(mut, dn, dpv, fin);
  }
};

void MDCache::_create_system_file(CDir *dir, std::string_view name, CInode *in, MDSContext *fin)
{
  dout(10) << "_create_system_file " << name << " in " << *dir << dendl;
  CDentry *dn = dir->add_null_dentry(name);

  dn->push_projected_linkage(in);
  version_t dpv = dn->pre_dirty();
  
  CDir *mdir = 0;
  auto inode = in->_get_inode();
  if (in->is_dir()) {
    inode->rstat.rsubdirs = 1;

    mdir = in->get_or_open_dirfrag(this, frag_t());
    mdir->mark_complete();
    mdir->_get_fnode()->version = mdir->pre_dirty();
  } else {
    inode->rstat.rfiles = 1;
  }

  inode->version = dn->pre_dirty();
  
  SnapRealm *realm = dir->get_inode()->find_snaprealm();
  dn->first = in->first = realm->get_newest_seq() + 1;

  MutationRef mut(new MutationImpl());

  // force some locks.  hacky.
  mds->locker->wrlock_force(&dir->inode->filelock, mut);
  mds->locker->wrlock_force(&dir->inode->nestlock, mut);

  mut->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, "create system file");

  if (!in->is_mdsdir()) {
    predirty_journal_parents(mut, &le->metablob, in, dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
    le->metablob.add_primary_dentry(dn, in, true);
  } else {
    predirty_journal_parents(mut, &le->metablob, in, dir, PREDIRTY_DIR, 1);
    journal_dirty_inode(mut.get(), &le->metablob, in);
    dn->push_projected_linkage(in->ino(), in->d_type());
    le->metablob.add_remote_dentry(dn, true, in->ino(), in->d_type());
    le->metablob.add_root(true, in);
  }
  if (mdir)
    le->metablob.add_new_dir(mdir); // dirty AND complete AND new

  mds->mdlog->submit_entry(le, new C_MDC_CreateSystemFile(this, mut, dn, dpv, fin));
  mds->mdlog->flush();
}

void MDCache::_create_system_file_finish(MutationRef& mut, CDentry *dn, version_t dpv, MDSContext *fin)
{
  dout(10) << "_create_system_file_finish " << *dn << dendl;
  
  dn->pop_projected_linkage();
  dn->mark_dirty(dpv, mut->ls);

  CInode *in = dn->get_linkage()->get_inode();
  in->mark_dirty(mut->ls);

  if (in->is_dir()) {
    CDir *dir = in->get_dirfrag(frag_t());
    ceph_assert(dir);
    dir->mark_dirty(mut->ls);
    dir->mark_new(mut->ls);
  }

  mut->apply();
  mds->locker->drop_locks(mut.get());
  mut->cleanup();

  fin->complete(0);

  //if (dir && MDS_INO_IS_MDSDIR(in->ino()))
  //migrator->export_dir(dir, (int)in->ino() - MDS_INO_MDSDIR_OFFSET);
}



struct C_MDS_RetryOpenRoot : public MDSInternalContext {
  MDCache *cache;
  explicit C_MDS_RetryOpenRoot(MDCache *c) : MDSInternalContext(c->mds), cache(c) {}
  void finish(int r) override {
    if (r < 0) {
      // If we can't open root, something disastrous has happened: mark
      // this rank damaged for operator intervention.  Note that
      // it is not okay to call suicide() here because we are in
      // a Finisher callback.
      cache->mds->damaged();
      ceph_abort();  // damaged should never return
    } else {
      cache->open_root();
    }
  }
};

void MDCache::open_root_inode(MDSContext *c)
{
  if (mds->get_nodeid() == mds->mdsmap->get_root()) {
    CInode *in;
    in = create_system_inode(CEPH_INO_ROOT, S_IFDIR|0755);  // initially inaccurate!
    in->fetch(c);
  } else {
    discover_base_ino(CEPH_INO_ROOT, c, mds->mdsmap->get_root());
  }
}

void MDCache::open_mydir_inode(MDSContext *c)
{
  CInode *in = create_system_inode(MDS_INO_MDSDIR(mds->get_nodeid()), S_IFDIR|0755);  // initially inaccurate!
  in->fetch(c);
}

void MDCache::open_mydir_frag(MDSContext *c)
{
  open_mydir_inode(
      new MDSInternalContextWrapper(mds,
	new LambdaContext([this, c](int r) {
	    if (r < 0) {
	      c->complete(r);
	      return;
	    }
	    CDir *mydir = myin->get_or_open_dirfrag(this, frag_t());
	    ceph_assert(mydir);
	    adjust_subtree_auth(mydir, mds->get_nodeid());
	    mydir->fetch(c);
	  })
	)
      );
}

void MDCache::open_root()
{
  dout(10) << "open_root" << dendl;

  if (!root) {
    open_root_inode(new C_MDS_RetryOpenRoot(this));
    return;
  }
  if (mds->get_nodeid() == mds->mdsmap->get_root()) {
    ceph_assert(root->is_auth());  
    CDir *rootdir = root->get_or_open_dirfrag(this, frag_t());
    ceph_assert(rootdir);
    if (!rootdir->is_subtree_root())
      adjust_subtree_auth(rootdir, mds->get_nodeid());   
    if (!rootdir->is_complete()) {
      rootdir->fetch(new C_MDS_RetryOpenRoot(this));
      return;
    }
  } else {
    ceph_assert(!root->is_auth());
    CDir *rootdir = root->get_dirfrag(frag_t());
    if (!rootdir) {
      open_remote_dirfrag(root, frag_t(), new C_MDS_RetryOpenRoot(this));
      return;
    }    
  }

  if (!myin) {
    CInode *in = create_system_inode(MDS_INO_MDSDIR(mds->get_nodeid()), S_IFDIR|0755);  // initially inaccurate!
    in->fetch(new C_MDS_RetryOpenRoot(this));
    return;
  }
  CDir *mydir = myin->get_or_open_dirfrag(this, frag_t());
  ceph_assert(mydir);
  adjust_subtree_auth(mydir, mds->get_nodeid());

  populate_mydir();
}

void MDCache::advance_stray() {
  // check whether the directory has been fragmented
  if (stray_fragmenting_index >= 0) {
    auto&& dfs = strays[stray_fragmenting_index]->get_dirfrags();
    bool any_fragmenting = false;
    for (const auto& dir : dfs) {
      if (dir->state_test(CDir::STATE_FRAGMENTING) ||
	  mds->balancer->is_fragment_pending(dir->dirfrag())) {
	any_fragmenting = true;
	break;
      }
    }
    if (!any_fragmenting)
      stray_fragmenting_index = -1;
  }

  for (int i = 1; i < NUM_STRAY; i++){
    stray_index = (stray_index + i) % NUM_STRAY;
    if (stray_index != stray_fragmenting_index)
      break;
  }

  if (stray_fragmenting_index == -1 && is_open()) {
    // Fragment later stray dir in advance. We don't choose past
    // stray dir because in-flight requests may still use it.
    stray_fragmenting_index = (stray_index + 3) % NUM_STRAY;
    auto&& dfs = strays[stray_fragmenting_index]->get_dirfrags();
    bool any_fragmenting = false;
    for (const auto& dir : dfs) {
      if (dir->should_split()) {
	mds->balancer->queue_split(dir, true);
	any_fragmenting = true;
      } else if (dir->should_merge()) {
	mds->balancer->queue_merge(dir);
	any_fragmenting = true;
      }
    }
    if (!any_fragmenting)
      stray_fragmenting_index = -1;
  }

  dout(10) << "advance_stray to index " << stray_index
	   << " fragmenting index " << stray_fragmenting_index << dendl;
}

void MDCache::populate_mydir()
{
  ceph_assert(myin);
  CDir *mydir = myin->get_or_open_dirfrag(this, frag_t());
  ceph_assert(mydir);

  dout(10) << "populate_mydir " << *mydir << dendl;

  if (!mydir->is_complete()) {
    mydir->fetch(new C_MDS_RetryOpenRoot(this));
    return;
  }

  if (mydir->get_version() == 0 && mydir->state_test(CDir::STATE_BADFRAG)) {
    // A missing dirfrag, we will recreate it.  Before that, we must dirty
    // it before dirtying any of the strays we create within it.
    mds->clog->warn() << "fragment " << mydir->dirfrag() << " was unreadable, "
      "recreating it now";
    LogSegment *ls = mds->mdlog->get_current_segment();
    mydir->state_clear(CDir::STATE_BADFRAG);
    mydir->mark_complete();
    mydir->_get_fnode()->version = mydir->pre_dirty();
    mydir->mark_dirty(ls);
  }

  // open or create stray
  uint64_t num_strays = 0;
  for (int i = 0; i < NUM_STRAY; ++i) {
    CachedStackStringStream css;
    *css << "stray" << i;
    CDentry *straydn = mydir->lookup(css->str());

    // allow for older fs's with stray instead of stray0
    if (straydn == NULL && i == 0)
      straydn = mydir->lookup("stray");

    if (!straydn || !straydn->get_linkage()->get_inode()) {
      _create_system_file(mydir, css->strv(), create_system_inode(MDS_INO_STRAY(mds->get_nodeid(), i), S_IFDIR),
			  new C_MDS_RetryOpenRoot(this));
      return;
    }
    ceph_assert(straydn);
    ceph_assert(strays[i]);
    // we make multiple passes through this method; make sure we only pin each stray once.
    if (!strays[i]->state_test(CInode::STATE_STRAYPINNED)) {
      strays[i]->get(CInode::PIN_STRAY);
      strays[i]->state_set(CInode::STATE_STRAYPINNED);
      strays[i]->get_stickydirs();
    }
    dout(20) << " stray num " << i << " is " << *strays[i] << dendl;

    // open all frags
    frag_vec_t leaves;
    strays[i]->dirfragtree.get_leaves(leaves);
    for (const auto& leaf : leaves) {
      CDir *dir = strays[i]->get_dirfrag(leaf);
      if (!dir) {
	dir = strays[i]->get_or_open_dirfrag(this, leaf);
      }

      // DamageTable applies special handling to strays: it will
      // have damaged() us out if one is damaged.
      ceph_assert(!dir->state_test(CDir::STATE_BADFRAG));

      if (dir->get_version() == 0) {
        dir->fetch_keys({}, new C_MDS_RetryOpenRoot(this));
        return;
      }

      if (dir->get_frag_size() > 0)
	num_strays += dir->get_frag_size();
    }
  }

  // okay!
  dout(10) << "populate_mydir done" << dendl;
  ceph_assert(!open);    
  open = true;
  mds->queue_waiters(waiting_for_open);

  stray_manager.set_num_strays(num_strays);
  stray_manager.activate();

  scan_stray_dir();
}

void MDCache::open_foreign_mdsdir(inodeno_t ino, MDSContext *fin)
{
  discover_base_ino(ino, fin, mds_rank_t(ino & (MAX_MDS-1)));
}

CDir *MDCache::get_stray_dir(CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);

  CInode *strayi = get_stray();
  ceph_assert(strayi);
  frag_t fg = strayi->pick_dirfrag(straydname);
  CDir *straydir = strayi->get_dirfrag(fg);
  ceph_assert(straydir);
  return straydir;
}

MDSCacheObject *MDCache::get_object(const MDSCacheObjectInfo &info)
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
// consistent hash ring

/*
 * hashing implementation based on Lamping and Veach's Jump Consistent Hash: https://arxiv.org/pdf/1406.2294.pdf
*/
mds_rank_t MDCache::hash_into_rank_bucket(inodeno_t ino, frag_t fg)
{
  const mds_rank_t max_mds = mds->mdsmap->get_max_mds();
  uint64_t hash = rjhash64(ino);
  if (fg)
    hash = rjhash64(hash + rjhash64(fg.value()));

  int64_t b = -1, j = 0;
  while (j < max_mds) {
    b = j;
    hash = hash*2862933555777941757ULL + 1;
    j = (b + 1) * (double(1LL << 31) / double((hash >> 33) + 1));
  }
  // verify bounds before returning
  auto result = mds_rank_t(b);
  ceph_assert(result >= 0 && result < max_mds);
  return result;
}


// ====================================================================
// subtree management

/*
 * adjust the dir_auth of a subtree.
 * merge with parent and/or child subtrees, if is it appropriate.
 * merge can ONLY happen if both parent and child have unambiguous auth.
 */
void MDCache::adjust_subtree_auth(CDir *dir, mds_authority_t auth, bool adjust_pop)
{
  dout(7) << "adjust_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir << dendl;

  show_subtrees();

  CDir *root;
  if (dir->inode->is_base()) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) {
      subtrees[root];
      root->get(CDir::PIN_SUBTREE);
    }
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  ceph_assert(root);
  ceph_assert(subtrees.count(root));
  dout(7) << " current root is " << *root << dendl;

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << dendl;
    ceph_assert(subtrees.count(dir) == 0);
    subtrees[dir];      // create empty subtree bounds list for me.
    dir->get(CDir::PIN_SUBTREE);

    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      ++next;
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
    if (adjust_pop && dir->is_auth()) {
      CDir *p = dir->get_parent_dir();
      while (p) {
	p->pop_auth_subtree.sub(dir->pop_auth_subtree);
	if (p->is_subtree_root()) break;
	p = p->inode->get_parent_dir();
      }
    }
  }

  show_subtrees();
}


void MDCache::try_subtree_merge(CDir *dir)
{
  dout(7) << "try_subtree_merge " << *dir << dendl;
  // record my old bounds
  auto oldbounds = subtrees.at(dir);

  set<CInode*> to_eval;
  // try merge at my root
  try_subtree_merge_at(dir, &to_eval);

  // try merge at my old bounds
  for (auto bound : oldbounds)
    try_subtree_merge_at(bound, &to_eval);

  if (!(mds->is_any_replay() || mds->is_resolve())) {
    for(auto in : to_eval)
      eval_subtree_root(in);
  }
}

void MDCache::try_subtree_merge_at(CDir *dir, set<CInode*> *to_eval, bool adjust_pop)
{
  dout(10) << "try_subtree_merge_at " << *dir << dendl;

  if (dir->dir_auth.second != CDIR_AUTH_UNKNOWN ||
      dir->state_test(CDir::STATE_EXPORTBOUND) ||
      dir->state_test(CDir::STATE_AUXSUBTREE))
    return;

  auto it = subtrees.find(dir);
  ceph_assert(it != subtrees.end());

  // merge with parent?
  CDir *parent = dir;  
  if (!dir->inode->is_base())
    parent = get_subtree_root(dir->get_parent_dir());
  
  if (parent != dir &&				// we have a parent,
      parent->dir_auth == dir->dir_auth) {	// auth matches,
    // merge with parent.
    dout(10) << "  subtree merge at " << *dir << dendl;
    dir->set_dir_auth(CDIR_AUTH_DEFAULT);
    
    // move our bounds under the parent
    subtrees[parent].insert(it->second.begin(), it->second.end());
    
    // we are no longer a subtree or bound
    dir->put(CDir::PIN_SUBTREE);
    subtrees.erase(it);
    subtrees[parent].erase(dir);

    // adjust popularity?
    if (adjust_pop && dir->is_auth()) {
      CDir *cur = dir;
      CDir *p = dir->get_parent_dir();
      while (p) {
	p->pop_auth_subtree.add(dir->pop_auth_subtree);
	p->pop_lru_subdirs.push_front(&cur->get_inode()->item_pop_lru);
	if (p->is_subtree_root()) break;
	cur = p;
	p = p->inode->get_parent_dir();
      }
    }

    if (to_eval && dir->get_inode()->is_auth())
      to_eval->insert(dir->get_inode());

    show_subtrees(15);
  }
}

void MDCache::eval_subtree_root(CInode *diri)
{
  // evaluate subtree inode filelock?
  //  (we should scatter the filelock on subtree bounds)
  ceph_assert(diri->is_auth());
  mds->locker->try_eval(diri, CEPH_LOCK_IFILE | CEPH_LOCK_INEST);
}


void MDCache::adjust_bounded_subtree_auth(CDir *dir, const set<CDir*>& bounds, mds_authority_t auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir
	  << " bounds " << bounds
	  << dendl;

  show_subtrees();

  CDir *root;
  if (dir->ino() == CEPH_INO_ROOT) {
    root = dir;  // bootstrap hack.
    if (subtrees.count(root) == 0) {
      subtrees[root];
      root->get(CDir::PIN_SUBTREE);
    }
  } else {
    root = get_subtree_root(dir);  // subtree root
  }
  ceph_assert(root);
  ceph_assert(subtrees.count(root));
  dout(7) << " current root is " << *root << dendl;

  mds_authority_t oldauth = dir->authority();

  if (root == dir) {
    // i am already a subtree.
    dir->set_dir_auth(auth);
  } else {
    // i am a new subtree.
    dout(10) << "  new subtree at " << *dir << dendl;
    ceph_assert(subtrees.count(dir) == 0);
    subtrees[dir];      // create empty subtree bounds list for me.
    dir->get(CDir::PIN_SUBTREE);
    
    // set dir_auth
    dir->set_dir_auth(auth);
    
    // move items nested beneath me, under me.
    set<CDir*>::iterator p = subtrees[root].begin();
    while (p != subtrees[root].end()) {
      set<CDir*>::iterator next = p;
      ++next;
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

  set<CInode*> to_eval;

  // verify/adjust bounds.
  // - these may be new, or
  // - beneath existing ambiguous bounds (which will be collapsed),
  // - but NOT beneath unambiguous bounds.
  for (const auto& bound : bounds) {
    // new bound?
    if (subtrees[dir].count(bound) == 0) {
      if (get_subtree_root(bound) == dir) {
	dout(10) << "  new bound " << *bound << ", adjusting auth back to old " << oldauth << dendl;
	adjust_subtree_auth(bound, oldauth);       // otherwise, adjust at bound.
      }
      else {
	dout(10) << "  want bound " << *bound << dendl;
	CDir *t = get_subtree_root(bound->get_parent_dir());
	if (subtrees[t].count(bound) == 0) {
	  ceph_assert(t != dir);
	  dout(10) << "  new bound " << *bound << dendl;
	  adjust_subtree_auth(bound, t->authority());
	}
	// make sure it's nested beneath ambiguous subtree(s)
	while (1) {
	  while (subtrees[dir].count(t) == 0)
	    t = get_subtree_root(t->get_parent_dir());
	  dout(10) << "  swallowing intervening subtree at " << *t << dendl;
	  adjust_subtree_auth(t, auth);
	  try_subtree_merge_at(t, &to_eval);
	  t = get_subtree_root(bound->get_parent_dir());
	  if (t == dir) break;
	}
      }
    }
    else {
      dout(10) << "  already have bound " << *bound << dendl;
    }
  }
  // merge stray bounds?
  while (!subtrees[dir].empty()) {
    set<CDir*> copy = subtrees[dir];
    for (set<CDir*>::iterator p = copy.begin(); p != copy.end(); ++p) {
      if (bounds.count(*p) == 0) {
	CDir *stray = *p;
	dout(10) << "  swallowing extra subtree at " << *stray << dendl;
	adjust_subtree_auth(stray, auth);
	try_subtree_merge_at(stray, &to_eval);
      }
    }
    // swallowing subtree may add new subtree bounds
    if (copy == subtrees[dir])
      break;
  }

  // bound should now match.
  verify_subtree_bounds(dir, bounds);

  show_subtrees();

  if (!(mds->is_any_replay() || mds->is_resolve())) {
    for(auto in : to_eval)
      eval_subtree_root(in);
  }
}


/*
 * return a set of CDir*'s that correspond to the given bound set.  Only adjust
 * fragmentation as necessary to get an equivalent bounding set.  That is, only
 * split if one of our frags spans the provided bounding set.  Never merge.
 */
void MDCache::get_force_dirfrag_bound_set(const vector<dirfrag_t>& dfs, set<CDir*>& bounds)
{
  dout(10) << "get_force_dirfrag_bound_set " << dfs << dendl;

  // sort by ino
  map<inodeno_t, fragset_t> byino;
  for (auto& frag : dfs) {
    byino[frag.ino].insert_raw(frag.frag);
  }
  dout(10) << " by ino: " << byino << dendl;

  for (map<inodeno_t,fragset_t>::iterator p = byino.begin(); p != byino.end(); ++p) {
    p->second.simplify();
    CInode *diri = get_inode(p->first);
    if (!diri)
      continue;
    dout(10) << " checking fragset " << p->second.get() << " on " << *diri << dendl;

    fragtree_t tmpdft;
    for (set<frag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
      tmpdft.force_to_leaf(g_ceph_context, *q);

    for (const auto& fg : p->second) {
      frag_vec_t leaves;
      diri->dirfragtree.get_leaves_under(fg, leaves);
      if (leaves.empty()) {
	frag_t approx_fg = diri->dirfragtree[fg.value()];
        frag_vec_t approx_leaves;
	tmpdft.get_leaves_under(approx_fg, approx_leaves);
	for (const auto& leaf : approx_leaves) {
	  if (p->second.get().count(leaf) == 0) {
	    // not bound, so the resolve message is from auth MDS of the dirfrag
	    force_dir_fragment(diri, leaf);
	  }
	}
      }

      auto&& [complete, sibs] = diri->get_dirfrags_under(fg);
      for (const auto& sib : sibs)
	bounds.insert(sib);
    }
  }
}

void MDCache::adjust_bounded_subtree_auth(CDir *dir, const vector<dirfrag_t>& bound_dfs, const mds_authority_t &auth)
{
  dout(7) << "adjust_bounded_subtree_auth " << dir->get_dir_auth() << " -> " << auth
	  << " on " << *dir << " bound_dfs " << bound_dfs << dendl;

  set<CDir*> bounds;
  get_force_dirfrag_bound_set(bound_dfs, bounds);
  adjust_bounded_subtree_auth(dir, bounds, auth);
}

void MDCache::map_dirfrag_set(const list<dirfrag_t>& dfs, set<CDir*>& result)
{
  dout(10) << "map_dirfrag_set " << dfs << dendl;

  // group by inode
  map<inodeno_t, fragset_t> ino_fragset;
  for (const auto &df : dfs) {
    ino_fragset[df.ino].insert_raw(df.frag);
  }
  // get frags
  for (map<inodeno_t, fragset_t>::iterator p = ino_fragset.begin();
       p != ino_fragset.end();
       ++p) {
    p->second.simplify();
    CInode *in = get_inode(p->first);
    if (!in)
      continue;

    frag_vec_t fgs;
    for (const auto& fg : p->second) {
      in->dirfragtree.get_leaves_under(fg, fgs);
    }

    dout(15) << "map_dirfrag_set " << p->second << " -> " << fgs
	     << " on " << *in << dendl;

    for (const auto& fg : fgs) {
      CDir *dir = in->get_dirfrag(fg);
      if (dir)
	result.insert(dir);
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

CDir *MDCache::get_projected_subtree_root(CDir *dir)
{
  // find the underlying dir that delegates (or is about to delegate) auth
  while (true) {
    if (dir->is_subtree_root()) 
      return dir;
    dir = dir->get_inode()->get_projected_parent_dir();
    if (!dir) 
      return 0;             // none
  }
}

void MDCache::remove_subtree(CDir *dir)
{
  dout(10) << "remove_subtree " << *dir << dendl;
  auto it = subtrees.find(dir);
  ceph_assert(it != subtrees.end());
  subtrees.erase(it);
  dir->put(CDir::PIN_SUBTREE);
  if (dir->get_parent_dir()) {
    CDir *p = get_subtree_root(dir->get_parent_dir());
    auto it = subtrees.find(p);
    ceph_assert(it != subtrees.end());
    auto count = it->second.erase(dir);
    ceph_assert(count == 1);
  }
}

void MDCache::get_subtree_bounds(CDir *dir, set<CDir*>& bounds)
{
  ceph_assert(subtrees.count(dir));
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
	ceph_assert(t);
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
  ceph_assert(subtrees.count(dir));
  if (bounds != subtrees[dir]) {
    dout(0) << "verify_subtree_bounds failed" << dendl;
    set<CDir*> b = bounds;
    for (auto &cd : subtrees[dir]) {
      if (bounds.count(cd)) {
	b.erase(cd);
	continue;
      }
      dout(0) << "  missing bound " << *cd << dendl;
    }
    for (const auto &cd : b)
      dout(0) << "    extra bound " << *cd << dendl;
  }
  ceph_assert(bounds == subtrees[dir]);
}

void MDCache::verify_subtree_bounds(CDir *dir, const list<dirfrag_t>& bounds)
{
  // for debugging only.
  ceph_assert(subtrees.count(dir));

  // make sure that any bounds i do have are properly noted as such.
  int failed = 0;
  for (const auto &fg : bounds) {
    CDir *bd = get_dirfrag(fg);
    if (!bd) continue;
    if (subtrees[dir].count(bd) == 0) {
      dout(0) << "verify_subtree_bounds failed: extra bound " << *bd << dendl;
      failed++;
    }
  }
  ceph_assert(failed == 0);
}

void MDCache::project_subtree_rename(CInode *diri, CDir *olddir, CDir *newdir)
{
  dout(10) << "project_subtree_rename " << *diri << " from " << *olddir
	   << " to " << *newdir << dendl;
  projected_subtree_renames[diri].push_back(pair<CDir*,CDir*>(olddir, newdir));
}

void MDCache::adjust_subtree_after_rename(CInode *diri, CDir *olddir, bool pop)
{
  dout(10) << "adjust_subtree_after_rename " << *diri << " from " << *olddir << dendl;

  CDir *newdir = diri->get_parent_dir();

  if (pop) {
    map<CInode*,list<pair<CDir*,CDir*> > >::iterator p = projected_subtree_renames.find(diri);
    ceph_assert(p != projected_subtree_renames.end());
    ceph_assert(!p->second.empty());
    ceph_assert(p->second.front().first == olddir);
    ceph_assert(p->second.front().second == newdir);
    p->second.pop_front();
    if (p->second.empty())
      projected_subtree_renames.erase(p);
  }

  // adjust total auth pin of freezing subtree
  if (olddir != newdir) {
    auto&& dfls = diri->get_nested_dirfrags();
    for (const auto& dir : dfls)
      olddir->adjust_freeze_after_rename(dir);
  }

  // adjust subtree
  // N.B. make sure subtree dirfrags are at the front of the list
  auto dfls = diri->get_subtree_dirfrags();
  diri->get_nested_dirfrags(dfls);
  for (const auto& dir : dfls) {
    dout(10) << "dirfrag " << *dir << dendl;
    CDir *oldparent = get_subtree_root(olddir);
    dout(10) << " old parent " << *oldparent << dendl;
    CDir *newparent = get_subtree_root(newdir);
    dout(10) << " new parent " << *newparent << dendl;

    auto& oldbounds = subtrees[oldparent];
    auto& newbounds = subtrees[newparent];

    if (olddir != newdir)
      mds->balancer->adjust_pop_for_rename(olddir, dir, false);

    if (oldparent == newparent) {
      dout(10) << "parent unchanged for " << *dir << " at " << *oldparent << dendl;
    } else if (dir->is_subtree_root()) {
      // children are fine.  change parent.
      dout(10) << "moving " << *dir << " from " << *oldparent << " to " << *newparent << dendl;
      {
        auto n = oldbounds.erase(dir);
        ceph_assert(n == 1);
      }
      newbounds.insert(dir);
      // caller is responsible for 'eval diri'
      try_subtree_merge_at(dir, NULL, false);
    } else {
      // mid-subtree.

      // see if any old bounds move to the new parent.
      std::vector<CDir*> tomove;
      for (const auto& bound : oldbounds) {
	CDir *broot = get_subtree_root(bound->get_parent_dir());
	if (broot != oldparent) {
	  ceph_assert(broot == newparent);
	  tomove.push_back(bound);
	}
      }
      for (const auto& bound : tomove) {
	dout(10) << "moving bound " << *bound << " from " << *oldparent << " to " << *newparent << dendl;
	oldbounds.erase(bound);
	newbounds.insert(bound);
      }	   

      // did auth change?
      if (oldparent->authority() != newparent->authority()) {
	adjust_subtree_auth(dir, oldparent->authority(), false);
	// caller is responsible for 'eval diri'
	try_subtree_merge_at(dir, NULL, false);
      }
    }

    if (olddir != newdir)
      mds->balancer->adjust_pop_for_rename(newdir, dir, true);
  }

  show_subtrees();
}

// ===================================
// journal and snap/cow helpers


/*
 * find first inode in cache that follows given snapid.  otherwise, return current.
 */
CInode *MDCache::pick_inode_snap(CInode *in, snapid_t follows)
{
  dout(10) << "pick_inode_snap follows " << follows << " on " << *in << dendl;
  ceph_assert(in->last == CEPH_NOSNAP);

  auto p = snap_inode_map.upper_bound(vinodeno_t(in->ino(), follows));
  if (p != snap_inode_map.end() && p->second->ino() == in->ino()) {
    dout(10) << "pick_inode_snap found " << *p->second << dendl;
    in = p->second;
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
  ceph_assert(last >= in->first);

  CInode *oldin = new CInode(this, true, in->first, last);
  auto _inode = CInode::allocate_inode(*in->get_previous_projected_inode());
  _inode->trim_client_ranges(last);
  oldin->reset_inode(std::move(_inode));
  auto _xattrs = in->get_previous_projected_xattrs();
  oldin->reset_xattrs(std::move(_xattrs));

  oldin->symlink = in->symlink;

  if (in->first < in->oldest_snap)
    in->oldest_snap = in->first;

  in->first = last+1;

  dout(10) << "cow_inode " << *in << " to " << *oldin << dendl;
  add_inode(oldin);

  if (in->last != CEPH_NOSNAP) {
    CInode *head_in = get_inode(in->ino());
    ceph_assert(head_in);
    auto ret = head_in->split_need_snapflush(oldin, in);
    if (ret.first) {
      oldin->client_snap_caps = in->client_snap_caps;
      if (!oldin->client_snap_caps.empty()) {
	for (int i = 0; i < num_cinode_locks; i++) {
	  SimpleLock *lock = oldin->get_lock(cinode_lock_info[i].lock);
	  ceph_assert(lock);
	  if (lock->get_state() != LOCK_SNAP_SYNC) {
	    ceph_assert(lock->is_stable());
	    lock->set_state(LOCK_SNAP_SYNC);  // gathering
	    oldin->auth_pin(lock);
	  }
	  lock->get_wrlock(true);
	}
      }
    }
    if (!ret.second) {
      auto client_snap_caps = std::move(in->client_snap_caps);
      in->client_snap_caps.clear();
      in->item_open_file.remove_myself();
      in->item_caps.remove_myself();
      // TODO hmm?

      if (!client_snap_caps.empty()) {
	MDSContext::vec finished;
	for (int i = 0; i < num_cinode_locks; i++) {
	  SimpleLock *lock = in->get_lock(cinode_lock_info[i].lock);
	  ceph_assert(lock);
	  ceph_assert(lock->get_state() == LOCK_SNAP_SYNC); // gathering
	  lock->put_wrlock();
	  if (!lock->get_num_wrlocks()) {
	    lock->set_state(LOCK_SYNC);
	    lock->take_waiting(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_RD, finished);
	    in->auth_unpin(lock);
	  }
	}
	mds->queue_waiters(finished);
      }
    }
    return oldin;
  }

  if (!in->client_caps.empty()) {
    const set<snapid_t>& snaps = in->find_snaprealm()->get_snaps();
    // clone caps?
    for (auto &p : in->client_caps) {
      client_t client = p.first;
      Capability *cap = &p.second;
      int issued = cap->need_snapflush() ? CEPH_CAP_ANY_WR : cap->issued();
      if ((issued & CEPH_CAP_ANY_WR) &&
	  cap->client_follows < last) {
	dout(10) << " client." << client << " cap " << ccap_string(issued) << dendl;
	oldin->client_snap_caps.insert(client);
	cap->client_follows = last;

	// we need snapflushes for any intervening snaps
	dout(10) << "  snaps " << snaps << dendl;
	for (auto q = snaps.lower_bound(oldin->first);
	     q != snaps.end() && *q <= last;
	     ++q) {
	  in->add_need_snapflush(oldin, *q, client);
	}
      } else {
	dout(10) << " ignoring client." << client << " cap follows " << cap->client_follows << dendl;
      }
    }

    if (!oldin->client_snap_caps.empty()) {
      for (int i = 0; i < num_cinode_locks; i++) {
	SimpleLock *lock = oldin->get_lock(cinode_lock_info[i].lock);
	ceph_assert(lock);
	if (lock->get_state() != LOCK_SNAP_SYNC) {
	  ceph_assert(lock->is_stable());
	  lock->set_state(LOCK_SNAP_SYNC);  // gathering
	  oldin->auth_pin(lock);
	}
	lock->get_wrlock(true);
      }
    }
  }
  return oldin;
}

void MDCache::journal_cow_dentry(MutationImpl *mut, EMetaBlob *metablob,
                                 CDentry *dn, snapid_t follows,
				 CInode **pcow_inode, CDentry::linkage_t *dnl)
{
  if (!dn) {
    dout(10) << "journal_cow_dentry got null CDentry, returning" << dendl;
    return;
  }
  dout(10) << "journal_cow_dentry follows " << follows << " on " << *dn << dendl;
  ceph_assert(dn->is_auth());

  // nothing to cow on a null dentry, fix caller
  if (!dnl)
    dnl = dn->get_projected_linkage();
  ceph_assert(!dnl->is_null());

  CInode *in = dnl->is_primary() ? dnl->get_inode() : NULL;
  bool cow_head = false;
  if (in && in->state_test(CInode::STATE_AMBIGUOUSAUTH)) {
    ceph_assert(in->is_frozen_inode());
    cow_head = true;
  }
  if (in && (in->is_multiversion() || cow_head)) {
    // multiversion inode.
    SnapRealm *realm = NULL;

    if (in->get_projected_parent_dn() != dn) {
      ceph_assert(follows == CEPH_NOSNAP);
      realm = dn->dir->inode->find_snaprealm();
      snapid_t dir_follows = get_global_snaprealm()->get_newest_seq();
      ceph_assert(dir_follows >= realm->get_newest_seq());

      if (dir_follows+1 > dn->first) {
	snapid_t oldfirst = dn->first;
	dn->first = dir_follows+1;
	if (realm->has_snaps_in_range(oldfirst, dir_follows)) {
	  CDir *dir = dn->dir;
	  CDentry *olddn = dir->add_remote_dentry(dn->get_name(), in->ino(), in->d_type(), dn->alternate_name, oldfirst, dir_follows);
	  dout(10) << " olddn " << *olddn << dendl;
	  ceph_assert(dir->is_projected());
	  olddn->set_projected_version(dir->get_projected_version());
	  metablob->add_remote_dentry(olddn, true);
	  mut->add_cow_dentry(olddn);
	  // FIXME: adjust link count here?  hmm.

	  if (dir_follows+1 > in->first)
	    in->cow_old_inode(dir_follows, cow_head);
	}
      }

      follows = dir_follows;
      if (in->snaprealm) {
	realm = in->snaprealm;
	ceph_assert(follows >= realm->get_newest_seq());
      }
    } else {
      realm = in->find_snaprealm();
      if (follows == CEPH_NOSNAP) {
	follows = get_global_snaprealm()->get_newest_seq();
	ceph_assert(follows >= realm->get_newest_seq());
      }
    }

    // already cloned?
    if (follows < in->first) {
      dout(10) << "journal_cow_dentry follows " << follows << " < first on " << *in << dendl;
      return;
    }

    if (!realm->has_snaps_in_range(in->first, follows)) {
      dout(10) << "journal_cow_dentry no snapshot follows " << follows << " on " << *in << dendl;
      in->first = follows + 1;
      return;
    }

    in->cow_old_inode(follows, cow_head);

  } else {
    SnapRealm *realm = dn->dir->inode->find_snaprealm();
    if (follows == CEPH_NOSNAP) {
      follows = get_global_snaprealm()->get_newest_seq();
      ceph_assert(follows >= realm->get_newest_seq());
    }

    // already cloned?
    if (follows < dn->first) {
      dout(10) << "journal_cow_dentry follows " << follows << " < first on " << *dn << dendl;
      return;
    }

    // update dn.first before adding old dentry to cdir's map
    snapid_t oldfirst = dn->first;
    dn->first = follows+1;

    if (!realm->has_snaps_in_range(oldfirst, follows)) {
      dout(10) << "journal_cow_dentry no snapshot follows " << follows << " on " << *dn << dendl;
      if (in)
	in->first = follows+1;
      return;
    }
    
    dout(10) << "    dn " << *dn << dendl;
    CDir *dir = dn->get_dir();
    ceph_assert(dir->is_projected());

    if (in) {
      CInode *oldin = cow_inode(in, follows);
      ceph_assert(in->is_projected());
      mut->add_cow_inode(oldin);
      if (pcow_inode)
	*pcow_inode = oldin;
      CDentry *olddn = dir->add_primary_dentry(dn->get_name(), oldin, dn->alternate_name, oldfirst, follows);
      dout(10) << " olddn " << *olddn << dendl;
      bool need_snapflush = !oldin->client_snap_caps.empty();
      if (need_snapflush) {
	mut->ls->open_files.push_back(&oldin->item_open_file);
	mds->locker->mark_need_snapflush_inode(oldin);
      }
      olddn->set_projected_version(dir->get_projected_version());
      metablob->add_primary_dentry(olddn, 0, true, false, false, need_snapflush);
      mut->add_cow_dentry(olddn);
    } else {
      ceph_assert(dnl->is_remote());
      CDentry *olddn = dir->add_remote_dentry(dn->get_name(), dnl->get_remote_ino(), dnl->get_remote_d_type(), dn->alternate_name, oldfirst, follows);
      dout(10) << " olddn " << *olddn << dendl;

      olddn->set_projected_version(dir->get_projected_version());
      metablob->add_remote_dentry(olddn, true);
      mut->add_cow_dentry(olddn);
    }
  }
}

void MDCache::journal_dirty_inode(MutationImpl *mut, EMetaBlob *metablob, CInode *in, snapid_t follows)
{
  if (in->is_base()) {
    metablob->add_root(true, in);
  } else {
    if (follows == CEPH_NOSNAP && in->last != CEPH_NOSNAP)
      follows = in->first - 1;
    CDentry *dn = in->get_projected_parent_dn();
    if (!dn->get_projected_linkage()->is_null())  // no need to cow a null dentry
      journal_cow_dentry(mut, metablob, dn, follows);
    if (in->get_projected_inode()->is_backtrace_updated()) {
      bool dirty_pool = in->get_projected_inode()->layout.pool_id !=
			in->get_previous_projected_inode()->layout.pool_id;
      metablob->add_primary_dentry(dn, in, true, true, dirty_pool);
    } else {
      metablob->add_primary_dentry(dn, in, true);
    }
  }
}



// nested ---------------------------------------------------------------

void MDCache::project_rstat_inode_to_frag(const MutationRef& mut,
					  CInode *cur, CDir *parent, snapid_t first,
					  int linkunlink, SnapRealm *prealm)
{
  CDentry *parentdn = cur->get_projected_parent_dn();

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

  if (!prealm)
      prealm = parent->inode->find_snaprealm();
  const set<snapid_t> snaps = prealm->get_snaps();

  if (cur->last != CEPH_NOSNAP) {
    ceph_assert(cur->dirty_old_rstats.empty());
    set<snapid_t>::const_iterator q = snaps.lower_bound(std::max(first, floor));
    if (q == snaps.end() || *q > cur->last)
      return;
  }

  if (cur->last >= floor) {
    bool update = true;
    if (cur->state_test(CInode::STATE_AMBIGUOUSAUTH) && cur->is_auth()) {
      // rename src inode is not projected in the peer rename prep case. so we should
      // avoid updateing the inode.
      ceph_assert(linkunlink < 0);
      ceph_assert(cur->is_frozen_inode());
      update = false;
    }
    // hacky
    const CInode::mempool_inode *pi;
    if (update && mut->is_projected(cur)) {
      pi = cur->_get_projected_inode();
    } else {
      pi = cur->get_projected_inode().get();
      if (update) {
	// new inode
	ceph_assert(pi->rstat == pi->accounted_rstat);
	update = false;
      }
    }
    _project_rstat_inode_to_frag(pi, std::max(first, floor), cur->last, parent,
				 linkunlink, update);
  }

  if (g_conf()->mds_snap_rstat) {
    for (const auto &p : cur->dirty_old_rstats) {
      const auto &old = cur->get_old_inodes()->at(p);
      snapid_t ofirst = std::max(old.first, floor);
      auto it = snaps.lower_bound(ofirst);
      if (it == snaps.end() || *it > p)
	continue;
      if (p >= floor)
	_project_rstat_inode_to_frag(&old.inode, ofirst, p, parent, 0, false);
    }
  }
  cur->dirty_old_rstats.clear();
}


void MDCache::_project_rstat_inode_to_frag(const CInode::mempool_inode* inode, snapid_t ofirst, snapid_t last,
					  CDir *parent, int linkunlink, bool update_inode)
{
  dout(10) << "_project_rstat_inode_to_frag [" << ofirst << "," << last << "]" << dendl;
  dout(20) << "  inode           rstat " << inode->rstat << dendl;
  dout(20) << "  inode accounted_rstat " << inode->accounted_rstat << dendl;
  nest_info_t delta;
  if (linkunlink == 0) {
    delta.add(inode->rstat);
    delta.sub(inode->accounted_rstat);
  } else if (linkunlink < 0) {
    delta.sub(inode->accounted_rstat);
  } else {
    delta.add(inode->rstat);
  }
  dout(20) << "                  delta " << delta << dendl;


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
    auto pf = parent->_get_projected_fnode();
    if (last == CEPH_NOSNAP) {
      if (g_conf()->mds_snap_rstat)
	first = std::max(ofirst, parent->first);
      else
	first = parent->first;
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
    } else if (!g_conf()->mds_snap_rstat) {
      // drop snapshots' rstats
      break;
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
      auto it = parent->dirty_old_rstat.lower_bound(last);
      if (it == parent->dirty_old_rstat.end()) {
	dout(20) << "  no dirty_old_rstat with last >= last " << last << dendl;
	if (!parent->dirty_old_rstat.empty() && parent->dirty_old_rstat.rbegin()->first >= first) {
	  dout(20) << "  last dirty_old_rstat ends at " << parent->dirty_old_rstat.rbegin()->first << dendl;
	  first = parent->dirty_old_rstat.rbegin()->first+1;
	}
      } else {
	// *it last is >= last
	if (it->second.first <= last) {
	  // *it intersects [first,last]
	  if (it->second.first < first) {
	    dout(10) << " splitting off left bit [" << it->second.first << "," << first-1 << "]" << dendl;
	    parent->dirty_old_rstat[first-1] = it->second;
	    it->second.first = first;
	  }
	  if (it->second.first > first)
	    first = it->second.first;
	  if (last < it->first) {
	    dout(10) << " splitting off right bit [" << last+1 << "," << it->first << "]" << dendl;
	    parent->dirty_old_rstat[last] = it->second;
	    it->second.first = last+1;
	  }
	} else {
	  // *it is to the _right_ of [first,last]
	  it = parent->dirty_old_rstat.lower_bound(first);
	  // new *it last is >= first
	  if (it->second.first <= last &&  // new *it isn't also to the right, and
	      it->first >= first) {        // it intersects our first bit,
	    dout(10) << " staying to the right of [" << it->second.first << "," << it->first << "]..." << dendl;
	    first = it->first+1;
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
    ceph_assert(last >= first);
    prstat->add(delta);
    dout(20) << "      result [" << first << "," << last << "] " << *prstat << " " << *parent << dendl;

    last = first-1;
  }

  if (update_inode) {
    auto _inode = const_cast<CInode::mempool_inode*>(inode);
    _inode->accounted_rstat = _inode->rstat;
  }
}

void MDCache::project_rstat_frag_to_inode(const nest_info_t& rstat,
					  const nest_info_t& accounted_rstat,
					  snapid_t ofirst, snapid_t last, 
					  CInode *pin, bool cow_head)
{
  dout(10) << "project_rstat_frag_to_inode [" << ofirst << "," << last << "]" << dendl;
  dout(20) << "  frag           rstat " << rstat << dendl;
  dout(20) << "  frag accounted_rstat " << accounted_rstat << dendl;
  nest_info_t delta = rstat;
  delta.sub(accounted_rstat);
  dout(20) << "                 delta " << delta << dendl;

  CInode::old_inode_map_ptr _old_inodes;
  while (last >= ofirst) {
    CInode::mempool_inode *pi;
    snapid_t first;
    if (last == pin->last) {
      pi = pin->_get_projected_inode();
      first = std::max(ofirst, pin->first);
      if (first > pin->first) {
	auto& old = pin->cow_old_inode(first-1, cow_head);
	dout(20) << "   cloned old_inode rstat is " << old.inode.rstat << dendl;
      }
    } else {
      if (!_old_inodes) {
	_old_inodes = CInode::allocate_old_inode_map();
	if (pin->is_any_old_inodes())
	  *_old_inodes = *pin->get_old_inodes();
      }
      if (last >= pin->first) {
	first = pin->first;
	pin->cow_old_inode(last, cow_head);
      } else {
	// our life is easier here because old_inodes is not sparse
	// (although it may not begin at snapid 1)
	auto it = _old_inodes->lower_bound(last);
	if (it == _old_inodes->end()) {
	  dout(10) << " no old_inode <= " << last << ", done." << dendl;
	  break;
	}
	first = it->second.first;
	if (first > last) {
	  dout(10) << " oldest old_inode is [" << first << "," << it->first << "], done." << dendl;
	  //assert(p == pin->old_inodes.begin());
	  break;
	}
	if (it->first > last) {
	  dout(10) << " splitting right old_inode [" << first << "," << it->first << "] to ["
		   << (last+1) << "," << it->first << "]" << dendl;
	  (*_old_inodes)[last] = it->second;
	  it->second.first = last+1;
	  pin->dirty_old_rstats.insert(it->first);
	}
      }
      if (first < ofirst) {
	dout(10) << " splitting left old_inode [" << first << "," << last << "] to ["
		 << first << "," << ofirst-1 << "]" << dendl;
	(*_old_inodes)[ofirst-1] = (*_old_inodes)[last];
	pin->dirty_old_rstats.insert(ofirst-1);
	(*_old_inodes)[last].first = first = ofirst;
      }
      pi = &(*_old_inodes)[last].inode;
      pin->dirty_old_rstats.insert(last);
    }
    dout(20) << " projecting to [" << first << "," << last << "] " << pi->rstat << dendl;
    pi->rstat.add(delta);
    dout(20) << "        result [" << first << "," << last << "] " << pi->rstat << dendl;

    last = first-1;
  }
  if (_old_inodes)
    pin->reset_old_inodes(std::move(_old_inodes));
}

void MDCache::broadcast_quota_to_client(CInode *in, client_t exclude_ct, bool quota_change)
{
  if (!(mds->is_active() || mds->is_stopping()))
    return;

  if (!in->is_auth() || in->is_frozen())
    return;

  const auto& pi = in->get_projected_inode();
  if (!pi->quota.is_enabled() && !quota_change)
    return;

  // creaete snaprealm for quota inode (quota was set before mimic)
  if (!in->get_projected_srnode())
    mds->server->create_quota_realm(in);

  for (auto &p : in->client_caps) {
    Capability *cap = &p.second;
    if (cap->is_noquota())
      continue;

    if (exclude_ct >= 0 && exclude_ct != p.first)
      goto update;

    if (cap->last_rbytes == pi->rstat.rbytes &&
        cap->last_rsize == pi->rstat.rsize())
      continue;

    if (pi->quota.max_files > 0) {
      if (pi->rstat.rsize() >= pi->quota.max_files)
        goto update;

      if ((abs(cap->last_rsize - pi->quota.max_files) >> 4) <
          abs(cap->last_rsize - pi->rstat.rsize()))
        goto update;
    }

    if (pi->quota.max_bytes > 0) {
      if (pi->rstat.rbytes > pi->quota.max_bytes - (pi->quota.max_bytes >> 3))
        goto update;

      if ((abs(cap->last_rbytes - pi->quota.max_bytes) >> 4) <
          abs(cap->last_rbytes - pi->rstat.rbytes))
        goto update;
    }

    continue;

update:
    cap->last_rsize = pi->rstat.rsize();
    cap->last_rbytes = pi->rstat.rbytes;

    auto msg = make_message<MClientQuota>();
    msg->ino = in->ino();
    msg->rstat = pi->rstat;
    msg->quota = pi->quota;
    mds->send_message_client_counted(msg, cap->get_session());
  }
  for (const auto &it : in->get_replicas()) {
    auto msg = make_message<MGatherCaps>();
    msg->ino = in->ino();
    mds->send_message_mds(msg, it.first);
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
void MDCache::predirty_journal_parents(MutationRef mut, EMetaBlob *blob,
				       CInode *in, CDir *parent,
				       int flags, int linkunlink,
				       snapid_t cfollows)
{
  bool primary_dn = flags & PREDIRTY_PRIMARY;
  bool do_parent_mtime = flags & PREDIRTY_DIR;
  bool shallow = flags & PREDIRTY_SHALLOW;

  // make sure stamp is set
  if (mut->get_mds_stamp() == utime_t())
    mut->set_mds_stamp(ceph_clock_now());

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
    ceph_assert(primary_dn);
    parent = in->get_projected_parent_dn()->get_dir();
  }

  if (flags == 0 && linkunlink == 0) {
    dout(10) << " no flags/linkunlink, just adding dir context to blob(s)" << dendl;
    blob->add_dir_context(parent);
    return;
  }

  // build list of inodes to wrlock, dirty, and update
  list<CInode*> lsi;
  CInode *cur = in;
  CDentry *parentdn = NULL;
  bool first = true;
  while (parent) {
    //assert(cur->is_auth() || !primary_dn);  // this breaks the rename auth twiddle hack
    ceph_assert(parent->is_auth());
    
    // opportunistically adjust parent dirfrag
    CInode *pin = parent->get_inode();

    // inode -> dirfrag
    mut->auth_pin(parent);

    auto pf = parent->project_fnode(mut);
    pf->version = parent->pre_dirty();

    if (do_parent_mtime || linkunlink) {
      ceph_assert(mut->is_wrlocked(&pin->filelock));
      ceph_assert(mut->is_wrlocked(&pin->nestlock));
      ceph_assert(cfollows == CEPH_NOSNAP);
      
      // update stale fragstat/rstat?
      parent->resync_accounted_fragstat();
      parent->resync_accounted_rstat();

      if (do_parent_mtime) {
	pf->fragstat.mtime = mut->get_op_stamp();
	pf->fragstat.change_attr++;
	dout(10) << "predirty_journal_parents bumping fragstat change_attr to " << pf->fragstat.change_attr << " on " << parent << dendl;
	if (pf->fragstat.mtime > pf->rstat.rctime) {
	  dout(10) << "predirty_journal_parents updating mtime on " << *parent << dendl;
	  pf->rstat.rctime = pf->fragstat.mtime;
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
    } else if (!linkunlink && !(pin->nestlock.can_wrlock(-1) &&
				pin->versionlock.can_wrlock())) {
      dout(20) << " unwritable parent nestlock " << pin->nestlock
	<< ", marking dirty rstat on " << *cur << dendl;
      cur->mark_dirty_rstat();
    } else {
      // if we don't hold a wrlock reference on this nestlock, take one,
      // because we are about to write into the dirfrag fnode and that needs
      // to commit before the lock can cycle.
      if (linkunlink) {
	ceph_assert(pin->nestlock.get_num_wrlocks() || mut->is_peer());
      }

      if (!mut->is_wrlocked(&pin->nestlock)) {
	dout(10) << " taking wrlock on " << pin->nestlock << " on " << *pin << dendl;
	mds->locker->wrlock_force(&pin->nestlock, mut);
      }

      // now we can project the inode rstat diff the dirfrag
      SnapRealm *prealm = pin->find_snaprealm();

      snapid_t follows = cfollows;
      if (follows == CEPH_NOSNAP)
	follows = prealm->get_newest_seq();

      snapid_t first = follows+1;

      // first, if the frag is stale, bring it back in sync.
      parent->resync_accounted_rstat();

      // now push inode rstats into frag
      project_rstat_inode_to_frag(mut, cur, parent, first, linkunlink, prealm);
      cur->clear_dirty_rstat();
    }

    bool stop = false;
    if (!pin->is_auth() || (!mut->is_auth_pinned(pin) && !pin->can_auth_pin())) {
      dout(10) << "predirty_journal_parents !auth or ambig or can't authpin on " << *pin << dendl;
      stop = true;
    }

    // delay propagating until later?
    if (!stop && !first &&
	g_conf()->mds_dirstat_min_interval > 0) {
      double since_last_prop = mut->get_mds_stamp() - pin->last_dirstat_prop;
      if (since_last_prop < g_conf()->mds_dirstat_min_interval) {
	dout(10) << "predirty_journal_parents last prop " << since_last_prop
		 << " < " << g_conf()->mds_dirstat_min_interval
		 << ", stopping" << dendl;
	stop = true;
      } else {
	dout(10) << "predirty_journal_parents last prop " << since_last_prop << " ago, continuing" << dendl;
      }
    }

    // can cast only because i'm passing nowait=true in the sole user
    if (!stop &&
	!mut->is_wrlocked(&pin->nestlock) &&
	(!pin->versionlock.can_wrlock() ||                   // make sure we can take versionlock, too
	 !mds->locker->wrlock_try(&pin->nestlock, mut)
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
      if (do_parent_mtime || linkunlink) {
	mds->locker->mark_updated_scatterlock(&pin->filelock);
	mut->ls->dirty_dirfrag_dir.push_back(&pin->item_dirty_dirfrag_dir);
	mut->add_updated_lock(&pin->filelock);
      }
      break;
    }
    if (!mut->is_wrlocked(&pin->versionlock))
      mds->locker->local_wrlock_grab(&pin->versionlock, mut);

    ceph_assert(mut->is_wrlocked(&pin->nestlock) || mut->is_peer());
    
    pin->last_dirstat_prop = mut->get_mds_stamp();

    // dirfrag -> diri
    mut->auth_pin(pin);
    lsi.push_front(pin);

    pin->pre_cow_old_inode();  // avoid cow mayhem!

    auto pi = pin->project_inode(mut);
    pi.inode->version = pin->pre_dirty();

    // dirstat
    if (do_parent_mtime || linkunlink) {
      dout(20) << "predirty_journal_parents add_delta " << pf->fragstat << dendl;
      dout(20) << "predirty_journal_parents         - " << pf->accounted_fragstat << dendl;
      bool touched_mtime = false, touched_chattr = false;
      pi.inode->dirstat.add_delta(pf->fragstat, pf->accounted_fragstat, &touched_mtime, &touched_chattr);
      pf->accounted_fragstat = pf->fragstat;
      if (touched_mtime)
	pi.inode->mtime = pi.inode->ctime = pi.inode->dirstat.mtime;
      if (touched_chattr)
	pi.inode->change_attr++;
      dout(20) << "predirty_journal_parents     gives " << pi.inode->dirstat << " on " << *pin << dendl;

      if (parent->get_frag() == frag_t()) { // i.e., we are the only frag
	if (pi.inode->dirstat.size() < 0)
	  ceph_assert(!"negative dirstat size" == g_conf()->mds_verify_scatter);
	if (pi.inode->dirstat.size() != pf->fragstat.size()) {
	  mds->clog->error() << "unmatched fragstat size on single dirfrag "
	     << parent->dirfrag() << ", inode has " << pi.inode->dirstat
	     << ", dirfrag has " << pf->fragstat;
	  
	  // trust the dirfrag for now
	  pi.inode->dirstat = pf->fragstat;

	  ceph_assert(!"unmatched fragstat size" == g_conf()->mds_verify_scatter);
	}
      }
    }

    // rstat
    dout(10) << "predirty_journal_parents frag->inode on " << *parent << dendl;

    // first, if the frag is stale, bring it back in sync.
    parent->resync_accounted_rstat();

    if (g_conf()->mds_snap_rstat) {
      for (auto &p : parent->dirty_old_rstat) {
	project_rstat_frag_to_inode(p.second.rstat, p.second.accounted_rstat, p.second.first,
				    p.first, pin, true);
      }
    }
    parent->dirty_old_rstat.clear();
    project_rstat_frag_to_inode(pf->rstat, pf->accounted_rstat, parent->first, CEPH_NOSNAP, pin, true);//false);

    pf->accounted_rstat = pf->rstat;

    if (parent->get_frag() == frag_t()) { // i.e., we are the only frag
      if (pi.inode->rstat.rbytes != pf->rstat.rbytes) {
	mds->clog->error() << "unmatched rstat rbytes on single dirfrag "
	  << parent->dirfrag() << ", inode has " << pi.inode->rstat
	  << ", dirfrag has " << pf->rstat;

	// trust the dirfrag for now
	pi.inode->rstat = pf->rstat;

	ceph_assert(!"unmatched rstat rbytes" == g_conf()->mds_verify_scatter);
      }
    }

    parent->check_rstats();
    broadcast_quota_to_client(pin);
    if (pin->is_base())
      break;
    // next parent!
    cur = pin;
    parentdn = pin->get_projected_parent_dn();
    ceph_assert(parentdn);
    parent = parentdn->get_dir();
    linkunlink = 0;
    do_parent_mtime = false;
    primary_dn = true;
    first = false;
  }

  // now, stick it in the blob
  ceph_assert(parent);
  ceph_assert(parent->is_auth());
  blob->add_dir_context(parent);
  blob->add_dir(parent, true);
  for (const auto& in : lsi) {
    journal_dirty_inode(mut.get(), blob, in);
  }
 
}





// ===================================
// peer requests


/*
 * some handlers for leader requests with peers.  we need to make
 * sure leader journal commits before we forget we leadered them and
 * remove them from the uncommitted_leaders map (used during recovery
 * to commit|abort peers).
 */
struct C_MDC_CommittedLeader : public MDCacheLogContext {
  metareqid_t reqid;
  C_MDC_CommittedLeader(MDCache *s, metareqid_t r) : MDCacheLogContext(s), reqid(r) {}
  void finish(int r) override {
    mdcache->_logged_leader_commit(reqid);
  }
};

void MDCache::log_leader_commit(metareqid_t reqid)
{
  dout(10) << "log_leader_commit " << reqid << dendl;
  uncommitted_leaders[reqid].committing = true;
  mds->mdlog->submit_entry(new ECommitted(reqid), new C_MDC_CommittedLeader(this, reqid));
}

void MDCache::_logged_leader_commit(metareqid_t reqid)
{
  dout(10) << "_logged_leader_commit " << reqid << dendl;
  ceph_assert(uncommitted_leaders.count(reqid));
  uncommitted_leaders[reqid].ls->uncommitted_leaders.erase(reqid);
  mds->queue_waiters(uncommitted_leaders[reqid].waiters);
  uncommitted_leaders.erase(reqid);
}

// while active...

void MDCache::committed_leader_peer(metareqid_t r, mds_rank_t from)
{
  dout(10) << "committed_leader_peer mds." << from << " on " << r << dendl;
  ceph_assert(uncommitted_leaders.count(r));
  uncommitted_leaders[r].peers.erase(from);
  if (!uncommitted_leaders[r].recovering && uncommitted_leaders[r].peers.empty())
    log_leader_commit(r);
}

void MDCache::logged_leader_update(metareqid_t reqid)
{
  dout(10) << "logged_leader_update " << reqid << dendl;
  ceph_assert(uncommitted_leaders.count(reqid));
  uncommitted_leaders[reqid].safe = true;
  auto p = pending_leaders.find(reqid);
  if (p != pending_leaders.end()) {
    pending_leaders.erase(p);
    if (pending_leaders.empty())
      process_delayed_resolve();
  }
}

/*
 * Leader may crash after receiving all peers' commit acks, but before journalling
 * the final commit. Peers may crash after journalling the peer commit, but before
 * sending commit ack to the leader. Commit leaders with no uncommitted peer when
 * resolve finishes.
 */
void MDCache::finish_committed_leaders()
{
  for (map<metareqid_t, uleader>::iterator p = uncommitted_leaders.begin();
       p != uncommitted_leaders.end();
       ++p) {
    p->second.recovering = false;
    if (!p->second.committing && p->second.peers.empty()) {
      dout(10) << "finish_committed_leaders " << p->first << dendl;
      log_leader_commit(p->first);
    }
  }
}

/*
 * at end of resolve... we must journal a commit|abort for all peer
 * updates, before moving on.
 * 
 * this is so that the leader can safely journal ECommitted on ops it
 * leaders when it reaches up:active (all other recovering nodes must
 * complete resolve before that happens).
 */
struct C_MDC_PeerCommit : public MDCacheLogContext {
  mds_rank_t from;
  metareqid_t reqid;
  C_MDC_PeerCommit(MDCache *c, int f, metareqid_t r) : MDCacheLogContext(c), from(f), reqid(r) {}
  void finish(int r) override {
    mdcache->_logged_peer_commit(from, reqid);
  }
};

void MDCache::_logged_peer_commit(mds_rank_t from, metareqid_t reqid)
{
  dout(10) << "_logged_peer_commit from mds." << from << " " << reqid << dendl;
  
  // send a message
  auto req = make_message<MMDSPeerRequest>(reqid, 0, MMDSPeerRequest::OP_COMMITTED);
  mds->send_message_mds(req, from);
}






// ====================================================================
// import map, recovery

void MDCache::_move_subtree_map_bound(dirfrag_t df, dirfrag_t oldparent, dirfrag_t newparent,
				      map<dirfrag_t,vector<dirfrag_t> >& subtrees)
{
  if (subtrees.count(oldparent)) {
      vector<dirfrag_t>& v = subtrees[oldparent];
      dout(10) << " removing " << df << " from " << oldparent << " bounds " << v << dendl;
      for (vector<dirfrag_t>::iterator it = v.begin(); it != v.end(); ++it)
	if (*it == df) {
	  v.erase(it);
	  break;
	}
    }
  if (subtrees.count(newparent)) {
    vector<dirfrag_t>& v = subtrees[newparent];
    dout(10) << " adding " << df << " to " << newparent << " bounds " << v << dendl;
    v.push_back(df);
  }
}

ESubtreeMap *MDCache::create_subtree_map() 
{
  dout(10) << "create_subtree_map " << num_subtrees() << " subtrees, " 
	   << num_subtrees_fullauth() << " fullauth"
	   << dendl;

  show_subtrees();

  ESubtreeMap *le = new ESubtreeMap();
  
  map<dirfrag_t, CDir*> dirs_to_add;

  if (myin) {
    CDir* mydir = myin->get_dirfrag(frag_t());
    dirs_to_add[mydir->dirfrag()] = mydir;
  }

  // include all auth subtrees, and their bounds.
  // and a spanning tree to tie it to the root.
  for (auto& [dir, bounds] : subtrees) {
    // journal subtree as "ours" if we are
    //   me, -2
    //   me, me
    //   me, !me (may be importing and ambiguous!)

    // so not
    //   !me, *
    if (dir->get_dir_auth().first != mds->get_nodeid())
      continue;

    if (migrator->is_ambiguous_import(dir->dirfrag()) ||
	my_ambiguous_imports.count(dir->dirfrag())) {
      dout(15) << " ambig subtree " << *dir << dendl;
      le->ambiguous_subtrees.insert(dir->dirfrag());
    } else {
      dout(15) << " auth subtree " << *dir << dendl;
    }

    dirs_to_add[dir->dirfrag()] = dir;
    le->subtrees[dir->dirfrag()].clear();

    // bounds
    size_t nbounds = bounds.size();
    if (nbounds > 3) {
      dout(15) << "  subtree has " << nbounds << " bounds" << dendl;
    }
    for (auto& bound : bounds) {
      if (nbounds <= 3) {
        dout(15) << "  subtree bound " << *bound << dendl;
      }
      dirs_to_add[bound->dirfrag()] = bound;
      le->subtrees[dir->dirfrag()].push_back(bound->dirfrag());
    }
  }

  // apply projected renames
  for (const auto& [diri, renames] : projected_subtree_renames) {
    for (const auto& [olddir, newdir] : renames) {
      dout(15) << " adjusting for projected rename of " << *diri << " to " << *newdir << dendl;

      auto&& dfls = diri->get_dirfrags();
      for (const auto& dir : dfls) {
	dout(15) << "dirfrag " << dir->dirfrag() << " " << *dir << dendl;
	CDir *oldparent = get_projected_subtree_root(olddir);
	dout(15) << " old parent " << oldparent->dirfrag() << " " << *oldparent << dendl;
	CDir *newparent = get_projected_subtree_root(newdir);
	dout(15) << " new parent " << newparent->dirfrag() << " " << *newparent << dendl;

	if (oldparent == newparent) {
	  dout(15) << "parent unchanged for " << dir->dirfrag() << " at "
		   << oldparent->dirfrag() << dendl;
	  continue;
	}

	if (dir->is_subtree_root()) {
	  if (le->subtrees.count(newparent->dirfrag()) &&
	      oldparent->get_dir_auth() != newparent->get_dir_auth())
	    dirs_to_add[dir->dirfrag()] = dir;
	  // children are fine.  change parent.
	  _move_subtree_map_bound(dir->dirfrag(), oldparent->dirfrag(), newparent->dirfrag(),
				  le->subtrees);
	} else {
	  // mid-subtree.

	  if (oldparent->get_dir_auth() != newparent->get_dir_auth()) {
	    dout(10) << " creating subtree for " << dir->dirfrag() << dendl;
	    // if oldparent is auth, subtree is mine; include it.
	    if (le->subtrees.count(oldparent->dirfrag())) {
	      dirs_to_add[dir->dirfrag()] = dir;
	      le->subtrees[dir->dirfrag()].clear();
	    }
	    // if newparent is auth, subtree is a new bound
	    if (le->subtrees.count(newparent->dirfrag())) {
	      dirs_to_add[dir->dirfrag()] = dir;
	      le->subtrees[newparent->dirfrag()].push_back(dir->dirfrag());  // newparent is auth; new bound
	    }
	    newparent = dir;
	  }
	  
	  // see if any old bounds move to the new parent.
	  for (auto& bound : subtrees.at(oldparent)) {
	    if (dir->contains(bound->get_parent_dir()))
	      _move_subtree_map_bound(bound->dirfrag(), oldparent->dirfrag(), newparent->dirfrag(),
				      le->subtrees);
	  }
	}
      }
    }
  }

  // simplify the journaled map.  our in memory map may have more
  // subtrees than needed due to migrations that are just getting
  // started or just completing.  but on replay, the "live" map will
  // be simple and we can do a straight comparison.
  for (auto& [frag, bfrags] : le->subtrees) {
    if (le->ambiguous_subtrees.count(frag))
      continue;
    unsigned i = 0;
    while (i < bfrags.size()) {
      dirfrag_t b = bfrags[i];
      if (le->subtrees.count(b) &&
	  le->ambiguous_subtrees.count(b) == 0) {
	auto& bb = le->subtrees.at(b);
	dout(10) << "simplify: " << frag << " swallowing " << b << " with bounds " << bb << dendl;
	for (auto& r : bb) {
	  bfrags.push_back(r);
        }
	dirs_to_add.erase(b);
	le->subtrees.erase(b);
	bfrags.erase(bfrags.begin() + i);
      } else {
	++i;
      }
    }
  }

  for (auto &p : dirs_to_add) {
    CDir *dir = p.second;
    le->metablob.add_dir_context(dir, EMetaBlob::TO_ROOT);
    le->metablob.add_dir(dir, false);
  }

  dout(15) << " subtrees " << le->subtrees << dendl;
  dout(15) << " ambiguous_subtrees " << le->ambiguous_subtrees << dendl;

  //le->metablob.print(cout);
  le->expire_pos = mds->mdlog->journaler->get_expire_pos();
  return le;
}

void MDCache::dump_resolve_status(Formatter *f) const
{
  f->open_object_section("resolve_status");
  f->dump_stream("resolve_gather") << resolve_gather;
  f->dump_stream("resolve_ack_gather") << resolve_ack_gather;
  f->close_section();
}

void MDCache::resolve_start(MDSContext *resolve_done_)
{
  dout(10) << "resolve_start" << dendl;
  ceph_assert(!resolve_done);
  resolve_done.reset(resolve_done_);

  if (mds->mdsmap->get_root() != mds->get_nodeid()) {
    // if we don't have the root dir, adjust it to UNKNOWN.  during
    // resolve we want mds0 to explicit claim the portion of it that
    // it owns, so that anything beyond its bounds get left as
    // unknown.
    CDir *rootdir = root->get_dirfrag(frag_t());
    if (rootdir)
      adjust_subtree_auth(rootdir, CDIR_AUTH_UNKNOWN);
  }
  resolve_gather = recovery_set;

  resolve_snapclient_commits = mds->snapclient->get_journaled_tids();
}

void MDCache::send_resolves()
{
  send_peer_resolves();

  if (!resolve_done) {
    // I'm survivor: refresh snap cache
    mds->snapclient->sync(
	new MDSInternalContextWrapper(mds,
	  new LambdaContext([this](int r) {
	    maybe_finish_peer_resolve();
	    })
	  )
	);
    dout(10) << "send_resolves waiting for snapclient cache to sync" << dendl;
    return;
  }
  if (!resolve_ack_gather.empty()) {
    dout(10) << "send_resolves still waiting for resolve ack from ("
	     << resolve_ack_gather << ")" << dendl;
    return;
  }
  if (!resolve_need_rollback.empty()) {
    dout(10) << "send_resolves still waiting for rollback to commit on ("
	     << resolve_need_rollback << ")" << dendl;
    return;
  }

  send_subtree_resolves();
}

void MDCache::send_peer_resolves()
{
  dout(10) << "send_peer_resolves" << dendl;

  map<mds_rank_t, ref_t<MMDSResolve>> resolves;

  if (mds->is_resolve()) {
    for (map<metareqid_t, upeer>::iterator p = uncommitted_peers.begin();
	 p != uncommitted_peers.end();
	 ++p) {
      mds_rank_t leader = p->second.leader;
      auto &m = resolves[leader];
      if (!m) m = make_message<MMDSResolve>();
      m->add_peer_request(p->first, false);
    }
  } else {
    set<mds_rank_t> resolve_set;
    mds->mdsmap->get_mds_set(resolve_set, MDSMap::STATE_RESOLVE);
    for (ceph::unordered_map<metareqid_t, MDRequestRef>::iterator p = active_requests.begin();
	 p != active_requests.end();
	 ++p) {
      MDRequestRef& mdr = p->second;
      if (!mdr->is_peer())
	continue;
      if (!mdr->peer_did_prepare() && !mdr->committing) {
	continue;
      }
      mds_rank_t leader = mdr->peer_to_mds;
      if (resolve_set.count(leader) || is_ambiguous_peer_update(p->first, leader)) {
	dout(10) << " including uncommitted " << *mdr << dendl;
	if (!resolves.count(leader))
	  resolves[leader] = make_message<MMDSResolve>();
	if (!mdr->committing &&
	    mdr->has_more() && mdr->more()->is_inode_exporter) {
	  // re-send cap exports
	  CInode *in = mdr->more()->rename_inode;
	  map<client_t, Capability::Export> cap_map;
	  in->export_client_caps(cap_map);
	  bufferlist bl;
          MMDSResolve::peer_inode_cap inode_caps(in->ino(), cap_map);
          encode(inode_caps, bl);
	  resolves[leader]->add_peer_request(p->first, bl);
	} else {
	  resolves[leader]->add_peer_request(p->first, mdr->committing);
	}
      }
    }
  }

  for (auto &p : resolves) {
    dout(10) << "sending peer resolve to mds." << p.first << dendl;
    mds->send_message_mds(p.second, p.first);
    resolve_ack_gather.insert(p.first);
  }
}

void MDCache::send_subtree_resolves()
{
  dout(10) << "send_subtree_resolves" << dendl;

  if (migrator->is_exporting() || migrator->is_importing()) {
    dout(7) << "send_subtree_resolves waiting, imports/exports still in progress" << dendl;
    migrator->show_importing();
    migrator->show_exporting();
    resolves_pending = true;
    return;  // not now
  }

  map<mds_rank_t, ref_t<MMDSResolve>> resolves;
  for (set<mds_rank_t>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (*p == mds->get_nodeid())
      continue;
    if (mds->is_resolve() || mds->mdsmap->is_resolve(*p))
      resolves[*p] = make_message<MMDSResolve>();
  }

  map<dirfrag_t, vector<dirfrag_t> > my_subtrees;
  map<dirfrag_t, vector<dirfrag_t> > my_ambig_imports;

  // known
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;

    // only our subtrees
    if (dir->authority().first != mds->get_nodeid()) 
      continue;

    if (mds->is_resolve() && my_ambiguous_imports.count(dir->dirfrag()))
      continue;  // we'll add it below
    
    if (migrator->is_ambiguous_import(dir->dirfrag())) {
      // ambiguous (mid-import)
      set<CDir*> bounds;
      get_subtree_bounds(dir, bounds);
      vector<dirfrag_t> dfls;
      for (set<CDir*>::iterator q = bounds.begin(); q != bounds.end(); ++q)
	dfls.push_back((*q)->dirfrag());

      my_ambig_imports[dir->dirfrag()] = dfls;
      dout(10) << " ambig " << dir->dirfrag() << " " << dfls << dendl;
    } else {
      // not ambiguous.
      for (auto &q : resolves) {
	resolves[q.first]->add_subtree(dir->dirfrag());
      }
      // bounds too
      vector<dirfrag_t> dfls;
      for (set<CDir*>::iterator q = subtrees[dir].begin();
	   q != subtrees[dir].end();
	   ++q) {
	CDir *bound = *q;
	dfls.push_back(bound->dirfrag());
      }

      my_subtrees[dir->dirfrag()] = dfls;
      dout(10) << " claim " << dir->dirfrag() << " " << dfls << dendl;
    }
  }

  // ambiguous
  for (map<dirfrag_t, vector<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
       p != my_ambiguous_imports.end();
       ++p) {
    my_ambig_imports[p->first] = p->second;
    dout(10) << " ambig " << p->first << " " << p->second << dendl;
  }

  // simplify the claimed subtree.
  for (auto p = my_subtrees.begin(); p != my_subtrees.end(); ++p) {
    unsigned i = 0;
    while (i < p->second.size()) {
      dirfrag_t b = p->second[i];
      if (my_subtrees.count(b)) {
	vector<dirfrag_t>& bb = my_subtrees[b];
	dout(10) << " simplify: " << p->first << " swallowing " << b << " with bounds " << bb << dendl;
	for (vector<dirfrag_t>::iterator r = bb.begin(); r != bb.end(); ++r)
	  p->second.push_back(*r);
	my_subtrees.erase(b);
	p->second.erase(p->second.begin() + i);
      } else {
	++i;
      }
    }
  }

  // send
  for (auto &p : resolves) {
    const ref_t<MMDSResolve> &m = p.second;
    if (mds->is_resolve()) {
      m->add_table_commits(TABLE_SNAP, resolve_snapclient_commits);
    } else {
      m->add_table_commits(TABLE_SNAP, mds->snapclient->get_journaled_tids());
    }
    m->subtrees = my_subtrees;
    m->ambiguous_imports = my_ambig_imports;
    dout(10) << "sending subtee resolve to mds." << p.first << dendl;
    mds->send_message_mds(m, p.first);
  }
  resolves_pending = false;
}

void MDCache::maybe_finish_peer_resolve() {
  if (resolve_ack_gather.empty() && resolve_need_rollback.empty()) {
    // snap cache get synced or I'm in resolve state
    if (mds->snapclient->is_synced() || resolve_done)
      send_subtree_resolves();
    process_delayed_resolve();
  }
}

void MDCache::handle_mds_failure(mds_rank_t who)
{
  dout(7) << "handle_mds_failure mds." << who << dendl;
  
  dout(1) << "handle_mds_failure mds." << who << " : recovery peers are " << recovery_set << dendl;

  resolve_gather.insert(who);
  discard_delayed_resolve(who);
  ambiguous_peer_updates.erase(who);

  rejoin_gather.insert(who);
  rejoin_sent.erase(who);        // i need to send another
  rejoin_ack_sent.erase(who);    // i need to send another
  rejoin_ack_gather.erase(who);  // i'll need/get another.

  dout(10) << " resolve_gather " << resolve_gather << dendl;
  dout(10) << " resolve_ack_gather " << resolve_ack_gather << dendl;
  dout(10) << " rejoin_sent " << rejoin_sent << dendl;
  dout(10) << " rejoin_gather " << rejoin_gather << dendl;
  dout(10) << " rejoin_ack_gather " << rejoin_ack_gather << dendl;

 
  // tell the migrator too.
  migrator->handle_mds_failure_or_stop(who);

  // tell the balancer too.
  mds->balancer->handle_mds_failure(who);

  // clean up any requests peer to/from this node
  list<MDRequestRef> finish;
  for (ceph::unordered_map<metareqid_t, MDRequestRef>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    MDRequestRef& mdr = p->second;
    // peer to the failed node?
    if (mdr->peer_to_mds == who) {
      if (mdr->peer_did_prepare()) {
	dout(10) << " peer request " << *mdr << " uncommitted, will resolve shortly" << dendl;
	if (is_ambiguous_peer_update(p->first, mdr->peer_to_mds))
	  remove_ambiguous_peer_update(p->first, mdr->peer_to_mds);

	if (!mdr->more()->waiting_on_peer.empty()) {
	  ceph_assert(mdr->more()->srcdn_auth_mds == mds->get_nodeid());
	  // will rollback, no need to wait
	  mdr->reset_peer_request();
	  mdr->more()->waiting_on_peer.clear();
	}
      } else if (!mdr->committing) {
	dout(10) << " peer request " << *mdr << " has no prepare, finishing up" << dendl;
	if (mdr->peer_request || mdr->peer_rolling_back())
	  mdr->aborted = true;
	else
	  finish.push_back(mdr);
      }
    }

    if (mdr->is_peer() && mdr->peer_did_prepare()) {
      if (mdr->more()->waiting_on_peer.count(who)) {
	ceph_assert(mdr->more()->srcdn_auth_mds == mds->get_nodeid());
	dout(10) << " peer request " << *mdr << " no longer need rename notity ack from mds."
		 << who << dendl;
	mdr->more()->waiting_on_peer.erase(who);
	if (mdr->more()->waiting_on_peer.empty() && mdr->peer_request)
	  mds->queue_waiter(new C_MDS_RetryRequest(this, mdr));
      }

      if (mdr->more()->srcdn_auth_mds == who &&
	  mds->mdsmap->is_clientreplay_or_active_or_stopping(mdr->peer_to_mds)) {
	// rename srcdn's auth mds failed, resolve even I'm a survivor.
	dout(10) << " peer request " << *mdr << " uncommitted, will resolve shortly" << dendl;
	add_ambiguous_peer_update(p->first, mdr->peer_to_mds);
      }
    } else if (mdr->peer_request) {
      const cref_t<MMDSPeerRequest> &peer_req = mdr->peer_request;
      // FIXME: Peer rename request can arrive after we notice mds failure.
      // 	This can cause mds to crash (does not affect integrity of FS).
      if (peer_req->get_op() == MMDSPeerRequest::OP_RENAMEPREP &&
	  peer_req->srcdn_auth == who)
	peer_req->mark_interrupted();
    }
    
    // failed node is peer?
    if (mdr->is_leader() && !mdr->committing) {
      if (mdr->more()->srcdn_auth_mds == who) {
	dout(10) << " leader request " << *mdr << " waiting for rename srcdn's auth mds."
		 << who << " to recover" << dendl;
	ceph_assert(mdr->more()->witnessed.count(who) == 0);
	if (mdr->more()->is_ambiguous_auth)
	  mdr->clear_ambiguous_auth();
	// rename srcdn's auth mds failed, all witnesses will rollback
	mdr->more()->witnessed.clear();
	pending_leaders.erase(p->first);
      }

      if (mdr->more()->witnessed.count(who)) {
	mds_rank_t srcdn_auth = mdr->more()->srcdn_auth_mds;
	if (srcdn_auth >= 0 && mdr->more()->waiting_on_peer.count(srcdn_auth)) {
	  dout(10) << " leader request " << *mdr << " waiting for rename srcdn's auth mds."
		   << mdr->more()->srcdn_auth_mds << " to reply" << dendl;
	  // waiting for the peer (rename srcdn's auth mds), delay sending resolve ack
	  // until either the request is committing or the peer also fails.
	  ceph_assert(mdr->more()->waiting_on_peer.size() == 1);
	  pending_leaders.insert(p->first);
	} else {
	  dout(10) << " leader request " << *mdr << " no longer witnessed by peer mds."
		   << who << " to recover" << dendl;
	  if (srcdn_auth >= 0)
	    ceph_assert(mdr->more()->witnessed.count(srcdn_auth) == 0);

	  // discard this peer's prepare (if any)
	  mdr->more()->witnessed.erase(who);
	}
      }
      
      if (mdr->more()->waiting_on_peer.count(who)) {
	dout(10) << " leader request " << *mdr << " waiting for peer mds." << who
		 << " to recover" << dendl;
	// retry request when peer recovers
	mdr->more()->waiting_on_peer.erase(who);
	if (mdr->more()->waiting_on_peer.empty())
	  mds->wait_for_active_peer(who, new C_MDS_RetryRequest(this, mdr));
      }

      if (mdr->locking && mdr->locking_target_mds == who)
	mdr->finish_locking(mdr->locking);
    }
  }

  for (map<metareqid_t, uleader>::iterator p = uncommitted_leaders.begin();
       p != uncommitted_leaders.end();
       ++p) {
    // The failed MDS may have already committed the peer update
    if (p->second.peers.count(who)) {
      p->second.recovering = true;
      p->second.peers.erase(who);
    }
  }

  while (!finish.empty()) {
    dout(10) << "cleaning up peer request " << *finish.front() << dendl;
    request_finish(finish.front());
    finish.pop_front();
  }

  kick_find_ino_peers(who);
  kick_open_ino_peers(who);

  for (map<dirfrag_t,fragment_info_t>::iterator p = fragments.begin();
       p != fragments.end(); ) {
    dirfrag_t df = p->first;
    fragment_info_t& info = p->second;

    if (info.is_fragmenting()) {
      if (info.notify_ack_waiting.erase(who) &&
	  info.notify_ack_waiting.empty()) {
	fragment_drop_locks(info);
	fragment_maybe_finish(p++);
      } else {
	++p;
      }
      continue;
    }

    ++p;
    dout(10) << "cancelling fragment " << df << " bit " << info.bits << dendl;
    std::vector<CDir*> dirs;
    info.dirs.swap(dirs);
    fragments.erase(df);
    fragment_unmark_unfreeze_dirs(dirs);
  }

  // MDCache::shutdown_export_strays() always exports strays to mds.0
  if (who == mds_rank_t(0))
    shutdown_exporting_strays.clear();

  show_subtrees();  
}

/*
 * handle_mds_recovery - called on another node's transition 
 * from resolve -> active.
 */
void MDCache::handle_mds_recovery(mds_rank_t who)
{
  dout(7) << "handle_mds_recovery mds." << who << dendl;

  // exclude all discover waiters. kick_discovers() will do the job
  static const uint64_t i_mask = CInode::WAIT_ANY_MASK & ~CInode::WAIT_DIR;
  static const uint64_t d_mask = CDir::WAIT_ANY_MASK & ~CDir::WAIT_DENTRY;

  MDSContext::vec waiters;

  // wake up any waiters in their subtrees
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;

    if (dir->authority().first != who ||
	dir->authority().second == mds->get_nodeid())
      continue;
    ceph_assert(!dir->is_auth());
   
    // wake any waiters
    std::queue<CDir*> q;
    q.push(dir);

    while (!q.empty()) {
      CDir *d = q.front();
      q.pop();
      d->take_waiting(d_mask, waiters);

      // inode waiters too
      for (auto &p : d->items) {
	CDentry *dn = p.second;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (dnl->is_primary()) {
	  dnl->get_inode()->take_waiting(i_mask, waiters);
	  
	  // recurse?
	  auto&& ls = dnl->get_inode()->get_dirfrags();
	  for (const auto& subdir : ls) {
	    if (!subdir->is_subtree_root())
	      q.push(subdir);
	  }
	}
      }
    }
  }

  kick_open_ino_peers(who);
  kick_find_ino_peers(who);

  // queue them up.
  mds->queue_waiters(waiters);
}

void MDCache::set_recovery_set(set<mds_rank_t>& s) 
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
void MDCache::handle_resolve(const cref_t<MMDSResolve> &m)
{
  dout(7) << "handle_resolve from " << m->get_source() << dendl;
  mds_rank_t from = mds_rank_t(m->get_source().num());

  if (mds->get_state() < MDSMap::STATE_RESOLVE) {
    if (mds->get_want_state() == CEPH_MDS_STATE_RESOLVE) {
      mds->wait_for_resolve(new C_MDS_RetryMessage(mds, m));
      return;
    }
    // wait until we reach the resolve stage!
    return;
  }

  discard_delayed_resolve(from);

  // ambiguous peer requests?
  if (!m->peer_requests.empty()) {
    if (mds->is_clientreplay() || mds->is_active() || mds->is_stopping()) {
      for (auto p = m->peer_requests.begin(); p != m->peer_requests.end(); ++p) {
	if (uncommitted_leaders.count(p->first) && !uncommitted_leaders[p->first].safe) {
	  ceph_assert(!p->second.committing);
	  pending_leaders.insert(p->first);
	}
      }

      if (!pending_leaders.empty()) {
	dout(10) << " still have pending updates, delay processing peer resolve" << dendl;
	delayed_resolve[from] = m;
	return;
      }
    }

    auto ack = make_message<MMDSResolveAck>();
    for (const auto &p : m->peer_requests) {
      if (uncommitted_leaders.count(p.first)) {  //mds->sessionmap.have_completed_request(p.first)) {
	// COMMIT
	if (p.second.committing) {
	  // already committing, waiting for the OP_COMMITTED peer reply
	  dout(10) << " already committing peer request " << p << " noop "<< dendl;
	} else {
	  dout(10) << " ambiguous peer request " << p << " will COMMIT" << dendl;
	  ack->add_commit(p.first);
	}
	uncommitted_leaders[p.first].peers.insert(from);   // wait for peer OP_COMMITTED before we log ECommitted

	if (p.second.inode_caps.length() > 0) {
	  // peer wants to export caps (rename)
	  ceph_assert(mds->is_resolve());
          MMDSResolve::peer_inode_cap inode_caps;
	  auto q = p.second.inode_caps.cbegin();
          decode(inode_caps, q);
	  inodeno_t ino = inode_caps.ino;
	  map<client_t,Capability::Export> cap_exports = inode_caps.cap_exports;
	  ceph_assert(get_inode(ino));

	  for (map<client_t,Capability::Export>::iterator q = cap_exports.begin();
	      q != cap_exports.end();
	      ++q) {
	    Capability::Import& im = rejoin_imported_caps[from][ino][q->first];
	    im.cap_id = ++last_cap_id; // assign a new cap ID
	    im.issue_seq = 1;
	    im.mseq = q->second.mseq;

	    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(q->first.v));
	    if (session)
	      rejoin_client_map.emplace(q->first, session->info.inst);
	  }

	  // will process these caps in rejoin stage
	  rejoin_peer_exports[ino].first = from;
	  rejoin_peer_exports[ino].second.swap(cap_exports);

	  // send information of imported caps back to peer
	  encode(rejoin_imported_caps[from][ino], ack->commit[p.first]);
	}
      } else {
	// ABORT
	dout(10) << " ambiguous peer request " << p << " will ABORT" << dendl;
	ceph_assert(!p.second.committing);
	ack->add_abort(p.first);
      }
    }
    mds->send_message(ack, m->get_connection());
    return;
  }

  if (!resolve_ack_gather.empty() || !resolve_need_rollback.empty()) {
    dout(10) << "delay processing subtree resolve" << dendl;
    delayed_resolve[from] = m;
    return;
  }

  bool survivor = false;
  // am i a surviving ambiguous importer?
  if (mds->is_clientreplay() || mds->is_active() || mds->is_stopping()) {
    survivor = true;
    // check for any import success/failure (from this node)
    map<dirfrag_t, vector<dirfrag_t> >::iterator p = my_ambiguous_imports.begin();
    while (p != my_ambiguous_imports.end()) {
      map<dirfrag_t, vector<dirfrag_t> >::iterator next = p;
      ++next;
      CDir *dir = get_dirfrag(p->first);
      ceph_assert(dir);
      dout(10) << "checking ambiguous import " << *dir << dendl;
      if (migrator->is_importing(dir->dirfrag()) &&
	  migrator->get_import_peer(dir->dirfrag()) == from) {
	ceph_assert(migrator->get_import_state(dir->dirfrag()) == Migrator::IMPORT_ACKING);
	
	// check if sender claims the subtree
	bool claimed_by_sender = false;
	for (const auto &q : m->subtrees) {
	  // an ambiguous import won't race with a refragmentation; it's appropriate to force here.
	  CDir *base = get_force_dirfrag(q.first, false);
	  if (!base || !base->contains(dir)) 
	    continue;  // base not dir or an ancestor of dir, clearly doesn't claim dir.

	  bool inside = true;
	  set<CDir*> bounds;
	  get_force_dirfrag_bound_set(q.second, bounds);
	  for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
	    CDir *bound = *p;
	    if (bound->contains(dir)) {
	      inside = false;  // nope, bound is dir or parent of dir, not inside.
	      break;
	    }
	  }
	  if (inside)
	    claimed_by_sender = true;
	}

	my_ambiguous_imports.erase(p);  // no longer ambiguous.
	if (claimed_by_sender) {
	  dout(7) << "ambiguous import failed on " << *dir << dendl;
	  migrator->import_reverse(dir);
	} else {
	  dout(7) << "ambiguous import succeeded on " << *dir << dendl;
	  migrator->import_finish(dir, true);
	}
      }
      p = next;
    }
  }    

  // update my dir_auth values
  //   need to do this on recoverying nodes _and_ bystanders (to resolve ambiguous
  //   migrations between other nodes)
  for (const auto& p : m->subtrees) {
    dout(10) << "peer claims " << p.first << " bounds " << p.second << dendl;
    CDir *dir = get_force_dirfrag(p.first, !survivor);
    if (!dir)
      continue;
    adjust_bounded_subtree_auth(dir, p.second, from);
    try_subtree_merge(dir);
  }

  show_subtrees();

  // note ambiguous imports too
  for (const auto& p : m->ambiguous_imports) {
    dout(10) << "noting ambiguous import on " << p.first << " bounds " << p.second << dendl;
    other_ambiguous_imports[from][p.first] = p.second;
  }

  // learn other mds' pendina snaptable commits. later when resolve finishes, we will reload
  // snaptable cache from snapserver. By this way, snaptable cache get synced among all mds
  for (const auto& p : m->table_clients) {
    dout(10) << " noting " << get_mdstable_name(p.type)
	     << " pending_commits " << p.pending_commits << dendl;
    MDSTableClient *client = mds->get_table_client(p.type);
    for (const auto& q : p.pending_commits)
      client->notify_commit(q);
  }
  
  // did i get them all?
  resolve_gather.erase(from);
  
  maybe_resolve_finish();
}

void MDCache::process_delayed_resolve()
{
  dout(10) << "process_delayed_resolve" << dendl;
  map<mds_rank_t, cref_t<MMDSResolve>> tmp;
  tmp.swap(delayed_resolve);
  for (auto &p : tmp) {
    handle_resolve(p.second);
  }
}

void MDCache::discard_delayed_resolve(mds_rank_t who)
{
  delayed_resolve.erase(who);
}

void MDCache::maybe_resolve_finish()
{
  ceph_assert(resolve_ack_gather.empty());
  ceph_assert(resolve_need_rollback.empty());

  if (!resolve_gather.empty()) {
    dout(10) << "maybe_resolve_finish still waiting for resolves ("
	     << resolve_gather << ")" << dendl;
    return;
  }

  dout(10) << "maybe_resolve_finish got all resolves+resolve_acks, done." << dendl;
  disambiguate_my_imports();
  finish_committed_leaders();

  if (resolve_done) {
    ceph_assert(mds->is_resolve());
    trim_unlinked_inodes();
    recalc_auth_bits(false);
    resolve_done.release()->complete(0);
  } else {
    // I am survivor.
    maybe_send_pending_rejoins();
  }
}

void MDCache::handle_resolve_ack(const cref_t<MMDSResolveAck> &ack)
{
  dout(10) << "handle_resolve_ack " << *ack << " from " << ack->get_source() << dendl;
  mds_rank_t from = mds_rank_t(ack->get_source().num());

  if (!resolve_ack_gather.count(from) ||
      mds->mdsmap->get_state(from) < MDSMap::STATE_RESOLVE) {
    return;
  }

  if (ambiguous_peer_updates.count(from)) {
    ceph_assert(mds->mdsmap->is_clientreplay_or_active_or_stopping(from));
    ceph_assert(mds->is_clientreplay() || mds->is_active() || mds->is_stopping());
  }

  for (const auto &p : ack->commit) {
    dout(10) << " commit on peer " << p.first << dendl;
    
    if (ambiguous_peer_updates.count(from)) {
      remove_ambiguous_peer_update(p.first, from);
      continue;
    }

    if (mds->is_resolve()) {
      // replay
      MDPeerUpdate *su = get_uncommitted_peer(p.first, from);
      ceph_assert(su);

      // log commit
      mds->mdlog->submit_entry(new EPeerUpdate(mds->mdlog, "unknown", p.first, from,
						      EPeerUpdate::OP_COMMIT, su->origop),
				     new C_MDC_PeerCommit(this, from, p.first));
      mds->mdlog->flush();

      finish_uncommitted_peer(p.first);
    } else {
      MDRequestRef mdr = request_get(p.first);
      // information about leader imported caps
      if (p.second.length() > 0)
	mdr->more()->inode_import.share(p.second);

      ceph_assert(mdr->peer_request == 0);  // shouldn't be doing anything!
      request_finish(mdr);
    }
  }

  for (const auto &metareq : ack->abort) {
    dout(10) << " abort on peer " << metareq << dendl;

    if (mds->is_resolve()) {
      MDPeerUpdate *su = get_uncommitted_peer(metareq, from);
      ceph_assert(su);

      // perform rollback (and journal a rollback entry)
      // note: this will hold up the resolve a bit, until the rollback entries journal.
      MDRequestRef null_ref;
      switch (su->origop) {
      case EPeerUpdate::LINK:
	mds->server->do_link_rollback(su->rollback, from, null_ref);
	break;
      case EPeerUpdate::RENAME:
	mds->server->do_rename_rollback(su->rollback, from, null_ref);
	break;
      case EPeerUpdate::RMDIR:
	mds->server->do_rmdir_rollback(su->rollback, from, null_ref);
	break;
      default:
	ceph_abort();
      }
    } else {
      MDRequestRef mdr = request_get(metareq);
      mdr->aborted = true;
      if (mdr->peer_request) {
	if (mdr->peer_did_prepare()) // journaling peer prepare ?
	  add_rollback(metareq, from);
      } else {
	request_finish(mdr);
      }
    }
  }

  if (!ambiguous_peer_updates.count(from)) {
    resolve_ack_gather.erase(from);
    maybe_finish_peer_resolve();
  }
}

void MDCache::add_uncommitted_peer(metareqid_t reqid, LogSegment *ls, mds_rank_t leader, MDPeerUpdate *su)
{
  auto const &ret = uncommitted_peers.emplace(std::piecewise_construct,
                                               std::forward_as_tuple(reqid),
                                               std::forward_as_tuple());
  ceph_assert(ret.second);
  ls->uncommitted_peers.insert(reqid);
  upeer &u = ret.first->second;
  u.leader = leader;
  u.ls = ls;
  u.su = su;
  if (su == nullptr) {
    return;
  }
  for(set<CInode*>::iterator p = su->olddirs.begin(); p != su->olddirs.end(); ++p)
    uncommitted_peer_rename_olddir[*p]++;
  for(set<CInode*>::iterator p = su->unlinked.begin(); p != su->unlinked.end(); ++p)
    uncommitted_peer_unlink[*p]++;
}

void MDCache::finish_uncommitted_peer(metareqid_t reqid, bool assert_exist)
{
  auto it = uncommitted_peers.find(reqid);
  if (it == uncommitted_peers.end()) {
    ceph_assert(!assert_exist);
    return;
  }
  upeer &u = it->second;
  MDPeerUpdate* su = u.su;

  if (!u.waiters.empty()) {
    mds->queue_waiters(u.waiters);
  }
  u.ls->uncommitted_peers.erase(reqid);
  uncommitted_peers.erase(it);

  if (su == nullptr) {
    return;
  }
  // discard the non-auth subtree we renamed out of
  for(set<CInode*>::iterator p = su->olddirs.begin(); p != su->olddirs.end(); ++p) {
    CInode *diri = *p;
    map<CInode*, int>::iterator it = uncommitted_peer_rename_olddir.find(diri);
    ceph_assert(it != uncommitted_peer_rename_olddir.end());
    it->second--;
    if (it->second == 0) {
      uncommitted_peer_rename_olddir.erase(it);
      auto&& ls = diri->get_dirfrags();
      for (const auto& dir : ls) {
	CDir *root = get_subtree_root(dir);
	if (root->get_dir_auth() == CDIR_AUTH_UNDEF) {
	  try_trim_non_auth_subtree(root);
	  if (dir != root)
	    break;
	}
      }
    } else
      ceph_assert(it->second > 0);
  }
  // removed the inodes that were unlinked by peer update
  for(set<CInode*>::iterator p = su->unlinked.begin(); p != su->unlinked.end(); ++p) {
    CInode *in = *p;
    map<CInode*, int>::iterator it = uncommitted_peer_unlink.find(in);
    ceph_assert(it != uncommitted_peer_unlink.end());
    it->second--;
    if (it->second == 0) {
      uncommitted_peer_unlink.erase(it);
      if (!in->get_projected_parent_dn())
	mds->mdcache->remove_inode_recursive(in);
    } else
      ceph_assert(it->second > 0);
  }
  delete su;
}

MDPeerUpdate* MDCache::get_uncommitted_peer(metareqid_t reqid, mds_rank_t leader)
{

  MDPeerUpdate* su = nullptr;
  auto it = uncommitted_peers.find(reqid);
  if (it != uncommitted_peers.end() &&
      it->second.leader == leader) {
    su = it->second.su;
  }
  return su;
}

void MDCache::finish_rollback(metareqid_t reqid, const MDRequestRef& mdr) {
  auto p = resolve_need_rollback.find(reqid);
  ceph_assert(p != resolve_need_rollback.end());
  if (mds->is_resolve()) {
    finish_uncommitted_peer(reqid, false);
  } else if (mdr) {
    finish_uncommitted_peer(mdr->reqid, mdr->more()->peer_update_journaled);
  }
  resolve_need_rollback.erase(p);
  maybe_finish_peer_resolve();
}

void MDCache::disambiguate_other_imports()
{
  dout(10) << "disambiguate_other_imports" << dendl;

  bool recovering = !(mds->is_clientreplay() || mds->is_active() || mds->is_stopping());
  // other nodes' ambiguous imports
  for (map<mds_rank_t, map<dirfrag_t, vector<dirfrag_t> > >::iterator p = other_ambiguous_imports.begin();
       p != other_ambiguous_imports.end();
       ++p) {
    mds_rank_t who = p->first;
    dout(10) << "ambiguous imports for mds." << who << dendl;

    for (map<dirfrag_t, vector<dirfrag_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << " ambiguous import " << q->first << " bounds " << q->second << dendl;
      // an ambiguous import will not race with a refragmentation; it's appropriate to force here.
      CDir *dir = get_force_dirfrag(q->first, recovering);
      if (!dir) continue;

      if (dir->is_ambiguous_auth() ||	// works for me_ambig or if i am a surviving bystander
	  dir->authority() == CDIR_AUTH_UNDEF) { // resolving
	dout(10) << "  mds." << who << " did import " << *dir << dendl;
	adjust_bounded_subtree_auth(dir, q->second, who);
	try_subtree_merge(dir);
      } else {
	dout(10) << "  mds." << who << " did not import " << *dir << dendl;
      }
    }
  }
  other_ambiguous_imports.clear();
}

void MDCache::disambiguate_my_imports()
{
  dout(10) << "disambiguate_my_imports" << dendl;

  if (!mds->is_resolve()) {
    ceph_assert(my_ambiguous_imports.empty());
    return;
  }

  disambiguate_other_imports();

  // my ambiguous imports
  mds_authority_t me_ambig(mds->get_nodeid(), mds->get_nodeid());
  while (!my_ambiguous_imports.empty()) {
    map<dirfrag_t, vector<dirfrag_t> >::iterator q = my_ambiguous_imports.begin();

    CDir *dir = get_dirfrag(q->first);
    ceph_assert(dir);
    
    if (dir->authority() != me_ambig) {
      dout(10) << "ambiguous import auth known, must not be me " << *dir << dendl;
      cancel_ambiguous_import(dir);

      mds->mdlog->submit_entry(new EImportFinish(dir, false));

      // subtree may have been swallowed by another node claiming dir
      // as their own.
      CDir *root = get_subtree_root(dir);
      if (root != dir)
	dout(10) << "  subtree root is " << *root << dendl;
      ceph_assert(root->dir_auth.first != mds->get_nodeid());  // no us!
      try_trim_non_auth_subtree(root);
    } else {
      dout(10) << "ambiguous import auth unclaimed, must be me " << *dir << dendl;
      finish_ambiguous_import(q->first);
      mds->mdlog->submit_entry(new EImportFinish(dir, true));
    }
  }
  ceph_assert(my_ambiguous_imports.empty());
  mds->mdlog->flush();

  // verify all my subtrees are unambiguous!
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    if (dir->is_ambiguous_dir_auth()) {
      dout(0) << "disambiguate_imports uh oh, dir_auth is still ambiguous for " << *dir << dendl;
    }
    ceph_assert(!dir->is_ambiguous_dir_auth());
  }

  show_subtrees();
}


void MDCache::add_ambiguous_import(dirfrag_t base, const vector<dirfrag_t>& bounds) 
{
  ceph_assert(my_ambiguous_imports.count(base) == 0);
  my_ambiguous_imports[base] = bounds;
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

void MDCache::cancel_ambiguous_import(CDir *dir)
{
  dirfrag_t df = dir->dirfrag();
  ceph_assert(my_ambiguous_imports.count(df));
  dout(10) << "cancel_ambiguous_import " << df
	   << " bounds " << my_ambiguous_imports[df]
	   << " " << *dir
	   << dendl;
  my_ambiguous_imports.erase(df);
}

void MDCache::finish_ambiguous_import(dirfrag_t df)
{
  ceph_assert(my_ambiguous_imports.count(df));
  vector<dirfrag_t> bounds;
  bounds.swap(my_ambiguous_imports[df]);
  my_ambiguous_imports.erase(df);
  
  dout(10) << "finish_ambiguous_import " << df
	   << " bounds " << bounds
	   << dendl;
  CDir *dir = get_dirfrag(df);
  ceph_assert(dir);
  
  // adjust dir_auth, import maps
  adjust_bounded_subtree_auth(dir, bounds, mds->get_nodeid());
  try_subtree_merge(dir);
}

void MDCache::remove_inode_recursive(CInode *in)
{
  dout(10) << "remove_inode_recursive " << *in << dendl;
  auto&& ls = in->get_dirfrags();
  for (const auto& subdir : ls) {
    dout(10) << " removing dirfrag " << *subdir << dendl;
    auto it = subdir->items.begin();
    while (it != subdir->items.end()) {
      CDentry *dn = it->second;
      ++it;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_primary()) {
	CInode *tin = dnl->get_inode();
	subdir->unlink_inode(dn, false);
	remove_inode_recursive(tin);
      }
      subdir->remove_dentry(dn);
    }
    
    if (subdir->is_subtree_root()) 
      remove_subtree(subdir);
    in->close_dirfrag(subdir->dirfrag().frag);
  }
  remove_inode(in);
}

bool MDCache::expire_recursive(CInode *in, expiremap &expiremap)
{
  ceph_assert(!in->is_auth());

  dout(10) << __func__ << ":" << *in << dendl;

  // Recurse into any dirfrags beneath this inode
  auto&& ls = in->get_dirfrags();
  for (const auto& subdir : ls) {
    if (!in->is_mdsdir() && subdir->is_subtree_root()) {
      dout(10) << __func__ << ": stray still has subtree " << *in << dendl;
      return true;
    }

    for (auto it = subdir->items.begin(); it != subdir->items.end();) {
      CDentry *dn = it->second;
      it++;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_primary()) {
	CInode *tin = dnl->get_inode();

        /* Remote strays with linkage (i.e. hardlinks) should not be
         * expired, because they may be the target of
         * a rename() as the owning MDS shuts down */
        if (!tin->is_stray() && tin->get_inode()->nlink) {
          dout(10) << __func__ << ": stray still has linkage " << *tin << dendl;
          return true;
        }

	const bool abort = expire_recursive(tin, expiremap);
        if (abort) {
          return true;
        }
      }
      if (dn->lru_is_expireable()) {
        trim_dentry(dn, expiremap);
      } else {
        dout(10) << __func__ << ": stray dn is not expireable " << *dn << dendl;
        return true;
      }
    }
  }

  return false;
}

void MDCache::trim_unlinked_inodes()
{
  dout(7) << "trim_unlinked_inodes" << dendl;
  int count = 0;
  vector<CInode*> q;
  for (auto &p : inode_map) {
    CInode *in = p.second;
    if (in->get_parent_dn() == NULL && !in->is_base()) {
      dout(7) << " will trim from " << *in << dendl;
      q.push_back(in);
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
  for (auto& in : q) {
    remove_inode_recursive(in);

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
}

/** recalc_auth_bits()
 * once subtree auth is disambiguated, we need to adjust all the 
 * auth and dirty bits in our cache before moving on.
 */
void MDCache::recalc_auth_bits(bool replay)
{
  dout(7) << "recalc_auth_bits " << (replay ? "(replay)" : "") <<  dendl;

  if (root) {
    root->inode_auth.first = mds->mdsmap->get_root();
    bool auth = mds->get_nodeid() == root->inode_auth.first;
    if (auth) {
      root->state_set(CInode::STATE_AUTH);
    } else {
      root->state_clear(CInode::STATE_AUTH);
      if (!replay)
	root->state_set(CInode::STATE_REJOINING);
    }
  }

  set<CInode*> subtree_inodes;
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    if (p->first->dir_auth.first == mds->get_nodeid())
      subtree_inodes.insert(p->first->inode);
  }

  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    if (p->first->inode->is_mdsdir()) {
      CInode *in = p->first->inode;
      bool auth = in->ino() == MDS_INO_MDSDIR(mds->get_nodeid());
      if (auth) {
	in->state_set(CInode::STATE_AUTH);
      } else {
	in->state_clear(CInode::STATE_AUTH);
	if (!replay)
	  in->state_set(CInode::STATE_REJOINING);
      }
    }

    std::queue<CDir*> dfq;  // dirfrag queue
    dfq.push(p->first);

    bool auth = p->first->authority().first == mds->get_nodeid();
    dout(10) << " subtree auth=" << auth << " for " << *p->first << dendl;

    while (!dfq.empty()) {
      CDir *dir = dfq.front();
      dfq.pop();

      // dir
      if (auth) {
	dir->state_set(CDir::STATE_AUTH);
      } else {
	dir->state_clear(CDir::STATE_AUTH);
	if (!replay) {
	  // close empty non-auth dirfrag
	  if (!dir->is_subtree_root() && dir->get_num_any() == 0) {
	    dir->inode->close_dirfrag(dir->get_frag());
	    continue;
	  }
	  dir->state_set(CDir::STATE_REJOINING);
	  dir->state_clear(CDir::STATE_COMPLETE);
	  if (dir->is_dirty())
	    dir->mark_clean();
	}
      }

      // dentries in this dir
      for (auto &p : dir->items) {
	// dn
	CDentry *dn = p.second;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (auth) {
	  dn->mark_auth();
	} else {
	  dn->clear_auth();
	  if (!replay) {
	    dn->state_set(CDentry::STATE_REJOINING);
	    if (dn->is_dirty())
	      dn->mark_clean();
	  }
	}

	if (dnl->is_primary()) {
	  // inode
	  CInode *in = dnl->get_inode();
	  if (auth) {
	    in->state_set(CInode::STATE_AUTH);
	  } else {
	    in->state_clear(CInode::STATE_AUTH);
	    if (!replay) {
	      in->state_set(CInode::STATE_REJOINING);
	      if (in->is_dirty())
		in->mark_clean();
	      if (in->is_dirty_parent())
		in->clear_dirty_parent();
	      // avoid touching scatterlocks for our subtree roots!
	      if (subtree_inodes.count(in) == 0)
		in->clear_scatter_dirty();
	    }
	  }
	  // recurse?
	  if (in->is_dir()) {
	    auto&& dfv = in->get_nested_dirfrags();
            for (const auto& dir : dfv) {
              dfq.push(dir);
            }
          }
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

void MDCache::dump_rejoin_status(Formatter *f) const
{
  f->open_object_section("rejoin_status");
  f->dump_stream("rejoin_gather") << rejoin_gather;
  f->dump_stream("rejoin_ack_gather") << rejoin_ack_gather;
  f->dump_unsigned("num_opening_inodes", cap_imports_num_opening);
  f->close_section();
}

void MDCache::rejoin_start(MDSContext *rejoin_done_)
{
  dout(10) << "rejoin_start" << dendl;
  ceph_assert(!rejoin_done);
  rejoin_done.reset(rejoin_done_);

  rejoin_gather = recovery_set;
  // need finish opening cap inodes before sending cache rejoins
  rejoin_gather.insert(mds->get_nodeid());
  process_imported_caps();
}

/*
 * rejoin phase!
 *
 * this initiates rejoin.  it should be called before we get any
 * rejoin or rejoin_ack messages (or else mdsmap distribution is broken).
 *
 * we start out by sending rejoins to everyone in the recovery set.
 *
 * if we are rejoin, send for all regions in our cache.
 * if we are active|stopping, send only to nodes that are rejoining.
 */
void MDCache::rejoin_send_rejoins()
{
  dout(10) << "rejoin_send_rejoins with recovery_set " << recovery_set << dendl;

  if (rejoin_gather.count(mds->get_nodeid())) {
    dout(7) << "rejoin_send_rejoins still processing imported caps, delaying" << dendl;
    rejoins_pending = true;
    return;
  }
  if (!resolve_gather.empty()) {
    dout(7) << "rejoin_send_rejoins still waiting for resolves ("
	    << resolve_gather << ")" << dendl;
    rejoins_pending = true;
    return;
  }

  ceph_assert(!migrator->is_importing());
  ceph_assert(!migrator->is_exporting());

  if (!mds->is_rejoin()) {
    disambiguate_other_imports();
  }

  map<mds_rank_t, ref_t<MMDSCacheRejoin>> rejoins;


  // if i am rejoining, send a rejoin to everyone.
  // otherwise, just send to others who are rejoining.
  for (const auto& rank : recovery_set) {
    if (rank == mds->get_nodeid())  continue;  // nothing to myself!
    if (rejoin_sent.count(rank)) continue;     // already sent a rejoin to this node!
    if (mds->is_rejoin())
      rejoins[rank] = make_message<MMDSCacheRejoin>(MMDSCacheRejoin::OP_WEAK);
    else if (mds->mdsmap->is_rejoin(rank))
      rejoins[rank] = make_message<MMDSCacheRejoin>(MMDSCacheRejoin::OP_STRONG);
  }

  if (mds->is_rejoin()) {
    map<client_t, pair<Session*, set<mds_rank_t> > > client_exports;
    for (auto& p : cap_exports) {
      mds_rank_t target = p.second.first;
      if (rejoins.count(target) == 0)
	continue;
      for (auto q = p.second.second.begin(); q != p.second.second.end(); ) {
	Session *session = nullptr;
	auto it = client_exports.find(q->first);
	if (it != client_exports.end()) {
	  session = it->second.first;
	  if (session)
	    it->second.second.insert(target);
	} else {
	  session = mds->sessionmap.get_session(entity_name_t::CLIENT(q->first.v));
	  auto& r = client_exports[q->first];
	  r.first = session;
	  if (session)
	    r.second.insert(target);
	}
	if (session) {
	  ++q;
	} else {
	  // remove reconnect with no session
	  p.second.second.erase(q++);
	}
      }
      rejoins[target]->cap_exports[p.first] = p.second.second;
    }
    for (auto& p : client_exports) {
      Session *session = p.second.first;
      for (auto& q : p.second.second) {
	auto rejoin =  rejoins[q];
	rejoin->client_map[p.first] = session->info.inst;
	rejoin->client_metadata_map[p.first] = session->info.client_metadata;
      }
    }
  }
  
  
  // check all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    ceph_assert(dir->is_subtree_root());
    if (dir->is_ambiguous_dir_auth()) {
      // exporter is recovering, importer is survivor.
      ceph_assert(rejoins.count(dir->authority().first));
      ceph_assert(!rejoins.count(dir->authority().second));
      continue;
    }

    // my subtree?
    if (dir->is_auth())
      continue;  // skip my own regions!

    mds_rank_t auth = dir->get_dir_auth().first;
    ceph_assert(auth >= 0);
    if (rejoins.count(auth) == 0)
      continue;   // don't care about this node's subtrees

    rejoin_walk(dir, rejoins[auth]);
  }
  
  // rejoin root inodes, too
  for (auto &p : rejoins) {
    if (mds->is_rejoin()) {
      // weak
      if (p.first == 0 && root) {
	p.second->add_weak_inode(root->vino());
	if (root->is_dirty_scattered()) {
	  dout(10) << " sending scatterlock state on root " << *root << dendl;
	  p.second->add_scatterlock_state(root);
	}
      }
      if (CInode *in = get_inode(MDS_INO_MDSDIR(p.first))) { 
	if (in)
	  p.second->add_weak_inode(in->vino());
      }
    } else {
      // strong
      if (p.first == 0 && root) {
	p.second->add_strong_inode(root->vino(),
				    root->get_replica_nonce(),
				    root->get_caps_wanted(),
				    root->filelock.get_state(),
				    root->nestlock.get_state(),
				    root->dirfragtreelock.get_state());
	root->state_set(CInode::STATE_REJOINING);
	if (root->is_dirty_scattered()) {
	  dout(10) << " sending scatterlock state on root " << *root << dendl;
	  p.second->add_scatterlock_state(root);
	}
      }

      if (CInode *in = get_inode(MDS_INO_MDSDIR(p.first))) {
	p.second->add_strong_inode(in->vino(),
				    in->get_replica_nonce(),
				    in->get_caps_wanted(),
				    in->filelock.get_state(),
				    in->nestlock.get_state(),
				    in->dirfragtreelock.get_state());
	in->state_set(CInode::STATE_REJOINING);
      }
    }
  }  

  if (!mds->is_rejoin()) {
    // i am survivor.  send strong rejoin.
    // note request remote_auth_pins, xlocks
    for (ceph::unordered_map<metareqid_t, MDRequestRef>::iterator p = active_requests.begin();
	 p != active_requests.end();
	 ++p) {
      MDRequestRef& mdr = p->second;
      if (mdr->is_peer())
	continue;
      // auth pins
      for (const auto& q : mdr->object_states) {
	if (q.second.remote_auth_pinned == MDS_RANK_NONE)
	  continue;
	if (!q.first->is_auth()) {
	  mds_rank_t target = q.second.remote_auth_pinned;
	  ceph_assert(target == q.first->authority().first);
	  if (rejoins.count(target) == 0) continue;
	  const auto& rejoin = rejoins[target];
	  
	  dout(15) << " " << *mdr << " authpin on " << *q.first << dendl;
	  MDSCacheObjectInfo i;
	  q.first->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_authpin(vinodeno_t(i.ino, i.snapid), mdr->reqid, mdr->attempt);
	  else
	    rejoin->add_dentry_authpin(i.dirfrag, i.dname, i.snapid, mdr->reqid, mdr->attempt);

	  if (mdr->has_more() && mdr->more()->is_remote_frozen_authpin &&
	      mdr->more()->rename_inode == q.first)
	    rejoin->add_inode_frozen_authpin(vinodeno_t(i.ino, i.snapid),
					     mdr->reqid, mdr->attempt);
	}
      }
      // xlocks
      for (const auto& q : mdr->locks) {
	auto lock = q.lock;
	auto obj = lock->get_parent();
	if (q.is_xlock() && !obj->is_auth()) {
	  mds_rank_t who = obj->authority().first;
	  if (rejoins.count(who) == 0) continue;
	  const auto& rejoin = rejoins[who];
	  
	  dout(15) << " " << *mdr << " xlock on " << *lock << " " << *obj << dendl;
	  MDSCacheObjectInfo i;
	  obj->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_xlock(vinodeno_t(i.ino, i.snapid), lock->get_type(),
				    mdr->reqid, mdr->attempt);
	  else
	    rejoin->add_dentry_xlock(i.dirfrag, i.dname, i.snapid,
				     mdr->reqid, mdr->attempt);
	} else if (q.is_remote_wrlock()) {
	  mds_rank_t who = q.wrlock_target;
	  if (rejoins.count(who) == 0) continue;
	  const auto& rejoin = rejoins[who];

	  dout(15) << " " << *mdr << " wrlock on " << *lock << " " << *obj << dendl;
	  MDSCacheObjectInfo i;
	  obj->set_object_info(i);
	  ceph_assert(i.ino);
	  rejoin->add_inode_wrlock(vinodeno_t(i.ino, i.snapid), lock->get_type(),
				   mdr->reqid, mdr->attempt);
	}
      }
    }
  }

  // send the messages
  for (auto &p : rejoins) {
    ceph_assert(rejoin_sent.count(p.first) == 0);
    ceph_assert(rejoin_ack_gather.count(p.first) == 0);
    rejoin_sent.insert(p.first);
    rejoin_ack_gather.insert(p.first);
    mds->send_message_mds(p.second, p.first);
  }
  rejoin_ack_gather.insert(mds->get_nodeid());   // we need to complete rejoin_gather_finish, too
  rejoins_pending = false;

  // nothing?
  if (mds->is_rejoin() && rejoin_gather.empty()) {
    dout(10) << "nothing to rejoin" << dendl;
    rejoin_gather_finish();
  }
}


/** 
 * rejoin_walk - build rejoin declarations for a subtree
 * 
 * @param dir subtree root
 * @param rejoin rejoin message
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
void MDCache::rejoin_walk(CDir *dir, const ref_t<MMDSCacheRejoin> &rejoin)
{
  dout(10) << "rejoin_walk " << *dir << dendl;

  std::vector<CDir*> nested;  // finish this dir, then do nested items
  
  if (mds->is_rejoin()) {
    // WEAK
    rejoin->add_weak_dirfrag(dir->dirfrag());
    for (auto &p : dir->items) {
      CDentry *dn = p.second;
      ceph_assert(dn->last == CEPH_NOSNAP);
      CDentry::linkage_t *dnl = dn->get_linkage();
      dout(15) << " add_weak_primary_dentry " << *dn << dendl;
      ceph_assert(dnl->is_primary());
      CInode *in = dnl->get_inode();
      ceph_assert(dnl->get_inode()->is_dir());
      rejoin->add_weak_primary_dentry(dir->ino(), dn->get_name(), dn->first, dn->last, in->ino());
      {
        auto&& dirs = in->get_nested_dirfrags();
        nested.insert(std::end(nested), std::begin(dirs), std::end(dirs));
      }
      if (in->is_dirty_scattered()) {
	dout(10) << " sending scatterlock state on " << *in << dendl;
	rejoin->add_scatterlock_state(in);
      }
    }
  } else {
    // STRONG
    dout(15) << " add_strong_dirfrag " << *dir << dendl;
    rejoin->add_strong_dirfrag(dir->dirfrag(), dir->get_replica_nonce(), dir->get_dir_rep());
    dir->state_set(CDir::STATE_REJOINING);

    for (auto it = dir->items.begin(); it != dir->items.end(); ) {
      CDentry *dn = it->second;
      ++it;
      dn->state_set(CDentry::STATE_REJOINING);
      CDentry::linkage_t *dnl = dn->get_linkage();
      CInode *in = dnl->is_primary() ? dnl->get_inode() : NULL;

      // trim snap dentries. because they may have been pruned by
      // their auth mds (snap deleted)
      if (dn->last != CEPH_NOSNAP) {
	if (in && !in->remote_parents.empty()) {
	  // unlink any stale remote snap dentry.
	  for (auto it2 = in->remote_parents.begin(); it2 != in->remote_parents.end(); ) {
	    CDentry *remote_dn = *it2;
	    ++it2;
	    ceph_assert(remote_dn->last != CEPH_NOSNAP);
	    remote_dn->unlink_remote(remote_dn->get_linkage());
	  }
	}
	if (dn->lru_is_expireable()) {
	  if (!dnl->is_null())
	    dir->unlink_inode(dn, false);
	  if (in)
	    remove_inode(in);
	  dir->remove_dentry(dn);
	  continue;
	} else {
	  // Inventing null/remote dentry shouldn't cause problem
	  ceph_assert(!dnl->is_primary());
	}
      }

      dout(15) << " add_strong_dentry " << *dn << dendl;
      rejoin->add_strong_dentry(dir->dirfrag(), dn->get_name(), dn->get_alternate_name(),
                                dn->first, dn->last,
				dnl->is_primary() ? dnl->get_inode()->ino():inodeno_t(0),
				dnl->is_remote() ? dnl->get_remote_ino():inodeno_t(0),
				dnl->is_remote() ? dnl->get_remote_d_type():0, 
				dn->get_replica_nonce(),
				dn->lock.get_state());
      dn->state_set(CDentry::STATE_REJOINING);
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	dout(15) << " add_strong_inode " << *in << dendl;
	rejoin->add_strong_inode(in->vino(),
				 in->get_replica_nonce(),
				 in->get_caps_wanted(),
				 in->filelock.get_state(),
				 in->nestlock.get_state(),
				 in->dirfragtreelock.get_state());
	in->state_set(CInode::STATE_REJOINING);
        {
          auto&& dirs = in->get_nested_dirfrags();
          nested.insert(std::end(nested), std::begin(dirs), std::end(dirs));
        }
	if (in->is_dirty_scattered()) {
	  dout(10) << " sending scatterlock state on " << *in << dendl;
	  rejoin->add_scatterlock_state(in);
	}
      }
    }
  }

  // recurse into nested dirs
  for (const auto& dir : nested) {
    rejoin_walk(dir, rejoin);
  }
}


/*
 * i got a rejoin.
 *  - reply with the lockstate
 *
 * if i am active|stopping, 
 *  - remove source from replica list for everything not referenced here.
 */
void MDCache::handle_cache_rejoin(const cref_t<MMDSCacheRejoin> &m)
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

  default: 
    ceph_abort();
  }
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
 */
void MDCache::handle_cache_rejoin_weak(const cref_t<MMDSCacheRejoin> &weak)
{
  mds_rank_t from = mds_rank_t(weak->get_source().num());

  // possible response(s)
  ref_t<MMDSCacheRejoin> ack;      // if survivor
  set<vinodeno_t> acked_inodes;  // if survivor
  set<SimpleLock *> gather_locks;  // if survivor
  bool survivor = false;  // am i a survivor?

  if (mds->is_clientreplay() || mds->is_active() || mds->is_stopping()) {
    survivor = true;
    dout(10) << "i am a surivivor, and will ack immediately" << dendl;
    ack = make_message<MMDSCacheRejoin>(MMDSCacheRejoin::OP_ACK);

    map<inodeno_t,map<client_t,Capability::Import> > imported_caps;

    // check cap exports
    for (auto p = weak->cap_exports.begin(); p != weak->cap_exports.end(); ++p) {
      CInode *in = get_inode(p->first);
      ceph_assert(!in || in->is_auth());
      for (auto q = p->second.begin(); q != p->second.end(); ++q) {
	dout(10) << " claiming cap import " << p->first << " client." << q->first << " on " << *in << dendl;
	Capability *cap = rejoin_import_cap(in, q->first, q->second, from);
	Capability::Import& im = imported_caps[p->first][q->first];
	if (cap) {
	  im.cap_id = cap->get_cap_id();
	  im.issue_seq = cap->get_last_seq();
	  im.mseq = cap->get_mseq();
	} else {
	  // all are zero
	}
      }
      mds->locker->eval(in, CEPH_CAP_LOCKS, true);
    }

    encode(imported_caps, ack->imported_caps);
  } else {
    ceph_assert(mds->is_rejoin());

    // we may have already received a strong rejoin from the sender.
    rejoin_scour_survivor_replicas(from, NULL, acked_inodes, gather_locks);
    ceph_assert(gather_locks.empty());

    // check cap exports.
    rejoin_client_map.insert(weak->client_map.begin(), weak->client_map.end());
    rejoin_client_metadata_map.insert(weak->client_metadata_map.begin(),
				      weak->client_metadata_map.end());

    for (auto p = weak->cap_exports.begin(); p != weak->cap_exports.end(); ++p) {
      CInode *in = get_inode(p->first);
      ceph_assert(!in || in->is_auth());
      // note
      for (auto q = p->second.begin(); q != p->second.end(); ++q) {
	dout(10) << " claiming cap import " << p->first << " client." << q->first << dendl;
	cap_imports[p->first][q->first][from] = q->second;
      }
    }
  }

  // assimilate any potentially dirty scatterlock state
  for (const auto &p : weak->inode_scatterlocks) {
    CInode *in = get_inode(p.first);
    ceph_assert(in);
    in->decode_lock_state(CEPH_LOCK_IFILE, p.second.file);
    in->decode_lock_state(CEPH_LOCK_INEST, p.second.nest);
    in->decode_lock_state(CEPH_LOCK_IDFT, p.second.dft);
    if (!survivor)
      rejoin_potential_updated_scatterlocks.insert(in);
  }

  // recovering peer may send incorrect dirfrags here.  we need to
  // infer which dirfrag they meant.  the ack will include a
  // strong_dirfrag that will set them straight on the fragmentation.
  
  // walk weak map
  set<CDir*> dirs_to_share;
  for (const auto &p : weak->weak_dirfrags) {
    CInode *diri = get_inode(p.ino);
    if (!diri)
      dout(0) << " missing dir ino " << p.ino << dendl;
    ceph_assert(diri);

    frag_vec_t leaves;
    if (diri->dirfragtree.is_leaf(p.frag)) {
      leaves.push_back(p.frag);
    } else {
      diri->dirfragtree.get_leaves_under(p.frag, leaves);
      if (leaves.empty())
	leaves.push_back(diri->dirfragtree[p.frag.value()]);
    }
    for (const auto& leaf : leaves) {
      CDir *dir = diri->get_dirfrag(leaf);
      if (!dir) {
	dout(0) << " missing dir for " << p.frag << " (which maps to " << leaf << ") on " << *diri << dendl;
	continue;
      }
      ceph_assert(dir);
      if (dirs_to_share.count(dir)) {
	dout(10) << " already have " << p.frag << " -> " << leaf << " " << *dir << dendl;
      } else {
	dirs_to_share.insert(dir);
	unsigned nonce = dir->add_replica(from);
	dout(10) << " have " << p.frag << " -> " << leaf << " " << *dir << dendl;
	if (ack) {
	  ack->add_strong_dirfrag(dir->dirfrag(), nonce, dir->dir_rep);
	  ack->add_dirfrag_base(dir);
	}
      }
    }
  }

  for (const auto &p : weak->weak) {
    CInode *diri = get_inode(p.first);
    if (!diri)
      dout(0) << " missing dir ino " << p.first << dendl;
    ceph_assert(diri);

    // weak dentries
    CDir *dir = 0;
    for (const auto &q : p.second) {
      // locate proper dirfrag.
      //  optimize for common case (one dirfrag) to avoid dirs_to_share set check
      frag_t fg = diri->pick_dirfrag(q.first.name);
      if (!dir || dir->get_frag() != fg) {
	dir = diri->get_dirfrag(fg);
	if (!dir)
	  dout(0) << " missing dir frag " << fg << " on " << *diri << dendl;
	ceph_assert(dir);
	ceph_assert(dirs_to_share.count(dir));
      }

      // and dentry
      CDentry *dn = dir->lookup(q.first.name, q.first.snapid);
      ceph_assert(dn);
      CDentry::linkage_t *dnl = dn->get_linkage();
      ceph_assert(dnl->is_primary());
      
      if (survivor && dn->is_replica(from)) 
	dentry_remove_replica(dn, from, gather_locks);
      unsigned dnonce = dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      if (ack) 
	ack->add_strong_dentry(dir->dirfrag(), dn->get_name(), dn->get_alternate_name(),
                               dn->first, dn->last,
			       dnl->get_inode()->ino(), inodeno_t(0), 0, 
			       dnonce, dn->lock.get_replica_state());

      // inode
      CInode *in = dnl->get_inode();
      ceph_assert(in);

      if (survivor && in->is_replica(from)) 
	inode_remove_replica(in, from, true, gather_locks);
      unsigned inonce = in->add_replica(from);
      dout(10) << " have " << *in << dendl;

      // scatter the dirlock, just in case?
      if (!survivor && in->is_dir() && in->has_subtree_root_dirfrag())
	in->filelock.set_state(LOCK_MIX);

      if (ack) {
	acked_inodes.insert(in->vino());
	ack->add_inode_base(in, mds->mdsmap->get_up_features());
	bufferlist bl;
	in->_encode_locks_state_for_rejoin(bl, from);
	ack->add_inode_locks(in, inonce, bl);
      }
    }
  }
  
  // weak base inodes?  (root, stray, etc.)
  for (set<vinodeno_t>::iterator p = weak->weak_inodes.begin();
       p != weak->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    ceph_assert(in);   // hmm fixme wrt stray?
    if (survivor && in->is_replica(from)) 
      inode_remove_replica(in, from, true, gather_locks);
    unsigned inonce = in->add_replica(from);
    dout(10) << " have base " << *in << dendl;
    
    if (ack) {
      acked_inodes.insert(in->vino());
      ack->add_inode_base(in, mds->mdsmap->get_up_features());
      bufferlist bl;
      in->_encode_locks_state_for_rejoin(bl, from);
      ack->add_inode_locks(in, inonce, bl);
    }
  }

  ceph_assert(rejoin_gather.count(from));
  rejoin_gather.erase(from);
  if (survivor) {
    // survivor.  do everything now.
    for (const auto &p : weak->inode_scatterlocks) {
      CInode *in = get_inode(p.first);
      ceph_assert(in);
      dout(10) << " including base inode (due to potential scatterlock update) " << *in << dendl;
      acked_inodes.insert(in->vino());
      ack->add_inode_base(in, mds->mdsmap->get_up_features());
    }

    rejoin_scour_survivor_replicas(from, ack, acked_inodes, gather_locks);
    mds->send_message(ack, weak->get_connection());

    for (set<SimpleLock*>::iterator p = gather_locks.begin(); p != gather_locks.end(); ++p) {
      if (!(*p)->is_stable())
	mds->locker->eval_gather(*p);
    }
  } else {
    // done?
    if (rejoin_gather.empty() && rejoin_ack_gather.count(mds->get_nodeid())) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}

/*
 * rejoin_scour_survivor_replica - remove source from replica list on unmentioned objects
 *
 * all validated replicas are acked with a strong nonce, etc.  if that isn't in the
 * ack, the replica dne, and we can remove it from our replica maps.
 */
void MDCache::rejoin_scour_survivor_replicas(mds_rank_t from, const cref_t<MMDSCacheRejoin> &ack,
					     set<vinodeno_t>& acked_inodes,
					     set<SimpleLock *>& gather_locks)
{
  dout(10) << "rejoin_scour_survivor_replicas from mds." << from << dendl;

  auto scour_func = [this, from, ack, &acked_inodes, &gather_locks] (CInode *in) {
    // inode?
    if (in->is_auth() &&
	in->is_replica(from) &&
	(ack == NULL || acked_inodes.count(in->vino()) == 0)) {
      inode_remove_replica(in, from, false, gather_locks);
      dout(10) << " rem " << *in << dendl;
    }

    if (!in->is_dir())
      return;
    
    const auto&& dfs = in->get_dirfrags();
    for (const auto& dir : dfs) {
      if (!dir->is_auth())
	continue;
      
      if (dir->is_replica(from) &&
	  (ack == NULL || ack->strong_dirfrags.count(dir->dirfrag()) == 0)) {
	dir->remove_replica(from);
	dout(10) << " rem " << *dir << dendl;
      } 
      
      // dentries
      for (auto &p : dir->items) {
	CDentry *dn = p.second;
	
	if (dn->is_replica(from)) {
          if (ack) {
            const auto it = ack->strong_dentries.find(dir->dirfrag());
            if (it != ack->strong_dentries.end() && it->second.count(string_snap_t(dn->get_name(), dn->last)) > 0) {
              continue;
            }
          }
	  dentry_remove_replica(dn, from, gather_locks);
	  dout(10) << " rem " << *dn << dendl;
	}
      }
    }
  };

  for (auto &p : inode_map)
    scour_func(p.second);
  for (auto &p : snap_inode_map)
    scour_func(p.second);
}


CInode *MDCache::rejoin_invent_inode(inodeno_t ino, snapid_t last)
{
  CInode *in = new CInode(this, true, 2, last);
  in->_get_inode()->ino = ino;
  in->state_set(CInode::STATE_REJOINUNDEF);
  add_inode(in);
  rejoin_undef_inodes.insert(in);
  dout(10) << " invented " << *in << dendl;
  return in;
}

CDir *MDCache::rejoin_invent_dirfrag(dirfrag_t df)
{
  CInode *in = get_inode(df.ino);
  if (!in)
    in = rejoin_invent_inode(df.ino, CEPH_NOSNAP);
  if (!in->is_dir()) {
    ceph_assert(in->state_test(CInode::STATE_REJOINUNDEF));
    in->_get_inode()->mode = S_IFDIR;
    in->_get_inode()->dir_layout.dl_dir_hash = g_conf()->mds_default_dir_hash;
  }
  CDir *dir = in->get_or_open_dirfrag(this, df.frag);
  dir->state_set(CDir::STATE_REJOINUNDEF);
  rejoin_undef_dirfrags.insert(dir);
  dout(10) << " invented " << *dir << dendl;
  return dir;
}

void MDCache::handle_cache_rejoin_strong(const cref_t<MMDSCacheRejoin> &strong)
{
  mds_rank_t from = mds_rank_t(strong->get_source().num());

  // only a recovering node will get a strong rejoin.
  if (!mds->is_rejoin()) {
    if (mds->get_want_state() == MDSMap::STATE_REJOIN) {
      mds->wait_for_rejoin(new C_MDS_RetryMessage(mds, strong));
      return;
    }
    ceph_abort_msg("got unexpected rejoin message during recovery");
  }

  // assimilate any potentially dirty scatterlock state
  for (const auto &p : strong->inode_scatterlocks) {
    CInode *in = get_inode(p.first);
    ceph_assert(in);
    in->decode_lock_state(CEPH_LOCK_IFILE, p.second.file);
    in->decode_lock_state(CEPH_LOCK_INEST, p.second.nest);
    in->decode_lock_state(CEPH_LOCK_IDFT, p.second.dft);
    rejoin_potential_updated_scatterlocks.insert(in);
  }

  rejoin_unlinked_inodes[from].clear();

  // surviving peer may send incorrect dirfrag here (maybe they didn't
  // get the fragment notify, or maybe we rolled back?).  we need to
  // infer the right frag and get them with the program.  somehow.
  // we don't normally send ACK.. so we'll need to bundle this with
  // MISSING or something.

  // strong dirfrags/dentries.
  //  also process auth_pins, xlocks.
  for (const auto &p : strong->strong_dirfrags) {
    auto& dirfrag = p.first;
    CInode *diri = get_inode(dirfrag.ino);
    if (!diri)
      diri = rejoin_invent_inode(dirfrag.ino, CEPH_NOSNAP);
    CDir *dir = diri->get_dirfrag(dirfrag.frag);
    bool refragged = false;
    if (dir) {
      dout(10) << " have " << *dir << dendl;
    } else {
      if (diri->state_test(CInode::STATE_REJOINUNDEF))
	dir = rejoin_invent_dirfrag(dirfrag_t(diri->ino(), frag_t()));
      else if (diri->dirfragtree.is_leaf(dirfrag.frag))
	dir = rejoin_invent_dirfrag(dirfrag);
    }
    if (dir) {
      dir->add_replica(from, p.second.nonce);
      dir->dir_rep = p.second.dir_rep;
    } else {
      dout(10) << " frag " << dirfrag << " doesn't match dirfragtree " << *diri << dendl;
      frag_vec_t leaves;
      diri->dirfragtree.get_leaves_under(dirfrag.frag, leaves);
      if (leaves.empty())
	leaves.push_back(diri->dirfragtree[dirfrag.frag.value()]);
      dout(10) << " maps to frag(s) " << leaves << dendl;
      for (const auto& leaf : leaves) {
	CDir *dir = diri->get_dirfrag(leaf);
	if (!dir)
	  dir = rejoin_invent_dirfrag(dirfrag_t(diri->ino(), leaf));
	else
	  dout(10) << " have(approx) " << *dir << dendl;
	dir->add_replica(from, p.second.nonce);
	dir->dir_rep = p.second.dir_rep;
      }
      refragged = true;
    }
    
    const auto it = strong->strong_dentries.find(dirfrag);
    if (it != strong->strong_dentries.end()) {
      const auto& dmap = it->second;
      for (const auto &q : dmap) {
        const string_snap_t& ss = q.first;
        const MMDSCacheRejoin::dn_strong& d = q.second;
        CDentry *dn;
        if (!refragged)
	  dn = dir->lookup(ss.name, ss.snapid);
        else {
	  frag_t fg = diri->pick_dirfrag(ss.name);
	  dir = diri->get_dirfrag(fg);
	  ceph_assert(dir);
	  dn = dir->lookup(ss.name, ss.snapid);
        }
        if (!dn) {
	  if (d.is_remote()) {
	    dn = dir->add_remote_dentry(ss.name, d.remote_ino, d.remote_d_type, mempool::mds_co::string(d.alternate_name), d.first, ss.snapid);
	  } else if (d.is_null()) {
	    dn = dir->add_null_dentry(ss.name, d.first, ss.snapid);
	  } else {
	    CInode *in = get_inode(d.ino, ss.snapid);
	    if (!in) in = rejoin_invent_inode(d.ino, ss.snapid);
	    dn = dir->add_primary_dentry(ss.name, in, mempool::mds_co::string(d.alternate_name), d.first, ss.snapid);
	  }
	  dout(10) << " invented " << *dn << dendl;
        }
        CDentry::linkage_t *dnl = dn->get_linkage();

        // dn auth_pin?
        const auto pinned_it = strong->authpinned_dentries.find(dirfrag);
        if (pinned_it != strong->authpinned_dentries.end()) {
          const auto peer_reqid_it = pinned_it->second.find(ss);
          if (peer_reqid_it != pinned_it->second.end()) {
            for (const auto &r : peer_reqid_it->second) {
	      dout(10) << " dn authpin by " << r << " on " << *dn << dendl;

	      // get/create peer mdrequest
	      MDRequestRef mdr;
	      if (have_request(r.reqid))
	        mdr = request_get(r.reqid);
	      else
	        mdr = request_start_peer(r.reqid, r.attempt, strong);
	      mdr->auth_pin(dn);
            }
          }
	}

        // dn xlock?
        const auto xlocked_it = strong->xlocked_dentries.find(dirfrag);
        if (xlocked_it != strong->xlocked_dentries.end()) {
          const auto ss_req_it = xlocked_it->second.find(ss);
          if (ss_req_it != xlocked_it->second.end()) {
	    const MMDSCacheRejoin::peer_reqid& r = ss_req_it->second;
	    dout(10) << " dn xlock by " << r << " on " << *dn << dendl;
	    MDRequestRef mdr = request_get(r.reqid);  // should have this from auth_pin above.
	    ceph_assert(mdr->is_auth_pinned(dn));
	    if (!mdr->is_xlocked(&dn->versionlock)) {
	      ceph_assert(dn->versionlock.can_xlock_local());
	      dn->versionlock.get_xlock(mdr, mdr->get_client());
	      mdr->emplace_lock(&dn->versionlock, MutationImpl::LockOp::XLOCK);
	    }
	    if (dn->lock.is_stable())
	      dn->auth_pin(&dn->lock);
	    dn->lock.set_state(LOCK_XLOCK);
	    dn->lock.get_xlock(mdr, mdr->get_client());
	    mdr->emplace_lock(&dn->lock, MutationImpl::LockOp::XLOCK);
          }
        }

        dn->add_replica(from, d.nonce);
        dout(10) << " have " << *dn << dendl;

        if (dnl->is_primary()) {
	  if (d.is_primary()) {
	    if (vinodeno_t(d.ino, ss.snapid) != dnl->get_inode()->vino()) {
	      // the survivor missed MDentryUnlink+MDentryLink messages ?
	      ceph_assert(strong->strong_inodes.count(dnl->get_inode()->vino()) == 0);
	      CInode *in = get_inode(d.ino, ss.snapid);
	      ceph_assert(in);
	      ceph_assert(in->get_parent_dn());
	      rejoin_unlinked_inodes[from].insert(in);
	      dout(7) << " sender has primary dentry but wrong inode" << dendl;
	    }
	  } else {
	    // the survivor missed MDentryLink message ?
	    ceph_assert(strong->strong_inodes.count(dnl->get_inode()->vino()) == 0);
	    dout(7) << " sender doesn't have primay dentry" << dendl;
	  }
        } else {
	  if (d.is_primary()) {
	    // the survivor missed MDentryUnlink message ?
	    CInode *in = get_inode(d.ino, ss.snapid);
	    ceph_assert(in);
	    ceph_assert(in->get_parent_dn());
	    rejoin_unlinked_inodes[from].insert(in);
	    dout(7) << " sender has primary dentry but we don't" << dendl;
	  }
        }
      }
    }
  }

  for (const auto &p : strong->strong_inodes) {
    CInode *in = get_inode(p.first);
    ceph_assert(in);
    in->add_replica(from, p.second.nonce);
    dout(10) << " have " << *in << dendl;

    const MMDSCacheRejoin::inode_strong& is = p.second;

    // caps_wanted
    if (is.caps_wanted) {
      in->set_mds_caps_wanted(from, is.caps_wanted);
      dout(15) << " inode caps_wanted " << ccap_string(is.caps_wanted)
	       << " on " << *in << dendl;
    }

    // scatterlocks?
    //  infer state from replica state:
    //   * go to MIX if they might have wrlocks
    //   * go to LOCK if they are LOCK (just bc identify_files_to_recover might start twiddling filelock)
    in->filelock.infer_state_from_strong_rejoin(is.filelock, !in->is_dir());  // maybe also go to LOCK
    in->nestlock.infer_state_from_strong_rejoin(is.nestlock, false);
    in->dirfragtreelock.infer_state_from_strong_rejoin(is.dftlock, false);

    // auth pin?
    const auto authpinned_inodes_it = strong->authpinned_inodes.find(in->vino());
    if (authpinned_inodes_it != strong->authpinned_inodes.end()) {
      for (const auto& r : authpinned_inodes_it->second) {
	dout(10) << " inode authpin by " << r << " on " << *in << dendl;

	// get/create peer mdrequest
	MDRequestRef mdr;
	if (have_request(r.reqid))
	  mdr = request_get(r.reqid);
	else
	  mdr = request_start_peer(r.reqid, r.attempt, strong);
	if (strong->frozen_authpin_inodes.count(in->vino())) {
	  ceph_assert(!in->get_num_auth_pins());
	  mdr->freeze_auth_pin(in);
	} else {
	  ceph_assert(!in->is_frozen_auth_pin());
	}
	mdr->auth_pin(in);
      }
    }
    // xlock(s)?
    const auto xlocked_inodes_it = strong->xlocked_inodes.find(in->vino());
    if (xlocked_inodes_it != strong->xlocked_inodes.end()) {
      for (const auto &q : xlocked_inodes_it->second) {
	SimpleLock *lock = in->get_lock(q.first);
	dout(10) << " inode xlock by " << q.second << " on " << *lock << " on " << *in << dendl;
	MDRequestRef mdr = request_get(q.second.reqid);  // should have this from auth_pin above.
	ceph_assert(mdr->is_auth_pinned(in));
	if (!mdr->is_xlocked(&in->versionlock)) {
	  ceph_assert(in->versionlock.can_xlock_local());
	  in->versionlock.get_xlock(mdr, mdr->get_client());
	  mdr->emplace_lock(&in->versionlock, MutationImpl::LockOp::XLOCK);
	}
	if (lock->is_stable())
	  in->auth_pin(lock);
	lock->set_state(LOCK_XLOCK);
	if (lock == &in->filelock)
	  in->loner_cap = -1;
	lock->get_xlock(mdr, mdr->get_client());
	mdr->emplace_lock(lock, MutationImpl::LockOp::XLOCK);
      }
    }
  }
  // wrlock(s)?
  for (const auto &p : strong->wrlocked_inodes) {
    CInode *in = get_inode(p.first);
    for (const auto &q : p.second) {
      SimpleLock *lock = in->get_lock(q.first);
      for (const auto &r : q.second) {
	dout(10) << " inode wrlock by " << r << " on " << *lock << " on " << *in << dendl;
	MDRequestRef mdr = request_get(r.reqid);  // should have this from auth_pin above.
	if (in->is_auth())
	  ceph_assert(mdr->is_auth_pinned(in));
	lock->set_state(LOCK_MIX);
	if (lock == &in->filelock)
	  in->loner_cap = -1;
	lock->get_wrlock(true);
	mdr->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);
      }
    }
  }

  // done?
  ceph_assert(rejoin_gather.count(from));
  rejoin_gather.erase(from);
  if (rejoin_gather.empty() && rejoin_ack_gather.count(mds->get_nodeid())) {
    rejoin_gather_finish();
  } else {
    dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
  }
}

void MDCache::handle_cache_rejoin_ack(const cref_t<MMDSCacheRejoin> &ack)
{
  dout(7) << "handle_cache_rejoin_ack from " << ack->get_source() << dendl;
  mds_rank_t from = mds_rank_t(ack->get_source().num());

  ceph_assert(mds->get_state() >= MDSMap::STATE_REJOIN);
  bool survivor = !mds->is_rejoin();

  // for sending cache expire message
  set<CInode*> isolated_inodes;
  set<CInode*> refragged_inodes;
  list<pair<CInode*,int> > updated_realms;

  // dirs
  for (const auto &p : ack->strong_dirfrags) {
    // we may have had incorrect dir fragmentation; refragment based
    // on what they auth tells us.
    CDir *dir = get_dirfrag(p.first);
    if (!dir) {
      dir = get_force_dirfrag(p.first, false);
      if (dir)
	refragged_inodes.insert(dir->get_inode());
    }
    if (!dir) {
      CInode *diri = get_inode(p.first.ino);
      if (!diri) {
	// barebones inode; the full inode loop below will clean up.
	diri = new CInode(this, false);
	auto _inode = diri->_get_inode();
	_inode->ino = p.first.ino;
	_inode->mode = S_IFDIR;
	_inode->dir_layout.dl_dir_hash = g_conf()->mds_default_dir_hash;

	add_inode(diri);
	if (MDS_INO_MDSDIR(from) == p.first.ino) {
	  diri->inode_auth = mds_authority_t(from, CDIR_AUTH_UNKNOWN);
	  dout(10) << " add inode " << *diri << dendl;
	} else {
	  diri->inode_auth = CDIR_AUTH_DEFAULT;
	  isolated_inodes.insert(diri);
	  dout(10) << " unconnected dirfrag " << p.first << dendl;
	}
      }
      // barebones dirfrag; the full dirfrag loop below will clean up.
      dir = diri->add_dirfrag(new CDir(diri, p.first.frag, this, false));
      if (MDS_INO_MDSDIR(from) == p.first.ino ||
	  (dir->authority() != CDIR_AUTH_UNDEF &&
	   dir->authority().first != from))
	adjust_subtree_auth(dir, from);
      dout(10) << " add dirfrag " << *dir << dendl;
    }

    dir->set_replica_nonce(p.second.nonce);
    dir->state_clear(CDir::STATE_REJOINING);
    dout(10) << " got " << *dir << dendl;

    // dentries
    auto it = ack->strong_dentries.find(p.first);
    if (it != ack->strong_dentries.end()) {
      for (const auto &q : it->second) {
        CDentry *dn = dir->lookup(q.first.name, q.first.snapid);
        if(!dn)
	  dn = dir->add_null_dentry(q.first.name, q.second.first, q.first.snapid);

        CDentry::linkage_t *dnl = dn->get_linkage();

        ceph_assert(dn->last == q.first.snapid);
        if (dn->first != q.second.first) {
	  dout(10) << " adjust dn.first " << dn->first << " -> " << q.second.first << " on " << *dn << dendl;
	  dn->first = q.second.first;
        }

        // may have bad linkage if we missed dentry link/unlink messages
        if (dnl->is_primary()) {
	  CInode *in = dnl->get_inode();
	  if (!q.second.is_primary() ||
	      vinodeno_t(q.second.ino, q.first.snapid) != in->vino()) {
	    dout(10) << " had bad linkage for " << *dn << ", unlinking " << *in << dendl;
	    dir->unlink_inode(dn);
	  }
        } else if (dnl->is_remote()) {
	  if (!q.second.is_remote() ||
	      q.second.remote_ino != dnl->get_remote_ino() ||
	      q.second.remote_d_type != dnl->get_remote_d_type()) {
	    dout(10) << " had bad linkage for " << *dn <<  dendl;
	    dir->unlink_inode(dn);
	  }
        } else {
	  if (!q.second.is_null())
	    dout(10) << " had bad linkage for " << *dn <<  dendl;
        }

	// hmm, did we have the proper linkage here?
	if (dnl->is_null() && !q.second.is_null()) {
	  if (q.second.is_remote()) {
	    dn->dir->link_remote_inode(dn, q.second.remote_ino, q.second.remote_d_type);
	  } else {
	    CInode *in = get_inode(q.second.ino, q.first.snapid);
	    if (!in) {
	      // barebones inode; assume it's dir, the full inode loop below will clean up.
	      in = new CInode(this, false, q.second.first, q.first.snapid);
	      auto _inode = in->_get_inode();
	      _inode->ino = q.second.ino;
	      _inode->mode = S_IFDIR;
	      _inode->dir_layout.dl_dir_hash = g_conf()->mds_default_dir_hash;
	      add_inode(in);
	      dout(10) << " add inode " << *in << dendl;
	    } else if (in->get_parent_dn()) {
	      dout(10) << " had bad linkage for " << *(in->get_parent_dn())
		       << ", unlinking " << *in << dendl;
	      in->get_parent_dir()->unlink_inode(in->get_parent_dn());
	    }
	    dn->dir->link_primary_inode(dn, in);
	    isolated_inodes.erase(in);
	  }
	}

        dn->set_replica_nonce(q.second.nonce);
        dn->lock.set_state_rejoin(q.second.lock, rejoin_waiters, survivor);
        dn->state_clear(CDentry::STATE_REJOINING);
        dout(10) << " got " << *dn << dendl;
      }
    }
  }

  for (const auto& in : refragged_inodes) {
    auto&& ls = in->get_nested_dirfrags();
    for (const auto& dir : ls) {
      if (dir->is_auth() || ack->strong_dirfrags.count(dir->dirfrag()))
	continue;
      ceph_assert(dir->get_num_any() == 0);
      in->close_dirfrag(dir->get_frag());
    }
  }

  // full dirfrags
  for (const auto &p : ack->dirfrag_bases) {
    CDir *dir = get_dirfrag(p.first);
    ceph_assert(dir);
    auto q = p.second.cbegin();
    dir->_decode_base(q);
    dout(10) << " got dir replica " << *dir << dendl;
  }

  // full inodes
  auto p = ack->inode_base.cbegin();
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    bufferlist basebl;
    decode(ino, p);
    decode(last, p);
    decode(basebl, p);
    CInode *in = get_inode(ino, last);
    ceph_assert(in);
    auto q = basebl.cbegin();
    snapid_t sseq = 0;
    if (in->snaprealm)
      sseq = in->snaprealm->srnode.seq;
    in->_decode_base(q);
    if (in->snaprealm && in->snaprealm->srnode.seq != sseq) {
      int snap_op = sseq > 0 ? CEPH_SNAP_OP_UPDATE : CEPH_SNAP_OP_SPLIT;
      updated_realms.push_back(pair<CInode*,int>(in, snap_op));
    }
    dout(10) << " got inode base " << *in << dendl;
  }

  // inodes
  p = ack->inode_locks.cbegin();
  //dout(10) << "inode_locks len " << ack->inode_locks.length() << " is " << ack->inode_locks << dendl;
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    __u32 nonce;
    bufferlist lockbl;
    decode(ino, p);
    decode(last, p);
    decode(nonce, p);
    decode(lockbl, p);
    
    CInode *in = get_inode(ino, last);
    ceph_assert(in);
    in->set_replica_nonce(nonce);
    auto q = lockbl.cbegin();
    in->_decode_locks_rejoin(q, rejoin_waiters, rejoin_eval_locks, survivor);
    in->state_clear(CInode::STATE_REJOINING);
    dout(10) << " got inode locks " << *in << dendl;
  }

  // FIXME: This can happen if entire subtree, together with the inode subtree root
  // belongs to, were trimmed between sending cache rejoin and receiving rejoin ack.
  ceph_assert(isolated_inodes.empty());

  map<inodeno_t,map<client_t,Capability::Import> > peer_imported;
  auto bp = ack->imported_caps.cbegin();
  decode(peer_imported, bp);

  for (map<inodeno_t,map<client_t,Capability::Import> >::iterator p = peer_imported.begin();
       p != peer_imported.end();
       ++p) {
    auto& ex = cap_exports.at(p->first);
    ceph_assert(ex.first == from);
    for (map<client_t,Capability::Import>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      auto r = ex.second.find(q->first);
      ceph_assert(r != ex.second.end());

      dout(10) << " exporting caps for client." << q->first << " ino " << p->first << dendl;
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(q->first.v));
      if (!session) {
	dout(10) << " no session for client." << p->first << dendl;
	ex.second.erase(r);
	continue;
      }

      // mark client caps stale.
      auto m = make_message<MClientCaps>(CEPH_CAP_OP_EXPORT, p->first, 0,
				       r->second.capinfo.cap_id, 0,
                                       mds->get_osd_epoch_barrier());
      m->set_cap_peer(q->second.cap_id, q->second.issue_seq, q->second.mseq,
		      (q->second.cap_id > 0 ? from : -1), 0);
      mds->send_message_client_counted(m, session);

      ex.second.erase(r);
    }
    ceph_assert(ex.second.empty());
  }

  for (auto p : updated_realms) {
    CInode *in = p.first;
    bool notify_clients;
    if (mds->is_rejoin()) {
      if (!rejoin_pending_snaprealms.count(in)) {
	in->get(CInode::PIN_OPENINGSNAPPARENTS);
	rejoin_pending_snaprealms.insert(in);
      }
      notify_clients = false;
    } else {
      // notify clients if I'm survivor
      notify_clients = true;
    }
    do_realm_invalidate_and_update_notify(in, p.second, notify_clients);
  }

  // done?
  ceph_assert(rejoin_ack_gather.count(from));
  rejoin_ack_gather.erase(from);
  if (!survivor) {
    if (rejoin_gather.empty()) {
      // eval unstable scatter locks after all wrlocks are rejoined.
      while (!rejoin_eval_locks.empty()) {
	SimpleLock *lock = rejoin_eval_locks.front();
	rejoin_eval_locks.pop_front();
	if (!lock->is_stable())
	  mds->locker->eval_gather(lock);
      }
    }

    if (rejoin_gather.empty() &&     // make sure we've gotten our FULL inodes, too.
	rejoin_ack_gather.empty()) {
      // finally, kickstart past snap parent opens
      open_snaprealms();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")"
	      << ", rejoin_ack from (" << rejoin_ack_gather << ")" << dendl;
    }
  } else {
    // survivor.
    mds->queue_waiters(rejoin_waiters);
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
      const auto&& dfls = in->get_dirfrags();
      for (const auto& dir : dfls) {
	dir->clear_replica_map();

	for (auto &p : dir->items) {
	  CDentry *dn = p.second;
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

  ceph_assert(rejoin_undef_inodes.empty());
}

void MDCache::rejoin_gather_finish() 
{
  dout(10) << "rejoin_gather_finish" << dendl;
  ceph_assert(mds->is_rejoin());
  ceph_assert(rejoin_ack_gather.count(mds->get_nodeid()));

  if (open_undef_inodes_dirfrags())
    return;

  if (process_imported_caps())
    return;

  choose_lock_states_and_reconnect_caps();

  identify_files_to_recover();
  rejoin_send_acks();
  
  // signal completion of fetches, rejoin_gather_finish, etc.
  rejoin_ack_gather.erase(mds->get_nodeid());

  // did we already get our acks too?
  if (rejoin_ack_gather.empty()) {
    // finally, open snaprealms
    open_snaprealms();
  }
}

class C_MDC_RejoinOpenInoFinish: public MDCacheContext {
  inodeno_t ino;
public:
  C_MDC_RejoinOpenInoFinish(MDCache *c, inodeno_t i) : MDCacheContext(c), ino(i) {}
  void finish(int r) override {
    mdcache->rejoin_open_ino_finish(ino, r);
  }
};

void MDCache::rejoin_open_ino_finish(inodeno_t ino, int ret)
{
  dout(10) << "open_caps_inode_finish ino " << ino << " ret " << ret << dendl;

  if (ret < 0) {
    cap_imports_missing.insert(ino);
  } else if (ret == mds->get_nodeid()) {
    ceph_assert(get_inode(ino));
  } else {
    auto p = cap_imports.find(ino);
    ceph_assert(p != cap_imports.end());
    for (auto q = p->second.begin(); q != p->second.end(); ++q) {
      ceph_assert(q->second.count(MDS_RANK_NONE));
      ceph_assert(q->second.size() == 1);
      rejoin_export_caps(p->first, q->first, q->second[MDS_RANK_NONE], ret);
    }
    cap_imports.erase(p);
  }

  ceph_assert(cap_imports_num_opening > 0);
  cap_imports_num_opening--;

  if (cap_imports_num_opening == 0) {
    if (rejoin_gather.empty() && rejoin_ack_gather.count(mds->get_nodeid()))
      rejoin_gather_finish();
    else if (rejoin_gather.count(mds->get_nodeid()))
      process_imported_caps();
  }
}

class C_MDC_RejoinSessionsOpened : public MDCacheLogContext {
public:
  map<client_t,pair<Session*,uint64_t> > session_map;
  C_MDC_RejoinSessionsOpened(MDCache *c) : MDCacheLogContext(c) {}
  void finish(int r) override {
    ceph_assert(r == 0);
    mdcache->rejoin_open_sessions_finish(session_map);
  }
};

void MDCache::rejoin_open_sessions_finish(map<client_t,pair<Session*,uint64_t> >& session_map)
{
  dout(10) << "rejoin_open_sessions_finish" << dendl;
  mds->server->finish_force_open_sessions(session_map);
  rejoin_session_map.swap(session_map);
  if (rejoin_gather.empty() && rejoin_ack_gather.count(mds->get_nodeid()))
    rejoin_gather_finish();
}

void MDCache::rejoin_prefetch_ino_finish(inodeno_t ino, int ret)
{
  auto p = cap_imports.find(ino);
  if (p != cap_imports.end()) {
    dout(10) << __func__ << " ino " << ino << " ret " << ret << dendl;
    if (ret < 0) {
      cap_imports_missing.insert(ino);
    } else if (ret != mds->get_nodeid()) {
      for (auto q = p->second.begin(); q != p->second.end(); ++q) {
	ceph_assert(q->second.count(MDS_RANK_NONE));
	ceph_assert(q->second.size() == 1);
	rejoin_export_caps(p->first, q->first, q->second[MDS_RANK_NONE], ret);
      }
      cap_imports.erase(p);
    }
  }
}

bool MDCache::process_imported_caps()
{
  dout(10) << "process_imported_caps" << dendl;

  if (!open_file_table.is_prefetched() &&
      open_file_table.prefetch_inodes()) {
    open_file_table.wait_for_prefetch(
	new MDSInternalContextWrapper(mds,
	  new LambdaContext([this](int r) {
	    ceph_assert(rejoin_gather.count(mds->get_nodeid()));
	    process_imported_caps();
	    })
	  )
	);
    return true;
  }

  open_ino_batch_start();

  for (auto& p : cap_imports) {
    CInode *in = get_inode(p.first);
    if (in) {
      ceph_assert(in->is_auth());
      cap_imports_missing.erase(p.first);
      continue;
    }
    if (cap_imports_missing.count(p.first) > 0)
      continue;

    uint64_t parent_ino = 0;
    std::string_view d_name;
    for (auto& q : p.second) {
      for (auto& r : q.second) {
	auto &icr = r.second;
	if (icr.capinfo.pathbase &&
	    icr.path.length() > 0 &&
	    icr.path.find('/') == string::npos) {
	  parent_ino = icr.capinfo.pathbase;
	  d_name = icr.path;
	  break;
	}
      }
      if (parent_ino)
	break;
    }

    dout(10) << "  opening missing ino " << p.first << dendl;
    cap_imports_num_opening++;
    auto fin = new C_MDC_RejoinOpenInoFinish(this, p.first);
    if (parent_ino) {
      vector<inode_backpointer_t> ancestors;
      ancestors.push_back(inode_backpointer_t(parent_ino, string{d_name}, 0));
      open_ino(p.first, (int64_t)-1, fin, false, false, &ancestors);
    } else {
      open_ino(p.first, (int64_t)-1, fin, false);
    }
    if (!(cap_imports_num_opening % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  open_ino_batch_submit();

  if (cap_imports_num_opening > 0)
    return true;

  // called by rejoin_gather_finish() ?
  if (rejoin_gather.count(mds->get_nodeid()) == 0) {
    if (!rejoin_client_map.empty() &&
	rejoin_session_map.empty()) {
      C_MDC_RejoinSessionsOpened *finish = new C_MDC_RejoinSessionsOpened(this);
      version_t pv = mds->server->prepare_force_open_sessions(rejoin_client_map,
							      rejoin_client_metadata_map,
							      finish->session_map);
      ESessions *le = new ESessions(pv, std::move(rejoin_client_map),
				    std::move(rejoin_client_metadata_map));
      mds->mdlog->submit_entry(le, finish);
      mds->mdlog->flush();
      rejoin_client_map.clear();
      rejoin_client_metadata_map.clear();
      return true;
    }

    // process caps that were exported by peer rename
    for (map<inodeno_t,pair<mds_rank_t,map<client_t,Capability::Export> > >::iterator p = rejoin_peer_exports.begin();
	 p != rejoin_peer_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      ceph_assert(in);
      for (map<client_t,Capability::Export>::iterator q = p->second.second.begin();
	   q != p->second.second.end();
	   ++q) {
	auto r = rejoin_session_map.find(q->first);
	if (r == rejoin_session_map.end())
	  continue;

	Session *session = r->second.first;
	Capability *cap = in->get_client_cap(q->first);
	if (!cap) {
	  cap = in->add_client_cap(q->first, session);
	  // add empty item to reconnected_caps
	  (void)reconnected_caps[p->first][q->first];
	}
	cap->merge(q->second, true);

	Capability::Import& im = rejoin_imported_caps[p->second.first][p->first][q->first];
	ceph_assert(cap->get_last_seq() == im.issue_seq);
	ceph_assert(cap->get_mseq() == im.mseq);
	cap->set_cap_id(im.cap_id);
	// send cap import because we assigned a new cap ID
	do_cap_import(session, in, cap, q->second.cap_id, q->second.seq, q->second.mseq - 1,
		      p->second.first, CEPH_CAP_FLAG_AUTH);
      }
    }
    rejoin_peer_exports.clear();
    rejoin_imported_caps.clear();

    // process cap imports
    //  ino -> client -> frommds -> capex
    for (auto p = cap_imports.begin(); p != cap_imports.end(); ) {
      CInode *in = get_inode(p->first);
      if (!in) {
	dout(10) << " still missing ino " << p->first
	         << ", will try again after replayed client requests" << dendl;
	++p;
	continue;
      }
      ceph_assert(in->is_auth());
      for (auto q = p->second.begin(); q != p->second.end(); ++q) {
	Session *session;
	{
	  auto r = rejoin_session_map.find(q->first);
	  session = (r != rejoin_session_map.end() ? r->second.first : nullptr);
	}

	for (auto r = q->second.begin(); r != q->second.end(); ++r) {
	  if (!session) {
	    if (r->first >= 0)
	      (void)rejoin_imported_caps[r->first][p->first][q->first]; // all are zero
	    continue;
	  }

	  Capability *cap = in->reconnect_cap(q->first, r->second, session);
	  add_reconnected_cap(q->first, in->ino(), r->second);
	  if (r->first >= 0) {
	    if (cap->get_last_seq() == 0) // don't increase mseq if cap already exists
	      cap->inc_mseq();
	    do_cap_import(session, in, cap, r->second.capinfo.cap_id, 0, 0, r->first, 0);

	    Capability::Import& im = rejoin_imported_caps[r->first][p->first][q->first];
	    im.cap_id = cap->get_cap_id();
	    im.issue_seq = cap->get_last_seq();
	    im.mseq = cap->get_mseq();
	  }
	}
      }
      cap_imports.erase(p++);  // remove and move on
    }
  } else {
    trim_non_auth();

    ceph_assert(rejoin_gather.count(mds->get_nodeid()));
    rejoin_gather.erase(mds->get_nodeid());
    ceph_assert(!rejoin_ack_gather.count(mds->get_nodeid()));
    maybe_send_pending_rejoins();
  }
  return false;
}

void MDCache::rebuild_need_snapflush(CInode *head_in, SnapRealm *realm,
				     client_t client, snapid_t snap_follows)
{
  dout(10) << "rebuild_need_snapflush " << snap_follows << " on " << *head_in << dendl;

  if (!realm->has_snaps_in_range(snap_follows + 1, head_in->first - 1))
    return;

  const set<snapid_t>& snaps = realm->get_snaps();
  snapid_t follows = snap_follows;

  while (true) {
    CInode *in = pick_inode_snap(head_in, follows);
    if (in == head_in)
      break;

    bool need_snapflush = false;
    for (auto p = snaps.lower_bound(std::max<snapid_t>(in->first, (follows + 1)));
	 p != snaps.end() && *p <= in->last;
	 ++p) {
      head_in->add_need_snapflush(in, *p, client);
      need_snapflush = true;
    }
    follows = in->last;
    if (!need_snapflush)
      continue;

    dout(10) << " need snapflush from client." << client << " on " << *in << dendl;

    if (in->client_snap_caps.empty()) {
      for (int i = 0; i < num_cinode_locks; i++) {
	int lockid = cinode_lock_info[i].lock;
	SimpleLock *lock = in->get_lock(lockid);
	ceph_assert(lock);
	in->auth_pin(lock);
	lock->set_state(LOCK_SNAP_SYNC);
	lock->get_wrlock(true);
      }
    }
    in->client_snap_caps.insert(client);
    mds->locker->mark_need_snapflush_inode(in);
  }
}

/*
 * choose lock states based on reconnected caps
 */
void MDCache::choose_lock_states_and_reconnect_caps()
{
  dout(10) << "choose_lock_states_and_reconnect_caps" << dendl;

  int count = 0;
  for (auto p : inode_map) {
    CInode *in = p.second;
    if (in->last != CEPH_NOSNAP)
      continue;
 
    if (in->is_auth() && !in->is_base() && in->get_inode()->is_dirty_rstat())
      in->mark_dirty_rstat();

    int dirty_caps = 0;
    auto q = reconnected_caps.find(in->ino());
    if (q != reconnected_caps.end()) {
      for (const auto &it : q->second)
	dirty_caps |= it.second.dirty_caps;
    }
    in->choose_lock_states(dirty_caps);
    dout(15) << " chose lock states on " << *in << dendl;

    if (in->snaprealm && !rejoin_pending_snaprealms.count(in)) {
      in->get(CInode::PIN_OPENINGSNAPPARENTS);
      rejoin_pending_snaprealms.insert(in);
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
}

void MDCache::prepare_realm_split(SnapRealm *realm, client_t client, inodeno_t ino,
				  map<client_t,ref_t<MClientSnap>>& splits)
{
  ref_t<MClientSnap> snap;
  auto it = splits.find(client);
  if (it != splits.end()) {
    snap = it->second;
    snap->head.op = CEPH_SNAP_OP_SPLIT;
  } else {
    snap = make_message<MClientSnap>(CEPH_SNAP_OP_SPLIT);
    splits.emplace(std::piecewise_construct, std::forward_as_tuple(client), std::forward_as_tuple(snap));
    snap->head.split = realm->inode->ino();
    snap->bl = mds->server->get_snap_trace(client, realm);

    for (const auto& child : realm->open_children)
      snap->split_realms.push_back(child->inode->ino());
  }
  snap->split_inos.push_back(ino);	
}

void MDCache::prepare_realm_merge(SnapRealm *realm, SnapRealm *parent_realm,
				  map<client_t,ref_t<MClientSnap>>& splits)
{
  ceph_assert(parent_realm);

  vector<inodeno_t> split_inos;
  vector<inodeno_t> split_realms;

  for (auto p = realm->inodes_with_caps.begin(); !p.end(); ++p)
    split_inos.push_back((*p)->ino());
  for (set<SnapRealm*>::iterator p = realm->open_children.begin();
       p != realm->open_children.end();
       ++p)
    split_realms.push_back((*p)->inode->ino());

  for (const auto& p : realm->client_caps) {
    ceph_assert(!p.second->empty());
    auto em = splits.emplace(std::piecewise_construct, std::forward_as_tuple(p.first), std::forward_as_tuple());
    if (em.second) {
      auto update = make_message<MClientSnap>(CEPH_SNAP_OP_SPLIT);
      update->head.split = parent_realm->inode->ino();
      update->split_inos = split_inos;
      update->split_realms = split_realms;
      update->bl = mds->server->get_snap_trace(p.first, parent_realm);
      em.first->second = std::move(update);
    }
  }
}

void MDCache::send_snaps(map<client_t,ref_t<MClientSnap>>& splits)
{
  dout(10) << "send_snaps" << dendl;
  
  for (auto &p : splits) {
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p.first.v));
    if (session) {
      dout(10) << " client." << p.first
	       << " split " << p.second->head.split
	       << " inos " << p.second->split_inos
	       << dendl;
      mds->send_message_client_counted(p.second, session);
    } else {
      dout(10) << " no session for client." << p.first << dendl;
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
  
  for (map<uint64_t,LogSegment*>::iterator p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       ++p) {
    LogSegment *ls = p->second;

    elist<CInode*>::iterator q = ls->open_files.begin(member_offset(CInode, item_open_file));
    while (!q.end()) {
      CInode *in = *q;
      ++q;
      if (in->last == CEPH_NOSNAP) {
	dout(10) << " unlisting unwanted/capless inode " << *in << dendl;
	in->item_open_file.remove_myself();
      } else {
	if (in->client_snap_caps.empty()) {
	  dout(10) << " unlisting flushed snap inode " << *in << dendl;
	  in->item_open_file.remove_myself();
	}
      }
    }
  }
}

void MDCache::dump_openfiles(Formatter *f)
{
  f->open_array_section("openfiles");
  for (auto p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       ++p) {
    LogSegment *ls = p->second;
    
    auto q = ls->open_files.begin(member_offset(CInode, item_open_file));
    while (!q.end()) {
      CInode *in = *q;
      ++q;
      if ((in->last == CEPH_NOSNAP && !in->is_any_caps_wanted())
          || (in->last != CEPH_NOSNAP && in->client_snap_caps.empty())) 
        continue;
      f->open_object_section("file");
      in->dump(f, CInode::DUMP_PATH | CInode::DUMP_INODE_STORE_BASE | CInode::DUMP_CAPS);
      f->close_section();
    }
  }
  f->close_section();
}

Capability* MDCache::rejoin_import_cap(CInode *in, client_t client, const cap_reconnect_t& icr, mds_rank_t frommds)
{
  dout(10) << "rejoin_import_cap for client." << client << " from mds." << frommds
	   << " on " << *in << dendl;
  Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(client.v));
  if (!session) {
    dout(10) << " no session for client." << client << dendl;
    return NULL;
  }

  Capability *cap = in->reconnect_cap(client, icr, session);

  if (frommds >= 0) {
    if (cap->get_last_seq() == 0) // don't increase mseq if cap already exists
      cap->inc_mseq();
    do_cap_import(session, in, cap, icr.capinfo.cap_id, 0, 0, frommds, 0);
  }

  return cap;
}

void MDCache::export_remaining_imported_caps()
{
  dout(10) << "export_remaining_imported_caps" << dendl;

  CachedStackStringStream css;

  int count = 0;
  for (auto p = cap_imports.begin(); p != cap_imports.end(); ++p) {
    *css << " ino " << p->first << "\n";
    for (auto q = p->second.begin(); q != p->second.end(); ++q) {
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(q->first.v));
      if (session) {
	// mark client caps stale.
	auto stale = make_message<MClientCaps>(CEPH_CAP_OP_EXPORT, p->first,
					       0, 0, 0,
					       mds->get_osd_epoch_barrier());
	stale->set_cap_peer(0, 0, 0, -1, 0);
	mds->send_message_client_counted(stale, q->first);
      }
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  for (map<inodeno_t, MDSContext::vec >::iterator p = cap_reconnect_waiters.begin();
       p != cap_reconnect_waiters.end();
       ++p)
    mds->queue_waiters(p->second);

  cap_imports.clear();
  cap_reconnect_waiters.clear();

  if (css->strv().length()) {
    mds->clog->warn() << "failed to reconnect caps for missing inodes:"
                      << css->strv();
  }
}

Capability* MDCache::try_reconnect_cap(CInode *in, Session *session)
{
  client_t client = session->info.get_client();
  Capability *cap = nullptr;
  const cap_reconnect_t *rc = get_replay_cap_reconnect(in->ino(), client);
  if (rc) {
    cap = in->reconnect_cap(client, *rc, session);
    dout(10) << "try_reconnect_cap client." << client
	     << " reconnect wanted " << ccap_string(rc->capinfo.wanted)
	     << " issue " << ccap_string(rc->capinfo.issued)
	     << " on " << *in << dendl;
    remove_replay_cap_reconnect(in->ino(), client);

    if (in->is_replicated()) {
      mds->locker->try_eval(in, CEPH_CAP_LOCKS);
    } else {
      int dirty_caps = 0;
      auto p = reconnected_caps.find(in->ino());
      if (p != reconnected_caps.end()) {
	auto q = p->second.find(client);
	if (q != p->second.end())
	  dirty_caps = q->second.dirty_caps;
      }
      in->choose_lock_states(dirty_caps);
      dout(15) << " chose lock states on " << *in << dendl;
    }

    map<inodeno_t, MDSContext::vec >::iterator it =
      cap_reconnect_waiters.find(in->ino());
    if (it != cap_reconnect_waiters.end()) {
      mds->queue_waiters(it->second);
      cap_reconnect_waiters.erase(it);
    }
  }
  return cap;
}



// -------
// cap imports and delayed snap parent opens

void MDCache::do_cap_import(Session *session, CInode *in, Capability *cap,
			    uint64_t p_cap_id, ceph_seq_t p_seq, ceph_seq_t p_mseq,
			    int peer, int p_flags)
{
  SnapRealm *realm = in->find_snaprealm();
  dout(10) << "do_cap_import " << session->info.inst.name << " mseq " << cap->get_mseq() << " on " << *in << dendl;
  if (cap->get_last_seq() == 0) // reconnected cap
    cap->inc_last_seq();
  cap->set_last_issue();
  cap->set_last_issue_stamp(ceph_clock_now());
  cap->clear_new();
  auto reap = make_message<MClientCaps>(CEPH_CAP_OP_IMPORT,
					in->ino(), realm->inode->ino(), cap->get_cap_id(),
					cap->get_last_seq(), cap->pending(), cap->wanted(),
					0, cap->get_mseq(), mds->get_osd_epoch_barrier());
  in->encode_cap_message(reap, cap);
  reap->snapbl = mds->server->get_snap_trace(session, realm);
  reap->set_cap_peer(p_cap_id, p_seq, p_mseq, peer, p_flags);
  mds->send_message_client_counted(reap, session);
}

void MDCache::do_delayed_cap_imports()
{
  dout(10) << "do_delayed_cap_imports" << dendl;

  ceph_assert(delayed_imported_caps.empty());
}

void MDCache::open_snaprealms()
{
  dout(10) << "open_snaprealms" << dendl;

  auto it = rejoin_pending_snaprealms.begin();
  while (it != rejoin_pending_snaprealms.end()) {
    CInode *in = *it;
    SnapRealm *realm = in->snaprealm;
    ceph_assert(realm);

    map<client_t,ref_t<MClientSnap>> splits;
    // finish off client snaprealm reconnects?
    auto q = reconnected_snaprealms.find(in->ino());
    if (q != reconnected_snaprealms.end()) {
      for (const auto& r : q->second)
	finish_snaprealm_reconnect(r.first, realm, r.second, splits);
      reconnected_snaprealms.erase(q);
    }

    for (auto p = realm->inodes_with_caps.begin(); !p.end(); ++p) {
      CInode *child = *p;
      auto q = reconnected_caps.find(child->ino());
      ceph_assert(q != reconnected_caps.end());
      for (auto r = q->second.begin(); r != q->second.end(); ++r) {
	Capability *cap = child->get_client_cap(r->first);
	if (!cap)
	  continue;
	if (r->second.snap_follows > 0) {
	  if (r->second.snap_follows < child->first - 1) {
	    rebuild_need_snapflush(child, realm, r->first, r->second.snap_follows);
	  } else if (r->second.snapflush) {
	    // When processing a cap flush message that is re-sent, it's possble
	    // that the sender has already released all WR caps. So we should
	    // force MDCache::cow_inode() to setup CInode::client_need_snapflush.
	    cap->mark_needsnapflush();
	  }
	}
	// make sure client's cap is in the correct snaprealm.
	if (r->second.realm_ino != in->ino()) {
	  prepare_realm_split(realm, r->first, child->ino(), splits);
	}
      }
    }

    rejoin_pending_snaprealms.erase(it++);
    in->put(CInode::PIN_OPENINGSNAPPARENTS);

    send_snaps(splits);
  }

  notify_global_snaprealm_update(CEPH_SNAP_OP_UPDATE);

  if (!reconnected_snaprealms.empty()) {
    dout(5) << "open_snaprealms has unconnected snaprealm:" << dendl;
    for (auto& p : reconnected_snaprealms) {
      CachedStackStringStream css;
      *css << " " << p.first << " {";
      bool first = true;
      for (auto& q : p.second) {
        if (!first)
          *css << ", ";
        *css << "client." << q.first << "/" << q.second;
      }
      *css << "}";
      dout(5) << css->strv() << dendl;
    }
  }
  ceph_assert(rejoin_waiters.empty());
  ceph_assert(rejoin_pending_snaprealms.empty());
  dout(10) << "open_snaprealms - all open" << dendl;
  do_delayed_cap_imports();

  ceph_assert(rejoin_done);
  rejoin_done.release()->complete(0);
  reconnected_caps.clear();
}

bool MDCache::open_undef_inodes_dirfrags()
{
  dout(10) << "open_undef_inodes_dirfrags "
	   << rejoin_undef_inodes.size() << " inodes "
	   << rejoin_undef_dirfrags.size() << " dirfrags" << dendl;

  // dirfrag -> (fetch_complete, keys_to_fetch)
  map<CDir*, pair<bool, std::vector<dentry_key_t> > > fetch_queue;
  for (auto& dir : rejoin_undef_dirfrags) {
    ceph_assert(dir->get_version() == 0);
    fetch_queue.emplace(std::piecewise_construct, std::make_tuple(dir), std::make_tuple());
  }

  if (g_conf().get_val<bool>("mds_dir_prefetch")) {
    for (auto& in : rejoin_undef_inodes) {
      ceph_assert(!in->is_base());
      ceph_assert(in->get_parent_dir());
      fetch_queue.emplace(std::piecewise_construct, std::make_tuple(in->get_parent_dir()), std::make_tuple());
    }
  } else {
    for (auto& in : rejoin_undef_inodes) {
      assert(!in->is_base());
      CDentry *dn = in->get_parent_dn();
      auto& p = fetch_queue[dn->get_dir()];

      if (dn->last != CEPH_NOSNAP) {
        p.first = true;
        p.second.clear();
      } else if (!p.first) {
        p.second.push_back(dn->key());
      }
    }
  }

  if (fetch_queue.empty())
    return false;

  MDSGatherBuilder gather(g_ceph_context,
      new MDSInternalContextWrapper(mds,
	new LambdaContext([this](int r) {
	    if (rejoin_gather.empty() && rejoin_ack_gather.count(mds->get_nodeid()))
	      rejoin_gather_finish();
	  })
	)
      );

  for (auto& p : fetch_queue) {
    CDir *dir = p.first;
    CInode *diri = dir->get_inode();
    if (diri->state_test(CInode::STATE_REJOINUNDEF))
      continue;
    if (dir->state_test(CDir::STATE_REJOINUNDEF))
      ceph_assert(diri->dirfragtree.is_leaf(dir->get_frag()));
    if (p.second.first || p.second.second.empty()) {
      dir->fetch(gather.new_sub());
    } else {
      dir->fetch_keys(p.second.second, gather.new_sub());
    }
  }
  ceph_assert(gather.has_subs());
  gather.activate();
  return true;
}

void MDCache::opened_undef_inode(CInode *in) {
  dout(10) << "opened_undef_inode " << *in << dendl;
  rejoin_undef_inodes.erase(in);
  if (in->is_dir()) {
    // FIXME: re-hash dentries if necessary
    ceph_assert(in->get_inode()->dir_layout.dl_dir_hash == g_conf()->mds_default_dir_hash);
    if (in->get_num_dirfrags() && !in->dirfragtree.is_leaf(frag_t())) {
      CDir *dir = in->get_dirfrag(frag_t());
      ceph_assert(dir);
      rejoin_undef_dirfrags.erase(dir);
      in->force_dirfrags();
      auto&& ls = in->get_dirfrags();
      for (const auto& dir : ls) {
	rejoin_undef_dirfrags.insert(dir);
      }
    }
  }
}

void MDCache::finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq,
					 map<client_t,ref_t<MClientSnap>>& updates)
{
  if (seq < realm->get_newest_seq()) {
    dout(10) << "finish_snaprealm_reconnect client." << client << " has old seq " << seq << " < " 
	     << realm->get_newest_seq() << " on " << *realm << dendl;
    auto snap = make_message<MClientSnap>(CEPH_SNAP_OP_UPDATE);
    snap->bl = mds->server->get_snap_trace(client, realm);
    updates.emplace(std::piecewise_construct, std::forward_as_tuple(client), std::forward_as_tuple(snap));
  } else {
    dout(10) << "finish_snaprealm_reconnect client." << client << " up to date"
	     << " on " << *realm << dendl;
  }
}



void MDCache::rejoin_send_acks()
{
  dout(7) << "rejoin_send_acks" << dendl;

  // replicate stray
  for (map<mds_rank_t, set<CInode*> >::iterator p = rejoin_unlinked_inodes.begin();
       p != rejoin_unlinked_inodes.end();
       ++p) {
    for (set<CInode*>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CInode *in = *q;
      dout(7) << " unlinked inode " << *in << dendl;
      // inode expired
      if (!in->is_replica(p->first))
	continue;
      while (1) {
	CDentry *dn = in->get_parent_dn();
	if (dn->is_replica(p->first))
	  break;
	dn->add_replica(p->first);
	CDir *dir = dn->get_dir();
	if (dir->is_replica(p->first))
	  break;
	dir->add_replica(p->first);
	in = dir->get_inode();
	if (in->is_replica(p->first))
	  break;
	in->add_replica(p->first);
	if (in->is_base())
	  break;
      }
    }
  }
  rejoin_unlinked_inodes.clear();
  
  // send acks to everyone in the recovery set
  map<mds_rank_t,ref_t<MMDSCacheRejoin>> acks;
  for (set<mds_rank_t>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (rejoin_ack_sent.count(*p))
      continue;
    acks[*p] = make_message<MMDSCacheRejoin>(MMDSCacheRejoin::OP_ACK);
  }

  rejoin_ack_sent = recovery_set;
  
  // walk subtrees
  for (map<CDir*,set<CDir*> >::iterator p = subtrees.begin(); 
       p != subtrees.end();
       ++p) {
    CDir *dir = p->first;
    if (!dir->is_auth())
      continue;
    dout(10) << "subtree " << *dir << dendl;
    
    // auth items in this subtree
    std::queue<CDir*> dq;
    dq.push(dir);

    while (!dq.empty()) {
      CDir *dir = dq.front();
      dq.pop();
      
      // dir
      for (auto &r : dir->get_replicas()) {
	auto it = acks.find(r.first);
	if (it == acks.end())
	  continue;
	it->second->add_strong_dirfrag(dir->dirfrag(), ++r.second, dir->dir_rep);
	it->second->add_dirfrag_base(dir);
      }
	   
      for (auto &p : dir->items) {
	CDentry *dn = p.second;
	CDentry::linkage_t *dnl = dn->get_linkage();

	// inode
	CInode *in = NULL;
	if (dnl->is_primary())
	  in = dnl->get_inode();

	// dentry
	for (auto &r : dn->get_replicas()) {
	  auto it = acks.find(r.first);
	  if (it == acks.end())
	    continue;
	  it->second->add_strong_dentry(dir->dirfrag(), dn->get_name(), dn->get_alternate_name(),
                                           dn->first, dn->last,
					   dnl->is_primary() ? dnl->get_inode()->ino():inodeno_t(0),
					   dnl->is_remote() ? dnl->get_remote_ino():inodeno_t(0),
					   dnl->is_remote() ? dnl->get_remote_d_type():0,
					   ++r.second,
					   dn->lock.get_replica_state());
	  // peer missed MDentrylink message ?
	  if (in && !in->is_replica(r.first))
	    in->add_replica(r.first);
	}
	
	if (!in)
	  continue;

	for (auto &r : in->get_replicas()) {
	  auto it = acks.find(r.first);
	  if (it == acks.end())
	    continue;
	  it->second->add_inode_base(in, mds->mdsmap->get_up_features());
	  bufferlist bl;
	  in->_encode_locks_state_for_rejoin(bl, r.first);
	  it->second->add_inode_locks(in, ++r.second, bl);
	}
	
	// subdirs in this subtree?
	{
          auto&& dirs = in->get_nested_dirfrags();
          for (const auto& dir : dirs) {
            dq.push(dir);
          }
        }
      }
    }
  }

  // base inodes too
  if (root && root->is_auth()) 
    for (auto &r : root->get_replicas()) {
      auto it = acks.find(r.first);
      if (it == acks.end())
	continue;
      it->second->add_inode_base(root, mds->mdsmap->get_up_features());
      bufferlist bl;
      root->_encode_locks_state_for_rejoin(bl, r.first);
      it->second->add_inode_locks(root, ++r.second, bl);
    }
  if (myin)
    for (auto &r : myin->get_replicas()) {
      auto it = acks.find(r.first);
      if (it == acks.end())
	continue;
      it->second->add_inode_base(myin, mds->mdsmap->get_up_features());
      bufferlist bl;
      myin->_encode_locks_state_for_rejoin(bl, r.first);
      it->second->add_inode_locks(myin, ++r.second, bl);
    }

  // include inode base for any inodes whose scatterlocks may have updated
  for (set<CInode*>::iterator p = rejoin_potential_updated_scatterlocks.begin();
       p != rejoin_potential_updated_scatterlocks.end();
       ++p) {
    CInode *in = *p;
    for (const auto &r : in->get_replicas()) {
      auto it = acks.find(r.first);
      if (it == acks.end())
	continue;
      it->second->add_inode_base(in, mds->mdsmap->get_up_features());
    }
  }

  // send acks
  for (auto p = acks.begin(); p != acks.end(); ++p) {
    encode(rejoin_imported_caps[p->first], p->second->imported_caps);
    mds->send_message_mds(p->second, p->first);
  }

  rejoin_imported_caps.clear();
}

class C_MDC_ReIssueCaps : public MDCacheContext {
  CInode *in;
public:
  C_MDC_ReIssueCaps(MDCache *mdc, CInode *i) :
    MDCacheContext(mdc), in(i)
  {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) override {
    if (!mdcache->mds->locker->eval(in, CEPH_CAP_LOCKS))
      mdcache->mds->locker->issue_caps(in);
    in->put(CInode::PIN_PTRWAITER);
  }
};

void MDCache::reissue_all_caps()
{
  dout(10) << "reissue_all_caps" << dendl;

  int count = 0;
  for (auto &p : inode_map) {
    int n = 1;
    CInode *in = p.second;
    if (in->is_head() && in->is_any_caps()) {
      // called by MDSRank::active_start(). There shouldn't be any frozen subtree.
      if (in->is_frozen_inode()) {
	in->add_waiter(CInode::WAIT_UNFREEZE, new C_MDC_ReIssueCaps(this, in));
	continue;
      }
      if (!mds->locker->eval(in, CEPH_CAP_LOCKS))
	n += mds->locker->issue_caps(in);
    }

    if ((count % mds->heartbeat_reset_grace()) + n >= mds->heartbeat_reset_grace())
      mds->heartbeat_reset();
    count += n;
  }
}


// ===============================================================================

struct C_MDC_QueuedCow : public MDCacheContext {
  CInode *in;
  MutationRef mut;
  C_MDC_QueuedCow(MDCache *mdc, CInode *i, MutationRef& m) :
    MDCacheContext(mdc), in(i), mut(m) {}
  void finish(int r) override {
    mdcache->_queued_file_recover_cow(in, mut);
  }
};


void MDCache::queue_file_recover(CInode *in)
{
  dout(10) << "queue_file_recover " << *in << dendl;
  ceph_assert(in->is_auth());

  // cow?
  /*
  SnapRealm *realm = in->find_snaprealm();
  set<snapid_t> s = realm->get_snaps();
  while (!s.empty() && *s.begin() < in->first)
    s.erase(s.begin());
  while (!s.empty() && *s.rbegin() > in->last)
    s.erase(*s.rbegin());
  dout(10) << " snaps in [" << in->first << "," << in->last << "] are " << s << dendl;
  if (s.size() > 1) {
    auto pi = in->project_inode(mut);
    pi.inode.version = in->pre_dirty();

    auto mut(std::make_shared<MutationImpl>());
    mut->ls = mds->mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mds->mdlog, "queue_file_recover cow");
    predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);

    s.erase(*s.begin());
    while (!s.empty()) {
      snapid_t snapid = *s.begin();
      CInode *cow_inode = 0;
      journal_cow_inode(mut, &le->metablob, in, snapid-1, &cow_inode);
      ceph_assert(cow_inode);
      recovery_queue.enqueue(cow_inode);
      s.erase(*s.begin());
    }
    
    in->parent->first = in->first;
    le->metablob.add_primary_dentry(in->parent, in, true);
    mds->mdlog->submit_entry(le, new C_MDC_QueuedCow(this, in, mut));
    mds->mdlog->flush();
  }
  */

  recovery_queue.enqueue(in);
}

void MDCache::_queued_file_recover_cow(CInode *in, MutationRef& mut)
{
  mut->apply();
  mds->locker->drop_locks(mut.get());
  mut->cleanup();
}


/*
 * called after recovery to recover file sizes for previously opened (for write)
 * files.  that is, those where max_size > size.
 */
void MDCache::identify_files_to_recover()
{
  dout(10) << "identify_files_to_recover" << dendl;
  int count = 0;

  // Clear the recover and check queues in case the monitor sends rejoin mdsmap twice.
  rejoin_recover_q.clear();
  rejoin_check_q.clear();

  for (auto &p : inode_map) {
    CInode *in = p.second;
    if (!in->is_auth())
      continue;

    if (in->last != CEPH_NOSNAP)
      continue;

    // Only normal files need file size recovery
    if (!in->is_file()) {
      continue;
    }
    
    bool recover = false;
    const auto& client_ranges = in->get_projected_inode()->client_ranges;
    if (!client_ranges.empty()) {
      in->mark_clientwriteable();
      for (auto& p : client_ranges) {
	Capability *cap = in->get_client_cap(p.first);
	if (cap) {
	  cap->mark_clientwriteable();
	} else {
	  dout(10) << " client." << p.first << " has range " << p.second << " but no cap on " << *in << dendl;
	  recover = true;
	  break;
	}
      }
    }

    if (recover) {
      if (in->filelock.is_stable()) {
	in->auth_pin(&in->filelock);
      } else {
	ceph_assert(in->filelock.get_state() == LOCK_XLOCKSNAP);
      }
      in->filelock.set_state(LOCK_PRE_SCAN);
      rejoin_recover_q.push_back(in);
    } else {
      rejoin_check_q.push_back(in);
    }

    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
}

void MDCache::start_files_to_recover()
{
  int count = 0;
  for (CInode *in : rejoin_check_q) {
    if (in->filelock.get_state() == LOCK_XLOCKSNAP)
      mds->locker->issue_caps(in);
    mds->locker->check_inode_max_size(in);
    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
  rejoin_check_q.clear();
  for (CInode *in : rejoin_recover_q) {
    mds->locker->file_recover(&in->filelock);
    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }
  if (!rejoin_recover_q.empty()) {
    rejoin_recover_q.clear();
    do_file_recover();
  }
}

void MDCache::do_file_recover()
{
  recovery_queue.advance();
}

// ===============================================================================


// ----------------------------
// truncate

class C_MDC_RetryTruncate : public MDCacheContext {
  CInode *in;
  LogSegment *ls;
public:
  C_MDC_RetryTruncate(MDCache *c, CInode *i, LogSegment *l) :
    MDCacheContext(c), in(i), ls(l) {}
  void finish(int r) override {
    mdcache->_truncate_inode(in, ls);
  }
};

void MDCache::truncate_inode(CInode *in, LogSegment *ls)
{
  const auto& pi = in->get_projected_inode();
  dout(10) << "truncate_inode "
	   << pi->truncate_from << " -> " << pi->truncate_size
	   << " on " << *in
	   << dendl;

  ls->truncating_inodes.insert(in);
  in->get(CInode::PIN_TRUNCATING);
  in->auth_pin(this);

  if (!in->client_need_snapflush.empty() &&
      (in->get_caps_issued() & CEPH_CAP_FILE_BUFFER)) {
    ceph_assert(in->filelock.is_xlocked());
    in->filelock.set_xlock_snap_sync(new C_MDC_RetryTruncate(this, in, ls));
    mds->locker->issue_caps(in);
    return;
  }

  _truncate_inode(in, ls);
}

struct C_IO_MDC_TruncateWriteFinish : public MDCacheIOContext {
  CInode *in;
  LogSegment *ls;
  uint32_t block_size;
  C_IO_MDC_TruncateWriteFinish(MDCache *c, CInode *i, LogSegment *l, uint32_t bs) :
    MDCacheIOContext(c, false), in(i), ls(l), block_size(bs) {
  }
  void finish(int r) override {
    ceph_assert(r == 0 || r == -CEPHFS_ENOENT);
    mdcache->truncate_inode_write_finish(in, ls, block_size);
  }
  void print(ostream& out) const override {
    out << "file_truncate_write(" << in->ino() << ")";
  }
};

struct C_IO_MDC_TruncateFinish : public MDCacheIOContext {
  CInode *in;
  LogSegment *ls;
  C_IO_MDC_TruncateFinish(MDCache *c, CInode *i, LogSegment *l) :
    MDCacheIOContext(c, false), in(i), ls(l) {
  }
  void finish(int r) override {
    ceph_assert(r == 0 || r == -CEPHFS_ENOENT);
    mdcache->truncate_inode_finish(in, ls);
  }
  void print(ostream& out) const override {
    out << "file_truncate(" << in->ino() << ")";
  }
};

void MDCache::_truncate_inode(CInode *in, LogSegment *ls)
{
  const auto& pi = in->get_inode();
  dout(10) << "_truncate_inode "
           << pi->truncate_from << " -> " << pi->truncate_size
           << " fscrypt last block length is " << pi->fscrypt_last_block.length()
           << " on " << *in << dendl;

  ceph_assert(pi->is_truncating());
  ceph_assert(pi->truncate_size < (1ULL << 63));
  ceph_assert(pi->truncate_from < (1ULL << 63));
  ceph_assert(pi->truncate_size < pi->truncate_from ||
              (pi->truncate_size == pi->truncate_from &&
	       pi->fscrypt_last_block.length()));


  SnapRealm *realm = in->find_snaprealm();
  SnapContext nullsnap;
  const SnapContext *snapc;
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnap;
    ceph_assert(in->last == CEPH_NOSNAP);
  }
  dout(10) << "_truncate_inode  snapc " << snapc << " on " << *in
           << " fscrypt_last_block length is " << pi->fscrypt_last_block.length()
           << dendl;
  auto layout = pi->layout;
  struct ceph_fscrypt_last_block_header header;
  memset(&header, 0, sizeof(header));
  bufferlist data;
  if (pi->fscrypt_last_block.length()) {
    auto bl = pi->fscrypt_last_block.cbegin();
    DECODE_START(1, bl);
    decode(header.change_attr, bl);
    decode(header.file_offset, bl);
    decode(header.block_size, bl);

    /*
     * The block_size will be in unit of KB, so if the last block is not
     * located in a file hole, the struct_len should be larger than the
     * header.block_size.
     */
    if (struct_len > header.block_size) {
      bl.copy(header.block_size, data);
    }
    DECODE_FINISH(bl);
  }

  if (data.length()) {
    dout(10) << "_truncate_inode write on inode " << *in << " change_attr: "
             << header.change_attr << " offset: " << header.file_offset << " blen: "
	     << header.block_size << dendl;
    filer.write(in->ino(), &layout, *snapc, header.file_offset, header.block_size,
                data, ceph::real_clock::zero(), 0,
                new C_OnFinisher(new C_IO_MDC_TruncateWriteFinish(this, in, ls,
                                                                  header.block_size),
                                 mds->finisher));
  } else { // located in file hole.
    uint64_t length = pi->truncate_from - pi->truncate_size;

    /*
     * When the fscrypt is enabled the truncate_from and truncate_size
     * possibly equal and both are aligned up to header.block_size. In
     * this case we will always request a larger length to make sure the
     * OSD won't miss truncating the last object.
     */
    if (pi->fscrypt_last_block.length()) {
      dout(10) << "_truncate_inode truncate on inode " << *in << " hits a hole!" << dendl;
      length += header.block_size;
    }
    ceph_assert(length);

    dout(10) << "_truncate_inode truncate on inode " << *in << dendl;
    filer.truncate(in->ino(), &layout, *snapc, pi->truncate_size, length,
                   pi->truncate_seq, ceph::real_clock::zero(), 0,
                   new C_OnFinisher(new C_IO_MDC_TruncateFinish(this, in, ls),
                                    mds->finisher));
  }

}

struct C_MDC_TruncateLogged : public MDCacheLogContext {
  CInode *in;
  MutationRef mut;
  C_MDC_TruncateLogged(MDCache *m, CInode *i, MutationRef& mu) :
    MDCacheLogContext(m), in(i), mut(mu) {}
  void finish(int r) override {
    mdcache->truncate_inode_logged(in, mut);
  }
};

void MDCache::truncate_inode_write_finish(CInode *in, LogSegment *ls,
                                          uint32_t block_size)
{
  const auto& pi = in->get_inode();
  dout(10) << "_truncate_inode_write "
	   << pi->truncate_from << " -> " << pi->truncate_size
	   << " on " << *in << dendl;

  ceph_assert(pi->is_truncating());
  ceph_assert(pi->truncate_size < (1ULL << 63));
  ceph_assert(pi->truncate_from < (1ULL << 63));
  ceph_assert(pi->truncate_size < pi->truncate_from ||
              (pi->truncate_size == pi->truncate_from &&
	       pi->fscrypt_last_block.length()));


  SnapRealm *realm = in->find_snaprealm();
  SnapContext nullsnap;
  const SnapContext *snapc;
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnap;
    ceph_assert(in->last == CEPH_NOSNAP);
  }
  dout(10) << "_truncate_inode_write  snapc " << snapc << " on " << *in
           << " fscrypt_last_block length is " << pi->fscrypt_last_block.length()
           << dendl;
  auto layout = pi->layout;
  /*
   * When the fscrypt is enabled the truncate_from and truncate_size
   * possibly equal and both are aligned up to header.block_size. In
   * this case we will always request a larger length to make sure the
   * OSD won't miss truncating the last object.
   */
  uint64_t length = pi->truncate_from - pi->truncate_size + block_size;
  filer.truncate(in->ino(), &layout, *snapc, pi->truncate_size, length,
                 pi->truncate_seq, ceph::real_clock::zero(), 0,
                 new C_OnFinisher(new C_IO_MDC_TruncateFinish(this, in, ls),
                                  mds->finisher));
}

void MDCache::truncate_inode_finish(CInode *in, LogSegment *ls)
{
  dout(10) << "truncate_inode_finish " << *in << dendl;
  
  set<CInode*>::iterator p = ls->truncating_inodes.find(in);
  ceph_assert(p != ls->truncating_inodes.end());
  ls->truncating_inodes.erase(p);

  MutationRef mut(new MutationImpl());
  mut->ls = mds->mdlog->get_current_segment();

  // update
  auto pi = in->project_inode(mut);
  pi.inode->version = in->pre_dirty();
  pi.inode->truncate_from = 0;
  pi.inode->truncate_pending--;
  pi.inode->fscrypt_last_block = bufferlist();

  EUpdate *le = new EUpdate(mds->mdlog, "truncate finish");

  predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  journal_dirty_inode(mut.get(), &le->metablob, in);
  le->metablob.add_truncate_finish(in->ino(), ls->seq);
  mds->mdlog->submit_entry(le, new C_MDC_TruncateLogged(this, in, mut));

  // flush immediately if there are readers/writers waiting
  if (in->is_waiter_for(CInode::WAIT_TRUNC) ||
      (in->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR)))
    mds->mdlog->flush();
}

void MDCache::truncate_inode_logged(CInode *in, MutationRef& mut)
{
  dout(10) << "truncate_inode_logged " << *in << dendl;
  mut->apply();
  mds->locker->drop_locks(mut.get());
  mut->cleanup();

  in->put(CInode::PIN_TRUNCATING);
  in->auth_unpin(this);

  MDSContext::vec waiters;
  in->take_waiting(CInode::WAIT_TRUNC, waiters);
  mds->queue_waiters(waiters);
}


void MDCache::add_recovered_truncate(CInode *in, LogSegment *ls)
{
  dout(20) << "add_recovered_truncate " << *in << " in log segment "
	   << ls->seq << "/" << ls->offset << dendl;
  ls->truncating_inodes.insert(in);
  in->get(CInode::PIN_TRUNCATING);
}

void MDCache::remove_recovered_truncate(CInode *in, LogSegment *ls)
{
  dout(20) << "remove_recovered_truncate " << *in << " in log segment "
	   << ls->seq << "/" << ls->offset << dendl;
  // if we have the logseg the truncate started in, it must be in our list.
  set<CInode*>::iterator p = ls->truncating_inodes.find(in);
  ceph_assert(p != ls->truncating_inodes.end());
  ls->truncating_inodes.erase(p);
  in->put(CInode::PIN_TRUNCATING);
}

void MDCache::start_recovered_truncates()
{
  dout(10) << "start_recovered_truncates" << dendl;
  for (map<uint64_t,LogSegment*>::iterator p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       ++p) {
    LogSegment *ls = p->second;
    for (set<CInode*>::iterator q = ls->truncating_inodes.begin();
	 q != ls->truncating_inodes.end();
	 ++q) {
      CInode *in = *q;
      in->auth_pin(this);

      if (!in->client_need_snapflush.empty() &&
	  (in->get_caps_issued() & CEPH_CAP_FILE_BUFFER)) {
	ceph_assert(in->filelock.is_stable());
	in->filelock.set_state(LOCK_XLOCKDONE);
	in->auth_pin(&in->filelock);
	in->filelock.set_xlock_snap_sync(new C_MDC_RetryTruncate(this, in, ls));
	// start_files_to_recover will revoke caps
	continue;
      }
      _truncate_inode(in, ls);
    }
  }
}


class C_MDS_purge_completed_finish : public MDCacheLogContext {
  interval_set<inodeno_t> inos;
  LogSegment *ls; 
  version_t inotablev;
public:
  C_MDS_purge_completed_finish(MDCache *m, const interval_set<inodeno_t>& _inos,
			       LogSegment *_ls, version_t iv)
    : MDCacheLogContext(m), inos(_inos), ls(_ls), inotablev(iv) {}
  void finish(int r) override {
    ceph_assert(r == 0);
    if (inotablev) {
      get_mds()->inotable->apply_release_ids(inos);
      ceph_assert(get_mds()->inotable->get_version() == inotablev);
    }
    ls->purge_inodes_finish(inos);
  }
};

void MDCache::start_purge_inodes(){
  dout(10) << "start_purge_inodes" << dendl;
  for (auto& p : mds->mdlog->segments){
    LogSegment *ls = p.second;
    if (ls->purging_inodes.size()){
      purge_inodes(ls->purging_inodes, ls);
    }
  }
}

void MDCache::purge_inodes(const interval_set<inodeno_t>& inos, LogSegment *ls)
{
  dout(10) << __func__ << " purging inos " << inos << " logseg " << ls->seq << dendl;
  // FIXME: handle non-default data pool and namespace

  auto cb = new LambdaContext([this, inos, ls](int r){
      ceph_assert(r == 0 || r == -2);
      mds->inotable->project_release_ids(inos);
      version_t piv = mds->inotable->get_projected_version();
      ceph_assert(piv != 0);
      mds->mdlog->submit_entry(new EPurged(inos, ls->seq, piv),
				     new C_MDS_purge_completed_finish(this, inos, ls, piv));
      mds->mdlog->flush();
    });
  
  C_GatherBuilder gather(g_ceph_context,
			  new C_OnFinisher(new MDSIOContextWrapper(mds, cb), mds->finisher));
  SnapContext nullsnapc;
  for (const auto& [start, len] : inos) {
    for (auto i = start; i < start + len ; i += 1) {
      filer.purge_range(i, &default_file_layout, nullsnapc, 0, 1,
			ceph::real_clock::now(), 0, gather.new_sub());
    }
  }
  gather.activate();
}

// ================================================================================
// cache trimming

std::pair<bool, uint64_t> MDCache::trim_lru(uint64_t count, expiremap& expiremap)
{
  bool is_standby_replay = mds->is_standby_replay();
  std::vector<CDentry *> unexpirables;
  uint64_t trimmed = 0;

  auto trim_threshold = g_conf().get_val<Option::size_t>("mds_cache_trim_threshold");

  dout(7) << "trim_lru trimming " << count
          << " items from LRU"
          << " size=" << lru.lru_get_size()
          << " mid=" << lru.lru_get_top()
          << " pintail=" << lru.lru_get_pintail()
          << " pinned=" << lru.lru_get_num_pinned()
          << dendl;

  dout(20) << "bottom_lru: " << bottom_lru.lru_get_size() << " items"
              ", " << bottom_lru.lru_get_top() << " top"
              ", " << bottom_lru.lru_get_bot() << " bot"
              ", " << bottom_lru.lru_get_pintail() << " pintail"
              ", " << bottom_lru.lru_get_num_pinned() << " pinned"
              << dendl;

  const uint64_t trim_counter_start = trim_counter.get();
  bool throttled = false;
  while (1) {
    throttled |= trim_counter_start+trimmed >= trim_threshold;
    if (throttled) break;
    CDentry *dn = static_cast<CDentry*>(bottom_lru.lru_expire());
    if (!dn)
      break;
    if (trim_dentry(dn, expiremap)) {
      unexpirables.push_back(dn);
    } else {
      trimmed++;
    }
  }

  for (auto &dn : unexpirables) {
    bottom_lru.lru_insert_mid(dn);
  }
  unexpirables.clear();

  dout(20) << "lru: " << lru.lru_get_size() << " items"
              ", " << lru.lru_get_top() << " top"
              ", " << lru.lru_get_bot() << " bot"
              ", " << lru.lru_get_pintail() << " pintail"
              ", " << lru.lru_get_num_pinned() << " pinned"
              << dendl;

  // trim dentries from the LRU until count is reached
  while (!throttled && (cache_toofull() || count > 0)) {
    throttled |= trim_counter_start+trimmed >= trim_threshold;
    if (throttled) break;
    CDentry *dn = static_cast<CDentry*>(lru.lru_expire());
    if (!dn) {
      break;
    }
    if ((is_standby_replay && dn->get_linkage()->inode &&
        dn->get_linkage()->inode->item_open_file.is_on_list())) {
      dout(20) << "unexpirable: " << *dn << dendl;
      unexpirables.push_back(dn);
    } else if (trim_dentry(dn, expiremap)) {
      unexpirables.push_back(dn);
    } else {
      trimmed++;
      if (count > 0) count--;
    }
  }
  trim_counter.hit(trimmed);

  for (auto &dn : unexpirables) {
    lru.lru_insert_mid(dn);
  }
  unexpirables.clear();

  dout(7) << "trim_lru trimmed " << trimmed << " items" << dendl;
  return std::pair<bool, uint64_t>(throttled, trimmed);
}

/*
 * note: only called while MDS is active or stopping... NOT during recovery.
 * however, we may expire a replica whose authority is recovering.
 *
 * @param count is number of dentries to try to expire
 */
std::pair<bool, uint64_t> MDCache::trim(uint64_t count)
{
  uint64_t used = cache_size();
  uint64_t limit = cache_memory_limit;
  expiremap expiremap;

  dout(7) << "trim bytes_used=" << bytes2str(used)
          << " limit=" << bytes2str(limit)
          << " reservation=" << cache_reservation
          << "% count=" << count << dendl;

  // process delayed eval_stray()
  stray_manager.advance_delayed();

  auto result = trim_lru(count, expiremap);
  auto& trimmed = result.second;

  // trim non-auth, non-bound subtrees
  for (auto p = subtrees.begin(); p != subtrees.end();) {
    CDir *dir = p->first;
    ++p;
    CInode *diri = dir->get_inode();
    if (dir->is_auth()) {
      if (diri->is_auth() && !diri->is_base()) {
        /* this situation should correspond to an export pin */
        if (dir->get_num_head_items() == 0 && dir->get_num_ref() == 1) {
          /* pinned empty subtree, try to drop */
          if (dir->state_test(CDir::STATE_AUXSUBTREE)) {
            dout(20) << "trimming empty pinned subtree " << *dir << dendl;
            dir->state_clear(CDir::STATE_AUXSUBTREE);
            remove_subtree(dir);
            diri->close_dirfrag(dir->dirfrag().frag);
          }
        }
      } else if (!diri->is_auth() && !diri->is_base() && dir->get_num_head_items() == 0) {
        if (dir->state_test(CDir::STATE_EXPORTING) ||
           !(mds->is_active() || mds->is_stopping()) ||
           dir->is_freezing() || dir->is_frozen())
          continue;

        migrator->export_empty_import(dir);
        ++trimmed;
      }
    } else if (!diri->is_auth() && dir->get_num_ref() <= 1) {
      // only subtree pin
      if (diri->get_num_ref() > diri->get_num_subtree_roots()) {
        continue;
      }

      // don't trim subtree root if its auth MDS is recovering.
      // This simplify the cache rejoin code.
      if (dir->is_subtree_root() && rejoin_ack_gather.count(dir->get_dir_auth().first))
        continue;
      trim_dirfrag(dir, 0, expiremap);
      ++trimmed;
    }
  }

  // trim root?
  if (mds->is_stopping() && root) {
    auto&& ls = root->get_dirfrags();
    for (const auto& dir : ls) {
      if (dir->get_num_ref() == 1) { // subtree pin
	trim_dirfrag(dir, 0, expiremap);
        ++trimmed;
      }
    }
    if (root->get_num_ref() == 0) {
      trim_inode(0, root, 0, expiremap);
      ++trimmed;
    }
  }

  std::set<mds_rank_t> stopping;
  mds->mdsmap->get_mds_set(stopping, MDSMap::STATE_STOPPING);
  stopping.erase(mds->get_nodeid());
  for (auto rank : stopping) {
    CInode* mdsdir_in = get_inode(MDS_INO_MDSDIR(rank));
    if (!mdsdir_in)
      continue;

    auto em = expiremap.emplace(std::piecewise_construct, std::forward_as_tuple(rank), std::forward_as_tuple());
    if (em.second) {
      em.first->second = make_message<MCacheExpire>(mds->get_nodeid());
    }

    dout(20) << __func__ << ": try expiring " << *mdsdir_in << " for stopping mds." << mds->get_nodeid() <<  dendl;

    const bool aborted = expire_recursive(mdsdir_in, expiremap);
    if (!aborted) {
      dout(20) << __func__ << ": successfully expired mdsdir" << dendl;
      auto&& ls = mdsdir_in->get_dirfrags();
      for (auto dir : ls) {
	if (dir->get_num_ref() == 1) {  // subtree pin
	  trim_dirfrag(dir, dir, expiremap);
          ++trimmed;
        }
      }
      if (mdsdir_in->get_num_ref() == 0) {
	trim_inode(NULL, mdsdir_in, NULL, expiremap);
        ++trimmed;
      }
    } else {
      dout(20) << __func__ << ": some unexpirable contents in mdsdir" << dendl;
    }
  }

  // Other rank's base inodes (when I'm stopping)
  if (mds->is_stopping()) {
    for (set<CInode*>::iterator p = base_inodes.begin();
         p != base_inodes.end();) {
      CInode *base_in = *p;
      ++p;
      if (MDS_INO_IS_MDSDIR(base_in->ino()) &&
	  MDS_INO_MDSDIR_OWNER(base_in->ino()) != mds->get_nodeid()) {
        dout(20) << __func__ << ": maybe trimming base: " << *base_in << dendl;
        if (base_in->get_num_ref() == 0) {
          trim_inode(NULL, base_in, NULL, expiremap);
          ++trimmed;
        }
      }
    }
  }

  // send any expire messages
  send_expire_messages(expiremap);

  return result;
}

void MDCache::send_expire_messages(expiremap& expiremap)
{
  // send expires
  for (const auto &p : expiremap) {
    if (mds->is_cluster_degraded() &&
	(mds->mdsmap->get_state(p.first) < MDSMap::STATE_REJOIN ||
	 (mds->mdsmap->get_state(p.first) == MDSMap::STATE_REJOIN &&
	  rejoin_sent.count(p.first) == 0))) {
      continue;
    }
    dout(7) << "sending cache_expire to " << p.first << dendl;
    mds->send_message_mds(p.second, p.first);
  }
  expiremap.clear();
}


bool MDCache::trim_dentry(CDentry *dn, expiremap& expiremap)
{
  dout(12) << "trim_dentry " << *dn << dendl;
  
  CDentry::linkage_t *dnl = dn->get_linkage();

  CDir *dir = dn->get_dir();
  ceph_assert(dir);
  
  CDir *con = get_subtree_root(dir);
  if (con)
    dout(12) << " in container " << *con << dendl;
  else {
    dout(12) << " no container; under a not-yet-linked dir" << dendl;
    ceph_assert(dn->is_auth());
  }

  // If replica dentry is not readable, it's likely we will receive
  // MDentryLink/MDentryUnlink message soon (It's possible we first
  // receive a MDentryUnlink message, then MDentryLink message)
  // MDentryLink message only replicates an inode, so we should
  // avoid trimming the inode's parent dentry. This is because that
  // unconnected replicas are problematic for subtree migration.
  if (!dn->is_auth() && !dn->lock.can_read(-1) &&
      !dn->get_dir()->get_inode()->is_stray())
    return true;

  // adjust the dir state
  // NOTE: we can safely remove a clean, null dentry without effecting
  //       directory completeness.
  // (check this _before_ we unlink the inode, below!)
  bool clear_complete = false;
  if (dn->is_auth() && !(dnl->is_null() && dn->is_clean()))
    clear_complete = true;

  // unlink the dentry
  if (dnl->is_remote()) {
    // just unlink.
    dir->unlink_inode(dn, false);
  } else if (dnl->is_primary()) {
    // expire the inode, too.
    CInode *in = dnl->get_inode();
    ceph_assert(in);
    if (trim_inode(dn, in, con, expiremap))
      return true; // purging stray instead of trimming
  } else {
    ceph_assert(dnl->is_null());
  }

  if (!dn->is_auth()) {
    // notify dentry authority.
    mds_authority_t auth = dn->authority();
    
    for (int p=0; p<2; p++) {
      mds_rank_t a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.
      
      dout(12) << "  sending expire to mds." << a << " on " << *dn << dendl;
      ceph_assert(a != mds->get_nodeid());
      auto em = expiremap.emplace(std::piecewise_construct, std::forward_as_tuple(a), std::forward_as_tuple());
      if (em.second)
	em.first->second = make_message<MCacheExpire>(mds->get_nodeid());
      em.first->second->add_dentry(con->dirfrag(), dir->dirfrag(), dn->get_name(), dn->last, dn->get_replica_nonce());
    }
  }

  if (clear_complete) {
    if (dn->last == CEPH_NOSNAP)
      dir->add_to_bloom(dn);
    dir->state_clear(CDir::STATE_COMPLETE);
  }

  // remove dentry
  dir->remove_dentry(dn);
  
  if (mds->logger) mds->logger->inc(l_mds_inodes_expired);
  return false;
}


void MDCache::trim_dirfrag(CDir *dir, CDir *con, expiremap& expiremap)
{
  dout(15) << "trim_dirfrag " << *dir << dendl;

  if (dir->is_subtree_root()) {
    ceph_assert(!dir->is_auth() ||
	   (!dir->is_replicated() && dir->inode->is_base()));
    remove_subtree(dir);	// remove from subtree map
  }
  ceph_assert(dir->get_num_ref() == 0);

  CInode *in = dir->get_inode();

  if (!dir->is_auth()) {
    mds_authority_t auth = dir->authority();
    
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
      mds_rank_t a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds." << a << " on   " << *dir << dendl;
      ceph_assert(a != mds->get_nodeid());
      auto em = expiremap.emplace(std::piecewise_construct, std::forward_as_tuple(a), std::forward_as_tuple());
      if (em.second)
	em.first->second = make_message<MCacheExpire>(mds->get_nodeid()); /* new */
      em.first->second->add_dir(condf, dir->dirfrag(), dir->replica_nonce);
    }
  }
  
  in->close_dirfrag(dir->dirfrag().frag);
}

/**
 * Try trimming an inode from the cache
 *
 * @return true if the inode is still in cache, else false if it was trimmed
 */
bool MDCache::trim_inode(CDentry *dn, CInode *in, CDir *con, expiremap& expiremap)
{
  dout(15) << "trim_inode " << *in << dendl;
  ceph_assert(in->get_num_ref() == 0);

  if (in->is_dir()) {
    // If replica inode's dirfragtreelock is not readable, it's likely
    // some dirfrags of the inode are being fragmented and we will receive
    // MMDSFragmentNotify soon. MMDSFragmentNotify only replicates the new
    // dirfrags, so we should avoid trimming these dirfrags' parent inode.
    // This is because that unconnected replicas are problematic for
    // subtree migration.
    //
    if (!in->is_auth() && !mds->locker->rdlock_try(&in->dirfragtreelock, -1)) {
      return true;
    }

    // DIR
    auto&& dfls = in->get_dirfrags();
    for (const auto& dir : dfls) {
      ceph_assert(!dir->is_subtree_root());
      trim_dirfrag(dir, con ? con:dir, expiremap);  // if no container (e.g. root dirfrag), use *p
    }
  }
  
  // INODE
  if (in->is_auth()) {
    // eval stray after closing dirfrags
    if (dn && !dn->state_test(CDentry::STATE_PURGING)) {
      maybe_eval_stray(in);
      if (dn->state_test(CDentry::STATE_PURGING) || dn->get_num_ref() > 0)
	return true;
    }
  } else {
    mds_authority_t auth = in->authority();
    
    dirfrag_t df;
    if (con)
      df = con->dirfrag();
    else
      df = dirfrag_t(0,frag_t());   // must be a root or stray inode.

    for (int p=0; p<2; p++) {
      mds_rank_t a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (con && mds->get_nodeid() == auth.second &&
	  con->is_importing()) break;                // don't send any expire while importing.
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds." << a << " on " << *in << dendl;
      ceph_assert(a != mds->get_nodeid());
      auto em = expiremap.emplace(std::piecewise_construct, std::forward_as_tuple(a), std::forward_as_tuple());
      if (em.second)
	em.first->second = make_message<MCacheExpire>(mds->get_nodeid()); /* new */
      em.first->second->add_inode(df, in->vino(), in->get_replica_nonce());
    }
  }

  /*
  if (in->is_auth()) {
    if (in->hack_accessed)
      mds->logger->inc("outt");
    else {
      mds->logger->inc("outut");
      mds->logger->fset("oututl", ceph_clock_now() - in->hack_load_stamp);
    }
  }
  */
    
  // unlink
  if (dn)
    dn->get_dir()->unlink_inode(dn, false);
  remove_inode(in);
  return false;
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
  
  // temporarily pin all subtree roots
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) 
    p->first->get(CDir::PIN_SUBTREETEMP);

  list<CDentry*> auth_list;
  
  // trim non-auth items from the lru
  for (;;) {
    CDentry *dn = NULL;
    if (bottom_lru.lru_get_size() > 0)
      dn = static_cast<CDentry*>(bottom_lru.lru_expire());
    if (!dn && lru.lru_get_size() > 0)
      dn = static_cast<CDentry*>(lru.lru_expire());
    if (!dn)
	break;

    CDentry::linkage_t *dnl = dn->get_linkage();

    if (dn->is_auth()) {
      // add back into lru (at the top)
      auth_list.push_back(dn);

      if (dnl->is_remote() && dnl->get_inode() && !dnl->get_inode()->is_auth())
	dn->unlink_remote(dnl);
    } else {
      // non-auth.  expire.
      CDir *dir = dn->get_dir();
      ceph_assert(dir);

      // unlink the dentry
      dout(10) << " removing " << *dn << dendl;
      if (dnl->is_remote()) {
	dir->unlink_inode(dn, false);
      } 
      else if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	dout(10) << " removing " << *in << dendl;
	auto&& ls = in->get_dirfrags();
	for (const auto& subdir : ls) {
	  ceph_assert(!subdir->is_subtree_root());
	  in->close_dirfrag(subdir->dirfrag().frag);
	}
	dir->unlink_inode(dn, false);
	remove_inode(in);
      } 
      else {
	ceph_assert(dnl->is_null());
      }

      ceph_assert(!dir->has_bloom());
      dir->remove_dentry(dn);
      // adjust the dir state
      dir->state_clear(CDir::STATE_COMPLETE);  // dir incomplete!
      // close empty non-auth dirfrag
      if (!dir->is_subtree_root() && dir->get_num_any() == 0)
	dir->inode->close_dirfrag(dir->get_frag());
    }
  }

  for (const auto& dn : auth_list) {
      if (dn->state_test(CDentry::STATE_BOTTOMLRU))
	bottom_lru.lru_insert_mid(dn);
      else
	lru.lru_insert_top(dn);
  }

  // move everything in the pintail to the top bit of the lru.
  lru.lru_touch_entire_pintail();

  // unpin all subtrees
  for (map<CDir*, set<CDir*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) 
    p->first->put(CDir::PIN_SUBTREETEMP);

  if (lru.lru_get_size() == 0 &&
      bottom_lru.lru_get_size() == 0) {
    // root, stray, etc.?
    auto p = inode_map.begin();
    while (p != inode_map.end()) {
      CInode *in = p->second;
      ++p;
      if (!in->is_auth()) {
	auto&& ls = in->get_dirfrags();
	for (const auto& dir : ls) {
	  dout(10) << " removing " << *dir << dendl;
	  ceph_assert(dir->get_num_ref() == 1);  // SUBTREE
	  remove_subtree(dir);
	  in->close_dirfrag(dir->dirfrag().frag);
	}
	dout(10) << " removing " << *in << dendl;
	ceph_assert(!in->get_parent_dn());
	ceph_assert(in->get_num_ref() == 0);
	remove_inode(in);
      }
    }
  }

  show_subtrees();
}

/**
 * Recursively trim the subtree rooted at directory to remove all
 * CInodes/CDentrys/CDirs that aren't links to remote MDSes, or ancestors
 * of those links. This is used to clear invalid data out of the cache.
 * Note that it doesn't clear the passed-in directory, since that's not
 * always safe.
 */
bool MDCache::trim_non_auth_subtree(CDir *dir)
{
  dout(10) << "trim_non_auth_subtree(" << dir << ") " << *dir << dendl;

  bool keep_dir = !can_trim_non_auth_dirfrag(dir);

  auto j = dir->begin();
  auto i = j;
  while (j != dir->end()) {
    i = j++;
    CDentry *dn = i->second;
    dout(10) << "trim_non_auth_subtree(" << dir << ") Checking dentry " << dn << dendl;
    CDentry::linkage_t *dnl = dn->get_linkage();
    if (dnl->is_primary()) { // check for subdirectories, etc
      CInode *in = dnl->get_inode();
      bool keep_inode = false;
      if (in->is_dir()) {
        auto&& subdirs = in->get_dirfrags();
        for (const auto& subdir : subdirs) {
          if (subdir->is_subtree_root()) {
            keep_inode = true;
            dout(10) << "trim_non_auth_subtree(" << dir << ") keeping " << *subdir << dendl;
          } else {
            if (trim_non_auth_subtree(subdir))
              keep_inode = true;
            else {
              in->close_dirfrag(subdir->get_frag());
              dir->state_clear(CDir::STATE_COMPLETE);  // now incomplete!
            }
          }
        }

      }
      if (!keep_inode) { // remove it!
        dout(20) << "trim_non_auth_subtree(" << dir << ") removing inode " << in << " with dentry" << dn << dendl;
        dir->unlink_inode(dn, false);
        remove_inode(in);
	ceph_assert(!dir->has_bloom());
        dir->remove_dentry(dn);
      } else {
        dout(20) << "trim_non_auth_subtree(" << dir << ") keeping inode " << in << " with dentry " << dn <<dendl;
	dn->clear_auth();
	in->state_clear(CInode::STATE_AUTH);
      }
    } else if (keep_dir && dnl->is_null()) { // keep null dentry for peer rollback
      dout(20) << "trim_non_auth_subtree(" << dir << ") keeping dentry " << dn <<dendl;
    } else { // just remove it
      dout(20) << "trim_non_auth_subtree(" << dir << ") removing dentry " << dn << dendl;
      if (dnl->is_remote())
        dir->unlink_inode(dn, false);
      dir->remove_dentry(dn);
    }
  }
  dir->state_clear(CDir::STATE_AUTH);
  /**
   * We've now checked all our children and deleted those that need it.
   * Now return to caller, and tell them if *we're* a keeper.
   */
  return keep_dir || dir->get_num_any();
}

/*
 * during replay, when we determine a subtree is no longer ours, we
 * try to trim it from our cache.  because subtrees must be connected
 * to the root, the fact that we can trim this tree may mean that our
 * children or parents can also be trimmed.
 */
void MDCache::try_trim_non_auth_subtree(CDir *dir)
{
  dout(10) << "try_trim_nonauth_subtree " << *dir << dendl;

  // can we now trim child subtrees?
  set<CDir*> bounds;
  get_subtree_bounds(dir, bounds);
  for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
    CDir *bd = *p;
    if (bd->get_dir_auth().first != mds->get_nodeid() &&  // we are not auth
	bd->get_num_any() == 0 && // and empty
	can_trim_non_auth_dirfrag(bd)) {
      CInode *bi = bd->get_inode();
      dout(10) << " closing empty non-auth child subtree " << *bd << dendl;
      remove_subtree(bd);
      bd->mark_clean();
      bi->close_dirfrag(bd->get_frag());
    }
  }

  if (trim_non_auth_subtree(dir)) {
    // keep
    try_subtree_merge(dir);
  } else {
    // can we trim this subtree (and possibly our ancestors) too?
    while (true) {
      CInode *diri = dir->get_inode();
      if (diri->is_base()) {
	if (!diri->is_root() && diri->authority().first != mds->get_nodeid()) {
	  dout(10) << " closing empty non-auth subtree " << *dir << dendl;
	  remove_subtree(dir);
	  dir->mark_clean();
	  diri->close_dirfrag(dir->get_frag());

	  dout(10) << " removing " << *diri << dendl;
	  ceph_assert(!diri->get_parent_dn());
	  ceph_assert(diri->get_num_ref() == 0);
	  remove_inode(diri);
	}
	break;
      }

      CDir *psub = get_subtree_root(diri->get_parent_dir());
      dout(10) << " parent subtree is " << *psub << dendl;
      if (psub->get_dir_auth().first == mds->get_nodeid())
	break;  // we are auth, keep.

      dout(10) << " closing empty non-auth subtree " << *dir << dendl;
      remove_subtree(dir);
      dir->mark_clean();
      diri->close_dirfrag(dir->get_frag());

      dout(10) << " parent subtree also non-auth: " << *psub << dendl;
      if (trim_non_auth_subtree(psub))
	break;
      dir = psub;
    }
  }

  show_subtrees();
}

void MDCache::standby_trim_segment(LogSegment *ls)
{
  ls->new_dirfrags.clear_list();
  ls->open_files.clear_list();

  while (!ls->dirty_dirfrags.empty()) {
    CDir *dir = ls->dirty_dirfrags.front();
    dir->mark_clean();
  }
  while (!ls->dirty_inodes.empty()) {
    CInode *in = ls->dirty_inodes.front();
    in->mark_clean();
  }
  while (!ls->dirty_dentries.empty()) {
    CDentry *dn = ls->dirty_dentries.front();
    dn->mark_clean();
  }
  while (!ls->dirty_parent_inodes.empty()) {
    CInode *in = ls->dirty_parent_inodes.front();
    in->clear_dirty_parent();
  }
  while (!ls->dirty_dirfrag_dir.empty()) {
    CInode *in = ls->dirty_dirfrag_dir.front();
    in->filelock.remove_dirty();
  }
  while (!ls->dirty_dirfrag_nest.empty()) {
    CInode *in = ls->dirty_dirfrag_nest.front();
    in->nestlock.remove_dirty();
  }
  while (!ls->dirty_dirfrag_dirfragtree.empty()) {
    CInode *in = ls->dirty_dirfrag_dirfragtree.front();
    in->dirfragtreelock.remove_dirty();
  }
  while (!ls->truncating_inodes.empty()) {
    auto it = ls->truncating_inodes.begin();
    CInode *in = *it;
    ls->truncating_inodes.erase(it);
    in->put(CInode::PIN_TRUNCATING);
  }
}

void MDCache::handle_cache_expire(const cref_t<MCacheExpire> &m)
{
  mds_rank_t from = mds_rank_t(m->get_from());
  
  dout(7) << "cache_expire from mds." << from << dendl;

  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    return;
  }

  set<SimpleLock *> gather_locks;
  // loop over realms
  for (const auto &p : m->realms) {
    // check container?
    if (p.first.ino > 0) {
      CInode *expired_inode = get_inode(p.first.ino);
      ceph_assert(expired_inode);  // we had better have this.
      CDir *parent_dir = expired_inode->get_approx_dirfrag(p.first.frag);
      ceph_assert(parent_dir);

      int export_state = -1;
      if (parent_dir->is_auth() && parent_dir->is_exporting()) {
	export_state = migrator->get_export_state(parent_dir);
	ceph_assert(export_state >= 0);
      }

      if (!parent_dir->is_auth() ||
	  (export_state != -1 &&
	   ((export_state == Migrator::EXPORT_WARNING &&
	     migrator->export_has_warned(parent_dir,from)) ||
	    export_state == Migrator::EXPORT_EXPORTING ||
	    export_state == Migrator::EXPORT_LOGGINGFINISH ||
	    (export_state == Migrator::EXPORT_NOTIFYING &&
	     !migrator->export_has_notified(parent_dir,from))))) {

	// not auth.
	dout(7) << "delaying nonauth|warned expires for " << *parent_dir << dendl;
	ceph_assert(parent_dir->is_frozen_tree_root());
	
	// make a message container

        auto em = delayed_expire[parent_dir].emplace(std::piecewise_construct, std::forward_as_tuple(from), std::forward_as_tuple());
        if (em.second)
	  em.first->second = make_message<MCacheExpire>(from); /* new */

	// merge these expires into it
	em.first->second->add_realm(p.first, p.second);
	continue;
      }
      ceph_assert(export_state <= Migrator::EXPORT_PREPPING ||
             (export_state == Migrator::EXPORT_WARNING &&
              !migrator->export_has_warned(parent_dir, from)));

      dout(7) << "expires for " << *parent_dir << dendl;
    } else {
      dout(7) << "containerless expires (root, stray inodes)" << dendl;
    }

    // INODES
    for (const auto &q : p.second.inodes) {
      CInode *in = get_inode(q.first);
      unsigned nonce = q.second;
      
      if (!in) {
	dout(0) << " inode expire on " << q.first << " from " << from 
		<< ", don't have it" << dendl;
	ceph_assert(in);
      }        
      ceph_assert(in->is_auth());
      dout(20) << __func__ << ": expiring inode " << *in << dendl;
      
      // check nonce
      if (nonce == in->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " inode expire on " << *in << " from mds." << from 
		<< " cached_by was " << in->get_replicas() << dendl;
	inode_remove_replica(in, from, false, gather_locks);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " inode expire on " << *in << " from mds." << from
		<< " with old nonce " << nonce
		<< " (current " << in->get_replica_nonce(from) << "), dropping" 
		<< dendl;
      }
    }
    
    // DIRS
    for (const auto &q : p.second.dirs) {
      CDir *dir = get_dirfrag(q.first);
      unsigned nonce = q.second;
      
      if (!dir) {
	CInode *diri = get_inode(q.first.ino);
	if (diri) {
	  if (mds->is_rejoin() &&
	      rejoin_ack_gather.count(mds->get_nodeid()) && // haven't sent rejoin ack yet
	      !diri->is_replica(from)) {
	    auto&& ls = diri->get_nested_dirfrags();
	    dout(7) << " dir expire on dirfrag " << q.first << " from mds." << from
		    << " while rejoining, inode isn't replicated" << dendl;
	    for (const auto& d : ls) {
	      dir = d;
	      if (dir->is_replica(from)) {
		dout(7) << " dir expire on " << *dir << " from mds." << from << dendl;
		dir->remove_replica(from);
	      }
	    }
	    continue;
	  }
	  CDir *other = diri->get_approx_dirfrag(q.first.frag);
	  if (other) {
	    dout(7) << " dir expire on dirfrag " << q.first << " from mds." << from
		    << " have " << *other << ", mismatched frags, dropping" << dendl;
	    continue;
	  }
	}
	dout(0) << " dir expire on " << q.first << " from " << from
		<< ", don't have it" << dendl;
	ceph_assert(dir);
      }
      dout(20) << __func__ << ": expiring dirfrag " << *dir << dendl;

      ceph_assert(dir->is_auth());

      // check nonce
      if (nonce == dir->get_replica_nonce(from)) {
	// remove from our cached_by
	dout(7) << " dir expire on " << *dir << " from mds." << from
		<< " replicas was " << dir->get_replicas() << dendl;
	dir->remove_replica(from);
      } 
      else {
	// this is an old nonce, ignore expire.
	dout(7) << " dir expire on " << *dir << " from mds." << from 
		<< " with old nonce " << nonce << " (current " << dir->get_replica_nonce(from)
		<< "), dropping" << dendl;
      }
    }
    
    // DENTRIES
    for (const auto &pd : p.second.dentries) {
      dout(10) << " dn expires in dir " << pd.first << dendl;
      CInode *diri = get_inode(pd.first.ino);
      ceph_assert(diri);
      CDir *dir = diri->get_dirfrag(pd.first.frag);
      
      if (!dir) {
	dout(0) << " dn expires on " << pd.first << " from " << from
		<< ", must have refragmented" << dendl;
      } else {
	ceph_assert(dir->is_auth());
      }
      
      for (const auto &p : pd.second) {
	unsigned nonce = p.second;
	CDentry *dn;
	
	if (dir) {
	  dn = dir->lookup(p.first.first, p.first.second);
	} else {
	  // which dirfrag for this dentry?
	  CDir *dir = diri->get_dirfrag(diri->pick_dirfrag(p.first.first));
	  ceph_assert(dir); 
	  ceph_assert(dir->is_auth());
	  dn = dir->lookup(p.first.first, p.first.second);
	}

	if (!dn) { 
	  if (dir)
	    dout(0) << "  missing dentry for " << p.first.first << " snap " << p.first.second << " in " << *dir << dendl;
	  else
	    dout(0) << "  missing dentry for " << p.first.first << " snap " << p.first.second << dendl;
	}
	ceph_assert(dn);
	
	if (nonce == dn->get_replica_nonce(from)) {
	  dout(7) << "  dentry_expire on " << *dn << " from mds." << from << dendl;
	  dentry_remove_replica(dn, from, gather_locks);
	} 
	else {
	  dout(7) << "  dentry_expire on " << *dn << " from mds." << from
		  << " with old nonce " << nonce << " (current " << dn->get_replica_nonce(from)
		  << "), dropping" << dendl;
	}
      }
    }
  }

  for (set<SimpleLock*>::iterator p = gather_locks.begin(); p != gather_locks.end(); ++p) {
    if (!(*p)->is_stable())
      mds->locker->eval_gather(*p);
  }
}

void MDCache::process_delayed_expire(CDir *dir)
{
  dout(7) << "process_delayed_expire on " << *dir << dendl;
  for (const auto &p : delayed_expire[dir]) {
    handle_cache_expire(p.second);
  }
  delayed_expire.erase(dir);  
}

void MDCache::discard_delayed_expire(CDir *dir)
{
  dout(7) << "discard_delayed_expire on " << *dir << dendl;
  delayed_expire.erase(dir);  
}

void MDCache::inode_remove_replica(CInode *in, mds_rank_t from, bool rejoin,
				   set<SimpleLock *>& gather_locks)
{
  in->remove_replica(from);
  in->set_mds_caps_wanted(from, 0);
  
  // note: this code calls _eval more often than it needs to!
  // fix lock
  if (in->authlock.remove_replica(from)) gather_locks.insert(&in->authlock);
  if (in->linklock.remove_replica(from)) gather_locks.insert(&in->linklock);
  if (in->snaplock.remove_replica(from)) gather_locks.insert(&in->snaplock);
  if (in->xattrlock.remove_replica(from)) gather_locks.insert(&in->xattrlock);
  if (in->flocklock.remove_replica(from)) gather_locks.insert(&in->flocklock);
  if (in->policylock.remove_replica(from)) gather_locks.insert(&in->policylock);

  // If 'rejoin' is true and the scatter lock is in LOCK_MIX_* state.
  // Don't remove the recovering mds from lock's gathering list because
  // it may hold rejoined wrlocks.
  if (in->dirfragtreelock.remove_replica(from, rejoin)) gather_locks.insert(&in->dirfragtreelock);
  if (in->filelock.remove_replica(from, rejoin)) gather_locks.insert(&in->filelock);
  if (in->nestlock.remove_replica(from, rejoin)) gather_locks.insert(&in->nestlock);
}

void MDCache::dentry_remove_replica(CDentry *dn, mds_rank_t from, set<SimpleLock *>& gather_locks)
{
  dn->remove_replica(from);

  // fix lock
  if (dn->lock.remove_replica(from))
    gather_locks.insert(&dn->lock);

  // Replicated strays might now be elegible for purge
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  if (dnl->is_primary()) {
    maybe_eval_stray(dnl->get_inode());
  }
}

void MDCache::trim_client_leases()
{
  utime_t now = ceph_clock_now();
  
  dout(10) << "trim_client_leases" << dendl;

  std::size_t pool = 0;
  for (const auto& list : client_leases) {
    pool += 1;
    if (list.empty())
      continue;

    auto before = list.size();
    while (!list.empty()) {
      ClientLease *r = list.front();
      if (r->ttl > now) break;
      CDentry *dn = static_cast<CDentry*>(r->parent);
      dout(10) << " expiring client." << r->client << " lease of " << *dn << dendl;
      dn->remove_client_lease(r, mds->locker);
    }
    auto after = list.size();
    dout(10) << "trim_client_leases pool " << pool << " trimmed "
	     << (before-after) << " leases, " << after << " left" << dendl;
  }
}

void MDCache::check_memory_usage()
{
  static MemoryModel mm(g_ceph_context);
  static MemoryModel::snap last;
  mm.sample(&last);
  static MemoryModel::snap baseline = last;

  // check client caps
  ceph_assert(CInode::count() == inode_map.size() + snap_inode_map.size() + num_shadow_inodes);
  double caps_per_inode = 0.0;
  if (CInode::count())
    caps_per_inode = (double)Capability::count() / (double)CInode::count();

  dout(2) << "Memory usage: "
	   << " total " << last.get_total()
	   << ", rss " << last.get_rss()
	   << ", heap " << last.get_heap()
	   << ", baseline " << baseline.get_heap()
	   << ", " << num_inodes_with_caps << " / " << CInode::count() << " inodes have caps"
	   << ", " << Capability::count() << " caps, " << caps_per_inode << " caps per inode"
	   << dendl;

  mds->update_mlogger();
  mds->mlogger->set(l_mdm_rss, last.get_rss());
  mds->mlogger->set(l_mdm_heap, last.get_heap());
}



// =========================================================================================
// shutdown

class C_MDC_ShutdownCheck : public MDCacheContext {
public:
  explicit C_MDC_ShutdownCheck(MDCache *m) : MDCacheContext(m) {}
  void finish(int) override {
    mdcache->shutdown_check();
  }
};

void MDCache::shutdown_check()
{
  dout(0) << "shutdown_check at " << ceph_clock_now() << dendl;

  // cache
  char old_val[32] = { 0 };
  char *o = old_val;
  g_conf().get_val("debug_mds", &o, sizeof(old_val));
  g_conf().set_val("debug_mds", "10");
  g_conf().apply_changes(nullptr);
  show_cache();
  g_conf().set_val("debug_mds", old_val);
  g_conf().apply_changes(nullptr);
  mds->timer.add_event_after(g_conf()->mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << "/" << bottom_lru.lru_get_size() << dendl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << dendl;


  if (mds->objecter->is_active()) {
    dout(0) << "objecter still active" << dendl;
    mds->objecter->dump_active();
  }
}


void MDCache::shutdown_start()
{
  dout(5) << "shutdown_start" << dendl;

  if (g_conf()->mds_shutdown_check)
    mds->timer.add_event_after(g_conf()->mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  //  g_conf()->debug_mds = 10;
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << dendl;
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_START);

  if (mds->is_stopped()) {
    dout(7) << " already shut down" << dendl;
    show_cache();
    show_subtrees();
    return true;
  }

  // empty stray dir
  bool strays_all_exported = shutdown_export_strays();

  // trim cache
  trim(UINT64_MAX);
  dout(5) << "lru size now " << lru.lru_get_size() << "/" << bottom_lru.lru_get_size() << dendl;
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_POSTTRIM);

  // Export all subtrees to another active (usually rank 0) if not rank 0
  int num_auth_subtree = 0;
  if (!subtrees.empty() && mds->get_nodeid() != 0) {
    dout(7) << "looking for subtrees to export" << dendl;
    std::vector<CDir*> ls;
    for (auto& [dir, bounds] : subtrees) {
      dout(10) << "  examining " << *dir << " bounds " << bounds << dendl;
      if (dir->get_inode()->is_mdsdir() || !dir->is_auth())
	continue;
      num_auth_subtree++;
      if (dir->is_frozen() ||
          dir->is_freezing() ||
          dir->is_ambiguous_dir_auth() ||
          dir->state_test(CDir::STATE_EXPORTING) ||
          dir->get_inode()->is_ephemerally_pinned()) {
        continue;
      }
      ls.push_back(dir);
    }

    migrator->clear_export_queue();
    // stopping mds does not call MDBalancer::tick()
    mds->balancer->handle_export_pins();
    for (const auto& dir : ls) {
      mds_rank_t dest = dir->get_inode()->authority().first;
      if (dest > 0 && !mds->mdsmap->is_active(dest))
	dest = 0;
      dout(7) << "sending " << *dir << " back to mds." << dest << dendl;
      migrator->export_dir_nicely(dir, dest);
      ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_POSTONEEXPORT);
    }
  }

  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_POSTALLEXPORTS);

  if (!strays_all_exported) {
    dout(7) << "waiting for strays to migrate" << dendl;
    return false;
  }

  if (num_auth_subtree > 0) {
    ceph_assert(mds->get_nodeid() > 0);
    dout(7) << "still have " << num_auth_subtree << " auth subtrees" << dendl;
    show_subtrees();
    return false;
  }

  // close out any sessions (and open files!) before we try to trim the log, etc.
  if (mds->sessionmap.have_unclosed_sessions()) {
    if (!mds->server->terminating_sessions)
      mds->server->terminate_sessions();
    return false;
  }
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_SESSIONTERMINATE);

  // Fully trim the log so that all objects in cache are clean and may be
  // trimmed by a future MDCache::trim. Note that MDSRank::tick does not
  // trim the log such that the cache eventually becomes clean.
  if (mds->mdlog->get_num_segments() > 0 && !mds->mdlog->is_capped()) {
    auto ls = mds->mdlog->get_current_segment();
    if (ls->num_events > 1 || !ls->dirty_dirfrags.empty()) {
      // Current segment contains events other than subtreemap or
      // there are dirty dirfrags (see CDir::log_mark_dirty())
      auto sle = create_subtree_map();
      mds->mdlog->submit_entry(sle);
      mds->mdlog->flush();
      ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_SUBTREEMAP);
    }
  }
  mds->mdlog->trim_all();
  if (mds->mdlog->get_num_segments() > 1) {
    dout(7) << "still >1 segments, waiting for log to trim" << dendl;
    return false;
  }
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_TRIMALL);

  // drop our reference to our stray dir inode
  for (int i = 0; i < NUM_STRAY; ++i) {
    if (strays[i] &&
	strays[i]->state_test(CInode::STATE_STRAYPINNED)) {
      strays[i]->state_clear(CInode::STATE_STRAYPINNED);
      strays[i]->put(CInode::PIN_STRAY);
      strays[i]->put_stickydirs();
    }
  }
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_STRAYPUT);

  CDir *mydir = myin ? myin->get_dirfrag(frag_t()) : NULL;
  if (mydir && !mydir->is_subtree_root())
    mydir = NULL;

  // subtrees map not empty yet?
  if (subtrees.size() > (mydir ? 1 : 0)) {
    dout(7) << "still have " << num_subtrees() << " subtrees" << dendl;
    show_subtrees();
    migrator->show_importing();
    migrator->show_exporting();
    if (!migrator->is_importing() && !migrator->is_exporting())
      show_cache();
    return false;
  }
  ceph_assert(!migrator->is_exporting());
  ceph_assert(!migrator->is_importing());

  // replicas may dirty scatter locks
  if (myin && myin->is_replicated()) {
    dout(7) << "still have replicated objects" << dendl;
    return false;
  }

  if ((myin && myin->get_num_auth_pins()) ||
      (mydir && (mydir->get_auth_pins() || mydir->get_dir_auth_pins()))) {
    dout(7) << "still have auth pinned objects" << dendl;
    return false;
  }

  // (only do this once!)
  if (!mds->mdlog->is_capped()) {
    dout(7) << "capping the mdlog" << dendl;
    mds->mdlog->submit_entry(new ELid());
    mds->mdlog->flush();
    mds->mdlog->cap();
    ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_LOGCAP);
    return false;
  }

  // filer active?
  if (mds->objecter->is_active()) {
    dout(7) << "objecter still active" << dendl;
    mds->objecter->dump_active();
    return false;
  }

  // trim what we can from the cache
  if (lru.lru_get_size() > 0 || bottom_lru.lru_get_size() > 0) {
    dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << "/" << bottom_lru.lru_get_size()  << dendl;
    show_cache();
    //dump();
    return false;
  }

  // make mydir subtree go away
  if (mydir) {
    if (mydir->get_num_ref() > 1) { // subtree pin
      dout(7) << "there's still reference to mydir " << *mydir << dendl;
      show_cache();
      return false;
    }

    remove_subtree(mydir);
    myin->close_dirfrag(mydir->get_frag());
  }
  ceph_assert(subtrees.empty());
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_EMPTYSUBTREES);

  if (myin) {
    remove_inode(myin);
    ceph_assert(!myin);
  }
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_MYINREMOVAL);

  if (global_snaprealm) {
    remove_inode(global_snaprealm->inode);
    global_snaprealm = nullptr;
  }
  ceph_assert(kill_shutdown_at != KILL_SHUTDOWN_AT::SHUTDOWN_GLOBALSNAPREALMREMOVAL);
  
  // done!
  dout(5) << "shutdown done." << dendl;
  return true;
}

bool MDCache::shutdown_export_strays()
{
  static const unsigned MAX_EXPORTING = 100;

  if (mds->get_nodeid() == 0)
    return true;

  if (shutdown_exporting_strays.size() * 3 >= MAX_EXPORTING * 2)
    return false;

  dout(10) << "shutdown_export_strays " << shutdown_export_next.first
	   << " '" << shutdown_export_next.second << "'" << dendl;

  bool mds0_active = mds->mdsmap->is_active(mds_rank_t(0));
  bool all_exported = false;

again:
  auto next = shutdown_export_next;

  for (int i = 0; i < NUM_STRAY; ++i) {
    CInode *strayi = strays[i];
    if (!strayi ||
	!strayi->state_test(CInode::STATE_STRAYPINNED))
      continue;
    if (strayi->ino() < next.first.ino)
      continue;

    deque<CDir*> dfls;
    strayi->get_dirfrags(dfls);

    while (!dfls.empty()) {
      CDir *dir = dfls.front();
      dfls.pop_front();

      if (dir->dirfrag() < next.first)
	continue;
      if (next.first < dir->dirfrag()) {
	next.first = dir->dirfrag();
	next.second.clear();
      }

      if (!dir->is_complete()) {
	MDSContext *fin = nullptr;
	if (shutdown_exporting_strays.empty()) {
	  fin = new MDSInternalContextWrapper(mds,
		  new LambdaContext([this](int r) {
		    shutdown_export_strays();
		  })
		);
	}
	dir->fetch(fin);
	goto done;
      }

      CDir::dentry_key_map::iterator it;
      if (next.second.empty()) {
	it = dir->begin();
      } else {
	auto hash = ceph_frag_value(strayi->hash_dentry_name(next.second));
	it = dir->lower_bound(dentry_key_t(0, next.second, hash));
      }

      for (; it != dir->end(); ++it) {
	CDentry *dn = it->second;
	CDentry::linkage_t *dnl = dn->get_projected_linkage();
	if (dnl->is_null())
	  continue;

	if (!mds0_active && !dn->state_test(CDentry::STATE_PURGING)) {
	  next.second = it->first.name;
	  goto done;
	}

	auto ret = shutdown_exporting_strays.insert(dnl->get_inode()->ino());
	if (!ret.second) {
	  dout(10) << "already exporting/purging " << *dn << dendl;
	  continue;
	}

	// Don't try to migrate anything that is actually
	// being purged right now
	if (!dn->state_test(CDentry::STATE_PURGING))
	  stray_manager.migrate_stray(dn, mds_rank_t(0));  // send to root!

	if (shutdown_exporting_strays.size() >= MAX_EXPORTING) {
	  ++it;
	  if (it != dir->end()) {
	    next.second = it->first.name;
	  } else {
	    if (dfls.empty())
	      next.first.ino.val++;
	    else
	      next.first = dfls.front()->dirfrag();
	    next.second.clear();
	  }
	  goto done;
	}
      }
    }
  }

  if (shutdown_exporting_strays.empty()) {
    dirfrag_t first_df(MDS_INO_STRAY(mds->get_nodeid(), 0), 0);
    if (first_df < shutdown_export_next.first ||
	!shutdown_export_next.second.empty()) {
      shutdown_export_next.first = first_df;
      shutdown_export_next.second.clear();
      goto again;
    }
    all_exported = true;
  }

done:
  shutdown_export_next = next;
  return all_exported;
}

// ========= messaging ==============

void MDCache::dispatch(const cref_t<Message> &m)
{
  switch (m->get_type()) {

    // RESOLVE
  case MSG_MDS_RESOLVE:
    handle_resolve(ref_cast<MMDSResolve>(m));
    break;
  case MSG_MDS_RESOLVEACK:
    handle_resolve_ack(ref_cast<MMDSResolveAck>(m));
    break;

    // REJOIN
  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin(ref_cast<MMDSCacheRejoin>(m));
    break;

  case MSG_MDS_DISCOVER:
    handle_discover(ref_cast<MDiscover>(m));
    break;
  case MSG_MDS_DISCOVERREPLY:
    handle_discover_reply(ref_cast<MDiscoverReply>(m));
    break;

  case MSG_MDS_DIRUPDATE:
    handle_dir_update(ref_cast<MDirUpdate>(m));
    break;

  case MSG_MDS_CACHEEXPIRE:
    handle_cache_expire(ref_cast<MCacheExpire>(m));
    break;

  case MSG_MDS_DENTRYLINK:
    handle_dentry_link(ref_cast<MDentryLink>(m));
    break;
  case MSG_MDS_DENTRYUNLINK:
    handle_dentry_unlink(ref_cast<MDentryUnlink>(m));
    break;

  case MSG_MDS_FRAGMENTNOTIFY:
    handle_fragment_notify(ref_cast<MMDSFragmentNotify>(m));
    break;
  case MSG_MDS_FRAGMENTNOTIFYACK:
    handle_fragment_notify_ack(ref_cast<MMDSFragmentNotifyAck>(m));
    break;

  case MSG_MDS_FINDINO:
    handle_find_ino(ref_cast<MMDSFindIno>(m));
    break;
  case MSG_MDS_FINDINOREPLY:
    handle_find_ino_reply(ref_cast<MMDSFindInoReply>(m));
    break;

  case MSG_MDS_OPENINO:
    handle_open_ino(ref_cast<MMDSOpenIno>(m));
    break;
  case MSG_MDS_OPENINOREPLY:
    handle_open_ino_reply(ref_cast<MMDSOpenInoReply>(m));
    break;

  case MSG_MDS_SNAPUPDATE:
    handle_snap_update(ref_cast<MMDSSnapUpdate>(m));
    break;
    
  default:
    derr << "cache unknown message " << m->get_type() << dendl;
    ceph_abort_msg("cache unknown message");
  }
}

/**
 * In 246f647566095c173e5e0e54661696cea230f96e, an updated rule for locking order
 * was established (differing from past strategies):
 *
 *  [The helper function is for requests that operate on two paths. It
 *  ensures that the two paths get locks in proper order.] The rule is:
 *
 *   1. Lock directory inodes or dentries according to which trees they
 *      are under. Lock objects under fs root before objects under mdsdir.
 *   2. Lock directory inodes or dentries according to their depth, in
 *      ascending order.
 *   3. Lock directory inodes or dentries according to inode numbers or
 *      dentries' parent inode numbers, in ascending order.
 *   4. Lock dentries in the same directory in order of their keys.
 *   5. Lock non-directory inodes according to inode numbers, in ascending
 *      order.
 */

int MDCache::path_traverse(const MDRequestRef& mdr, MDSContextFactory& cf,
                           const filepath& path, int flags,
                           vector<CDentry*> *pdnvec, CInode **pin)
{
  bool discover = (flags & MDS_TRAVERSE_DISCOVER);
  bool forward = !discover;
  bool path_locked = (flags & MDS_TRAVERSE_PATH_LOCKED);
  bool want_dentry = (flags & MDS_TRAVERSE_WANT_DENTRY);
  bool want_inode = (flags & MDS_TRAVERSE_WANT_INODE);
  bool want_auth = (flags & MDS_TRAVERSE_WANT_AUTH);
  bool rdlock_snap = (flags & (MDS_TRAVERSE_RDLOCK_SNAP | MDS_TRAVERSE_RDLOCK_SNAP2));
  bool rdlock_path = (flags & MDS_TRAVERSE_RDLOCK_PATH);
  bool xlock_dentry = (flags & MDS_TRAVERSE_XLOCK_DENTRY);
  bool rdlock_authlock = (flags & MDS_TRAVERSE_RDLOCK_AUTHLOCK);

  if (forward)
    ceph_assert(mdr);  // forward requires a request

  snapid_t snapid = CEPH_NOSNAP;
  if (mdr)
    mdr->snapid = snapid;

  client_t client = mdr ? mdr->get_client() : -1;

  if (mds->logger) mds->logger->inc(l_mds_traverse);

  dout(7) << "traverse: opening base ino " << path.get_ino() << " snap " << snapid << dendl;
  CInode *cur = get_inode(path.get_ino());
  if (!cur) {
    if (MDS_INO_IS_MDSDIR(path.get_ino())) {
      open_foreign_mdsdir(path.get_ino(), cf.build());
      return 1;
    }
    if (MDS_INO_IS_STRAY(path.get_ino())) {
      mds_rank_t rank = MDS_INO_STRAY_OWNER(path.get_ino());
      unsigned idx = MDS_INO_STRAY_INDEX(path.get_ino());
      filepath path(strays[idx]->get_parent_dn()->get_name(),
		    MDS_INO_MDSDIR(rank));
      MDRequestRef null_ref;
      return path_traverse(null_ref, cf, path, MDS_TRAVERSE_DISCOVER, nullptr);
    }
    return -CEPHFS_ESTALE;
  }
  if (cur->state_test(CInode::STATE_PURGING))
    return -CEPHFS_ESTALE;

  if (flags & MDS_TRAVERSE_CHECK_LOCKCACHE)
    mds->locker->find_and_attach_lock_cache(mdr, cur);

  if (mdr && mdr->lock_cache) {
    if (flags & MDS_TRAVERSE_WANT_DIRLAYOUT)
      mdr->dir_layout = mdr->lock_cache->get_dir_layout();
  } else if (rdlock_snap) {
    int n = (flags & MDS_TRAVERSE_RDLOCK_SNAP2) ? 1 : 0;
    if ((n == 0 && !(mdr->locking_state & MutationImpl::SNAP_LOCKED)) ||
	(n == 1 && !(mdr->locking_state & MutationImpl::SNAP2_LOCKED))) {
      bool want_layout = (flags & MDS_TRAVERSE_WANT_DIRLAYOUT);
      if (!mds->locker->try_rdlock_snap_layout(cur, mdr, n, want_layout))
	return 1;
    }
  }

  // start trace
  if (pdnvec)
    pdnvec->clear();
  if (pin)
    *pin = cur;

  CInode *target_inode = nullptr;
  MutationImpl::LockOpVec lov;
  int r;

  for (unsigned depth = 0; depth < path.depth(); ) {
    dout(12) << "traverse: path seg depth " << depth << " '" << path[depth]
	     << "' snapid " << snapid << dendl;
    
    if (!cur->is_dir()) {
      dout(7) << "traverse: " << *cur << " not a dir " << dendl;
      return -CEPHFS_ENOTDIR;
    }

    // walk into snapdir?
    if (path[depth].length() == 0) {
      dout(10) << "traverse: snapdir" << dendl;
      if (!mdr || depth > 0) // snapdir must be the first component
	return -CEPHFS_EINVAL;
      snapid = CEPH_SNAPDIR;
      mdr->snapid = snapid;
      depth++;
      continue;
    }
    // walk thru snapdir?
    if (snapid == CEPH_SNAPDIR) {
      if (!mdr)
	return -CEPHFS_EINVAL;
      SnapRealm *realm = cur->find_snaprealm();
      snapid = realm->resolve_snapname(path[depth], cur->ino());
      dout(10) << "traverse: snap " << path[depth] << " -> " << snapid << dendl;
      if (!snapid) {
	if (pdnvec)
	  pdnvec->clear();   // do not confuse likes of rdlock_path_pin_ref();
	return -CEPHFS_ENOENT;
      }
      if (depth == path.depth() - 1)
	target_inode = cur;
      mdr->snapid = snapid;
      depth++;
      continue;
    }

    /* path_traverse may add dentries to cache: acquire quiescelock */
    lov.clear();
    lov.add_rdlock(&cur->quiescelock);
    if (!mds->locker->acquire_locks(mdr, lov)) {
      return 1;
    }

    // open dir
    frag_t fg = cur->pick_dirfrag(path[depth]);
    CDir *curdir = cur->get_dirfrag(fg);
    if (!curdir) {
      if (cur->is_auth()) {
        // parent dir frozen_dir?
        if (cur->is_frozen()) {
          dout(7) << "traverse: " << *cur << " is frozen, waiting" << dendl;
          cur->add_waiter(CDir::WAIT_UNFREEZE, cf.build());
          return 1;
        }
        curdir = cur->get_or_open_dirfrag(this, fg);
      } else {
        // discover?
	dout(10) << "traverse: need dirfrag " << fg << ", doing discover from " << *cur << dendl;
	discover_path(cur, snapid, path.postfixpath(depth), cf.build(),
		      path_locked);
	if (mds->logger) mds->logger->inc(l_mds_traverse_discover);
        return 1;
      }
    }
    ceph_assert(curdir);

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
      curdir->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req, fin));
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // Defer the auth check until the target inode is determined not to exist
    // if want_inode is true.
    if (want_auth && want_dentry && !want_inode && depth == path.depth() - 1 &&
        (r = maybe_request_forward_to_auth(mdr, cf, curdir)) != 0)
      return r;

    // Before doing dirfrag->dn lookup, compare with DamageTable's
    // record of which dentries were unreadable
    if (mds->damage_table.is_dentry_damaged(curdir, path[depth], snapid)) {
      dout(4) << "traverse: stopped lookup at damaged dentry "
              << *curdir << "/" << path[depth] << " snap=" << snapid << dendl;
      return -CEPHFS_EIO;
    }

    // dentry
    CDentry *dn = curdir->lookup(path[depth], snapid);
    if (dn) {
      if (dn->state_test(CDentry::STATE_PURGING))
	return -CEPHFS_ENOENT;

      CDentry::linkage_t *dnl = dn->get_projected_linkage();
      // If an auth check was deferred before and the target inode is found
      // not to exist now, do the auth check here if necessary.
      if (want_auth && want_dentry && want_inode && depth == path.depth() - 1 &&
	  dnl->is_null() && (r = maybe_request_forward_to_auth(mdr, cf, dn)) != 0)
	return r;

      if (rdlock_path) {
	lov.clear();
	// do not xlock the tail dentry if target inode exists and caller wants it
	if (xlock_dentry && (dnl->is_null() || !want_inode) &&
	    depth == path.depth() - 1) {
	  ceph_assert(dn->is_auth());
	  if (depth > 0 || !mdr->lock_cache) {
	    lov.add_wrlock(&cur->filelock);
	    lov.add_wrlock(&cur->nestlock);
	    if (rdlock_authlock)
	      lov.add_rdlock(&cur->authlock);
	  }
	  lov.add_xlock(&dn->lock);
	} else {
	  // force client to flush async dir operation if necessary
	  if (cur->filelock.is_cached())
	    lov.add_wrlock(&cur->filelock);
	  lov.add_rdlock(&dn->lock);
	}
	if (!mds->locker->acquire_locks(mdr, lov)) {
	  dout(10) << "traverse: failed to rdlock " << dn->lock << " " << *dn << dendl;
	  return 1;
	}
      } else if (!path_locked &&
		 !dn->lock.can_read(client) &&
		 !(dn->lock.is_xlocked() && dn->lock.get_xlock_by() == mdr)) {
	dout(10) << "traverse: non-readable dentry at " << *dn << dendl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, cf.build());
	if (mds->logger)
	  mds->logger->inc(l_mds_traverse_lock);
	if (dn->is_auth() && dn->lock.is_unstable_and_locked())
	  mds->mdlog->flush();
	return 1;
      }

      if (pdnvec)
	pdnvec->push_back(dn);

      // can we conclude CEPHFS_ENOENT?
      if (dnl->is_null()) {
	dout(10) << "traverse: null+readable dentry at " << *dn << dendl;
	if (depth == path.depth() - 1) {
	  if (want_dentry)
	    break;
	} else {
	  if (pdnvec)
	    pdnvec->clear();   // do not confuse likes of rdlock_path_pin_ref();
	}
	return -CEPHFS_ENOENT;
      }

      // do we have inode?
      CInode *in = dnl->get_inode();
      if (!in) {
        ceph_assert(dnl->is_remote());
        // do i have it?
        in = get_inode(dnl->get_remote_ino());
        if (in) {
	  dout(7) << "linking in remote in " << *in << dendl;
	  dn->link_remote(dnl, in);
	} else {
          dout(7) << "remote link to " << dnl->get_remote_ino() << ", which i don't have" << dendl;
	  ceph_assert(mdr);  // we shouldn't hit non-primary dentries doing a non-mdr traversal!
          if (mds->damage_table.is_remote_damaged(dnl->get_remote_ino())) {
            dout(4) << "traverse: remote dentry points to damaged ino "
                    << *dn << dendl;
            return -CEPHFS_EIO;
          }
          open_remote_dentry(dn, true, cf.build(),
			     (path_locked && depth == path.depth() - 1));
	  if (mds->logger) mds->logger->inc(l_mds_traverse_remote_ino);
          return 1;
        }
      }

      cur = in;

      if (rdlock_snap && !(want_dentry && !want_inode && depth == path.depth() - 1)) {
	lov.clear();
	lov.add_rdlock(&cur->snaplock);
	if (!mds->locker->acquire_locks(mdr, lov)) {
	  dout(10) << "traverse: failed to rdlock " << cur->snaplock << " " << *cur << dendl;
	  return 1;
	}
      }

      if (depth == path.depth() - 1)
	target_inode = cur;

      // add to trace, continue.
      touch_inode(cur);
      if (pin)
	*pin = cur;
      depth++;
      continue;
    }

    ceph_assert(!dn);

    // MISS.  dentry doesn't exist.
    dout(12) << "traverse: miss on dentry " << path[depth] << " in " << *curdir << dendl;

    if (curdir->is_auth()) {
      // dentry is mine.
      if (curdir->is_complete() ||
	  (snapid == CEPH_NOSNAP &&
	   curdir->has_bloom() &&
	   !curdir->is_in_bloom(path[depth]))) {
        // file not found
	if (pdnvec) {
	  // instantiate a null dn?
	  if (depth < path.depth() - 1) {
	    dout(20) << " didn't traverse full path; not returning pdnvec" << dendl;
	  } else if (snapid < CEPH_MAXSNAP) {
	    dout(20) << " not adding null for snapid " << snapid << dendl;
	  } else if (curdir->is_frozen()) {
	    dout(7) << "traverse: " << *curdir << " is frozen, waiting" << dendl;
	    curdir->add_waiter(CDir::WAIT_UNFREEZE, cf.build());
	    return 1;
	  } else {
	    // create a null dentry
	    dn = curdir->add_null_dentry(path[depth]);
	    dout(20) << " added null " << *dn << dendl;

	    if (rdlock_path) {
	      lov.clear();
	      if (xlock_dentry) {
		if (depth > 0 || !mdr->lock_cache) {
		  lov.add_wrlock(&cur->filelock);
		  lov.add_wrlock(&cur->nestlock);
		  if (rdlock_authlock)
		    lov.add_rdlock(&cur->authlock);
		}
		lov.add_xlock(&dn->lock);
	      } else {
		// force client to flush async dir operation if necessary
		if (cur->filelock.is_cached())
		  lov.add_wrlock(&cur->filelock);
		lov.add_rdlock(&dn->lock);
	      }
	      if (!mds->locker->acquire_locks(mdr, lov)) {
		dout(10) << "traverse: failed to rdlock " << dn->lock << " " << *dn << dendl;
		return 1;
	      }
	    }
	  }
	  if (dn) {
	    pdnvec->push_back(dn);
	    if (want_dentry)
	      break;
	  } else {
	    pdnvec->clear();   // do not confuse likes of rdlock_path_pin_ref();
	  }
	}
        return -CEPHFS_ENOENT;
      } else {

        // Check DamageTable for missing fragments before trying to fetch
        // this
        if (mds->damage_table.is_dirfrag_damaged(curdir)) {
          dout(4) << "traverse: damaged dirfrag " << *curdir
                  << ", blocking fetch" << dendl;
          return -CEPHFS_EIO;
        }

	// directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << dendl;
        touch_inode(cur);
        curdir->fetch(path[depth], snapid, cf.build());
	if (mds->logger) mds->logger->inc(l_mds_traverse_dir_fetch);
        return 1;
      }
    } else {
      // dirfrag/dentry is not mine.

      if (forward &&
	  mdr && mdr->client_request &&
	  (int)depth < mdr->client_request->get_num_fwd()){
	dout(7) << "traverse: snap " << snapid << " and depth " << depth
		<< " < fwd " << mdr->client_request->get_num_fwd()
		<< ", discovering instead of forwarding" << dendl;
	discover = true;
      }

      if (discover) {
	dout(7) << "traverse: discover from " << path[depth] << " from " << *curdir << dendl;
	discover_path(curdir, snapid, path.postfixpath(depth), cf.build(),
		      path_locked);
	if (mds->logger) mds->logger->inc(l_mds_traverse_discover);
        return 1;
      } 
      if (forward) {
        // forward
        dout(7) << "traverse: not auth for " << path << " in " << *curdir << dendl;

	r = maybe_request_forward_to_auth(mdr, cf, curdir);
	ceph_assert(r != 0);

	if (r == 2 && mds->logger)
	  mds->logger->inc(l_mds_traverse_forward);

	return r;
      }
    }

    ceph_abort();  // i shouldn't get here
  }

  if (path.depth() == 0) {
    dout(7) << "no tail dentry, base " << *cur << dendl;
    if (want_dentry && !want_inode) {
      return -CEPHFS_ENOENT;
    }
    target_inode = cur;
  }

  if (target_inode) {
    dout(7) << "found target " << *target_inode << dendl;
    if (want_auth && !(want_dentry && !want_inode) &&
	(r = maybe_request_forward_to_auth(mdr, cf, target_inode)) != 0)
      return r;
  }

  // success.
  if (mds->logger) mds->logger->inc(l_mds_traverse_hit);
  dout(10) << "path_traverse finish on snapid " << snapid << dendl;
  if (mdr)
    ceph_assert(mdr->snapid == snapid);

  if (flags & MDS_TRAVERSE_RDLOCK_SNAP)
    mdr->locking_state |= MutationImpl::SNAP_LOCKED;
  else if (flags & MDS_TRAVERSE_RDLOCK_SNAP2)
    mdr->locking_state |= MutationImpl::SNAP2_LOCKED;

  if (rdlock_path)
    mdr->locking_state |= MutationImpl::PATH_LOCKED;

  return 0;
}

int MDCache::maybe_request_forward_to_auth(const MDRequestRef& mdr, MDSContextFactory& cf,
					   MDSCacheObject *p)
{
  if (p->is_ambiguous_auth()) {
    dout(7) << "waiting for single auth on " << *p << dendl;
    p->add_waiter(CInode::WAIT_SINGLEAUTH, cf.build());
    return 1;
  }
  if (!p->is_auth()) {
    dout(7) << "fw to auth for " << *p << dendl;
    request_forward(mdr, p->authority().first);
    return 2;
  }
  return 0;
}

CInode *MDCache::cache_traverse(const filepath& fp)
{
  dout(10) << "cache_traverse " << fp << dendl;

  CInode *in;
  unsigned depth = 0;
  char mdsdir_name[16];
  sprintf(mdsdir_name, "~mds%d", mds->get_nodeid());

  if (fp.get_ino()) {
    in = get_inode(fp.get_ino());
  } else if (fp.depth() > 0 && (fp[0] == "~mdsdir" || fp[0] == mdsdir_name)) {
    in = myin;
    depth = 1;
  } else {
    in = root;
  }
  if (!in)
    return NULL;

  for (; depth < fp.depth(); depth++) {
    std::string_view dname = fp[depth];
    frag_t fg = in->pick_dirfrag(dname);
    dout(20) << " " << depth << " " << dname << " frag " << fg << " from " << *in << dendl;
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
 * @param diri base inode
 * @param approxfg approximate fragment.
 * @param fin completion callback
 */
void MDCache::open_remote_dirfrag(CInode *diri, frag_t approxfg, MDSContext *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << dendl;
  ceph_assert(diri->is_dir());
  ceph_assert(!diri->is_auth());
  ceph_assert(diri->get_dirfrag(approxfg) == 0);

  discover_dir_frag(diri, approxfg, fin);
}


/** 
 * get_dentry_inode - get or open inode
 *
 * @param dn the dentry
 * @param mdr current request
 *
 * will return inode for primary, or link up/open up remote link's inode as necessary.
 * If it's not available right now, puts mdr on wait list and returns null.
 */
CInode *MDCache::get_dentry_inode(CDentry *dn, const MDRequestRef& mdr, bool projected)
{
  CDentry::linkage_t *dnl;
  if (projected)
    dnl = dn->get_projected_linkage();
  else
    dnl = dn->get_linkage();

  ceph_assert(!dnl->is_null());
  
  if (dnl->is_primary())
    return dnl->inode;

  ceph_assert(dnl->is_remote());
  CInode *in = get_inode(dnl->get_remote_ino());
  if (in) {
    dout(7) << "get_dentry_inode linking in remote in " << *in << dendl;
    dn->link_remote(dnl, in);
    return in;
  } else {
    dout(10) << "get_dentry_inode on remote dn, opening inode for " << *dn << dendl;
    open_remote_dentry(dn, projected, new C_MDS_RetryRequest(this, mdr));
    return 0;
  }
}

struct C_MDC_OpenRemoteDentry : public MDCacheContext {
  CDentry *dn;
  inodeno_t ino;
  MDSContext *onfinish;
  bool want_xlocked;
  C_MDC_OpenRemoteDentry(MDCache *m, CDentry *d, inodeno_t i, MDSContext *f, bool wx) :
    MDCacheContext(m), dn(d), ino(i), onfinish(f), want_xlocked(wx) {
    dn->get(MDSCacheObject::PIN_PTRWAITER);
  }
  void finish(int r) override {
    mdcache->_open_remote_dentry_finish(dn, ino, onfinish, want_xlocked, r);
    dn->put(MDSCacheObject::PIN_PTRWAITER);
  }
};

void MDCache::open_remote_dentry(CDentry *dn, bool projected, MDSContext *fin, bool want_xlocked)
{
  dout(10) << "open_remote_dentry " << *dn << dendl;
  CDentry::linkage_t *dnl = projected ? dn->get_projected_linkage() : dn->get_linkage();
  inodeno_t ino = dnl->get_remote_ino();
  int64_t pool = dnl->get_remote_d_type() == DT_DIR ? mds->get_metadata_pool() : -1;
  open_ino(ino, pool,
      new C_MDC_OpenRemoteDentry(this, dn, ino, fin, want_xlocked), true, want_xlocked); // backtrace
}

void MDCache::_open_remote_dentry_finish(CDentry *dn, inodeno_t ino, MDSContext *fin,
					 bool want_xlocked, int r)
{
  if (r < 0) {
    CDentry::linkage_t *dnl = dn->get_projected_linkage();
    if (dnl->is_remote() && dnl->get_remote_ino() == ino) {
      dout(0) << "open_remote_dentry_finish bad remote dentry " << *dn << dendl;
      dn->state_set(CDentry::STATE_BADREMOTEINO);

      std::string path;
      CDir *dir = dn->get_dir();
      if (dir) {
	dir->get_inode()->make_path_string(path);
	path += "/";
        path += dn->get_name();
      }

      bool fatal = mds->damage_table.notify_remote_damaged(ino, path);
      if (fatal) {
	mds->damaged();
	ceph_abort();  // unreachable, damaged() respawns us
      }
    } else {
      r = 0;
    }
  }
  fin->complete(r < 0 ? r : 0);
}


void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  // empty trace if we're a base inode
  if (in->is_base())
    return;

  CInode *parent = in->get_parent_inode();
  ceph_assert(parent);
  make_trace(trace, parent);

  CDentry *dn = in->get_parent_dn();
  dout(15) << "make_trace adding " << *dn << dendl;
  trace.push_back(dn);
}


// -------------------------------------------------------------------------------
// Open inode by inode number

class C_IO_MDC_OpenInoBacktraceFetched : public MDCacheIOContext {
  inodeno_t ino;
  public:
  bufferlist bl;
  C_IO_MDC_OpenInoBacktraceFetched(MDCache *c, inodeno_t i) :
    MDCacheIOContext(c), ino(i) {}
  void finish(int r) override {
    mdcache->_open_ino_backtrace_fetched(ino, bl, r);
  }
  void print(ostream& out) const override {
    out << "openino_backtrace_fetch" << ino << ")";
  }
};

struct C_MDC_OpenInoTraverseDir : public MDCacheContext {
  inodeno_t ino;
  cref_t<MMDSOpenIno> msg;
  bool parent;
  public:
  C_MDC_OpenInoTraverseDir(MDCache *c, inodeno_t i, const cref_t<MMDSOpenIno> &m,  bool p) :
    MDCacheContext(c), ino(i), msg(m), parent(p) {}
  void finish(int r) override {
    if (r < 0 && !parent)
      r = -CEPHFS_EAGAIN;
    if (msg) {
      mdcache->handle_open_ino(msg, r);
      return;
    }
    auto& info = mdcache->opening_inodes.at(ino);
    mdcache->_open_ino_traverse_dir(ino, info, r);
  }
};

struct C_MDC_OpenInoParentOpened : public MDCacheContext {
  inodeno_t ino;
  public:
  C_MDC_OpenInoParentOpened(MDCache *c, inodeno_t i) : MDCacheContext(c), ino(i) {}
  void finish(int r) override {
    mdcache->_open_ino_parent_opened(ino, r);
  }
};

void MDCache::_open_ino_backtrace_fetched(inodeno_t ino, bufferlist& bl, int err)
{
  dout(10) << "_open_ino_backtrace_fetched ino " << ino << " errno " << err << dendl;

  open_ino_info_t& info = opening_inodes.at(ino);

  CInode *in = get_inode(ino);
  if (in) {
    dout(10) << " found cached " << *in << dendl;
    open_ino_finish(ino, info, in->authority().first);
    return;
  }

  inode_backtrace_t backtrace;
  if (err == 0) {
    try {
      decode(backtrace, bl);
    } catch (const buffer::error &decode_exc) {
      derr << "corrupt backtrace on ino x0" << std::hex << ino
           << std::dec << ": " << decode_exc.what() << dendl;
      open_ino_finish(ino, info, -CEPHFS_EIO);
      return;
    }
    if (backtrace.pool != info.pool && backtrace.pool != -1) {
      dout(10) << " old object in pool " << info.pool
	       << ", retrying pool " << backtrace.pool << dendl;
      info.pool = backtrace.pool;
      C_IO_MDC_OpenInoBacktraceFetched *fin =
	new C_IO_MDC_OpenInoBacktraceFetched(this, ino);
      fetch_backtrace(ino, info.pool, fin->bl,
		      new C_OnFinisher(fin, mds->finisher));
      return;
    }
  } else if (err == -CEPHFS_ENOENT) {
    int64_t meta_pool = mds->get_metadata_pool();
    if (info.pool != meta_pool) {
      dout(10) << " no object in pool " << info.pool
	       << ", retrying pool " << meta_pool << dendl;
      info.pool = meta_pool;
      C_IO_MDC_OpenInoBacktraceFetched *fin =
	new C_IO_MDC_OpenInoBacktraceFetched(this, ino);
      fetch_backtrace(ino, info.pool, fin->bl,
		      new C_OnFinisher(fin, mds->finisher));
      return;
    }
    err = 0; // backtrace.ancestors.empty() is checked below
  }

  if (err == 0) {
    if (backtrace.ancestors.empty()) {
      dout(10) << " got empty backtrace " << dendl;
      err = -CEPHFS_ESTALE;
    } else if (!info.ancestors.empty()) {
      if (info.ancestors[0] == backtrace.ancestors[0]) {
	dout(10) << " got same parents " << info.ancestors[0] << " 2 times" << dendl;
	err = -CEPHFS_EINVAL;
      } else {
	info.last_err = 0;
      }
    }
  }
  if (err) {
    dout(0) << " failed to open ino " << ino << " err " << err << "/" << info.last_err << dendl;
    if (info.last_err)
      err = info.last_err;
    open_ino_finish(ino, info, err);
    return;
  }

  dout(10) << " got backtrace " << backtrace << dendl;
  info.ancestors = backtrace.ancestors;

  _open_ino_traverse_dir(ino, info, 0);
}

void MDCache::_open_ino_parent_opened(inodeno_t ino, int ret)
{
  dout(10) << "_open_ino_parent_opened ino " << ino << " ret " << ret << dendl;

  open_ino_info_t& info = opening_inodes.at(ino);

  CInode *in = get_inode(ino);
  if (in) {
    dout(10) << " found cached " << *in << dendl;
    open_ino_finish(ino, info, in->authority().first);
    return;
  }

  if (ret == mds->get_nodeid()) {
    _open_ino_traverse_dir(ino, info, 0);
  } else {
    if (ret >= 0) {
      mds_rank_t checked_rank = mds_rank_t(ret);
      info.check_peers = true;
      info.auth_hint = checked_rank;
      info.checked.erase(checked_rank);
    }
    do_open_ino(ino, info, ret);
  }
}

void MDCache::_open_ino_traverse_dir(inodeno_t ino, open_ino_info_t& info, int ret)
{
  dout(10) << __func__ << ": ino " << ino << " ret " << ret << dendl;

  CInode *in = get_inode(ino);
  if (in) {
    dout(10) << " found cached " << *in << dendl;
    open_ino_finish(ino, info, in->authority().first);
    return;
  }

  if (ret) {
    do_open_ino(ino, info, ret);
    return;
  }

  mds_rank_t hint = info.auth_hint;
  ret = open_ino_traverse_dir(ino, NULL, info.ancestors,
			      info.discover, info.want_xlocked, &hint);
  if (ret > 0)
    return;
  if (hint != mds->get_nodeid())
    info.auth_hint = hint;
  do_open_ino(ino, info, ret);
}

void MDCache::_open_ino_fetch_dir(inodeno_t ino, const cref_t<MMDSOpenIno> &m, bool parent,
				  CDir *dir, std::string_view dname)
{
  if (dir->state_test(CDir::STATE_REJOINUNDEF))
    ceph_assert(dir->get_inode()->dirfragtree.is_leaf(dir->get_frag()));

  auto fin = new C_MDC_OpenInoTraverseDir(this, ino, m, parent);
  if (open_ino_batch && !dname.empty()) {
    auto& p = open_ino_batched_fetch[dir];
    p.first.emplace_back(dname);
    p.second.emplace_back(fin);
    return;
  }

  dir->fetch(dname, CEPH_NOSNAP, fin);
  if (mds->logger)
    mds->logger->inc(l_mds_openino_dir_fetch);
}

int MDCache::open_ino_traverse_dir(inodeno_t ino, const cref_t<MMDSOpenIno> &m,
				   const vector<inode_backpointer_t>& ancestors,
				   bool discover, bool want_xlocked, mds_rank_t *hint)
{
  dout(10) << "open_ino_traverse_dir ino " << ino << " " << ancestors << dendl;
  int err = 0;
  for (unsigned i = 0; i < ancestors.size(); i++) {
    const auto& ancestor = ancestors.at(i);
    CInode *diri = get_inode(ancestor.dirino);

    if (!diri) {
      if (discover && MDS_INO_IS_MDSDIR(ancestor.dirino)) {
	open_foreign_mdsdir(ancestor.dirino, new C_MDC_OpenInoTraverseDir(this, ino, m, i == 0));
	return 1;
      }
      continue;
    }

    if (diri->state_test(CInode::STATE_REJOINUNDEF)) {
      CDentry *dn = diri->get_parent_dn();
      CDir *dir = dn->get_dir();
      while (dir->state_test(CDir::STATE_REJOINUNDEF) &&
	     dir->get_inode()->state_test(CInode::STATE_REJOINUNDEF)) {
	dn = dir->get_inode()->get_parent_dn();
	dir = dn->get_dir();
      }
      _open_ino_fetch_dir(ino, m, i == 0, dir, dn->name);
      return 1;
    }

    if (!diri->is_dir()) {
      dout(10) << " " << *diri << " is not dir" << dendl;
      if (i == 0)
	err = -CEPHFS_ENOTDIR;
      break;
    }

    const string& name = ancestor.dname;
    frag_t fg = diri->pick_dirfrag(name);
    CDir *dir = diri->get_dirfrag(fg);
    if (!dir) {
      if (diri->is_auth()) {
	if (diri->is_frozen()) {
	  dout(10) << " " << *diri << " is frozen, waiting " << dendl;
	  diri->add_waiter(CDir::WAIT_UNFREEZE, new C_MDC_OpenInoTraverseDir(this, ino, m, i == 0));
	  return 1;
	}
	dir = diri->get_or_open_dirfrag(this, fg);
      } else if (discover) {
	open_remote_dirfrag(diri, fg, new C_MDC_OpenInoTraverseDir(this, ino, m, i == 0));
	return 1;
      }
    }
    if (dir) {
      inodeno_t next_ino = i > 0 ? ancestors.at(i-1).dirino : ino;
      CDentry *dn = dir->lookup(name);
      CDentry::linkage_t *dnl = dn ? dn->get_linkage() : NULL;
      if (dir->is_auth()) {
	if (dnl && dnl->is_primary() &&
	    dnl->get_inode()->state_test(CInode::STATE_REJOINUNDEF)) {
	  dout(10) << " fetching undef " << *dnl->get_inode() << dendl;
	  _open_ino_fetch_dir(ino, m, i == 0, dir, name);
	  return 1;
	}

	if (!dnl && !dir->is_complete() &&
	    (!dir->has_bloom() || dir->is_in_bloom(name))) {
	  dout(10) << " fetching incomplete " << *dir << dendl;
	  _open_ino_fetch_dir(ino, m, i == 0, dir, name);
	  return 1;
	}

	dout(10) << " no ino " << next_ino << " in " << *dir << dendl;
	if (i == 0)
	  err = -CEPHFS_ENOENT;
      } else if (discover) {
	if (!dnl) {
	  filepath path(name, 0);
	  discover_path(dir, CEPH_NOSNAP, path, new C_MDC_OpenInoTraverseDir(this, ino, m, i == 0),
			(i == 0 && want_xlocked));
	  return 1;
	}
	if (dnl->is_null() && !dn->lock.can_read(-1)) {
	  dout(10) << " null " << *dn << " is not readable, waiting" << dendl;
	  dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDC_OpenInoTraverseDir(this, ino, m, i == 0));
	  return 1;
	}
	dout(10) << " no ino " << next_ino << " in " << *dir << dendl;
	if (i == 0)
	  err = -CEPHFS_ENOENT;
      }
    }
    if (hint && i == 0)
      *hint = dir ? dir->authority().first : diri->authority().first;
    break;
  }
  return err;
}

void MDCache::open_ino_finish(inodeno_t ino, open_ino_info_t& info, int ret)
{
  dout(10) << "open_ino_finish ino " << ino << " ret " << ret << dendl;

  MDSContext::vec waiters;
  waiters.swap(info.waiters);
  opening_inodes.erase(ino);
  finish_contexts(g_ceph_context, waiters, ret);
}

void MDCache::do_open_ino(inodeno_t ino, open_ino_info_t& info, int err)
{
  if (err < 0 && err != -CEPHFS_EAGAIN) {
    info.checked.clear();
    info.checking = MDS_RANK_NONE;
    info.check_peers = true;
    info.fetch_backtrace = true;
    if (info.discover) {
      info.discover = false;
      info.ancestors.clear();
    }
    if (err != -CEPHFS_ENOENT && err != -CEPHFS_ENOTDIR)
      info.last_err = err;
  }

  if (info.check_peers || info.discover) {
    if (info.discover) {
      // got backtrace from peer, but failed to find inode. re-check peers
      info.discover = false;
      info.ancestors.clear();
      info.checked.clear();
    }
    info.check_peers = false;
    info.checking = MDS_RANK_NONE;
    do_open_ino_peer(ino, info);
  } else if (info.fetch_backtrace) {
    info.check_peers = true;
    info.fetch_backtrace = false;
    info.checking = mds->get_nodeid();
    info.checked.clear();
    C_IO_MDC_OpenInoBacktraceFetched *fin =
      new C_IO_MDC_OpenInoBacktraceFetched(this, ino);
    fetch_backtrace(ino, info.pool, fin->bl,
		    new C_OnFinisher(fin, mds->finisher));
  } else {
    ceph_assert(!info.ancestors.empty());
    info.checking = mds->get_nodeid();
    open_ino(info.ancestors[0].dirino, mds->get_metadata_pool(),
	     new C_MDC_OpenInoParentOpened(this, ino), info.want_replica);
  }
}

void MDCache::do_open_ino_peer(inodeno_t ino, open_ino_info_t& info)
{
  set<mds_rank_t> all, active;
  mds->mdsmap->get_mds_set(all);
  if (mds->get_state() == MDSMap::STATE_REJOIN)
    mds->mdsmap->get_mds_set_lower_bound(active, MDSMap::STATE_REJOIN);
  else
    mds->mdsmap->get_mds_set_lower_bound(active, MDSMap::STATE_CLIENTREPLAY);

  dout(10) << "do_open_ino_peer " << ino << " active " << active
	   << " all " << all << " checked " << info.checked << dendl;

  mds_rank_t whoami = mds->get_nodeid();
  mds_rank_t peer = MDS_RANK_NONE;
  if (info.auth_hint >= 0 && info.auth_hint != whoami) {
    if (active.count(info.auth_hint)) {
      peer = info.auth_hint;
      info.auth_hint = MDS_RANK_NONE;
    }
  } else {
    for (set<mds_rank_t>::iterator p = active.begin(); p != active.end(); ++p)
      if (*p != whoami && info.checked.count(*p) == 0) {
	peer = *p;
	break;
      }
  }
  if (peer < 0) {
    all.erase(whoami);
    if (all != info.checked) {
      dout(10) << " waiting for more peers to be active" << dendl;
    } else {
      dout(10) << " all MDS peers have been checked " << dendl;
      do_open_ino(ino, info, 0);
    }
  } else {
    info.checking = peer;
    vector<inode_backpointer_t> *pa = NULL;
    // got backtrace from peer or backtrace just fetched
    if (info.discover || !info.fetch_backtrace)
      pa = &info.ancestors;
    mds->send_message_mds(make_message<MMDSOpenIno>(info.tid, ino, pa), peer);
    if (mds->logger)
      mds->logger->inc(l_mds_openino_peer_discover);
  }
}

void MDCache::handle_open_ino(const cref_t<MMDSOpenIno> &m, int err)
{
  if (mds->get_state() < MDSMap::STATE_REJOIN &&
      mds->get_want_state() != CEPH_MDS_STATE_REJOIN) {
    return;
  }

  dout(10) << "handle_open_ino " << *m << " err " << err << dendl;

  auto from = mds_rank_t(m->get_source().num());
  inodeno_t ino = m->ino;
  ref_t<MMDSOpenInoReply> reply;
  CInode *in = get_inode(ino);
  if (in) {
    dout(10) << " have " << *in << dendl;
    reply = make_message<MMDSOpenInoReply>(m->get_tid(), ino, mds_rank_t(0));
    if (in->is_auth()) {
      touch_inode(in);
      while (1) {
	CDentry *pdn = in->get_parent_dn();
	if (!pdn)
	  break;
	CInode *diri = pdn->get_dir()->get_inode();
	reply->ancestors.push_back(inode_backpointer_t(diri->ino(), pdn->get_name(),
						       in->get_version()));
	in = diri;
      }
    } else {
      reply->hint = in->authority().first;
    }
  } else if (err < 0) {
    reply = make_message<MMDSOpenInoReply>(m->get_tid(), ino, MDS_RANK_NONE, err);
  } else {
    mds_rank_t hint = MDS_RANK_NONE;
    int ret = open_ino_traverse_dir(ino, m, m->ancestors, false, false, &hint);
    if (ret > 0)
      return;
    reply = make_message<MMDSOpenInoReply>(m->get_tid(), ino, hint, ret);
  }
  mds->send_message_mds(reply, from);
}

void MDCache::handle_open_ino_reply(const cref_t<MMDSOpenInoReply> &m)
{
  dout(10) << "handle_open_ino_reply " << *m << dendl;

  inodeno_t ino = m->ino;
  mds_rank_t from = mds_rank_t(m->get_source().num());
  auto it = opening_inodes.find(ino);
  if (it != opening_inodes.end() && it->second.checking == from) {
    open_ino_info_t& info = it->second;
    info.checking = MDS_RANK_NONE;
    info.checked.insert(from);

    CInode *in = get_inode(ino);
    if (in) {
      dout(10) << " found cached " << *in << dendl;
      open_ino_finish(ino, info, in->authority().first);
    } else if (!m->ancestors.empty()) {
      dout(10) << " found ino " << ino << " on mds." << from << dendl;
      if (!info.want_replica) {
	open_ino_finish(ino, info, from);
	return;
      }

      info.ancestors = m->ancestors;
      info.auth_hint = from;
      info.checking = mds->get_nodeid();
      info.discover = true;
      _open_ino_traverse_dir(ino, info, 0);
    } else if (m->error) {
      dout(10) << " error " << m->error << " from mds." << from << dendl;
      do_open_ino(ino, info, m->error);
    } else {
      if (m->hint >= 0 && m->hint != mds->get_nodeid()) {
	info.auth_hint = m->hint;
	info.checked.erase(m->hint);
      }
      do_open_ino_peer(ino, info);
    }
  }
}

void MDCache::kick_open_ino_peers(mds_rank_t who)
{
  dout(10) << "kick_open_ino_peers mds." << who << dendl;

  for (map<inodeno_t, open_ino_info_t>::iterator p = opening_inodes.begin();
       p != opening_inodes.end();
       ++p) {
    open_ino_info_t& info = p->second;
    if (info.checking == who) {
      dout(10) << "  kicking ino " << p->first << " who was checking mds." << who << dendl;
      info.checking = MDS_RANK_NONE;
      do_open_ino_peer(p->first, info);
    } else if (info.checking == MDS_RANK_NONE) {
      dout(10) << "  kicking ino " << p->first << " who was waiting" << dendl;
      do_open_ino_peer(p->first, info);
    }
  }
}

void MDCache::open_ino_batch_start()
{
  dout(10) << __func__ << dendl;
  open_ino_batch = true;
}

void MDCache::open_ino_batch_submit()
{
  dout(10) << __func__ << dendl;
  open_ino_batch = false;

  for (auto& [dir, p] : open_ino_batched_fetch) {
    CInode *in = dir->inode;
    std::vector<dentry_key_t> keys;
    for (auto& dname : p.first)
      keys.emplace_back(CEPH_NOSNAP, dname, in->hash_dentry_name(dname));
    dir->fetch_keys(keys,
	  new MDSInternalContextWrapper(mds,
	    new LambdaContext([this, waiters = std::move(p.second)](int r) mutable {
	      mds->queue_waiters_front(waiters);
	    })
	  )
	);
    if (mds->logger)
      mds->logger->inc(l_mds_openino_dir_fetch);
  }
  open_ino_batched_fetch.clear();
}

void MDCache::open_ino(inodeno_t ino, int64_t pool, MDSContext* fin,
		       bool want_replica, bool want_xlocked,
		       vector<inode_backpointer_t> *ancestors_hint,
		       mds_rank_t auth_hint)
{
  dout(10) << "open_ino " << ino << " pool " << pool << " want_replica "
	   << want_replica << dendl;

  auto it = opening_inodes.find(ino);
  if (it != opening_inodes.end()) {
    open_ino_info_t& info = it->second;
    if (want_replica) {
      info.want_replica = true;
      if (want_xlocked && !info.want_xlocked) {
	if (!info.ancestors.empty()) {
	  CInode *diri = get_inode(info.ancestors[0].dirino);
	  if (diri) {
	    frag_t fg = diri->pick_dirfrag(info.ancestors[0].dname);
	    CDir *dir = diri->get_dirfrag(fg);
	    if (dir && !dir->is_auth()) {
	      filepath path(info.ancestors[0].dname, 0);
	      discover_path(dir, CEPH_NOSNAP, path, NULL, true);
	    }
	  }
	}
	info.want_xlocked = true;
      }
    }
    info.waiters.push_back(fin);
  } else {
    open_ino_info_t& info = opening_inodes[ino];
    info.want_replica = want_replica;
    info.want_xlocked = want_xlocked;
    info.tid = ++open_ino_last_tid;
    info.pool = pool >= 0 ? pool : default_file_layout.pool_id;
    info.waiters.push_back(fin);
    if (auth_hint != MDS_RANK_NONE)
      info.auth_hint = auth_hint;
    if (ancestors_hint) {
      info.ancestors = std::move(*ancestors_hint);
      info.fetch_backtrace = false;
      info.checking = mds->get_nodeid();
      _open_ino_traverse_dir(ino, info, 0);
    } else {
      do_open_ino(ino, info, 0);
    }
  }
}

/* ---------------------------- */

/*
 * search for a given inode on MDS peers.  optionally start with the given node.


 TODO 
  - recover from mds node failure, recovery
  - traverse path

 */
void MDCache::find_ino_peers(inodeno_t ino, MDSContext *c,
			     mds_rank_t hint, bool path_locked)
{
  dout(5) << "find_ino_peers " << ino << " hint " << hint << dendl;
  CInode *in = get_inode(ino);
  if (in && in->state_test(CInode::STATE_PURGING)) {
    c->complete(-CEPHFS_ESTALE);
    return;
  }
  ceph_assert(!in);
  
  ceph_tid_t tid = ++find_ino_peer_last_tid;
  find_ino_peer_info_t& fip = find_ino_peer[tid];
  fip.ino = ino;
  fip.tid = tid;
  fip.fin = c;
  fip.path_locked = path_locked;
  fip.hint = hint;
  _do_find_ino_peer(fip);
}

void MDCache::_do_find_ino_peer(find_ino_peer_info_t& fip)
{
  set<mds_rank_t> all, active;
  mds->mdsmap->get_mds_set(all);
  mds->mdsmap->get_mds_set_lower_bound(active, MDSMap::STATE_CLIENTREPLAY);

  dout(10) << "_do_find_ino_peer " << fip.tid << " " << fip.ino
	   << " active " << active << " all " << all
	   << " checked " << fip.checked
	   << dendl;
    
  mds_rank_t m = MDS_RANK_NONE;
  if (fip.hint >= 0) {
    m = fip.hint;
    fip.hint = MDS_RANK_NONE;
  } else {
    for (set<mds_rank_t>::iterator p = active.begin(); p != active.end(); ++p)
      if (*p != mds->get_nodeid() &&
	  fip.checked.count(*p) == 0) {
	m = *p;
	break;
      }
  }
  if (m == MDS_RANK_NONE) {
    all.erase(mds->get_nodeid());
    if (all != fip.checked) {
      dout(10) << "_do_find_ino_peer waiting for more peers to be active" << dendl;
    } else {
      dout(10) << "_do_find_ino_peer failed on " << fip.ino << dendl;
      fip.fin->complete(-CEPHFS_ESTALE);
      find_ino_peer.erase(fip.tid);
    }
  } else {
    fip.checking = m;
    mds->send_message_mds(make_message<MMDSFindIno>(fip.tid, fip.ino), m);
  }
}

void MDCache::handle_find_ino(const cref_t<MMDSFindIno> &m)
{
  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    return;
  }

  dout(10) << "handle_find_ino " << *m << dendl;
  auto r = make_message<MMDSFindInoReply>(m->tid);
  CInode *in = get_inode(m->ino);
  if (in) {
    in->make_path(r->path);
    dout(10) << " have " << r->path << " " << *in << dendl;

    /*
     * If the the CInode was just created by using openc in current
     * auth MDS, but the client just sends a getattr request to another
     * replica MDS. Then here it will make a path of '#INODE-NUMBER'
     * only because the CInode hasn't been linked yet, and the replica
     * MDS will keep retrying until the auth MDS flushes the mdlog and
     * the C_MDS_openc_finish and link_primary_inode are called at most
     * 5 seconds later.
     */
    if (!in->get_parent_dn() && in->is_auth()) {
      mds->mdlog->flush();
    }
  }
  mds->send_message_mds(r, mds_rank_t(m->get_source().num()));
}


void MDCache::handle_find_ino_reply(const cref_t<MMDSFindInoReply> &m)
{
  auto p = find_ino_peer.find(m->tid);
  if (p != find_ino_peer.end()) {
    dout(10) << "handle_find_ino_reply " << *m << dendl;
    find_ino_peer_info_t& fip = p->second;

    // success?
    if (get_inode(fip.ino)) {
      dout(10) << "handle_find_ino_reply successfully found " << fip.ino << dendl;
      mds->queue_waiter(fip.fin);
      find_ino_peer.erase(p);
      return;
    }

    mds_rank_t from = mds_rank_t(m->get_source().num());
    if (fip.checking == from)
      fip.checking = MDS_RANK_NONE;
    fip.checked.insert(from);

    if (!m->path.empty()) {
      // we got a path!
      vector<CDentry*> trace;
      CF_MDS_RetryMessageFactory cf(mds, m);
      MDRequestRef null_ref;
      int flags = MDS_TRAVERSE_DISCOVER;
      if (fip.path_locked)
	flags |= MDS_TRAVERSE_PATH_LOCKED;
      int r = path_traverse(null_ref, cf, m->path, flags, &trace);
      if (r > 0)
	return; 
      dout(0) << "handle_find_ino_reply failed with " << r << " on " << m->path 
	      << ", retrying" << dendl;
      fip.checked.clear();
      _do_find_ino_peer(fip);
    } else {
      // nope, continue.
      _do_find_ino_peer(fip);
    }      
  } else {
    dout(10) << "handle_find_ino_reply tid " << m->tid << " dne" << dendl;
  }  
}

void MDCache::kick_find_ino_peers(mds_rank_t who)
{
  // find_ino_peers requests we should move on from
  for (map<ceph_tid_t,find_ino_peer_info_t>::iterator p = find_ino_peer.begin();
       p != find_ino_peer.end();
       ++p) {
    find_ino_peer_info_t& fip = p->second;
    if (fip.checking == who) {
      dout(10) << "kicking find_ino_peer " << fip.tid << " who was checking mds." << who << dendl;
      fip.checking = MDS_RANK_NONE;
      _do_find_ino_peer(fip);
    } else if (fip.checking == MDS_RANK_NONE) {
      dout(10) << "kicking find_ino_peer " << fip.tid << " who was waiting" << dendl;
      _do_find_ino_peer(fip);
    }
  }
}

/* ---------------------------- */

int MDCache::get_num_client_requests()
{
  int count = 0;
  for (ceph::unordered_map<metareqid_t, MDRequestRef>::iterator p = active_requests.begin();
      p != active_requests.end();
      ++p) {
    MDRequestRef& mdr = p->second;
    if (mdr->reqid.name.is_client() && !mdr->is_peer())
      count++;
  }
  return count;
}

MDRequestRef MDCache::request_start(const cref_t<MClientRequest>& req)
{
  // did we win a forward race against a peer?
  if (active_requests.count(req->get_reqid())) {
    MDRequestRef& mdr = active_requests[req->get_reqid()];
    ceph_assert(mdr);
    if (mdr->is_peer()) {
      dout(10) << "request_start already had " << *mdr << ", waiting for finish" << dendl;
      mdr->more()->waiting_for_finish.push_back(new C_MDS_RetryMessage(mds, req));
    } else {
      dout(10) << "request_start already processing " << *mdr << ", dropping new msg" << dendl;
    }
    return MDRequestRef();
  }

  // register new client request
  MDRequestImpl::Params params;
  params.reqid = req->get_reqid();
  params.attempt = req->get_num_fwd();
  params.client_req = req;
  params.initiated = req->get_recv_stamp();
  params.throttled = req->get_throttle_stamp();
  params.all_read = req->get_recv_complete_stamp();
  params.dispatched = req->get_dispatch_stamp();

  MDRequestRef mdr =
      mds->op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params*>(&params);
  active_requests[params.reqid] = mdr;
  mdr->set_op_stamp(req->get_stamp());
  dout(7) << "request_start " << *mdr << dendl;
  return mdr;
}

MDRequestRef MDCache::request_start_peer(metareqid_t ri, __u32 attempt, const cref_t<Message> &m)
{
  int by = m->get_source().num();
  MDRequestImpl::Params params;
  params.reqid = ri;
  params.attempt = attempt;
  params.triggering_peer_req = m;
  params.peer_to = by;
  params.initiated = m->get_recv_stamp();
  params.throttled = m->get_throttle_stamp();
  params.all_read = m->get_recv_complete_stamp();
  params.dispatched = m->get_dispatch_stamp();
  MDRequestRef mdr =
      mds->op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params*>(&params);
  ceph_assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_peer " << *mdr << " by mds." << by << dendl;
  return mdr;
}

MDRequestRef MDCache::request_start_internal(int op)
{
  utime_t now = ceph_clock_now();
  MDRequestImpl::Params params;
  params.reqid.name = entity_name_t::MDS(mds->get_nodeid());
  params.reqid.tid = mds->issue_tid();
  params.initiated = now;
  params.throttled = now;
  params.all_read = now;
  params.dispatched = now;
  params.internal_op = op;
  MDRequestRef mdr =
      mds->op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params*>(&params);

  if (active_requests.count(mdr->reqid)) {
    auto& _mdr = active_requests[mdr->reqid];
    dout(0) << __func__ << " existing " << *_mdr << " op " << _mdr->internal_op << dendl;
    dout(0) << __func__ << " new " << *mdr << " op " << op << dendl;
    ceph_abort();
  }
  active_requests[mdr->reqid] = mdr;
  dout(7) << __func__ << " " << *mdr << " op " << op << dendl;
  return mdr;
}

MDRequestRef MDCache::request_get(metareqid_t rid)
{
  ceph::unordered_map<metareqid_t, MDRequestRef>::iterator p = active_requests.find(rid);
  ceph_assert(p != active_requests.end());
  dout(7) << "request_get " << rid << " " << *p->second << dendl;
  return p->second;
}

void MDCache::request_finish(const MDRequestRef& mdr)
{
  dout(7) << "request_finish " << *mdr << dendl;
  mdr->mark_event("finishing request");

  // peer finisher?
  if (mdr->has_more() && mdr->more()->peer_commit) {
    Context *fin = mdr->more()->peer_commit;
    mdr->more()->peer_commit = 0;
    int ret;
    if (mdr->aborted) {
      mdr->aborted = false;
      ret = -1;
      mdr->more()->peer_rolling_back = true;
    } else {
      ret = 0;
      mdr->committing = true;
    }
    fin->complete(ret);   // this must re-call request_finish.
    return; 
  }

  switch(mdr->internal_op) {
    case CEPH_MDS_OP_FRAGMENTDIR:
      logger->inc(l_mdss_ireq_fragmentdir);
      break;
    case CEPH_MDS_OP_EXPORTDIR:
      logger->inc(l_mdss_ireq_exportdir);
      break;
    case CEPH_MDS_OP_ENQUEUE_SCRUB:
      logger->inc(l_mdss_ireq_enqueue_scrub);
      break;
    case CEPH_MDS_OP_FLUSH:
      logger->inc(l_mdss_ireq_flush);
      break;
    case CEPH_MDS_OP_REPAIR_FRAGSTATS:
      logger->inc(l_mdss_ireq_fragstats);
      break;
    case CEPH_MDS_OP_REPAIR_INODESTATS:
      logger->inc(l_mdss_ireq_inodestats);
      break;
  }

  request_cleanup(mdr);
}


void MDCache::request_forward(const MDRequestRef& mdr, mds_rank_t who, int port)
{
  CachedStackStringStream css;
  *css << "forwarding request to mds." << who;
  mdr->mark_event(css->strv());
  if (mdr->client_request && mdr->client_request->get_source().is_client()) {
    dout(7) << "request_forward " << *mdr << " to mds." << who << " req "
            << *mdr->client_request << dendl;
    if (mdr->is_batch_head()) {
      mdr->release_batch_op()->forward(who);
    } else {
      mds->forward_message_mds(mdr, who);
    }
    if (mds->logger) mds->logger->inc(l_mds_forward);
  } else if (mdr->internal_op >= 0) {
    dout(10) << "request_forward on internal op; cancelling" << dendl;
    mdr->internal_op_finish->complete(-CEPHFS_EXDEV);
  } else {
    dout(7) << "request_forward drop " << *mdr << " req " << *mdr->client_request
            << " was from mds" << dendl;
  }
  request_cleanup(mdr);
}


void MDCache::dispatch_request(const MDRequestRef& mdr)
{
  if (mdr->client_request) {
    mds->server->dispatch_client_request(mdr);
  } else if (mdr->peer_request) {
    mds->server->dispatch_peer_request(mdr);
  } else {
    switch (mdr->internal_op) {
    case CEPH_MDS_OP_FRAGMENTDIR:
      dispatch_fragment_dir(mdr);
      break;
    case CEPH_MDS_OP_EXPORTDIR:
      migrator->dispatch_export_dir(mdr, 0);
      break;
    case CEPH_MDS_OP_ENQUEUE_SCRUB:
      enqueue_scrub_work(mdr);
      break;
    case CEPH_MDS_OP_FLUSH:
      flush_dentry_work(mdr);
      break;
    case CEPH_MDS_OP_REPAIR_FRAGSTATS:
      repair_dirfrag_stats_work(mdr);
      break;
    case CEPH_MDS_OP_REPAIR_INODESTATS:
      repair_inode_stats_work(mdr);
      break;
    case CEPH_MDS_OP_RDLOCK_FRAGSSTATS:
      rdlock_dirfrags_stats_work(mdr);
      break;
    default:
      ceph_abort();
    }
  }
}


void MDCache::request_drop_foreign_locks(const MDRequestRef& mdr)
{
  if (!mdr->has_more())
    return;

  // clean up peers
  //  (will implicitly drop remote dn pins)
  for (set<mds_rank_t>::iterator p = mdr->more()->peers.begin();
       p != mdr->more()->peers.end();
       ++p) {
    auto r = make_message<MMDSPeerRequest>(mdr->reqid, mdr->attempt,
					    MMDSPeerRequest::OP_FINISH);

    if (mdr->killed && !mdr->committing) {
      r->mark_abort();
    } else if (mdr->more()->srcdn_auth_mds == *p &&
	       mdr->more()->inode_import.length() > 0) {
      // information about rename imported caps
      r->inode_export = std::move(mdr->more()->inode_import);
    }

    mds->send_message_mds(r, *p);
  }

  /* strip foreign xlocks out of lock lists, since the OP_FINISH drops them
   * implicitly. Note that we don't call the finishers -- there shouldn't
   * be any on a remote lock and the request finish wakes up all
   * the waiters anyway! */

  for (auto it = mdr->locks.begin(); it != mdr->locks.end(); ) {
    SimpleLock *lock = it->lock;
    if (it->is_xlock() && !lock->get_parent()->is_auth()) {
      dout(10) << "request_drop_foreign_locks forgetting lock " << *lock
	       << " on " << lock->get_parent() << dendl;
      lock->put_xlock();
      mdr->locks.erase(it++);
    } else if (it->is_remote_wrlock()) {
      dout(10) << "request_drop_foreign_locks forgetting remote_wrlock " << *lock
	       << " on mds." << it->wrlock_target << " on " << *lock->get_parent() << dendl;
      if (it->is_wrlock()) {
	it->clear_remote_wrlock();
	++it;
      } else {
	mdr->locks.erase(it++);
      }
    } else {
      ++it;
    }
  }

  mdr->more()->peers.clear(); /* we no longer have requests out to them, and
                                * leaving them in can cause double-notifies as
                                * this function can get called more than once */
}

void MDCache::request_drop_non_rdlocks(const MDRequestRef& mdr)
{
  request_drop_foreign_locks(mdr);
  mds->locker->drop_non_rdlocks(mdr.get());
}

void MDCache::request_drop_locks(const MDRequestRef& mdr)
{
  request_drop_foreign_locks(mdr);
  mds->locker->drop_locks(mdr.get());
}

void MDCache::request_cleanup(const MDRequestRef& mdr)
{
  dout(15) << "request_cleanup " << *mdr << dendl;

  if (mdr->has_more()) {
    if (mdr->more()->is_ambiguous_auth)
      mdr->clear_ambiguous_auth();
    if (!mdr->more()->waiting_for_finish.empty())
      mds->queue_waiters(mdr->more()->waiting_for_finish);
  }

  request_drop_locks(mdr);

  // drop (local) auth pins
  mdr->drop_local_auth_pins();

  // drop stickydirs
  mdr->put_stickydirs();

  mds->locker->kick_cap_releases(mdr);

  // drop cache pins
  mdr->drop_pins();

  // remove from session
  mdr->item_session_request.remove_myself();

  // remove from map
  active_requests.erase(mdr->reqid);

  // queue next replay op?
  if (mdr->is_queued_for_replay() && !mdr->get_queued_next_replay_op()) {
    mdr->set_queued_next_replay_op();
    mds->queue_one_replay();
  }

  if (mds->logger)
    log_stat();

  mdr->mark_event("cleaned up request");
}

void MDCache::request_kill(const MDRequestRef& mdr)
{
  if (mdr->killed) {
    /* ignore duplicate kills */
    return;
  }

  // rollback peer requests is tricky. just let the request proceed.
  if (mdr->has_more() &&
      (!mdr->more()->witnessed.empty() || !mdr->more()->waiting_on_peer.empty())) {
    if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
      ceph_assert(mdr->more()->witnessed.empty());
      mdr->aborted = true;
      dout(10) << "request_kill " << *mdr << " -- waiting for peer reply, delaying" << dendl;
    } else {
      dout(10) << "request_kill " << *mdr << " -- already started peer prep, no-op" << dendl;
    }

    ceph_assert(mdr->used_prealloc_ino == 0);
    ceph_assert(mdr->prealloc_inos.empty());

    mdr->session = NULL;
    mdr->item_session_request.remove_myself();
    return;
  }

  if (mdr->killed) {
    /* ignore duplicate kills */
    return;
  }

  mdr->killed = true;
  mdr->mark_event("killing request");

  if (mdr->committing) {
    dout(10) << "request_kill " << *mdr << " -- already committing, remove it from sesssion requests" << dendl;
    mdr->item_session_request.remove_myself();
  } else {
    dout(10) << "request_kill " << *mdr << dendl;
    if (mdr->internal_op_finish) {
      mdr->internal_op_finish->complete(-CEPHFS_ECANCELED);
      mdr->internal_op_finish = nullptr;
    }
    request_cleanup(mdr);
  }
}

// -------------------------------------------------------------------------------
// SNAPREALMS

void MDCache::create_global_snaprealm()
{
  CInode *in = new CInode(this); // dummy inode
  create_unlinked_system_inode(in, CEPH_INO_GLOBAL_SNAPREALM, S_IFDIR|0755);
  add_inode(in);
  global_snaprealm = in->snaprealm;
}

void MDCache::do_realm_invalidate_and_update_notify(CInode *in, int snapop, bool notify_clients)
{
  dout(10) << "do_realm_invalidate_and_update_notify " << *in->snaprealm << " " << *in << dendl;

  vector<inodeno_t> split_inos;
  vector<inodeno_t> split_realms;

  if (notify_clients) {
    if (snapop == CEPH_SNAP_OP_SPLIT) {
      // notify clients of update|split
      for (auto p = in->snaprealm->inodes_with_caps.begin(); !p.end(); ++p)
	split_inos.push_back((*p)->ino());

      for (auto& r : in->snaprealm->open_children)
	split_realms.push_back(r->inode->ino());
    }
  }

  map<client_t, ref_t<MClientSnap>> updates;
  list<SnapRealm*> q;
  q.push_back(in->snaprealm);
  while (!q.empty()) {
    SnapRealm *realm = q.front();
    q.pop_front();

    dout(10) << " realm " << *realm << " on " << *realm->inode << dendl;
    realm->invalidate_cached_snaps();

    if (notify_clients) {
      for (const auto& p : realm->client_caps) {
        const auto& client = p.first;
        const auto& caps = p.second;
	ceph_assert(!caps->empty());

        auto em = updates.emplace(std::piecewise_construct, std::forward_as_tuple(client), std::forward_as_tuple());
        if (em.second) {
          auto update = make_message<MClientSnap>(CEPH_SNAP_OP_SPLIT);
	  update->head.split = in->ino();
	  update->split_inos = split_inos;
	  update->split_realms = split_realms;
	  update->bl = mds->server->get_snap_trace(em.first->first, in->snaprealm);
	  em.first->second = std::move(update);
	}
      }
    }

    // notify for active children, too.
    dout(10) << " " << realm << " open_children are " << realm->open_children << dendl;
    for (auto& r : realm->open_children)
      q.push_back(r);
  }

  if (notify_clients)
    send_snaps(updates);
}

void MDCache::send_snap_update(CInode *in, version_t stid, int snap_op)
{
  dout(10) << __func__ << " " << *in << " stid " << stid << dendl;
  ceph_assert(in->is_auth());

  set<mds_rank_t> mds_set;
  if (stid > 0) {
    mds->mdsmap->get_mds_set_lower_bound(mds_set, MDSMap::STATE_RESOLVE);
    mds_set.erase(mds->get_nodeid());
  } else {
    in->list_replicas(mds_set);
  }

  if (!mds_set.empty()) {
    bufferlist snap_blob;
    in->encode_snap(snap_blob);

    for (auto p : mds_set) {
      auto m = make_message<MMDSSnapUpdate>(in->ino(), stid, snap_op);
      m->snap_blob = snap_blob;
      mds->send_message_mds(m, p);
    }
  }

  if (stid > 0)
    notify_global_snaprealm_update(snap_op);
}

void MDCache::handle_snap_update(const cref_t<MMDSSnapUpdate> &m)
{
  mds_rank_t from = mds_rank_t(m->get_source().num());
  dout(10) << __func__ << " " << *m << " from mds." << from << dendl;

  if (mds->get_state() < MDSMap::STATE_RESOLVE &&
      mds->get_want_state() != CEPH_MDS_STATE_RESOLVE) {
    return;
  }

  // null rejoin_done means open_snaprealms() has already been called
  bool notify_clients = mds->get_state() > MDSMap::STATE_REJOIN ||
			(mds->is_rejoin() && !rejoin_done);

  if (m->get_tid() > 0) {
    mds->snapclient->notify_commit(m->get_tid());
    if (notify_clients)
      notify_global_snaprealm_update(m->get_snap_op());
  }

  CInode *in = get_inode(m->get_ino());
  if (in) {
    ceph_assert(!in->is_auth());
    if (mds->get_state() > MDSMap::STATE_REJOIN ||
	(mds->is_rejoin() && !in->is_rejoining())) {
      auto p = m->snap_blob.cbegin();
      in->decode_snap(p);

      if (!notify_clients) {
	if (!rejoin_pending_snaprealms.count(in)) {
	  in->get(CInode::PIN_OPENINGSNAPPARENTS);
	  rejoin_pending_snaprealms.insert(in);
	}
      }
      do_realm_invalidate_and_update_notify(in, m->get_snap_op(), notify_clients);
    }
  }
}

void MDCache::notify_global_snaprealm_update(int snap_op)
{
  if (snap_op != CEPH_SNAP_OP_DESTROY)
    snap_op = CEPH_SNAP_OP_UPDATE;
  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (auto &session : sessions) {
    if (!session->is_open() && !session->is_stale())
      continue;
    auto update = make_message<MClientSnap>(snap_op);
    update->head.split = global_snaprealm->inode->ino();
    update->bl = mds->server->get_snap_trace(session, global_snaprealm);
    mds->send_message_client_counted(update, session);
  }
}

// -------------------------------------------------------------------------------
// STRAYS

struct C_MDC_RetryScanStray : public MDCacheContext {
  dirfrag_t next;
  C_MDC_RetryScanStray(MDCache *c,  dirfrag_t n) : MDCacheContext(c), next(n) { }
  void finish(int r) override {
    mdcache->scan_stray_dir(next);
  }
};

void MDCache::scan_stray_dir(dirfrag_t next)
{
  dout(10) << "scan_stray_dir " << next << dendl;

  if (next.ino)
    next.frag = strays[MDS_INO_STRAY_INDEX(next.ino)]->dirfragtree[next.frag.value()];

  for (int i = 0; i < NUM_STRAY; ++i) {
    if (strays[i]->ino() < next.ino)
      continue;

    std::vector<CDir*> ls;
    strays[i]->get_dirfrags(ls);

    for (const auto& dir : ls) {
      if (dir->get_frag() < next.frag)
	continue;

      if (!dir->can_auth_pin()) {
	dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDC_RetryScanStray(this, dir->dirfrag()));
	return;
      }

      if (!dir->is_complete()) {
	dir->fetch(new C_MDC_RetryScanStray(this, dir->dirfrag()));
	return;
      }

      for (auto &p : dir->items) {
	CDentry *dn = p.second;
	dn->state_set(CDentry::STATE_STRAY);
	CDentry::linkage_t *dnl = dn->get_projected_linkage();
	if (dnl->is_primary()) {
	  CInode *in = dnl->get_inode();
	  if (in->get_inode()->nlink == 0)
	    in->state_set(CInode::STATE_ORPHAN);
	  maybe_eval_stray(in);
	}
      }
    }
    next.frag = frag_t();
  }
}

void MDCache::fetch_backtrace(inodeno_t ino, int64_t pool, bufferlist& bl, Context *fin)
{
  object_t oid = CInode::get_object_name(ino, frag_t(), "");
  mds->objecter->getxattr(oid, object_locator_t(pool), "parent", CEPH_NOSNAP, &bl, 0, fin);
  if (mds->logger)
    mds->logger->inc(l_mds_openino_backtrace_fetch);
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
  auto dis = make_message<MDiscover>(d.ino, d.frag, d.snap, d.want_path,
				     d.want_base_dir, d.path_locked);
  logger->inc(l_mdc_dir_send_discover);
  dis->set_tid(d.tid);
  mds->send_message_mds(dis, d.mds);
}

void MDCache::discover_base_ino(inodeno_t want_ino,
				MDSContext *onfinish,
				mds_rank_t from) 
{
  dout(7) << "discover_base_ino " << want_ino << " from mds." << from << dendl;
  if (waiting_for_base_ino[from].count(want_ino) == 0) {
    discover_info_t& d = _create_discover(from);
    d.ino = want_ino;
    _send_discover(d);
  }
  waiting_for_base_ino[from][want_ino].push_back(onfinish);
}


void MDCache::discover_dir_frag(CInode *base,
				frag_t approx_fg,
				MDSContext *onfinish,
				mds_rank_t from)
{
  if (from < 0)
    from = base->authority().first;

  dirfrag_t df(base->ino(), approx_fg);
  dout(7) << "discover_dir_frag " << df
	  << " from mds." << from << dendl;

  if (!base->is_waiting_for_dir(approx_fg) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.pin_base(base);
    d.ino = base->ino();
    d.frag = approx_fg;
    d.want_base_dir = true;
    _send_discover(d);
  }

  if (onfinish) 
    base->add_dir_waiter(approx_fg, onfinish);
}

struct C_MDC_RetryDiscoverPath : public MDCacheContext {
  CInode *base;
  snapid_t snapid;
  filepath path;
  mds_rank_t from;
  C_MDC_RetryDiscoverPath(MDCache *c, CInode *b, snapid_t s, filepath &p, mds_rank_t f) :
    MDCacheContext(c), base(b), snapid(s), path(p), from(f)  {}
  void finish(int r) override {
    mdcache->discover_path(base, snapid, path, 0, from);
  }
};

void MDCache::discover_path(CInode *base,
			    snapid_t snap,
			    filepath want_path,
			    MDSContext *onfinish,
			    bool path_locked,
			    mds_rank_t from)
{
  if (from < 0)
    from = base->authority().first;

  dout(7) << "discover_path " << base->ino() << " " << want_path << " snap " << snap << " from mds." << from
	  << (path_locked ? " path_locked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(10) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverPath(this, base, snap, want_path, from);
    base->add_waiter(CInode::WAIT_SINGLEAUTH, onfinish);
    return;
  } else if (from == mds->get_nodeid()) {
    MDSContext::vec finished;
    base->take_waiting(CInode::WAIT_DIR, finished);
    mds->queue_waiters(finished);
    return;
  }

  frag_t fg = base->pick_dirfrag(want_path[0]);
  if ((path_locked && want_path.depth() == 1) ||
      !base->is_waiting_for_dir(fg) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.ino = base->ino();
    d.pin_base(base);
    d.frag = fg;
    d.snap = snap;
    d.want_path = want_path;
    d.want_base_dir = true;
    d.path_locked = path_locked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_dir_waiter(fg, onfinish);
}

struct C_MDC_RetryDiscoverPath2 : public MDCacheContext {
  CDir *base;
  snapid_t snapid;
  filepath path;
  C_MDC_RetryDiscoverPath2(MDCache *c, CDir *b, snapid_t s, filepath &p) :
    MDCacheContext(c), base(b), snapid(s), path(p) {}
  void finish(int r) override {
    mdcache->discover_path(base, snapid, path, 0);
  }
};

void MDCache::discover_path(CDir *base,
			    snapid_t snap,
			    filepath want_path,
			    MDSContext *onfinish,
			    bool path_locked)
{
  mds_rank_t from = base->authority().first;

  dout(7) << "discover_path " << base->dirfrag() << " " << want_path << " snap " << snap << " from mds." << from
	  << (path_locked ? " path_locked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(7) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverPath2(this, base, snap, want_path);
    base->add_waiter(CDir::WAIT_SINGLEAUTH, onfinish);
    return;
  }

  if ((path_locked && want_path.depth() == 1) ||
      !base->is_waiting_for_dentry(want_path[0].c_str(), snap) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.ino = base->ino();
    d.pin_base(base->inode);
    d.frag = base->get_frag();
    d.snap = snap;
    d.want_path = want_path;
    d.want_base_dir = false;
    d.path_locked = path_locked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_dentry_waiter(want_path[0], snap, onfinish);
}

void MDCache::kick_discovers(mds_rank_t who)
{
  for (map<ceph_tid_t,discover_info_t>::iterator p = discovers.begin();
       p != discovers.end();
       ++p) {
    if (p->second.mds != who)
      continue;
    _send_discover(p->second);
  }
}


void MDCache::handle_discover(const cref_t<MDiscover> &dis) 
{
  mds_rank_t whoami = mds->get_nodeid();
  mds_rank_t from = mds_rank_t(dis->get_source().num());

  ceph_assert(from != whoami);

  if (mds->get_state() <= MDSMap::STATE_REJOIN) {
    if (mds->get_state() < MDSMap::STATE_REJOIN &&
	mds->get_want_state() < CEPH_MDS_STATE_REJOIN) {
      return;
    }

    // proceed if requester is in the REJOIN stage, the request is from parallel_fetch().
    // delay processing request from survivor because we may not yet choose lock states.
    if (!mds->mdsmap->is_rejoin(from)) {
      dout(0) << "discover_reply not yet active(|still rejoining), delaying" << dendl;
      mds->wait_for_replay(new C_MDS_RetryMessage(mds, dis));
      return;
    }
  }


  CInode *cur = 0;
  auto reply = make_message<MDiscoverReply>(*dis);

  snapid_t snapid = dis->get_snapid();

  logger->inc(l_mdc_dir_handle_discover);

  // get started.
  if (MDS_INO_IS_BASE(dis->get_base_ino()) &&
      !dis->wants_base_dir() && dis->get_want().depth() == 0) {
    // wants root
    dout(7) << "handle_discover from mds." << from
	    << " wants base + " << dis->get_want().get_path()
	    << " snap " << snapid
	    << dendl;

    cur = get_inode(dis->get_base_ino());
    ceph_assert(cur);

    // add root
    reply->starts_with = MDiscoverReply::INODE;
    encode_replica_inode(cur, from, reply->trace, mds->mdsmap->get_up_features());
    dout(10) << "added base " << *cur << dendl;
  }
  else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino(), snapid);
    if (!cur && snapid != CEPH_NOSNAP) {
      cur = get_inode(dis->get_base_ino());
      if (cur && !cur->is_multiversion())
	cur = NULL;  // nope!
    }
    
    if (!cur) {
      dout(7) << "handle_discover mds." << from 
	      << " don't have base ino " << dis->get_base_ino() << "." << snapid
	      << dendl;
      if (!dis->wants_base_dir() && dis->get_want().depth() > 0)
	reply->set_error_dentry(dis->get_dentry(0));
      reply->set_flag_error_dir();
    } else if (dis->wants_base_dir()) {
      dout(7) << "handle_discover mds." << from 
	      << " wants basedir+" << dis->get_want().get_path() 
	      << " has " << *cur 
	      << dendl;
    } else {
      dout(7) << "handle_discover mds." << from 
	      << " wants " << dis->get_want().get_path()
	      << " has " << *cur
	      << dendl;
    }
  }

  ceph_assert(reply);
  
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
      ceph_assert(dis->wants_base_dir() || MDS_INO_IS_BASE(dis->get_base_ino()));
      fg = dis->get_base_dir_frag();
      if (!cur->dirfragtree.is_leaf(fg))
	fg = cur->dirfragtree[fg.value()];
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

    if (!curdir) { // open dir?
      if (cur->is_frozen()) {
	if (!reply->is_empty()) {
	  dout(7) << *cur << " is frozen, non-empty reply, stopping" << dendl;
	  break;
	}
	dout(7) << *cur << " is frozen, empty reply, waiting" << dendl;
	cur->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	return;
      }
      curdir = cur->get_or_open_dirfrag(this, fg);
    } else if (curdir->is_frozen_tree() ||
	       (curdir->is_frozen_dir() && fragment_are_all_frozen(curdir))) {
      if (!reply->is_empty()) {
	dout(7) << *curdir << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
      if (dis->wants_base_dir() && dis->get_base_dir_frag() != curdir->get_frag()) {
	dout(7) << *curdir << " is frozen, dirfrag mismatch, stopping" << dendl;
	reply->set_flag_error_dir();
	break;
      }
      dout(7) << *curdir << " is frozen, empty reply, waiting" << dendl;
      curdir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
      return;
    }
    
    // add dir
    if (curdir->get_version() == 0) {
      // fetch newly opened dir
    } else if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "handle_discover not adding unwanted base dir " << *curdir << dendl;
      // make sure the base frag is correct, though, in there was a refragment since the
      // original request was sent.
      reply->set_base_dir_frag(curdir->get_frag());
    } else {
      ceph_assert(!curdir->is_ambiguous_auth()); // would be frozen.
      if (!reply->trace.length())
	reply->starts_with = MDiscoverReply::DIR;
      encode_replica_dir(curdir, from, reply->trace);
      dout(7) << "handle_discover added dir " << *curdir << dendl;
    }

    // lookup
    CDentry *dn = 0;
    std::string_view dname;
    if (dis->get_want().depth() > 0)
      dname = dis->get_dentry(i);
    if (curdir->get_version() == 0) {
      // fetch newly opened dir
      ceph_assert(!curdir->has_bloom());
    } else if (dname.size() > 0) {
      // lookup dentry
      dn = curdir->lookup(dname, snapid);
    } else 
      break; // done!
          
    // incomplete dir?
    if (!dn) {
      if (!curdir->is_complete() &&
	  !(dname.size() > 0 &&
	    snapid == CEPH_NOSNAP &&
	    curdir->has_bloom() &&
	    !curdir->is_in_bloom(dname))) {
	// readdir
	dout(7) << "incomplete dir contents for " << *curdir << ", fetching" << dendl;
	if (reply->is_empty()) {
	  // fetch and wait
	  curdir->fetch(dname, snapid, new C_MDS_RetryMessage(mds, dis),
			dis->wants_base_dir() && curdir->get_version() == 0);
	  return;
	} else {
	  // initiate fetch, but send what we have so far
	  curdir->fetch(dname, snapid, nullptr);
	  break;
	}
      }

      if (snapid != CEPH_NOSNAP && !reply->is_empty()) {
	dout(7) << "dentry " << dis->get_dentry(i) << " snap " << snapid
		<< " dne, non-empty reply, stopping" << dendl;
	break;
      }

      // send null dentry
      dout(7) << "dentry " << dis->get_dentry(i) << " dne, returning null in "
	      << *curdir << dendl;
      if (snapid == CEPH_NOSNAP)
	dn = curdir->add_null_dentry(dis->get_dentry(i));
      else
	dn = curdir->add_null_dentry(dis->get_dentry(i), snapid, snapid);
    }
    ceph_assert(dn);

    // don't add replica to purging dentry/inode
    if (dn->state_test(CDentry::STATE_PURGING)) {
      if (reply->is_empty())
	reply->set_flag_error_dn(dis->get_dentry(i));
      break;
    }

    CDentry::linkage_t *dnl = dn->get_linkage();

    // xlocked dentry?
    //  ...always block on non-tail items (they are unrelated)
    //  ...allow xlocked tail disocvery _only_ if explicitly requested
    if (dn->lock.is_xlocked()) {
      // is this the last (tail) item in the discover traversal?
      if (dis->is_path_locked()) {
	dout(7) << "handle_discover allowing discovery of xlocked " << *dn << dendl;
      } else if (reply->is_empty()) {
	dout(7) << "handle_discover blocking on xlocked " << *dn << dendl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryMessage(mds, dis));
	return;
      } else {
	dout(7) << "handle_discover non-empty reply, xlocked tail " << *dn << dendl;
	break;
      }
    }

    // frozen inode?
    bool tailitem = (dis->get_want().depth() == 0) || (i == dis->get_want().depth() - 1);
    if (dnl->is_primary() && dnl->get_inode()->is_frozen_inode()) {
      if (tailitem && dis->is_path_locked()) {
	dout(7) << "handle_discover allowing discovery of frozen tail " << *dnl->get_inode() << dendl;
      } else if (reply->is_empty()) {
	dout(7) << *dnl->get_inode() << " is frozen, empty reply, waiting" << dendl;
	dnl->get_inode()->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	return;
      } else {
	dout(7) << *dnl->get_inode() << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }

    // add dentry
    if (!reply->trace.length())
      reply->starts_with = MDiscoverReply::DENTRY;
    encode_replica_dentry(dn, from, reply->trace);
    dout(7) << "handle_discover added dentry " << *dn << dendl;
    
    if (!dnl->is_primary()) break;  // stop on null or remote link.
    
    // add inode
    CInode *next = dnl->get_inode();
    ceph_assert(next->is_auth());
    
    encode_replica_inode(next, from, reply->trace, mds->mdsmap->get_up_features());
    dout(7) << "handle_discover added inode " << *next << dendl;
    
    // descend, keep going.
    cur = next;
    continue;
  }

  // how did we do?
  ceph_assert(!reply->is_empty());
  dout(7) << "handle_discover sending result back to asker mds." << from << dendl;
  mds->send_message(reply, dis->get_connection());
}

void MDCache::handle_discover_reply(const cref_t<MDiscoverReply> &m)
{
  /*
  if (mds->get_state() < MDSMap::STATE_ACTIVE) {
    dout(0) << "discover_reply NOT ACTIVE YET" << dendl;
    return;
  }
  */
  dout(7) << "discover_reply " << *m << dendl;
  if (m->is_flag_error_dir()) 
    dout(7) << " flag error, dir" << dendl;
  if (m->is_flag_error_dn()) 
    dout(7) << " flag error, dentry = " << m->get_error_dentry() << dendl;

  MDSContext::vec finished, error;
  mds_rank_t from = mds_rank_t(m->get_source().num());

  // starting point
  CInode *cur = get_inode(m->get_base_ino());
  auto p = m->trace.cbegin();

  int next = m->starts_with;

  // decrement discover counters
  if (m->get_tid()) {
    map<ceph_tid_t,discover_info_t>::iterator p = discovers.find(m->get_tid());
    if (p != discovers.end()) {
      dout(10) << " found tid " << m->get_tid() << dendl;
      discovers.erase(p);
    } else {
      dout(10) << " tid " << m->get_tid() << " not found, must be dup reply" << dendl;
    }
  }

  // discover may start with an inode
  if (!p.end() && next == MDiscoverReply::INODE) {
    decode_replica_inode(cur, p, NULL, finished);
    dout(7) << "discover_reply got base inode " << *cur << dendl;
    ceph_assert(cur->is_base());
    
    next = MDiscoverReply::DIR;
    
    // take waiters?
    if (cur->is_base() &&
	waiting_for_base_ino[from].count(cur->ino())) {
      finished.swap(waiting_for_base_ino[from][cur->ino()]);
      waiting_for_base_ino[from].erase(cur->ino());
    }
  }
  ceph_assert(cur);
  
  // loop over discover results.
  // indexes follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  while (!p.end()) {
    // dir
    frag_t fg;
    CDir *curdir = nullptr;
    if (next == MDiscoverReply::DIR) {
      decode_replica_dir(curdir, p, cur, mds_rank_t(m->get_source().num()), finished);
      if (cur->ino() == m->get_base_ino() && curdir->get_frag() != m->get_base_dir_frag()) {
	ceph_assert(m->get_wanted_base_dir());
	cur->take_dir_waiting(m->get_base_dir_frag(), finished);
      }
    } else {
      // note: this can only happen our first way around this loop.
      if (p.end() && m->is_flag_error_dn()) {
	fg = cur->pick_dirfrag(m->get_error_dentry());
	curdir = cur->get_dirfrag(fg);
      } else
	curdir = cur->get_dirfrag(m->get_base_dir_frag());
    }

    if (p.end())
      break;
    
    // dentry
    CDentry *dn = nullptr;
    decode_replica_dentry(dn, p, curdir, finished);
    
    if (p.end())
      break;

    // inode
    decode_replica_inode(cur, p, dn, finished);

    next = MDiscoverReply::DIR;
  }

  // dir error?
  // or dir_auth hint?
  if (m->is_flag_error_dir() && !cur->is_dir()) {
    // not a dir.
    cur->take_waiting(CInode::WAIT_DIR, error);
  } else if (m->is_flag_error_dir() || m->get_dir_auth_hint() != CDIR_AUTH_UNKNOWN) {
    mds_rank_t who = m->get_dir_auth_hint();
    if (who == mds->get_nodeid()) who = -1;
    if (who >= 0)
      dout(7) << " dir_auth_hint is " << m->get_dir_auth_hint() << dendl;


    if (m->get_wanted_base_dir()) {
      frag_t fg = m->get_base_dir_frag();
      CDir *dir = cur->get_dirfrag(fg);

      if (cur->is_waiting_for_dir(fg)) {
	if (cur->is_auth())
	  cur->take_waiting(CInode::WAIT_DIR, finished);
	else if (dir || !cur->dirfragtree.is_leaf(fg))
	  cur->take_dir_waiting(fg, finished);
	else
	  discover_dir_frag(cur, fg, 0, who);
      } else
	dout(7) << " doing nothing, nobody is waiting for dir" << dendl;
    }

    // try again?
    if (m->get_error_dentry().length()) {
      frag_t fg = cur->pick_dirfrag(m->get_error_dentry());
      CDir *dir = cur->get_dirfrag(fg);
      // wanted a dentry
      if (dir && dir->is_waiting_for_dentry(m->get_error_dentry(), m->get_wanted_snapid())) {
	if (dir->is_auth() || dir->lookup(m->get_error_dentry())) {
	  dir->take_dentry_waiting(m->get_error_dentry(), m->get_wanted_snapid(),
				   m->get_wanted_snapid(), finished);
	} else {
	  filepath relpath(m->get_error_dentry(), 0);
	  discover_path(dir, m->get_wanted_snapid(), relpath, 0, m->is_path_locked());
	}
      } else
	dout(7) << " doing nothing, have dir but nobody is waiting on dentry "
		<< m->get_error_dentry() << dendl;
    }
  } else if (m->is_flag_error_dn()) {
    frag_t fg = cur->pick_dirfrag(m->get_error_dentry());
    CDir *dir = cur->get_dirfrag(fg);
    if (dir && !dir->is_auth()) {
      dir->take_dentry_waiting(m->get_error_dentry(), m->get_wanted_snapid(),
			       m->get_wanted_snapid(), error);
    }
  }

  // waiters
  finish_contexts(g_ceph_context, error, -CEPHFS_ENOENT);  // finish errors directly
  mds->queue_waiters(finished);
}



// ----------------------------
// REPLICAS


void MDCache::encode_replica_dir(CDir *dir, mds_rank_t to, bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  dirfrag_t df = dir->dirfrag();
  encode(df, bl);
  __u32 nonce = dir->add_replica(to);
  encode(nonce, bl);
  dir->_encode_base(bl);
  ENCODE_FINISH(bl);
}

void MDCache::encode_replica_dentry(CDentry *dn, mds_rank_t to, bufferlist& bl)
{
  ENCODE_START(2, 1, bl);
  encode(dn->get_name(), bl);
  encode(dn->last, bl);

  __u32 nonce = dn->add_replica(to);
  encode(nonce, bl);
  encode(dn->first, bl);
  encode(dn->linkage.remote_ino, bl);
  encode(dn->linkage.remote_d_type, bl);
  dn->lock.encode_state_for_replica(bl);
  bool need_recover = mds->get_state() < MDSMap::STATE_ACTIVE;
  encode(need_recover, bl);
  encode(dn->alternate_name, bl);
  ENCODE_FINISH(bl);
}

void MDCache::encode_replica_inode(CInode *in, mds_rank_t to, bufferlist& bl,
			      uint64_t features)
{
  ceph_assert(in->is_auth());

  ENCODE_START(2, 1, bl);
  encode(in->ino(), bl);  // bleh, minor assymetry here
  encode(in->last, bl);

  __u32 nonce = in->add_replica(to);
  encode(nonce, bl);

  in->_encode_base(bl, features);
  in->_encode_locks_state_for_replica(bl, mds->get_state() < MDSMap::STATE_ACTIVE);

  __u32 state = in->state;
  encode(state, bl);

  ENCODE_FINISH(bl);
}

void MDCache::decode_replica_dir(CDir *&dir, bufferlist::const_iterator& p, CInode *diri, mds_rank_t from,
			       MDSContext::vec& finished)
{
  DECODE_START(1, p);
  dirfrag_t df;
  decode(df, p);

  ceph_assert(diri->ino() == df.ino);

  // add it (_replica_)
  dir = diri->get_dirfrag(df.frag);

  if (dir) {
    // had replica. update w/ new nonce.
    __u32 nonce;
    decode(nonce, p);
    dir->set_replica_nonce(nonce);
    dir->_decode_base(p);
    dout(7) << __func__ << " had " << *dir << " nonce " << dir->replica_nonce << dendl;
  } else {
    // force frag to leaf in the diri tree
    if (!diri->dirfragtree.is_leaf(df.frag)) {
      dout(7) << __func__ << " forcing frag " << df.frag << " to leaf in the fragtree "
	      << diri->dirfragtree << dendl;
      diri->dirfragtree.force_to_leaf(g_ceph_context, df.frag);
    }
    // add replica.
    dir = diri->add_dirfrag( new CDir(diri, df.frag, this, false) );
    __u32 nonce;
    decode(nonce, p);
    dir->set_replica_nonce(nonce);
    dir->_decode_base(p);
    // is this a dir_auth delegation boundary?
    if (from != diri->authority().first ||
	diri->is_ambiguous_auth() ||
	diri->is_base())
      adjust_subtree_auth(dir, from);
    
    dout(7) << __func__ << " added " << *dir << " nonce " << dir->replica_nonce << dendl;
    // get waiters
    diri->take_dir_waiting(df.frag, finished);
  }
  DECODE_FINISH(p);
}

void MDCache::decode_replica_dentry(CDentry *&dn, bufferlist::const_iterator& p, CDir *dir, MDSContext::vec& finished)
{
  DECODE_START(1, p);
  string name;
  snapid_t last;
  decode(name, p);
  decode(last, p);

  dn = dir->lookup(name, last);
  
  // have it?
  bool is_new = false;
  if (dn) {
    is_new = false;
    dout(7) << __func__ << " had " << *dn << dendl;
  } else {
    is_new = true;
    dn = dir->add_null_dentry(name, 1 /* this will get updated below */, last);
    dout(7) << __func__ << " added " << *dn << dendl;
  }
  
  __u32 nonce;
  decode(nonce, p);
  dn->set_replica_nonce(nonce); 
  decode(dn->first, p);

  inodeno_t rino;
  unsigned char rdtype;
  decode(rino, p);
  decode(rdtype, p);
  dn->lock.decode_state(p, is_new);

  bool need_recover;
  decode(need_recover, p);

  mempool::mds_co::string alternate_name;
  if (struct_v >= 2) {
    decode(alternate_name, p);
  }

  if (is_new) {
    dn->set_alternate_name(std::move(alternate_name));
    if (rino)
      dir->link_remote_inode(dn, rino, rdtype);
    if (need_recover)
      dn->lock.mark_need_recover();
  } else {
    ceph_assert(dn->alternate_name == alternate_name);
  }

  dir->take_dentry_waiting(name, dn->first, dn->last, finished);
  DECODE_FINISH(p);
}

void MDCache::decode_replica_inode(CInode *&in, bufferlist::const_iterator& p, CDentry *dn, MDSContext::vec& finished)
{
  DECODE_START(2, p);
  inodeno_t ino;
  snapid_t last;
  __u32 nonce;
  decode(ino, p);
  decode(last, p);
  decode(nonce, p);
  in = get_inode(ino, last);
  if (!in) {
    in = new CInode(this, false, 2, last);
    in->set_replica_nonce(nonce);
    in->_decode_base(p);
    in->_decode_locks_state_for_replica(p, true);
    add_inode(in);
    if (in->ino() == CEPH_INO_ROOT)
      in->inode_auth.first = 0;
    else if (in->is_mdsdir())
      in->inode_auth.first = in->ino() - MDS_INO_MDSDIR_OFFSET;
    dout(10) << __func__ << " added " << *in << dendl;
    if (dn) {
      ceph_assert(dn->get_linkage()->is_null());
      dn->dir->link_primary_inode(dn, in);
    }
  } else {
    in->set_replica_nonce(nonce);
    in->_decode_base(p);
    in->_decode_locks_state_for_replica(p, false);
    dout(10) << __func__ << " had " << *in << dendl;
  }

  if (dn) {
    if (!dn->get_linkage()->is_primary() || dn->get_linkage()->get_inode() != in)
      dout(10) << __func__ << " different linkage in dentry " << *dn << dendl;
  }

  if (struct_v >= 2) {
    __u32 s;
    decode(s, p);
    s &= CInode::MASK_STATE_REPLICATED;
    if (s & CInode::STATE_RANDEPHEMERALPIN) {
      dout(10) << "replica inode is random ephemeral pinned" << dendl;
      in->set_ephemeral_pin(false, true);
    }
  }

  DECODE_FINISH(p); 
}

 
void MDCache::encode_replica_stray(CDentry *straydn, mds_rank_t who, bufferlist& bl)
{
  ceph_assert(straydn->get_num_auth_pins());
  ENCODE_START(2, 1, bl);
  uint64_t features = mds->mdsmap->get_up_features();
  encode_replica_inode(get_myin(), who, bl, features);
  encode_replica_dir(straydn->get_dir()->inode->get_parent_dn()->get_dir(), who, bl);
  encode_replica_dentry(straydn->get_dir()->inode->get_parent_dn(), who, bl);
  encode_replica_inode(straydn->get_dir()->inode, who, bl, features);
  encode_replica_dir(straydn->get_dir(), who, bl);
  encode_replica_dentry(straydn, who, bl);
  if (!straydn->get_projected_linkage()->is_null()) {
    encode_replica_inode(straydn->get_projected_linkage()->get_inode(), who, bl, features);
  }
  ENCODE_FINISH(bl);
}
   
void MDCache::decode_replica_stray(CDentry *&straydn, CInode **in, const bufferlist &bl, mds_rank_t from)
{
  MDSContext::vec finished;
  auto p = bl.cbegin();

  DECODE_START(2, p);
  CInode *mdsin = nullptr;
  decode_replica_inode(mdsin, p, NULL, finished);
  CDir *mdsdir = nullptr;
  decode_replica_dir(mdsdir, p, mdsin, from, finished);
  CDentry *straydirdn = nullptr; 
  decode_replica_dentry(straydirdn, p, mdsdir, finished);
  CInode *strayin = nullptr;
  decode_replica_inode(strayin, p, straydirdn, finished);
  CDir *straydir = nullptr;
  decode_replica_dir(straydir, p, strayin, from, finished);

  decode_replica_dentry(straydn, p, straydir, finished);
  if (struct_v >= 2 && in) {
    decode_replica_inode(*in, p, straydn, finished);
  }
  if (!finished.empty())
    mds->queue_waiters(finished);
  DECODE_FINISH(p);
}


int MDCache::send_dir_updates(CDir *dir, bool bcast)
{
  // this is an FYI, re: replication

  set<mds_rank_t> who;
  if (bcast) {
    set<mds_rank_t> mds_set;
    mds->get_mds_map()->get_active_mds_set(mds_set);

    set<mds_rank_t> replica_set;
    for (const auto &p : dir->get_replicas()) {
      replica_set.insert(p.first);
    }

    std::set_difference(mds_set.begin(), mds_set.end(),
                        replica_set.begin(), replica_set.end(),
                        std::inserter(who, who.end()));
  } else {
    for (const auto &p : dir->get_replicas()) {
      who.insert(p.first);
    }
  }
  
  dout(7) << "sending dir_update on " << *dir << " bcast " << bcast << " to " << who << dendl;

  filepath path;
  dir->inode->make_path(path);

  std::set<int32_t> dir_rep_set;
  for (const auto &r : dir->dir_rep_by) {
    dir_rep_set.insert(r);
  }

  mds_rank_t whoami = mds->get_nodeid();
  for (set<mds_rank_t>::iterator it = who.begin();
       it != who.end();
       ++it) {
    if (*it == whoami) continue;
    //if (*it == except) continue;
    dout(7) << "sending dir_update on " << *dir << " to " << *it << dendl;

    logger->inc(l_mdc_dir_update);
    mds->send_message_mds(make_message<MDirUpdate>(mds->get_nodeid(), dir->dirfrag(), dir->dir_rep, dir_rep_set, path, bcast), *it);
  }

  return 0;
}

void MDCache::handle_dir_update(const cref_t<MDirUpdate> &m)
{
  dirfrag_t df = m->get_dirfrag();
  CDir *dir = get_dirfrag(df);
  logger->inc(l_mdc_dir_update_receipt);
  if (!dir) {
    dout(5) << "dir_update on " << df << ", don't have it" << dendl;

    // discover it?
    if (m->should_discover()) {
      // only try once! 
      // this is key to avoid a fragtree update race, among other things.
      m->inc_tried_discover();
      vector<CDentry*> trace;
      CInode *in;
      filepath path = m->get_path();
      dout(5) << "trying discover on dir_update for " << path << dendl;
      logger->inc(l_mdc_dir_try_discover);
      CF_MDS_RetryMessageFactory cf(mds, m);
      MDRequestRef null_ref;
      int r = path_traverse(null_ref, cf, path, MDS_TRAVERSE_DISCOVER, &trace, &in);
      if (r > 0)
        return;
      if (r == 0 &&
	  in->ino() == df.ino &&
	  in->get_approx_dirfrag(df.frag) == NULL) {
	open_remote_dirfrag(in, df.frag, new C_MDS_RetryMessage(mds, m));
	return;
      }
    }

    return;
  }

  if (!m->has_tried_discover()) {
    // Update if it already exists. Othwerwise it got updated by discover reply.
    dout(5) << "dir_update on " << *dir << dendl;
    dir->dir_rep = m->get_dir_rep();
    dir->dir_rep_by.clear();
    for (const auto &e : m->get_dir_rep_by()) {
      dir->dir_rep_by.insert(e);
    }
  }
}





// LINK

void MDCache::encode_remote_dentry_link(CDentry::linkage_t *dnl, bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  inodeno_t ino = dnl->get_remote_ino();
  encode(ino, bl);
  __u8 d_type = dnl->get_remote_d_type();
  encode(d_type, bl);
  ENCODE_FINISH(bl);
}

void MDCache::decode_remote_dentry_link(CDir *dir, CDentry *dn, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  inodeno_t ino;
  __u8 d_type;
  decode(ino, p);
  decode(d_type, p);
  dout(10) << __func__ << "  remote " << ino << " " << d_type << dendl;
  dir->link_remote_inode(dn, ino, d_type);
  DECODE_FINISH(p);
}

void MDCache::send_dentry_link(CDentry *dn, const MDRequestRef& mdr)
{
  dout(7) << __func__ << " " << *dn << dendl;

  CDir *subtree = get_subtree_root(dn->get_dir());
  for (const auto &p : dn->get_replicas()) {
    // don't tell (rename) witnesses; they already know
    if (mdr.get() && mdr->more()->witnessed.count(p.first))
      continue;
    if (mds->mdsmap->get_state(p.first) < MDSMap::STATE_REJOIN ||
	(mds->mdsmap->get_state(p.first) == MDSMap::STATE_REJOIN &&
	 rejoin_gather.count(p.first)))
      continue;
    CDentry::linkage_t *dnl = dn->get_linkage();
    auto m = make_message<MDentryLink>(subtree->dirfrag(), dn->get_dir()->dirfrag(), dn->get_name(), dnl->is_primary());
    if (dnl->is_primary()) {
      dout(10) << __func__ << "  primary " << *dnl->get_inode() << dendl;
      encode_replica_inode(dnl->get_inode(), p.first, m->bl,
		      mds->mdsmap->get_up_features());
    } else if (dnl->is_remote()) {
      encode_remote_dentry_link(dnl, m->bl);
    } else
      ceph_abort();   // aie, bad caller!
    mds->send_message_mds(m, p.first);
  }
}

void MDCache::handle_dentry_link(const cref_t<MDentryLink> &m)
{
  CDentry *dn = NULL;
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(7) << __func__ << " don't have dirfrag " << m->get_dirfrag() << dendl;
  } else {
    dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << __func__ << " don't have dentry " << *dir << " dn " << m->get_dn() << dendl;
    } else {
      dout(7) << __func__ << " on " << *dn << dendl;
      CDentry::linkage_t *dnl = dn->get_linkage();

      ceph_assert(!dn->is_auth());
      ceph_assert(dnl->is_null());
    }
  }

  auto p = m->bl.cbegin();
  MDSContext::vec finished;
  if (dn) {
    if (m->get_is_primary()) {
      // primary link.
      CInode *in = nullptr;
      decode_replica_inode(in, p, dn, finished);
    } else {
      // remote link, easy enough.
      decode_remote_dentry_link(dir, dn, p);
    }
  } else {
    ceph_abort();
  }

  if (!finished.empty())
    mds->queue_waiters(finished);

  return;
}


// UNLINK

void MDCache::send_dentry_unlink(CDentry *dn, CDentry *straydn, const MDRequestRef& mdr)
{
  dout(10) << __func__ << " " << *dn << dendl;
  // share unlink news with replicas
  set<mds_rank_t> replicas;
  dn->list_replicas(replicas);
  bufferlist snapbl;
  if (straydn) {
    straydn->list_replicas(replicas);
    CInode *strayin = straydn->get_linkage()->get_inode();
    strayin->encode_snap_blob(snapbl);
  }
  for (set<mds_rank_t>::iterator it = replicas.begin();
       it != replicas.end();
       ++it) {
    // don't tell (rmdir) witnesses; they already know
    if (mdr.get() && mdr->more()->witnessed.count(*it))
      continue;

    if (mds->mdsmap->get_state(*it) < MDSMap::STATE_REJOIN ||
	(mds->mdsmap->get_state(*it) == MDSMap::STATE_REJOIN &&
	 rejoin_gather.count(*it)))
      continue;

    auto unlink = make_message<MDentryUnlink>(dn->get_dir()->dirfrag(), dn->get_name());
    if (straydn) {
      encode_replica_stray(straydn, *it, unlink->straybl);
      unlink->snapbl = snapbl;
    }
    mds->send_message_mds(unlink, *it);
  }
}

void MDCache::handle_dentry_unlink(const cref_t<MDentryUnlink> &m)
{
  // straydn
  CDentry *straydn = nullptr;
  CInode *strayin = nullptr;
  if (m->straybl.length())
    decode_replica_stray(straydn, &strayin, m->straybl, mds_rank_t(m->get_source().num()));

  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(7) << __func__ << " don't have dirfrag " << m->get_dirfrag() << dendl;
  } else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << __func__ << " don't have dentry " << *dir << " dn " << m->get_dn() << dendl;
    } else {
      dout(7) << __func__ << " on " << *dn << dendl;
      CDentry::linkage_t *dnl = dn->get_linkage();

      // open inode?
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	dn->dir->unlink_inode(dn);
	ceph_assert(straydn);
	straydn->dir->link_primary_inode(straydn, in);

	// in->first is lazily updated on replica; drag it forward so
	// that we always keep it in sync with the dnq
	ceph_assert(straydn->first >= in->first);
	in->first = straydn->first;

	// update subtree map?
	if (in->is_dir()) 
	  adjust_subtree_after_rename(in, dir, false);

	if (m->snapbl.length()) {
	  bool hadrealm = (in->snaprealm ? true : false);
	  in->decode_snap_blob(m->snapbl);
	  ceph_assert(in->snaprealm);
	  if (!hadrealm)
	    do_realm_invalidate_and_update_notify(in, CEPH_SNAP_OP_SPLIT, false);
	}

	// send caps to auth (if we're not already)
	if (in->is_any_caps() &&
	    !in->state_test(CInode::STATE_EXPORTINGCAPS))
	  migrator->export_caps(in);
	
	straydn = NULL;
      } else {
	ceph_assert(!straydn);
	ceph_assert(dnl->is_remote());
	dn->dir->unlink_inode(dn);
      }
      ceph_assert(dnl->is_null());
    }
  }

  // race with trim_dentry()
  if (straydn) {
    ceph_assert(straydn->get_num_ref() == 0);
    ceph_assert(straydn->get_linkage()->is_null());
    expiremap ex;
    trim_dentry(straydn, ex);
    send_expire_messages(ex);
  }
}






// ===================================================================



// ===================================================================
// FRAGMENT


/** 
 * adjust_dir_fragments -- adjust fragmentation for a directory
 *
 * @param diri directory inode
 * @param basefrag base fragment
 * @param bits bit adjustment.  positive for split, negative for merge.
 */
void MDCache::adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
				   std::vector<CDir*>* resultfrags,
				   MDSContext::vec& waiters,
				   bool replay)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " " << bits 
	   << " on " << *diri << dendl;

  auto&& p = diri->get_dirfrags_under(basefrag);

  adjust_dir_fragments(diri, p.second, basefrag, bits, resultfrags, waiters, replay);
}

CDir *MDCache::force_dir_fragment(CInode *diri, frag_t fg, bool replay)
{
  CDir *dir = diri->get_dirfrag(fg);
  if (dir)
    return dir;

  dout(10) << "force_dir_fragment " << fg << " on " << *diri << dendl;

  std::vector<CDir*> src, result;
  MDSContext::vec waiters;

  // split a parent?
  frag_t parent = diri->dirfragtree.get_branch_or_leaf(fg);
  while (1) {
    CDir *pdir = diri->get_dirfrag(parent);
    if (pdir) {
      int split = fg.bits() - parent.bits();
      dout(10) << " splitting parent by " << split << " " << *pdir << dendl;
      src.push_back(pdir);
      adjust_dir_fragments(diri, src, parent, split, &result, waiters, replay);
      dir = diri->get_dirfrag(fg);
      if (dir) {
	dout(10) << "force_dir_fragment result " << *dir << dendl;
	break;
      }
    }
    if (parent == frag_t())
      break;
    frag_t last = parent;
    parent = parent.parent();
    dout(10) << " " << last << " parent is " << parent << dendl;
  }

  if (!dir) {
    // hoover up things under fg?
    {
      auto&& p = diri->get_dirfrags_under(fg);
      src.insert(std::end(src), std::cbegin(p.second), std::cend(p.second));
    }
    if (src.empty()) {
      dout(10) << "force_dir_fragment no frags under " << fg << dendl;
    } else {
      dout(10) << " will combine frags under " << fg << ": " << src << dendl;
      adjust_dir_fragments(diri, src, fg, 0, &result, waiters, replay);
      dir = result.front();
      dout(10) << "force_dir_fragment result " << *dir << dendl;
    }
  }
  if (!replay)
    mds->queue_waiters(waiters);
  return dir;
}

void MDCache::adjust_dir_fragments(CInode *diri,
				   const std::vector<CDir*>& srcfrags,
				   frag_t basefrag, int bits,
				   std::vector<CDir*>* resultfrags,
				   MDSContext::vec& waiters,
				   bool replay)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " bits " << bits
	   << " srcfrags " << srcfrags
	   << " on " << *diri << dendl;

  // adjust fragtree
  // yuck.  we may have discovered the inode while it was being fragmented.
  if (!diri->dirfragtree.is_leaf(basefrag))
    diri->dirfragtree.force_to_leaf(g_ceph_context, basefrag);

  if (bits > 0)
    diri->dirfragtree.split(basefrag, bits);
  dout(10) << " new fragtree is " << diri->dirfragtree << dendl;

  if (srcfrags.empty())
    return;

  // split
  CDir *parent_dir = diri->get_parent_dir();
  CDir *parent_subtree = 0;
  if (parent_dir)
    parent_subtree = get_subtree_root(parent_dir);

  ceph_assert(srcfrags.size() >= 1);
  if (bits > 0) {
    // SPLIT
    ceph_assert(srcfrags.size() == 1);
    CDir *dir = srcfrags.front();

    dir->split(bits, resultfrags, waiters, replay);

    // did i change the subtree map?
    if (dir->is_subtree_root()) {
      // new frags are now separate subtrees
      for (const auto& dir : *resultfrags) {
	subtrees[dir].clear();   // new frag is now its own subtree
      }
      
      // was i a bound?
      if (parent_subtree) {
	ceph_assert(subtrees[parent_subtree].count(dir));
	subtrees[parent_subtree].erase(dir);
	for (const auto& dir : *resultfrags) {
	  ceph_assert(dir->is_subtree_root());
	  subtrees[parent_subtree].insert(dir);
	}
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
    }
    
    diri->close_dirfrag(dir->get_frag());
    
  } else {
    // MERGE

    // are my constituent bits subtrees?  if so, i will be too.
    // (it's all or none, actually.)
    bool any_subtree = false, any_non_subtree = false;
    for (const auto& dir : srcfrags) {
      if (dir->is_subtree_root())
	any_subtree = true;
      else
	any_non_subtree = true;
    }
    ceph_assert(!any_subtree || !any_non_subtree);

    set<CDir*> new_bounds;
    if (any_subtree)  {
      for (const auto& dir : srcfrags) {
	// this simplifies the code that find subtrees underneath the dirfrag
	if (!dir->is_subtree_root()) {
	  dir->state_set(CDir::STATE_AUXSUBTREE);
	  adjust_subtree_auth(dir, mds->get_nodeid());
	}
      }

      for (const auto& dir : srcfrags) {
	ceph_assert(dir->is_subtree_root());
	dout(10) << " taking srcfrag subtree bounds from " << *dir << dendl;
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

    if (any_subtree) {
      ceph_assert(f->is_subtree_root());
      subtrees[f].swap(new_bounds);
      if (parent_subtree)
	subtrees[parent_subtree].insert(f);
      
      show_subtrees(10);
    }

    resultfrags->push_back(f);
  }
}


class C_MDC_FragmentFrozen : public MDSInternalContext {
  MDCache *mdcache;
  MDRequestRef mdr;
public:
  C_MDC_FragmentFrozen(MDCache *m, const MDRequestRef& r) :
    MDSInternalContext(m->mds), mdcache(m), mdr(r) {}
  void finish(int r) override {
    mdcache->fragment_frozen(mdr, r);
  }
};

bool MDCache::can_fragment(CInode *diri, const std::vector<CDir*>& dirs)
{
  if (is_readonly()) {
    dout(7) << "can_fragment: read-only FS, no fragmenting for now" << dendl;
    return false;
  }
  if (mds->is_cluster_degraded()) {
    dout(7) << "can_fragment: cluster degraded, no fragmenting for now" << dendl;
    return false;
  }
  if (diri->get_parent_dir() &&
      diri->get_parent_dir()->get_inode()->is_stray()) {
    dout(7) << "can_fragment: i won't merge|split anything in stray" << dendl;
    return false;
  }
  if (diri->is_mdsdir() || diri->ino() == CEPH_INO_CEPH) {
    dout(7) << "can_fragment: i won't fragment mdsdir or .ceph" << dendl;
    return false;
  }

  for (const auto& dir : dirs) {
    if (dir->scrub_is_in_progress()) {
      dout(7) << "can_fragment: scrub in progress " << *dir << dendl;
      return false;
    }

    if (dir->state_test(CDir::STATE_FRAGMENTING)) {
      dout(7) << "can_fragment: already fragmenting " << *dir << dendl;
      return false;
    }
    if (!dir->is_auth()) {
      dout(7) << "can_fragment: not auth on " << *dir << dendl;
      return false;
    }
    if (dir->is_bad()) {
      dout(7) << "can_fragment: bad dirfrag " << *dir << dendl;
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
  dout(7) << __func__ << " " << *dir << " bits " << bits << dendl;
  ceph_assert(dir->is_auth());
  CInode *diri = dir->inode;

  std::vector<CDir*> dirs;
  dirs.push_back(dir);

  if (!can_fragment(diri, dirs)) {
    dout(7) << __func__ << " cannot fragment right now, dropping" << dendl;
    return;
  }

  if (dir->frag.bits() + bits > 24) {
    dout(7) << __func__ << " frag bits > 24, dropping" << dendl;
    return;
  }

  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_FRAGMENTDIR);
  mdr->more()->fragment_base = dir->dirfrag();

  ceph_assert(fragments.count(dir->dirfrag()) == 0);
  fragment_info_t& info = fragments[dir->dirfrag()];
  info.mdr = mdr;
  info.dirs.push_back(dir);
  info.bits = bits;
  info.last_cum_auth_pins_change = ceph_clock_now();

  fragment_freeze_dirs(dirs);
  // initial mark+complete pass
  fragment_mark_and_complete(mdr);
}

void MDCache::merge_dir(CInode *diri, frag_t frag)
{
  dout(7) << "merge_dir to " << frag << " on " << *diri << dendl;

  auto&& [all, dirs] = diri->get_dirfrags_under(frag);
  if (!all) {
    dout(7) << "don't have all frags under " << frag << " for " << *diri << dendl;
    return;
  }

  if (diri->dirfragtree.is_leaf(frag)) {
    dout(10) << " " << frag << " already a leaf for " << *diri << dendl;
    return;
  }

  if (!can_fragment(diri, dirs))
    return;

  CDir *first = dirs.front();
  int bits = first->get_frag().bits() - frag.bits();
  dout(10) << " we are merging by " << bits << " bits" << dendl;

  dirfrag_t basedirfrag(diri->ino(), frag);
  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_FRAGMENTDIR);
  mdr->more()->fragment_base = basedirfrag;

  ceph_assert(fragments.count(basedirfrag) == 0);
  fragment_info_t& info = fragments[basedirfrag];
  info.mdr = mdr;
  info.dirs = dirs;
  info.bits = -bits;
  info.last_cum_auth_pins_change = ceph_clock_now();

  fragment_freeze_dirs(dirs);
  // initial mark+complete pass
  fragment_mark_and_complete(mdr);
}

void MDCache::fragment_freeze_dirs(const std::vector<CDir*>& dirs)
{
  bool any_subtree = false, any_non_subtree = false;
  for (const auto& dir : dirs) {
    dir->auth_pin(dir);  // until we mark and complete them
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->freeze_dir();
    ceph_assert(dir->is_freezing_dir());

    if (dir->is_subtree_root())
      any_subtree = true;
    else
      any_non_subtree = true;
  }

  if (any_subtree && any_non_subtree) {
    // either all dirfrags are subtree roots or all are not.
    for (const auto& dir : dirs) {
      if (dir->is_subtree_root()) {
	ceph_assert(dir->state_test(CDir::STATE_AUXSUBTREE));
      } else {
	dir->state_set(CDir::STATE_AUXSUBTREE);
	adjust_subtree_auth(dir, mds->get_nodeid());
      }
    }
  }
}

class C_MDC_FragmentMarking : public MDCacheContext {
  MDRequestRef mdr;
public:
  C_MDC_FragmentMarking(MDCache *m, const MDRequestRef& r) : MDCacheContext(m), mdr(r) {}
  void finish(int r) override {
    mdcache->fragment_mark_and_complete(mdr);
  }
};

void MDCache::fragment_mark_and_complete(const MDRequestRef& mdr)
{
  dirfrag_t basedirfrag = mdr->more()->fragment_base;
  map<dirfrag_t,fragment_info_t>::iterator it = fragments.find(basedirfrag);
  if (it == fragments.end() || it->second.mdr != mdr) {
    dout(7) << "fragment_mark_and_complete " << basedirfrag << " must have aborted" << dendl;
    request_finish(mdr);
    return;
  }

  fragment_info_t& info = it->second;
  CInode *diri = info.dirs.front()->get_inode();
  dout(10) << "fragment_mark_and_complete " << info.dirs << " on " << *diri << dendl;

  MDSGatherBuilder gather(g_ceph_context);
  
  for (const auto& dir : info.dirs) {
    bool ready = true;
    if (!dir->is_complete()) {
      dout(15) << " fetching incomplete " << *dir << dendl;
      dir->fetch(gather.new_sub(), true);  // ignore authpinnability
      ready = false;
    } else if (dir->get_frag() == frag_t()) {
      // The COMPLETE flag gets lost if we fragment a new dirfrag, then rollback
      // the operation. To avoid CDir::fetch() complaining about missing object,
      // we commit new dirfrag first.
      if (dir->state_test(CDir::STATE_CREATING)) {
	dout(15) << " waiting until new dir gets journaled " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_CREATED, gather.new_sub());
	ready = false;
      } else if (dir->is_new()) {
	dout(15) << " committing new " << *dir << dendl;
	ceph_assert(dir->is_dirty());
	dir->commit(0, gather.new_sub(), true);
	ready = false;
      }
    }
    if (!ready)
      continue;

    if (!dir->state_test(CDir::STATE_DNPINNEDFRAG)) {
      dout(15) << " marking " << *dir << dendl;
      for (auto &p : dir->items) {
	CDentry *dn = p.second;
	dn->get(CDentry::PIN_FRAGMENTING);
	ceph_assert(!dn->state_test(CDentry::STATE_FRAGMENTING));
	dn->state_set(CDentry::STATE_FRAGMENTING);
      }
      dir->state_set(CDir::STATE_DNPINNEDFRAG);
      dir->auth_unpin(dir);
    } else {
      dout(15) << " already marked " << *dir << dendl;
    }
  }
  if (gather.has_subs()) {
    gather.set_finisher(new C_MDC_FragmentMarking(this, mdr));
    gather.activate();
    return;
  }

  for (const auto& dir : info.dirs) {
    if (!dir->is_frozen_dir()) {
      ceph_assert(dir->is_freezing_dir());
      dir->add_waiter(CDir::WAIT_FROZEN, gather.new_sub());
    }
  }
  if (gather.has_subs()) {
    gather.set_finisher(new C_MDC_FragmentFrozen(this, mdr));
    gather.activate();
    // flush log so that request auth_pins are retired
    mds->mdlog->flush();
    return;
  }

  fragment_frozen(mdr, 0);
}

void MDCache::fragment_unmark_unfreeze_dirs(const std::vector<CDir*>& dirs)
{
  dout(10) << "fragment_unmark_unfreeze_dirs " << dirs << dendl;
  for (const auto& dir : dirs) {
    dout(10) << " frag " << *dir << dendl;

    ceph_assert(dir->state_test(CDir::STATE_FRAGMENTING));
    dir->state_clear(CDir::STATE_FRAGMENTING);

    if (dir->state_test(CDir::STATE_DNPINNEDFRAG)) {
      dir->state_clear(CDir::STATE_DNPINNEDFRAG);

      for (auto &p : dir->items) {
	CDentry *dn = p.second;
	ceph_assert(dn->state_test(CDentry::STATE_FRAGMENTING));
	dn->state_clear(CDentry::STATE_FRAGMENTING);
	dn->put(CDentry::PIN_FRAGMENTING);
      }
    } else {
      dir->auth_unpin(dir);
    }

    dir->unfreeze_dir();
  }
}

bool MDCache::fragment_are_all_frozen(CDir *dir)
{
  ceph_assert(dir->is_frozen_dir());
  map<dirfrag_t,fragment_info_t>::iterator p;
  for (p = fragments.lower_bound(dirfrag_t(dir->ino(), 0));
       p != fragments.end() && p->first.ino == dir->ino();
       ++p) {
    if (p->first.frag.contains(dir->get_frag()))
      return p->second.all_frozen;
  }
  ceph_abort();
  return false;
}

void MDCache::fragment_freeze_inc_num_waiters(CDir *dir)
{
  map<dirfrag_t,fragment_info_t>::iterator p;
  for (p = fragments.lower_bound(dirfrag_t(dir->ino(), 0));
       p != fragments.end() && p->first.ino == dir->ino();
       ++p) {
    if (p->first.frag.contains(dir->get_frag())) {
      p->second.num_remote_waiters++;
      return;
    }
  }
  ceph_abort();
}

void MDCache::find_stale_fragment_freeze()
{
  dout(10) << "find_stale_fragment_freeze" << dendl;
  // see comment in Migrator::find_stale_export_freeze()
  utime_t now = ceph_clock_now();
  utime_t cutoff = now;
  cutoff -= g_conf()->mds_freeze_tree_timeout;

  for (map<dirfrag_t,fragment_info_t>::iterator p = fragments.begin();
       p != fragments.end(); ) {
    dirfrag_t df = p->first;
    fragment_info_t& info = p->second;
    ++p;
    if (info.all_frozen)
      continue;
    CDir *dir;
    int total_auth_pins = 0;
    for (const auto& d : info.dirs) {
      dir = d;
      if (!dir->state_test(CDir::STATE_DNPINNEDFRAG)) {
	total_auth_pins = -1;
	break;
      }
      if (dir->is_frozen_dir())
	continue;
      total_auth_pins += dir->get_auth_pins() + dir->get_dir_auth_pins();
    }
    if (total_auth_pins < 0)
      continue;
    if (info.last_cum_auth_pins != total_auth_pins) {
      info.last_cum_auth_pins = total_auth_pins;
      info.last_cum_auth_pins_change = now;
      continue;
    }
    if (info.last_cum_auth_pins_change >= cutoff)
      continue;
    dir = info.dirs.front();
    if (info.num_remote_waiters > 0 ||
	(!dir->inode->is_root() && dir->get_parent_dir()->is_freezing())) {
      dout(10) << " cancel fragmenting " << df << " bit " << info.bits << dendl;
      std::vector<CDir*> dirs;
      info.dirs.swap(dirs);
      fragments.erase(df);
      fragment_unmark_unfreeze_dirs(dirs);
    }
  }
}

class C_MDC_FragmentPrep : public MDCacheLogContext {
  MDRequestRef mdr;
public:
  C_MDC_FragmentPrep(MDCache *m, const MDRequestRef& r) : MDCacheLogContext(m),  mdr(r) {}
  void finish(int r) override {
    mdcache->_fragment_logged(mdr);
  }
};

class C_MDC_FragmentStore : public MDCacheContext {
  MDRequestRef mdr;
public:
  C_MDC_FragmentStore(MDCache *m, const MDRequestRef& r) : MDCacheContext(m), mdr(r) {}
  void finish(int r) override {
    mdcache->_fragment_stored(mdr);
  }
};

class C_MDC_FragmentCommit : public MDCacheLogContext {
  dirfrag_t basedirfrag;
  MDRequestRef mdr;
public:
  C_MDC_FragmentCommit(MDCache *m, dirfrag_t df, const MDRequestRef& r) :
    MDCacheLogContext(m), basedirfrag(df), mdr(r) {}
  void finish(int r) override {
    mdcache->_fragment_committed(basedirfrag, mdr);
  }
};

class C_IO_MDC_FragmentPurgeOld : public MDCacheIOContext {
  dirfrag_t basedirfrag;
  int bits;
  MDRequestRef mdr;
public:
  C_IO_MDC_FragmentPurgeOld(MDCache *m, dirfrag_t f, int b,
			    const MDRequestRef& r) :
    MDCacheIOContext(m), basedirfrag(f), bits(b), mdr(r) {}
  void finish(int r) override {
    ceph_assert(r == 0 || r == -CEPHFS_ENOENT);
    mdcache->_fragment_old_purged(basedirfrag, bits, mdr);
  }
  void print(ostream& out) const override {
    out << "fragment_purge_old(" << basedirfrag << ")";
  }
};

void MDCache::fragment_frozen(const MDRequestRef& mdr, int r)
{
  dirfrag_t basedirfrag = mdr->more()->fragment_base;
  map<dirfrag_t,fragment_info_t>::iterator it = fragments.find(basedirfrag);
  if (it == fragments.end() || it->second.mdr != mdr) {
    dout(7) << "fragment_frozen " << basedirfrag << " must have aborted" << dendl;
    request_finish(mdr);
    return;
  }

  ceph_assert(r == 0);
  fragment_info_t& info = it->second;
  dout(10) << "fragment_frozen " << basedirfrag.frag << " by " << info.bits
	   << " on " << info.dirs.front()->get_inode() << dendl;

  info.all_frozen = true;
  dispatch_fragment_dir(mdr);
}

void MDCache::dispatch_fragment_dir(const MDRequestRef& mdr)
{
  dirfrag_t basedirfrag = mdr->more()->fragment_base;
  map<dirfrag_t,fragment_info_t>::iterator it = fragments.find(basedirfrag);
  if (it == fragments.end() || it->second.mdr != mdr) {
    dout(7) << "dispatch_fragment_dir " << basedirfrag << " must have aborted" << dendl;
    request_finish(mdr);
    return;
  }

  fragment_info_t& info = it->second;
  CInode *diri = info.dirs.front()->get_inode();

  dout(10) << "dispatch_fragment_dir " << basedirfrag << " bits " << info.bits
	   << " on " << *diri << dendl;

  if (mdr->more()->peer_error)
    mdr->aborted = true;

  if (!mdr->aborted) {
    MutationImpl::LockOpVec lov;
    lov.add_wrlock(&diri->dirfragtreelock);
    // prevent a racing gather on any other scatterlocks too
    lov.lock_scatter_gather(&diri->nestlock);
    lov.lock_scatter_gather(&diri->filelock);
    if (!mds->locker->acquire_locks(mdr, lov, NULL, true)) {
      if (!mdr->aborted)
	return;
    }
  }

  if (mdr->aborted) {
    dout(10) << " can't auth_pin " << *diri << ", requeuing dir "
	     << info.dirs.front()->dirfrag() << dendl;
    if (info.bits > 0)
      mds->balancer->queue_split(info.dirs.front(), false);
    else
      mds->balancer->queue_merge(info.dirs.front());
    fragment_unmark_unfreeze_dirs(info.dirs);
    fragments.erase(it);
    request_finish(mdr);
    return;
  }

  mdr->ls = mds->mdlog->get_current_segment();
  EFragment *le = new EFragment(mds->mdlog, EFragment::OP_PREPARE, basedirfrag, info.bits);

  for (const auto& dir : info.dirs) {
    dirfrag_rollback rollback;
    rollback.fnode = dir->fnode;
    le->add_orig_frag(dir->get_frag(), &rollback);
  }

  // refragment
  MDSContext::vec waiters;
  adjust_dir_fragments(diri, info.dirs, basedirfrag.frag, info.bits,
		       &info.resultfrags, waiters, false);
  if (g_conf()->mds_debug_frag)
    diri->verify_dirfrags();
  mds->queue_waiters(waiters);

  for (const auto& fg : le->orig_frags)
    ceph_assert(!diri->dirfragtree.is_leaf(fg));

  le->metablob.add_dir_context(info.resultfrags.front());
  for (const auto& dir : info.resultfrags) {
    if (diri->is_auth()) {
      le->metablob.add_fragmented_dir(dir, false, false);
    } else {
      dir->state_set(CDir::STATE_DIRTYDFT);
      le->metablob.add_fragmented_dir(dir, false, true);
    }
  }

  // dft lock
  if (diri->is_auth()) {
    // journal dirfragtree
    auto pi = diri->project_inode(mdr);
    pi.inode->version = diri->pre_dirty();
    predirty_journal_parents(mdr, &le->metablob, diri, 0, PREDIRTY_PRIMARY);
    journal_dirty_inode(mdr.get(), &le->metablob, diri);
  } else {
    mds->locker->mark_updated_scatterlock(&diri->dirfragtreelock);
    mdr->ls->dirty_dirfrag_dirfragtree.push_back(&diri->item_dirty_dirfrag_dirfragtree);
    mdr->add_updated_lock(&diri->dirfragtreelock);
  }

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

  add_uncommitted_fragment(basedirfrag, info.bits, le->orig_frags, mdr->ls);
  mds->server->submit_mdlog_entry(le, new C_MDC_FragmentPrep(this, mdr),
                                  mdr, __func__);
  mds->mdlog->flush();
}

void MDCache::_fragment_logged(const MDRequestRef& mdr)
{
  dirfrag_t basedirfrag = mdr->more()->fragment_base;
  auto& info = fragments.at(basedirfrag);
  CInode *diri = info.resultfrags.front()->get_inode();

  dout(10) << "fragment_logged " << basedirfrag << " bits " << info.bits
	   << " on " << *diri << dendl;
  mdr->mark_event("prepare logged");

  mdr->apply();  // mark scatterlock

  // store resulting frags
  MDSGatherBuilder gather(g_ceph_context, new C_MDC_FragmentStore(this, mdr));

  for (const auto& dir : info.resultfrags) {
    dout(10) << " storing result frag " << *dir << dendl;

    dir->mark_dirty(mdr->ls);
    dir->mark_new(mdr->ls);

    // freeze and store them too
    dir->auth_pin(this);
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->commit(0, gather.new_sub(), true);  // ignore authpinnability
  }

  gather.activate();
}

void MDCache::_fragment_stored(const MDRequestRef& mdr)
{
  dirfrag_t basedirfrag = mdr->more()->fragment_base;
  fragment_info_t &info = fragments.at(basedirfrag);
  CDir *first = info.resultfrags.front();
  CInode *diri = first->get_inode();

  dout(10) << "fragment_stored " << basedirfrag << " bits " << info.bits
	   << " on " << *diri << dendl;
  mdr->mark_event("new frags stored");

  // tell peers
  mds_rank_t diri_auth = (first->is_subtree_root() && !diri->is_auth()) ?
			  diri->authority().first : CDIR_AUTH_UNKNOWN;
  for (const auto &p : first->get_replicas()) {
    if (mds->mdsmap->get_state(p.first) < MDSMap::STATE_REJOIN ||
	(mds->mdsmap->get_state(p.first) == MDSMap::STATE_REJOIN &&
	 rejoin_gather.count(p.first)))
      continue;

    auto notify = make_message<MMDSFragmentNotify>(basedirfrag, info.bits, mdr->reqid.tid);
    if (diri_auth != CDIR_AUTH_UNKNOWN && // subtree root
	diri_auth != p.first) { // not auth mds of diri
      /*
       * In the nornal case, mds does not trim dir inode whose child dirfrags
       * are likely being fragmented (see trim_inode()). But when fragmenting
       * subtree roots, following race can happen:
       *
       * - mds.a (auth mds of dirfrag) sends fragment_notify message to
       *   mds.c and drops wrlock on dirfragtreelock.
       * - mds.b (auth mds of dir inode) changes dirfragtreelock state to
       *   SYNC and send lock message mds.c
       * - mds.c receives the lock message and changes dirfragtreelock state
       *   to SYNC
       * - mds.c trim dirfrag and dir inode from its cache
       * - mds.c receives the fragment_notify message
       *
       * So we need to ensure replicas have received the notify, then unlock
       * the dirfragtreelock.
       */
      notify->mark_ack_wanted();
      info.notify_ack_waiting.insert(p.first);
    }

    // freshly replicate new dirs to peers
    for (const auto& dir : info.resultfrags) {
      encode_replica_dir(dir, p.first, notify->basebl);
    }

    mds->send_message_mds(notify, p.first);
  }

  // journal commit
  EFragment *le = new EFragment(mds->mdlog, EFragment::OP_COMMIT, basedirfrag, info.bits);
  mds->mdlog->submit_entry(le, new C_MDC_FragmentCommit(this, basedirfrag, mdr));


  // unfreeze resulting frags
  for (const auto& dir : info.resultfrags) {
    dout(10) << " result frag " << *dir << dendl;

    for (auto &p : dir->items) {
      CDentry *dn = p.second;
      ceph_assert(dn->state_test(CDentry::STATE_FRAGMENTING));
      dn->state_clear(CDentry::STATE_FRAGMENTING);
      dn->put(CDentry::PIN_FRAGMENTING);
    }

    // unfreeze
    dir->unfreeze_dir();
  }

  if (info.notify_ack_waiting.empty()) {
    fragment_drop_locks(info);
  } else {
    mds->locker->drop_locks_for_fragment_unfreeze(mdr.get());
  }
}

void MDCache::_fragment_committed(dirfrag_t basedirfrag, const MDRequestRef& mdr)
{
  dout(10) << "fragment_committed " << basedirfrag << dendl;
  if (mdr)
    mdr->mark_event("commit logged");

  ufragment &uf = uncommitted_fragments.at(basedirfrag);

  // remove old frags
  C_GatherBuilder gather(
    g_ceph_context,
    new C_OnFinisher(
      new C_IO_MDC_FragmentPurgeOld(this, basedirfrag, uf.bits, mdr),
      mds->finisher));

  SnapContext nullsnapc;
  object_locator_t oloc(mds->get_metadata_pool());
  for (const auto& fg : uf.old_frags) {
    object_t oid = CInode::get_object_name(basedirfrag.ino, fg, "");
    ObjectOperation op;
    if (fg == frag_t()) {
      // backtrace object
      dout(10) << " truncate orphan dirfrag " << oid << dendl;
      op.truncate(0);
      op.omap_clear();
    } else {
      dout(10) << " removing orphan dirfrag " << oid << dendl;
      op.remove();
    }
    mds->objecter->mutate(oid, oloc, op, nullsnapc,
			  ceph::real_clock::now(),
			  0, gather.new_sub());
  }

  ceph_assert(gather.has_subs());
  gather.activate();
}

void MDCache::_fragment_old_purged(dirfrag_t basedirfrag, int bits, const MDRequestRef& mdr)
{
  dout(10) << "fragment_old_purged " << basedirfrag << dendl;
  if (mdr)
    mdr->mark_event("old frags purged");

  EFragment *le = new EFragment(mds->mdlog, EFragment::OP_FINISH, basedirfrag, bits);
  mds->mdlog->submit_entry(le);

  finish_uncommitted_fragment(basedirfrag, EFragment::OP_FINISH);

  if (mds->logger) {
    if (bits > 0) {
      mds->logger->inc(l_mds_dir_split);
    } else {
      mds->logger->inc(l_mds_dir_merge);
    }
  }

  if (mdr) {
    auto it = fragments.find(basedirfrag);
    ceph_assert(it != fragments.end());
    it->second.finishing = true;
    if (it->second.notify_ack_waiting.empty())
      fragment_maybe_finish(it);
    else
      mdr->mark_event("wating for notify acks");
  }
}

void MDCache::fragment_drop_locks(fragment_info_t& info)
{
  mds->locker->drop_locks(info.mdr.get());
  request_finish(info.mdr);
  //info.mdr.reset();
}

void MDCache::fragment_maybe_finish(const fragment_info_iterator& it)
{
  if (!it->second.finishing)
    return;

  // unmark & auth_unpin
  for (const auto &dir : it->second.resultfrags) {
    dir->state_clear(CDir::STATE_FRAGMENTING);
    dir->auth_unpin(this);

    // In case the resulting fragments are beyond the split size,
    // we might need to split them again right away (they could
    // have been taking inserts between unfreezing and getting
    // here)
    mds->balancer->maybe_fragment(dir, false);
  }

  fragments.erase(it);
}


void MDCache::handle_fragment_notify_ack(const cref_t<MMDSFragmentNotifyAck> &ack)
{
  dout(10) << "handle_fragment_notify_ack " << *ack << " from " << ack->get_source() << dendl;
  mds_rank_t from = mds_rank_t(ack->get_source().num());

  if (mds->get_state() < MDSMap::STATE_ACTIVE) {
    return;
  }

  auto it = fragments.find(ack->get_base_dirfrag());
  if (it == fragments.end() ||
      it->second.get_tid() != ack->get_tid()) {
    dout(10) << "handle_fragment_notify_ack obsolete message, dropping" << dendl;
    return;
  }

  if (it->second.notify_ack_waiting.erase(from) &&
      it->second.notify_ack_waiting.empty()) {
    fragment_drop_locks(it->second);
    fragment_maybe_finish(it);
  }
}

void MDCache::handle_fragment_notify(const cref_t<MMDSFragmentNotify> &notify)
{
  dout(10) << "handle_fragment_notify " << *notify << " from " << notify->get_source() << dendl;
  mds_rank_t from = mds_rank_t(notify->get_source().num());

  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    return;
  }

  CInode *diri = get_inode(notify->get_ino());
  if (diri) {
    frag_t base = notify->get_basefrag();
    int bits = notify->get_bits();

/*
    if ((bits < 0 && diri->dirfragtree.is_leaf(base)) ||
	(bits > 0 && !diri->dirfragtree.is_leaf(base))) {
      dout(10) << " dft " << diri->dirfragtree << " state doesn't match " << base << " by " << bits
	       << ", must have found out during resolve/rejoin?  ignoring. " << *diri << dendl;
      return;
    }
*/

    // refragment
    MDSContext::vec waiters;
    std::vector<CDir*> resultfrags;
    adjust_dir_fragments(diri, base, bits, &resultfrags, waiters, false);
    if (g_conf()->mds_debug_frag)
      diri->verify_dirfrags();
    
    for (const auto& dir : resultfrags) {
      diri->take_dir_waiting(dir->get_frag(), waiters);
    }

    // add new replica dirs values
    auto p = notify->basebl.cbegin();
    while (!p.end()) {
      CDir *tmp_dir = nullptr;
      decode_replica_dir(tmp_dir, p, diri, from, waiters);
    }

    mds->queue_waiters(waiters);
  } else {
    ceph_abort();
  }

  if (notify->is_ack_wanted()) {
    auto ack = make_message<MMDSFragmentNotifyAck>(notify->get_base_dirfrag(),
					     notify->get_bits(), notify->get_tid());
    mds->send_message_mds(ack, from);
  }
}

void MDCache::add_uncommitted_fragment(dirfrag_t basedirfrag, int bits, const frag_vec_t& old_frags,
				       LogSegment *ls, bufferlist *rollback)
{
  dout(10) << "add_uncommitted_fragment: base dirfrag " << basedirfrag << " bits " << bits << dendl;
  ceph_assert(!uncommitted_fragments.count(basedirfrag));
  ufragment& uf = uncommitted_fragments[basedirfrag];
  uf.old_frags = old_frags;
  uf.bits = bits;
  uf.ls = ls;
  ls->uncommitted_fragments.insert(basedirfrag);
  if (rollback)
    uf.rollback.swap(*rollback);
}

void MDCache::finish_uncommitted_fragment(dirfrag_t basedirfrag, int op)
{
  dout(10) << "finish_uncommitted_fragments: base dirfrag " << basedirfrag
	   << " op " << EFragment::op_name(op) << dendl;
  map<dirfrag_t, ufragment>::iterator it = uncommitted_fragments.find(basedirfrag);
  if (it != uncommitted_fragments.end()) {
    ufragment& uf = it->second;
    if (op != EFragment::OP_FINISH && !uf.old_frags.empty()) {
      uf.committed = true;
    } else {
      uf.ls->uncommitted_fragments.erase(basedirfrag);
      mds->queue_waiters(uf.waiters);
      uncommitted_fragments.erase(it);
    }
  }
}

void MDCache::rollback_uncommitted_fragment(dirfrag_t basedirfrag, frag_vec_t&& old_frags)
{
  dout(10) << "rollback_uncommitted_fragment: base dirfrag " << basedirfrag
           << " old_frags (" << old_frags << ")" << dendl;
  map<dirfrag_t, ufragment>::iterator it = uncommitted_fragments.find(basedirfrag);
  if (it != uncommitted_fragments.end()) {
    ufragment& uf = it->second;
    if (!uf.old_frags.empty()) {
      uf.old_frags = std::move(old_frags);
      uf.committed = true;
    } else {
      uf.ls->uncommitted_fragments.erase(basedirfrag);
      uncommitted_fragments.erase(it);
    }
  }
}

void MDCache::wait_for_uncommitted_fragments(MDSContext* finisher)
{
  MDSGatherBuilder gather(g_ceph_context, finisher);
  for (auto& p : uncommitted_fragments) {
    p.second.waiters.push_back(gather.new_sub());
  }
  gather.activate();
}

struct C_MDC_FragmentRollback : public MDCacheLogContext {
  MutationRef mut;
  C_MDC_FragmentRollback(MDCache *c, MutationRef& m) :
    MDCacheLogContext(c), mut(m) {}
  void finish(int r) override {
    mut->apply();
    get_mds()->locker->drop_locks(mut.get());
    mut->cleanup();
  }
};

void MDCache::rollback_uncommitted_fragments()
{
  dout(10) << "rollback_uncommitted_fragments: " << uncommitted_fragments.size() << " pending" << dendl;
  for (map<dirfrag_t, ufragment>::iterator p = uncommitted_fragments.begin();
       p != uncommitted_fragments.end();
       ++p) {
    ufragment &uf = p->second;
    CInode *diri = get_inode(p->first.ino);
    ceph_assert(diri);

    if (uf.committed) {
      _fragment_committed(p->first, MDRequestRef());
      continue;
    }

    dout(10) << " rolling back " << p->first << " refragment by " << uf.bits << " bits" << dendl;

    MutationRef mut(new MutationImpl());
    mut->ls = mds->mdlog->get_current_segment();
    EFragment *le = new EFragment(mds->mdlog, EFragment::OP_ROLLBACK, p->first, uf.bits);
    bool diri_auth = (diri->authority() != CDIR_AUTH_UNDEF);

    frag_vec_t old_frags;
    diri->dirfragtree.get_leaves_under(p->first.frag, old_frags);

    std::vector<CDir*> resultfrags;
    if (uf.old_frags.empty()) {
      // created by old format EFragment
      MDSContext::vec waiters;
      adjust_dir_fragments(diri, p->first.frag, -uf.bits, &resultfrags, waiters, true);
    } else {
      auto bp = uf.rollback.cbegin();
      for (const auto& fg : uf.old_frags) {
	CDir *dir = force_dir_fragment(diri, fg);
	resultfrags.push_back(dir);

	dirfrag_rollback rollback;
	decode(rollback, bp);

	dir->fnode = rollback.fnode;

	dir->mark_dirty(mut->ls);

	if (!(dir->get_fnode()->rstat == dir->get_fnode()->accounted_rstat)) {
	  dout(10) << "    dirty nestinfo on " << *dir << dendl;
	  mds->locker->mark_updated_scatterlock(&diri->nestlock);
	  mut->ls->dirty_dirfrag_nest.push_back(&diri->item_dirty_dirfrag_nest);
	  mut->add_updated_lock(&diri->nestlock);
	}
	if (!(dir->get_fnode()->fragstat == dir->get_fnode()->accounted_fragstat)) {
	  dout(10) << "    dirty fragstat on " << *dir << dendl;
	  mds->locker->mark_updated_scatterlock(&diri->filelock);
	  mut->ls->dirty_dirfrag_dir.push_back(&diri->item_dirty_dirfrag_dir);
	  mut->add_updated_lock(&diri->filelock);
	}

	le->add_orig_frag(dir->get_frag());
	le->metablob.add_dir_context(dir);
	if (diri_auth) {
	  le->metablob.add_fragmented_dir(dir, true, false);
	} else {
	  dout(10) << "    dirty dirfragtree on " << *dir << dendl;
	  dir->state_set(CDir::STATE_DIRTYDFT);
	  le->metablob.add_fragmented_dir(dir, true, true);
	}
      }
    }

    if (diri_auth) {
      auto pi = diri->project_inode(mut);
      pi.inode->version = diri->pre_dirty();
      predirty_journal_parents(mut, &le->metablob, diri, 0, PREDIRTY_PRIMARY);
      le->metablob.add_primary_dentry(diri->get_projected_parent_dn(), diri, true);
    } else {
      mds->locker->mark_updated_scatterlock(&diri->dirfragtreelock);
      mut->ls->dirty_dirfrag_dirfragtree.push_back(&diri->item_dirty_dirfrag_dirfragtree);
      mut->add_updated_lock(&diri->dirfragtreelock);
    }

    if (g_conf()->mds_debug_frag)
      diri->verify_dirfrags();

    for (const auto& leaf : old_frags) {
      ceph_assert(!diri->dirfragtree.is_leaf(leaf));
    }

    mds->mdlog->submit_entry(le, new C_MDC_FragmentRollback(this, mut));

    uf.old_frags.swap(old_frags);
    _fragment_committed(p->first, MDRequestRef());
  }
}

void MDCache::force_readonly()
{
  if (is_readonly())
    return;

  dout(1) << "force file system read-only" << dendl;
  mds->clog->warn() << "force file system read-only";

  set_readonly();

  mds->server->force_clients_readonly();

  // revoke write caps
  int count = 0;
  for (auto &p : inode_map) {
    CInode *in = p.second;
    if (in->is_head())
      mds->locker->eval(in, CEPH_CAP_LOCKS);
    if (!(++count % mds->heartbeat_reset_grace()))
      mds->heartbeat_reset();
  }

  mds->mdlog->flush();
}


// ==============================================================
// debug crap

void MDCache::show_subtrees(int dbl, bool force_print)
{
  if (g_conf()->mds_thrash_exports)
    dbl += 15;

  //dout(10) << "show_subtrees" << dendl;

  if (!g_conf()->subsys.should_gather(ceph_subsys_mds, dbl))
    return;  // i won't print anything.

  if (subtrees.empty()) {
    dout(ceph::dout::need_dynamic(dbl)) << "show_subtrees - no subtrees"
					<< dendl;
    return;
  }

  if (!force_print && subtrees.size() > SUBTREES_COUNT_THRESHOLD &&
      !g_conf()->subsys.should_gather<ceph_subsys_mds, 25>()) {
    dout(ceph::dout::need_dynamic(dbl)) << "number of subtrees = " << subtrees.size() << "; not "
		"printing subtrees" << dendl;
    return;
  }

  // root frags
  std::vector<CDir*> basefrags;
  for (set<CInode*>::iterator p = base_inodes.begin();
       p != base_inodes.end();
       ++p) 
    (*p)->get_dirfrags(basefrags);
  //dout(15) << "show_subtrees, base dirfrags " << basefrags << dendl;
  dout(15) << "show_subtrees" << dendl;

  // queue stuff
  list<pair<CDir*,int> > q;
  string indent;
  set<CDir*> seen;

  // calc max depth
  for (const auto& dir : basefrags) {
    q.emplace_back(dir, 0);
  }

  set<CDir*> subtrees_seen;

  unsigned int depth = 0;
  while (!q.empty()) {
    CDir *dir = q.front().first;
    unsigned int d = q.front().second;
    q.pop_front();

    if (subtrees.count(dir) == 0) continue;

    subtrees_seen.insert(dir);

    if (d > depth) depth = d;

    // sanity check
    //dout(25) << "saw depth " << d << " " << *dir << dendl;
    if (seen.count(dir)) dout(0) << "aah, already seen " << *dir << dendl;
    ceph_assert(seen.count(dir) == 0);
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

  if (!force_print && depth > SUBTREES_DEPTH_THRESHOLD &&
      !g_conf()->subsys.should_gather<ceph_subsys_mds, 25>()) {
    dout(ceph::dout::need_dynamic(dbl)) << "max depth among subtrees = " << depth << "; not printing "
		"subtrees" << dendl;
    return;
  }

  // print tree
  for (const auto& dir : basefrags) {
    q.emplace_back(dir, 0);
  }

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
      snprintf(s, sizeof(s), "%2d   ", int(dir->get_dir_auth().first));
    else
      snprintf(s, sizeof(s), "%2d,%2d", int(dir->get_dir_auth().first), int(dir->get_dir_auth().second));
    
    // print
    dout(ceph::dout::need_dynamic(dbl)) << indent << "|_" << pad << s
					<< " " << auth << *dir << dendl;

    if (dir->ino() == CEPH_INO_ROOT)
      ceph_assert(dir->inode == root);
    if (dir->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
      ceph_assert(dir->inode == myin);
    if (dir->inode->is_stray() && (MDS_INO_STRAY_OWNER(dir->ino()) == mds->get_nodeid()))
      ceph_assert(strays[MDS_INO_STRAY_INDEX(dir->ino())] == dir->inode);

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
  ceph_assert(lost == 0);
}

void MDCache::show_cache()
{
  if (!g_conf()->subsys.should_gather<ceph_subsys_mds, 7>())
    return;
  dout(7) << "show_cache" << dendl;

  auto show_func = [this](CInode *in) {
    // unlinked?
    if (!in->parent)
      dout(7) << " unlinked " << *in << dendl;

    // dirfrags?
    auto&& dfs = in->get_dirfrags();
    for (const auto& dir : dfs) {
      dout(7) << "  dirfrag " << *dir << dendl;

      for (auto &p : dir->items) {
	CDentry *dn = p.second;
	dout(7) << "   dentry " << *dn << dendl;
	CDentry::linkage_t *dnl = dn->get_linkage();
	if (dnl->is_primary() && dnl->get_inode()) 
	  dout(7) << "    inode " << *dnl->get_inode() << dendl;
      }
    }
  };

  for (auto &p : inode_map)
    show_func(p.second);
  for (auto &p : snap_inode_map)
    show_func(p.second);
}

void MDCache::cache_status(Formatter *f)
{
  f->open_object_section("cache");

  f->open_object_section("pool");
  mempool::get_pool(mempool::mds_co::id).dump(f);
  f->close_section();

  f->close_section();
}

void MDCache::dump_tree(CInode *in, const int cur_depth, const int max_depth, Formatter *f) 
{
  ceph_assert(in);
  if ((max_depth >= 0) && (cur_depth > max_depth)) {
    return;
  }
  auto&& ls = in->get_dirfrags();
  for (const auto &subdir : ls) {
    for (const auto &p : subdir->items) {
      CDentry *dn = p.second;
      CInode *in = dn->get_linkage()->get_inode();
      if (in) {
        dump_tree(in, cur_depth + 1, max_depth, f);
      }
    }
  }
  f->open_object_section("inode");
  in->dump(f, CInode::DUMP_DEFAULT | CInode::DUMP_DIRFRAGS);
  f->close_section();
}

int MDCache::dump_cache(std::string_view file_name, double timeout)
{
  return dump_cache(file_name, NULL, timeout);
}

int MDCache::dump_cache(Formatter *f, double timeout)
{
  return dump_cache(std::string_view(""), f, timeout);
}

/**
 * Dump the metadata cache, either to a Formatter, if
 * provided, else to a plain text file.
 */
int MDCache::dump_cache(std::string_view fn, Formatter *f, double timeout)
{
  int r = 0;

  // dumping large caches may cause mds to hang or worse get killed.
  // so, disallow the dump if the cache size exceeds the configured
  // threshold, which is 1G for formatter and unlimited for file (note
  // that this can be jacked up by the admin... and is nothing but foot
  // shooting, but the option itself is for devs and hence dangerous to
  // tune). TODO: remove this when fixed.
  uint64_t threshold = f ?
    g_conf().get_val<Option::size_t>("mds_dump_cache_threshold_formatter") :
    g_conf().get_val<Option::size_t>("mds_dump_cache_threshold_file");

  if (threshold && cache_size() > threshold) {
    if (f) {
      CachedStackStringStream css;
      *css << "cache usage exceeds dump threshold";
      f->open_object_section("result");
      f->dump_string("error", css->strv());
      f->close_section();
    } else {
      derr << "cache usage exceeds dump threshold" << dendl;
      r = -CEPHFS_EINVAL;
    }
    return r;
  }

  r = 0;
  int fd = -1;

  if (f) {
    f->open_array_section("inodes");
  } else {
    char path[PATH_MAX] = "";
    if (fn.length()) {
      snprintf(path, sizeof path, "%s", fn.data());
    } else {
      snprintf(path, sizeof path, "cachedump.%d.mds%d", (int)mds->mdsmap->get_epoch(), int(mds->get_nodeid()));
    }

    dout(1) << "dump_cache to " << path << dendl;

    fd = ::open(path, O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC, 0600);
    if (fd < 0) {
      derr << "failed to open " << path << ": " << cpp_strerror(errno) << dendl;
      return errno;
    }
  }

  auto dump_func = [fd, f](CInode *in) {
    int r;
    if (f) {
      f->open_object_section("inode");
      in->dump(f, CInode::DUMP_DEFAULT | CInode::DUMP_DIRFRAGS);
      f->close_section();
      return 1;
    } 
    CachedStackStringStream css;
    *css << *in << std::endl;
    auto sv = css->strv();
    r = safe_write(fd, sv.data(), sv.size());
    if (r < 0)
      return r;
    auto&& dfs = in->get_dirfrags();
    for (auto &dir : dfs) {
      CachedStackStringStream css2;
      *css2 << " " << *dir << std::endl;
      auto sv = css2->strv();
      r = safe_write(fd, sv.data(), sv.size());
      if (r < 0)
        return r;
      for (auto &p : dir->items) {
	CDentry *dn = p.second;
        CachedStackStringStream css3;
        *css3 << "  " << *dn << std::endl;
        auto sv = css3->strv();
        r = safe_write(fd, sv.data(), sv.size());
        if (r < 0)
          return r;
      }
      dir->check_rstats();
    }
    return 1;
  };

  auto start = mono_clock::now();
  int64_t count = 0;
  for (auto &p : inode_map) {
    r = dump_func(p.second);
    if (r < 0)
      goto out;
    if (!(++count % 1000) &&
	timeout > 0 &&
	std::chrono::duration<double>(mono_clock::now() - start).count() > timeout) {
      r = -ETIMEDOUT;
      goto out;
    }
  }
  for (auto &p : snap_inode_map) {
    r = dump_func(p.second);
    if (r < 0)
      goto out;
    if (!(++count % 1000) &&
		timeout > 0 &&
	std::chrono::duration<double>(mono_clock::now() - start).count() > timeout) {
      r = -ETIMEDOUT;
      goto out;
    }

  }
  r = 0;

 out:
  if (f) {
    if (r == -ETIMEDOUT)
    {
      f->close_section();
      f->open_object_section("result");
      f->dump_string("error", "the operation timeout");
    }
    f->close_section();  // inodes
  } else {
    if (r == -ETIMEDOUT)
    {
      CachedStackStringStream css;
      *css << "error : the operation timeout" << std::endl;
      auto sv = css->strv();
      r = safe_write(fd, sv.data(), sv.size());
    }
    ::close(fd);
  }
  return r;
}

void C_MDS_RetryRequest::finish(int r)
{
  mdr->retry++;
  cache->dispatch_request(mdr);
}

MDSContext *CF_MDS_RetryRequestFactory::build()
{
  if (drop_locks) {
    mdcache->mds->locker->drop_locks(mdr.get(), nullptr);
    mdr->drop_local_auth_pins();
  }
  return new C_MDS_RetryRequest(mdcache, mdr);
}

class C_MDS_EnqueueScrub : public Context
{
  std::string tag;
  Formatter *formatter;
  Context *on_finish;
public:
  ScrubHeaderRef header;
  C_MDS_EnqueueScrub(std::string_view tag, Formatter *f, Context *fin) :
    tag(tag), formatter(f), on_finish(fin), header(nullptr) {}

  void finish(int r) override {
    formatter->open_object_section("results");
    formatter->dump_int("return_code", r);
    if (r == 0) {
      formatter->dump_string("scrub_tag", tag);
      formatter->dump_string("mode", "asynchronous");
    }
    formatter->close_section();

    r = 0;
    if (on_finish)
      on_finish->complete(r);
  }
};

void MDCache::enqueue_scrub(
    std::string_view path,
    std::string_view tag,
    bool force, bool recursive, bool repair,
    bool scrub_mdsdir, Formatter *f, Context *fin)
{
  dout(10) << __func__ << " " << path << dendl;

  filepath fp;
  if (path.compare(0, 4, "~mds") == 0) {
    mds_rank_t rank;
    if (path == "~mdsdir") {
      rank = mds->get_nodeid();
    } else {
      std::string err;
      rank = strict_strtoll(path.substr(4), 10, &err);
      if (!err.empty())
	rank = MDS_RANK_NONE;
    }
    if (rank >= 0 && rank < MAX_MDS)
      fp.set_path("", MDS_INO_MDSDIR(rank));
  }
  if (fp.get_ino() == inodeno_t(0))
    fp.set_path(path);

  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_ENQUEUE_SCRUB);
  mdr->set_filepath(fp);

  bool is_internal = false;
  std::string tag_str(tag);
  if (tag_str.empty()) {
    uuid_d uuid_gen;
    uuid_gen.generate_random();
    tag_str = uuid_gen.to_string();
    is_internal = true;
  }

  C_MDS_EnqueueScrub *cs = new C_MDS_EnqueueScrub(tag_str, f, fin);
  cs->header = std::make_shared<ScrubHeader>(tag_str, is_internal, force,
                                             recursive, repair, scrub_mdsdir);

  mdr->internal_op_finish = cs;
  enqueue_scrub_work(mdr);
}

void MDCache::enqueue_scrub_work(const MDRequestRef& mdr)
{
  CInode *in;
  CF_MDS_RetryRequestFactory cf(this, mdr, true);
  int r = path_traverse(mdr, cf, mdr->get_filepath(),
			MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_RDLOCK_PATH,
			nullptr, &in);
  if (r > 0)
    return;
  if (r < 0) {
    mds->server->respond_to_request(mdr, r);
    return;
  }

  // Cannot scrub same dentry twice at same time
  if (in->scrub_is_in_progress()) {
    mds->server->respond_to_request(mdr, -CEPHFS_EBUSY);
    return;
  } else {
    in->scrub_info();
  }

  C_MDS_EnqueueScrub *cs = static_cast<C_MDS_EnqueueScrub*>(mdr->internal_op_finish);
  ScrubHeaderRef& header = cs->header;

  r = mds->scrubstack->enqueue(in, header, !header->get_recursive());

  mds->server->respond_to_request(mdr, r);
}

struct C_MDC_RespondInternalRequest : public MDCacheLogContext {
  MDRequestRef mdr;
  C_MDC_RespondInternalRequest(MDCache *c, const MDRequestRef& m) :
    MDCacheLogContext(c), mdr(m) {}
  void finish(int r) override {
    mdr->apply();
    get_mds()->server->respond_to_request(mdr, r);
  }
};

struct C_MDC_ScrubRepaired : public MDCacheContext {
  ScrubHeaderRef header;
public:
  C_MDC_ScrubRepaired(MDCache *m, const ScrubHeaderRef& h)
    : MDCacheContext(m), header(h) {
    header->inc_num_pending();
  }
  void finish(int r) override {
    header->dec_num_pending();
  }
};

void MDCache::repair_dirfrag_stats(CDir *dir)
{
  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_REPAIR_FRAGSTATS);
  mdr->pin(dir);
  mdr->internal_op_private = dir;
  if (dir->scrub_is_in_progress())
    mdr->internal_op_finish = new C_MDC_ScrubRepaired(this, dir->get_scrub_header());
  else
    mdr->internal_op_finish = new C_MDSInternalNoop;
  repair_dirfrag_stats_work(mdr);
}

void MDCache::repair_dirfrag_stats_work(const MDRequestRef& mdr)
{
  CDir *dir = static_cast<CDir*>(mdr->internal_op_private);
  dout(10) << __func__ << " " << *dir << dendl;

  if (!dir->is_auth()) {
    mds->server->respond_to_request(mdr, -CEPHFS_ESTALE);
    return;
  }

  if (!mdr->is_auth_pinned(dir) && !dir->can_auth_pin()) {
    dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(this, mdr));

    mds->locker->drop_locks(mdr.get());
    mdr->drop_local_auth_pins();
    if (mdr->is_any_remote_auth_pin())
      mds->locker->notify_freeze_waiter(dir);
    return;
  }

  mdr->auth_pin(dir);

  MutationImpl::LockOpVec lov;
  CInode *diri = dir->inode;
  lov.add_rdlock(&diri->dirfragtreelock);
  lov.add_wrlock(&diri->nestlock);
  lov.add_wrlock(&diri->filelock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  if (!dir->is_complete()) {
    dir->fetch(new C_MDS_RetryRequest(this, mdr));
    return;
  }

  frag_info_t frag_info;
  nest_info_t nest_info;
  for (auto it = dir->begin(); it != dir->end(); ++it) {
    CDentry *dn = it->second;
    if (dn->last != CEPH_NOSNAP)
      continue;
    CDentry::linkage_t *dnl = dn->get_projected_linkage();
    if (dnl->is_primary()) {
      CInode *in = dnl->get_inode();
      nest_info.add(in->get_projected_inode()->accounted_rstat);
      if (in->is_dir())
	frag_info.nsubdirs++;
      else
	frag_info.nfiles++;
    } else if (dnl->is_remote())
      frag_info.nfiles++;
  }

  auto pf = dir->get_projected_fnode();
  bool good_fragstat = frag_info.same_sums(pf->fragstat);
  bool good_rstat = nest_info.same_sums(pf->rstat);
  if (good_fragstat && good_rstat) {
    dout(10) << __func__ << " no corruption found" << dendl;
    mds->server->respond_to_request(mdr, 0);
    return;
  }

  auto _pf = dir->project_fnode(mdr);
  _pf->version = dir->pre_dirty();
  pf = _pf;

  mdr->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, "repair_dirfrag");

  if (!good_fragstat) {
    if (pf->fragstat.mtime > frag_info.mtime)
      frag_info.mtime = pf->fragstat.mtime;
    if (pf->fragstat.change_attr > frag_info.change_attr)
      frag_info.change_attr = pf->fragstat.change_attr;
    _pf->fragstat = frag_info;
    mds->locker->mark_updated_scatterlock(&diri->filelock);
    mdr->ls->dirty_dirfrag_dir.push_back(&diri->item_dirty_dirfrag_dir);
    mdr->add_updated_lock(&diri->filelock);
  }

  if (!good_rstat) {
    if (pf->rstat.rctime > nest_info.rctime)
      nest_info.rctime = pf->rstat.rctime;
    _pf->rstat = nest_info;
    mds->locker->mark_updated_scatterlock(&diri->nestlock);
    mdr->ls->dirty_dirfrag_nest.push_back(&diri->item_dirty_dirfrag_nest);
    mdr->add_updated_lock(&diri->nestlock);
  }

  le->metablob.add_dir_context(dir);
  le->metablob.add_dir(dir, true);

  mds->mdlog->submit_entry(le, new C_MDC_RespondInternalRequest(this, mdr));
}

void MDCache::repair_inode_stats(CInode *diri)
{
  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_REPAIR_INODESTATS);
  mdr->auth_pin(diri); // already auth pinned by CInode::validate_disk_state()
  mdr->internal_op_private = diri;
  if (diri->scrub_is_in_progress())
    mdr->internal_op_finish = new C_MDC_ScrubRepaired(this, diri->get_scrub_header());
  else
    mdr->internal_op_finish = new C_MDSInternalNoop;
  repair_inode_stats_work(mdr);
}

void MDCache::repair_inode_stats_work(const MDRequestRef& mdr)
{
  CInode *diri = static_cast<CInode*>(mdr->internal_op_private);
  dout(10) << __func__ << " " << *diri << dendl;

  if (!diri->is_auth()) {
    mds->server->respond_to_request(mdr, -CEPHFS_ESTALE);
    return;
  }
  if (!diri->is_dir()) {
    mds->server->respond_to_request(mdr, -CEPHFS_ENOTDIR);
    return;
  }

  MutationImpl::LockOpVec lov;

  if (mdr->ls) // already marked filelock/nestlock dirty ?
    goto do_rdlocks;

  lov.add_rdlock(&diri->dirfragtreelock);
  lov.add_wrlock(&diri->nestlock);
  lov.add_wrlock(&diri->filelock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  // Fetch all dirfrags and mark filelock/nestlock dirty. This will tirgger
  // the scatter-gather process, which will fix any fragstat/rstat errors.
  {
    frag_vec_t leaves;
    diri->dirfragtree.get_leaves(leaves);
    for (const auto& leaf : leaves) {
      CDir *dir = diri->get_dirfrag(leaf);
      if (!dir) {
        ceph_assert(mdr->is_auth_pinned(diri));
        dir = diri->get_or_open_dirfrag(this, leaf);
      }
      if (dir->get_version() == 0) {
        ceph_assert(dir->is_auth());
        dir->fetch_keys({}, new C_MDS_RetryRequest(this, mdr));
        return;
      }
    }
  }

  diri->state_set(CInode::STATE_REPAIRSTATS);
  mdr->ls = mds->mdlog->get_current_segment();
  mds->locker->mark_updated_scatterlock(&diri->filelock);
  mdr->ls->dirty_dirfrag_dir.push_back(&diri->item_dirty_dirfrag_dir);
  mds->locker->mark_updated_scatterlock(&diri->nestlock);
  mdr->ls->dirty_dirfrag_nest.push_back(&diri->item_dirty_dirfrag_nest);

  mds->locker->drop_locks(mdr.get());

do_rdlocks:
  // force the scatter-gather process
  lov.clear();
  lov.add_rdlock(&diri->dirfragtreelock);
  lov.add_rdlock(&diri->nestlock);
  lov.add_rdlock(&diri->filelock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;

  diri->state_clear(CInode::STATE_REPAIRSTATS);

  frag_info_t dir_info;
  nest_info_t nest_info;
  nest_info.rsubdirs = 1; // it gets one to account for self
  if (const sr_t *srnode = diri->get_projected_srnode(); srnode)
    nest_info.rsnaps = srnode->snaps.size();

  {
    frag_vec_t leaves;
    diri->dirfragtree.get_leaves(leaves);
    for (const auto& leaf : leaves) {
      CDir *dir = diri->get_dirfrag(leaf);
      ceph_assert(dir);
      ceph_assert(dir->get_version() > 0);
      dir_info.add(dir->get_fnode()->accounted_fragstat);
      nest_info.add(dir->get_fnode()->accounted_rstat);
    }
  }

  if (!dir_info.same_sums(diri->get_inode()->dirstat) ||
      !nest_info.same_sums(diri->get_inode()->rstat)) {
    dout(10) << __func__ << " failed to fix fragstat/rstat on "
	     << *diri << dendl;
  }

  mds->server->respond_to_request(mdr, 0);
}

void MDCache::rdlock_dirfrags_stats(CInode *diri, MDSInternalContext* fin)
{
  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_RDLOCK_FRAGSSTATS);
  mdr->auth_pin(diri); // already auth pinned by CInode::validate_disk_state()
  mdr->internal_op_private = diri;
  mdr->internal_op_finish = fin;
  return rdlock_dirfrags_stats_work(mdr);
}

void MDCache::rdlock_dirfrags_stats_work(const MDRequestRef& mdr)
{
  CInode *diri = static_cast<CInode*>(mdr->internal_op_private);
  dout(10) << __func__ << " " << *diri << dendl;
  if (!diri->is_auth()) {
    mds->server->respond_to_request(mdr, -CEPHFS_ESTALE);
    return;
  }
  if (!diri->is_dir()) {
    mds->server->respond_to_request(mdr, -CEPHFS_ENOTDIR);
    return;
  }

  MutationImpl::LockOpVec lov;
  lov.add_rdlock(&diri->dirfragtreelock);
  lov.add_rdlock(&diri->nestlock);
  lov.add_rdlock(&diri->filelock);
  if (!mds->locker->acquire_locks(mdr, lov))
    return;
  dout(10) << __func__ << " start dirfrags : " << *diri << dendl;

  mds->server->respond_to_request(mdr, 0);
  return;
}

void MDCache::flush_dentry(std::string_view path, Context *fin)
{
  if (is_readonly()) {
    dout(10) << __func__ << ": read-only FS" << dendl;
    fin->complete(-CEPHFS_EROFS);
    return;
  }
  dout(10) << "flush_dentry " << path << dendl;
  MDRequestRef mdr = request_start_internal(CEPH_MDS_OP_FLUSH);
  filepath fp(path);
  mdr->set_filepath(fp);
  mdr->internal_op_finish = fin;
  flush_dentry_work(mdr);
}

class C_FinishIOMDR : public MDSContext {
protected:
  MDSRank *mds;
  MDRequestRef mdr;
  MDSRank *get_mds() override { return mds; }
public:
  C_FinishIOMDR(MDSRank *mds_, const MDRequestRef& mdr_) : mds(mds_), mdr(mdr_) {}
  void finish(int r) override { mds->server->respond_to_request(mdr, r); }
};

void MDCache::flush_dentry_work(const MDRequestRef& mdr)
{
  MutationImpl::LockOpVec lov;
  CInode *in = mds->server->rdlock_path_pin_ref(mdr, true);
  if (!in)
    return;

  ceph_assert(in->is_auth());
  in->flush(new C_FinishIOMDR(mds, mdr));
}


/**
 * Initialize performance counters with global perfcounter
 * collection.
 */
void MDCache::register_perfcounters()
{
    PerfCountersBuilder pcb(g_ceph_context, "mds_cache", l_mdc_first, l_mdc_last);

    pcb.add_u64_counter(l_mdc_dir_update, "dir_update",
                        "Directory replication directives");
    pcb.add_u64_counter(l_mdc_dir_update_receipt, "dir_update_receipt",
                        "Directory replication directives received");
    pcb.add_u64_counter(l_mdc_dir_try_discover, "dir_try_discover",
                        "Directory replication attempt to discover");
    pcb.add_u64_counter(l_mdc_dir_send_discover, "dir_send_discover",
                        "Directory replication discovery message sent");
    pcb.add_u64_counter(l_mdc_dir_handle_discover, "dir_handle_discover",
                        "Directory replication discovery message handled");

    // Stray/purge statistics
    pcb.add_u64(l_mdc_num_strays, "num_strays", "Stray dentries", "stry",
                PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64(l_mdc_num_recovering_enqueued,
                "num_recovering_enqueued", "Files waiting for recovery", "recy",
                PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64_counter(l_mdc_recovery_completed,
                        "recovery_completed", "File recoveries completed", "recd",
                        PerfCountersBuilder::PRIO_INTERESTING);

    // useful recovery queue statistics
    pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_u64(l_mdc_num_recovering_processing, "num_recovering_processing",
                "Files currently being recovered");
    pcb.add_u64(l_mdc_num_recovering_prioritized, "num_recovering_prioritized",
                "Files waiting for recovery with elevated priority");
    pcb.add_u64_counter(l_mdc_recovery_started, "recovery_started",
                        "File recoveries started");

    // along with other stray dentries stats
    pcb.add_u64(l_mdc_num_strays_delayed, "num_strays_delayed",
                "Stray dentries delayed");
    pcb.add_u64(l_mdc_num_strays_enqueuing, "num_strays_enqueuing",
                "Stray dentries enqueuing for purge");
    pcb.add_u64_counter(l_mdc_strays_created, "strays_created",
                        "Stray dentries created");
    pcb.add_u64_counter(l_mdc_strays_enqueued, "strays_enqueued",
                        "Stray dentries enqueued for purge");
    pcb.add_u64_counter(l_mdc_strays_reintegrated, "strays_reintegrated",
                        "Stray dentries reintegrated");
    pcb.add_u64_counter(l_mdc_strays_migrated, "strays_migrated",
                        "Stray dentries migrated");

    // low prio internal request stats
    pcb.add_u64_counter(l_mdss_ireq_enqueue_scrub, "ireq_enqueue_scrub",
                        "Internal Request type enqueue scrub");
    pcb.add_u64_counter(l_mdss_ireq_exportdir, "ireq_exportdir",
                        "Internal Request type export dir");
    pcb.add_u64_counter(l_mdss_ireq_flush, "ireq_flush",
                        "Internal Request type flush");
    pcb.add_u64_counter(l_mdss_ireq_fragmentdir, "ireq_fragmentdir",
                        "Internal Request type fragmentdir");
    pcb.add_u64_counter(l_mdss_ireq_fragstats, "ireq_fragstats",
                        "Internal Request type frag stats");
    pcb.add_u64_counter(l_mdss_ireq_inodestats, "ireq_inodestats",
                        "Internal Request type inode stats");

    logger.reset(pcb.create_perf_counters());
    g_ceph_context->get_perfcounters_collection()->add(logger.get());
    recovery_queue.set_logger(logger.get());
    stray_manager.set_logger(logger.get());
}

/**
 * Call this when putting references to an inode/dentry or
 * when attempting to trim it.
 *
 * If this inode is no longer linked by anyone, and this MDS
 * rank holds the primary dentry, and that dentry is in a stray
 * directory, then give up the dentry to the StrayManager, never
 * to be seen again by MDCache.
 *
 * @param delay if true, then purgeable inodes are stashed til
 *              the next trim(), rather than being purged right
 *              away.
 */
void MDCache::maybe_eval_stray(CInode *in, bool delay) {
  if (in->get_inode()->nlink > 0 || in->is_base() || is_readonly() ||
      mds->get_state() <= MDSMap::STATE_REJOIN)
    return;

  CDentry *dn = in->get_projected_parent_dn();

  if (dn->state_test(CDentry::STATE_PURGING)) {
    /* We have already entered the purging process, no need
     * to re-evaluate me ! */
    return;
  }

  if (dn->get_dir()->get_inode()->is_stray()) {
    if (delay)
      stray_manager.queue_delayed(dn);
    else
      stray_manager.eval_stray(dn);
  }
}

void MDCache::clear_dirty_bits_for_stray(CInode* diri) {
  dout(10) << __func__ << " " << *diri << dendl;
  ceph_assert(diri->get_projected_parent_dir()->inode->is_stray());
  auto&& ls = diri->get_dirfrags();
  for (auto &p : ls) {
    if (p->is_auth() && !(p->is_frozen() || p->is_freezing()))
      p->try_remove_dentries_for_stray();
  }
  if (!diri->snaprealm) {
    if (diri->is_auth())
      diri->clear_dirty_rstat();
    diri->clear_scatter_dirty();
  }
}

bool MDCache::dump_inode(Formatter *f, uint64_t number) {
  CInode *in = get_inode(number);
  if (!in) {
    return false;
  }
  f->open_object_section("inode");
  in->dump(f, CInode::DUMP_DEFAULT | CInode::DUMP_PATH);
  f->close_section();
  return true;
}

void MDCache::dump_dir(Formatter *f, CDir *dir, bool dentry_dump) {
  f->open_object_section("dir");
  dir->dump(f, dentry_dump ? CDir::DUMP_ALL : CDir::DUMP_DEFAULT);
  f->close_section();
}

void MDCache::handle_mdsmap(const MDSMap &mdsmap, const MDSMap &oldmap) {
  const mds_rank_t max_mds = mdsmap.get_max_mds();

  // process export_pin_delayed_queue whenever a new MDSMap received
  auto &q = export_pin_delayed_queue;
  for (auto it = q.begin(); it != q.end(); ) {
    auto *in = *it;
    mds_rank_t export_pin = in->get_export_pin(false);
    dout(10) << " delayed export_pin=" << export_pin << " on " << *in 
             << " max_mds=" << max_mds << dendl;
    if (export_pin >= mdsmap.get_max_mds()) {
      it++;
      continue;
    }

    in->state_clear(CInode::STATE_DELAYEDEXPORTPIN);
    it = q.erase(it);
    in->queue_export_pin(export_pin);
  }

  if (mdsmap.get_max_mds() != oldmap.get_max_mds()) {
    dout(10) << "Checking ephemerally pinned directories for redistribute due to max_mds change." << dendl;
    /* copy to vector to avoid removals during iteration */
    std::vector<CInode*> migrate;
    migrate.assign(export_ephemeral_pins.begin(), export_ephemeral_pins.end());
    for (auto& in : migrate) {
      in->maybe_export_pin();
    }
  }

  if (max_mds <= 1) {
    export_ephemeral_dist_frag_bits = 0;
  } else {
    double want = g_conf().get_val<double>("mds_export_ephemeral_distributed_factor");
    want *= max_mds;
    unsigned n = 0;
    while ((1U << n) < (unsigned)want)
      ++n;
    export_ephemeral_dist_frag_bits = n;
  }
}

void MDCache::upkeep_main(void)
{
  std::unique_lock lock(upkeep_mutex);
  while (!upkeep_trim_shutdown.load()) {
    auto now = clock::now();
    auto since = now-upkeep_last_trim;
    auto trim_interval = clock::duration(g_conf().get_val<std::chrono::seconds>("mds_cache_trim_interval"));
    if (since >= trim_interval*.90) {
      lock.unlock(); /* mds_lock -> upkeep_mutex */
      std::scoped_lock mds_lock(mds->mds_lock);
      lock.lock();
      if (upkeep_trim_shutdown.load())
        return;
      check_memory_usage();
      if (mds->is_cache_trimmable()) {
        dout(20) << "upkeep thread trimming cache; last trim " << since << " ago" << dendl;
        bool active_with_clients = mds->is_active() || mds->is_clientreplay() || mds->is_stopping();
        if (active_with_clients) {
          trim_client_leases();
        }
        if (is_open() || mds->is_standby_replay()) {
          trim();
        }
        if (active_with_clients) {
          auto recall_flags = Server::RecallFlags::ENFORCE_MAX|Server::RecallFlags::ENFORCE_LIVENESS;
          if (cache_toofull()) {
            recall_flags = recall_flags|Server::RecallFlags::TRIM;
          }
          mds->server->recall_client_state(nullptr, recall_flags);
        }
        upkeep_last_trim = now = clock::now();
      } else {
        dout(10) << "cache not ready for trimming" << dendl;
      }
    } else {
      trim_interval -= since;
    }
    since = now-upkeep_last_release;
    auto release_interval = clock::duration(g_conf().get_val<std::chrono::seconds>("mds_cache_release_free_interval"));
    if (since >= release_interval*.90) {
      /* XXX not necessary once MDCache uses PriorityCache */
      dout(10) << "releasing free memory" << dendl;
      ceph_heap_release_free_memory();
      upkeep_last_release = clock::now();
    } else {
      release_interval -= since;
    }
    auto interval = std::min(release_interval, trim_interval);
    dout(20) << "upkeep thread waiting interval " << interval << dendl;
    upkeep_cvar.wait_for(lock, interval);
  }
}
