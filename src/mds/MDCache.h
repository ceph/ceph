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
#ifndef CEPH_MDCACHE_H
#define CEPH_MDCACHE_H

#include <atomic>
#include <chrono>
#include <string_view>
#include <thread>

#include "common/DecayCounter.h"
#include "include/common_fwd.h"
#include "include/types.h"
#include "include/filepath.h"
#include "include/elist.h"

#include "messages/MCacheExpire.h"
#include "messages/MClientQuota.h"
#include "messages/MClientRequest.h"
#include "messages/MClientSnap.h"
#include "messages/MDentryLink.h"
#include "messages/MDentryUnlink.h"
#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"
#include "messages/MGatherCaps.h"
#include "messages/MGenericMessage.h"
#include "messages/MInodeFileCaps.h"
#include "messages/MLock.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSFindIno.h"
#include "messages/MMDSFindInoReply.h"
#include "messages/MMDSFragmentNotify.h"
#include "messages/MMDSFragmentNotifyAck.h"
#include "messages/MMDSOpenIno.h"
#include "messages/MMDSOpenInoReply.h"
#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSPeerRequest.h"
#include "messages/MMDSSnapUpdate.h"

#include "osdc/Filer.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "include/Context.h"
#include "events/EMetaBlob.h"
#include "RecoveryQueue.h"
#include "StrayManager.h"
#include "OpenFileTable.h"
#include "MDSContext.h"
#include "MDSMap.h"
#include "Mutation.h"

class MDSRank;
class Session;
class Migrator;

class Session;

class ESubtreeMap;

enum {
  l_mdc_first = 3000,

  // dir updates for replication
  l_mdc_dir_update,
  l_mdc_dir_update_receipt,
  l_mdc_dir_try_discover,
  l_mdc_dir_send_discover,
  l_mdc_dir_handle_discover,

  // How many inodes currently in stray dentries
  l_mdc_num_strays,
  // How many stray dentries are currently delayed for purge due to refs
  l_mdc_num_strays_delayed,
  // How many stray dentries are currently being enqueued for purge
  l_mdc_num_strays_enqueuing,

  // How many dentries have ever been added to stray dir
  l_mdc_strays_created,
  // How many dentries have been passed on to PurgeQueue
  l_mdc_strays_enqueued,
  // How many strays have been reintegrated?
  l_mdc_strays_reintegrated,
  // How many strays have been migrated?
  l_mdc_strays_migrated,

  // How many inode sizes currently being recovered
  l_mdc_num_recovering_processing,
  // How many inodes currently waiting to have size recovered
  l_mdc_num_recovering_enqueued,
  // How many inodes waiting with elevated priority for recovery
  l_mdc_num_recovering_prioritized,
  // How many inodes ever started size recovery
  l_mdc_recovery_started,
  // How many inodes ever completed size recovery
  l_mdc_recovery_completed,

  l_mdss_ireq_quiesce_path,
  l_mdss_ireq_quiesce_inode,
  l_mdss_ireq_enqueue_scrub,
  l_mdss_ireq_exportdir,
  l_mdss_ireq_flush,
  l_mdss_ireq_fragmentdir,
  l_mdss_ireq_fragstats,
  l_mdss_ireq_inodestats,

  l_mdc_last,
};

// flags for path_traverse();
static const int MDS_TRAVERSE_DISCOVER		= (1 << 0);
static const int MDS_TRAVERSE_PATH_LOCKED	= (1 << 1);
static const int MDS_TRAVERSE_WANT_DENTRY	= (1 << 2);
static const int MDS_TRAVERSE_WANT_AUTH		= (1 << 3);
static const int MDS_TRAVERSE_RDLOCK_SNAP	= (1 << 4);
static const int MDS_TRAVERSE_RDLOCK_SNAP2	= (1 << 5);
static const int MDS_TRAVERSE_WANT_DIRLAYOUT	= (1 << 6);
static const int MDS_TRAVERSE_RDLOCK_PATH	= (1 << 7);
static const int MDS_TRAVERSE_XLOCK_DENTRY	= (1 << 8);
static const int MDS_TRAVERSE_RDLOCK_AUTHLOCK	= (1 << 9);
static const int MDS_TRAVERSE_CHECK_LOCKCACHE	= (1 << 10);
static const int MDS_TRAVERSE_WANT_INODE	= (1 << 11);


// flags for predirty_journal_parents()
static const int PREDIRTY_PRIMARY = 1; // primary dn, adjust nested accounting
static const int PREDIRTY_DIR = 2;     // update parent dir mtime/size
static const int PREDIRTY_SHALLOW = 4; // only go to immediate parent (for easier rollback)

using namespace std::literals::chrono_literals;

class MDCache {
 public:
  typedef std::map<mds_rank_t, ref_t<MCacheExpire>> expiremap;

  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  // -- discover --
  struct discover_info_t {
    discover_info_t() {}
    ~discover_info_t() {
      if (basei)
	basei->put(MDSCacheObject::PIN_DISCOVERBASE);
    }
    void pin_base(CInode *b) {
      basei = b;
      basei->get(MDSCacheObject::PIN_DISCOVERBASE);
    }

    ceph_tid_t tid = 0;
    mds_rank_t mds = -1;
    inodeno_t ino;
    frag_t frag;
    snapid_t snap = CEPH_NOSNAP;
    filepath want_path;
    CInode *basei = nullptr;
    bool want_base_dir = false;
    bool path_locked = false;
  };

  // [reconnect/rejoin caps]
  struct reconnected_cap_info_t {
    reconnected_cap_info_t() {}
    inodeno_t realm_ino = 0;
    snapid_t snap_follows = 0;
    int dirty_caps = 0;
    bool snapflush = 0;
  };

  // -- find_ino_peer --
  struct find_ino_peer_info_t {
    find_ino_peer_info_t() {}
    inodeno_t ino;
    ceph_tid_t tid = 0;
    MDSContext *fin = nullptr;
    bool path_locked = false;
    mds_rank_t hint = MDS_RANK_NONE;
    mds_rank_t checking = MDS_RANK_NONE;
    std::set<mds_rank_t> checked;
  };

  friend class C_MDC_RejoinOpenInoFinish;
  friend class C_MDC_RejoinSessionsOpened;

  friend class Locker;
  friend class Migrator;
  friend class MDBalancer;

  // StrayManager needs to be able to remove_inode() from us
  // when it is done purging
  friend class StrayManager;

  explicit MDCache(MDSRank *m, PurgeQueue &purge_queue_);
  ~MDCache();

  void insert_taken_inos(inodeno_t ino) {
    replay_taken_inos.insert(ino);
  }
  void clear_taken_inos(inodeno_t ino) {
    replay_taken_inos.erase(ino);
  }
  bool test_and_clear_taken_inos(inodeno_t ino) {
    return replay_taken_inos.erase(ino) != 0;
  }
  bool is_taken_inos_empty(void) {
    return replay_taken_inos.empty();
  }

  uint64_t cache_limit_memory(void) {
    return cache_memory_limit;
  }
  double cache_toofull_ratio(void) const {
    double memory_reserve = cache_memory_limit*(1.0-cache_reservation);
    return fmax(0.0, (cache_size()-memory_reserve)/memory_reserve);
  }
  bool cache_toofull(void) const {
    return cache_toofull_ratio() > 0.0;
  }
  uint64_t cache_size(void) const {
    return mempool::get_pool(mempool::mds_co::id).allocated_bytes();
  }
  bool cache_overfull(void) const {
    return cache_size() > cache_memory_limit*cache_health_threshold;
  }

  void advance_stray();

  unsigned get_ephemeral_dist_frag_bits() const {
    return export_ephemeral_dist_frag_bits;
  }
  bool get_export_ephemeral_distributed_config(void) const {
    return export_ephemeral_distributed_config;
  }

  bool get_export_ephemeral_random_config(void) const {
    return export_ephemeral_random_config;
  }

  bool get_symlink_recovery(void) const {
    return symlink_recovery;
  }

  /**
   * Call this when you know that a CDentry is ready to be passed
   * on to StrayManager (i.e. this is a stray you've just created)
   */
  void notify_stray(CDentry *dn) {
    ceph_assert(dn->get_dir()->get_inode()->is_stray());
    if (dn->state_test(CDentry::STATE_PURGING))
      return;

    stray_manager.eval_stray(dn);
  }

  mds_rank_t hash_into_rank_bucket(inodeno_t ino, frag_t fg=0);

  void maybe_eval_stray(CInode *in, bool delay=false);
  void clear_dirty_bits_for_stray(CInode* diri);

  bool is_readonly() { return readonly; }
  void force_readonly();

  static file_layout_t gen_default_file_layout(const MDSMap &mdsmap);
  static file_layout_t gen_default_log_layout(const MDSMap &mdsmap);

  void register_perfcounters();

  void touch_client_lease(ClientLease *r, int pool, utime_t ttl) {
    client_leases[pool].push_back(&r->item_lease);
    r->ttl = ttl;
  }

  void notify_stray_removed()
  {
    stray_manager.notify_stray_removed();
  }

  void notify_stray_created()
  {
    stray_manager.notify_stray_created();
  }

  void eval_remote(CDentry *dn)
  {
    stray_manager.eval_remote(dn);
  }

  void _send_discover(discover_info_t& dis);
  discover_info_t& _create_discover(mds_rank_t mds) {
    ceph_tid_t t = ++discover_last_tid;
    discover_info_t& d = discovers[t];
    d.tid = t;
    d.mds = mds;
    return d;
  }

  void discover_base_ino(inodeno_t want_ino, MDSContext *onfinish, mds_rank_t from=MDS_RANK_NONE);
  void discover_dir_frag(CInode *base, frag_t approx_fg, MDSContext *onfinish,
			 mds_rank_t from=MDS_RANK_NONE);
  void discover_path(CInode *base, snapid_t snap, filepath want_path, MDSContext *onfinish,
		     bool path_locked=false, mds_rank_t from=MDS_RANK_NONE);
  void discover_path(CDir *base, snapid_t snap, filepath want_path, MDSContext *onfinish,
		     bool path_locked=false);
  void kick_discovers(mds_rank_t who);  // after a failure.

  // adjust subtree auth specification
  //  dir->dir_auth
  //  imports/exports/nested_exports
  //  join/split subtrees as appropriate
  bool is_subtrees() { return !subtrees.empty(); }
  template<typename T>
  void get_subtrees(T& c) {
    if constexpr (std::is_same_v<T, std::vector<CDir*>>)
      c.reserve(c.size() + subtrees.size());
    for (const auto& p : subtrees) {
      c.push_back(p.first);
    }
  }
  void adjust_subtree_auth(CDir *root, mds_authority_t auth, bool adjust_pop=true);
  void adjust_subtree_auth(CDir *root, mds_rank_t a, mds_rank_t b=CDIR_AUTH_UNKNOWN) {
    adjust_subtree_auth(root, mds_authority_t(a,b));
  }
  void adjust_bounded_subtree_auth(CDir *dir, const std::set<CDir*>& bounds, mds_authority_t auth);
  void adjust_bounded_subtree_auth(CDir *dir, const std::set<CDir*>& bounds, mds_rank_t a) {
    adjust_bounded_subtree_auth(dir, bounds, mds_authority_t(a, CDIR_AUTH_UNKNOWN));
  }
  void adjust_bounded_subtree_auth(CDir *dir, const std::vector<dirfrag_t>& bounds, const mds_authority_t &auth);
  void adjust_bounded_subtree_auth(CDir *dir, const std::vector<dirfrag_t>& bounds, mds_rank_t a) {
    adjust_bounded_subtree_auth(dir, bounds, mds_authority_t(a, CDIR_AUTH_UNKNOWN));
  }
  void map_dirfrag_set(const std::list<dirfrag_t>& dfs, std::set<CDir*>& result);
  void try_subtree_merge(CDir *root);
  void try_subtree_merge_at(CDir *root, std::set<CInode*> *to_eval, bool adjust_pop=true);
  void eval_subtree_root(CInode *diri);
  CDir *get_subtree_root(CDir *dir);
  CDir *get_projected_subtree_root(CDir *dir);
  bool is_leaf_subtree(CDir *dir) {
    ceph_assert(subtrees.count(dir));
    return subtrees[dir].empty();
  }
  void remove_subtree(CDir *dir);
  bool is_subtree(CDir *root) {
    return subtrees.count(root);
  }
  void get_subtree_bounds(CDir *root, std::set<CDir*>& bounds);
  void get_wouldbe_subtree_bounds(CDir *root, std::set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const std::set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const std::list<dirfrag_t>& bounds);

  void project_subtree_rename(CInode *diri, CDir *olddir, CDir *newdir);
  void adjust_subtree_after_rename(CInode *diri, CDir *olddir, bool pop);

  auto get_auth_subtrees() {
    std::vector<CDir*> c;
    for (auto& p : subtrees) {
      auto& root = p.first;
      if (root->is_auth()) {
        c.push_back(root);
      }
    }
    return c;
  }

  auto get_fullauth_subtrees() {
    std::vector<CDir*> c;
    for (auto& p : subtrees) {
      auto& root = p.first;
      if (root->is_full_dir_auth()) {
        c.push_back(root);
      }
    }
    return c;
  }
  auto num_subtrees_fullauth() const {
    std::size_t n = 0;
    for (auto& p : subtrees) {
      auto& root = p.first;
      if (root->is_full_dir_auth()) {
        ++n;
      }
    }
    return n;
  }

  auto num_subtrees_fullnonauth() const {
    std::size_t n = 0;
    for (auto& p : subtrees) {
      auto& root = p.first;
      if (root->is_full_dir_nonauth()) {
        ++n;
      }
    }
    return n;
  }

  auto num_subtrees() const {
    return subtrees.size();
  }

  int get_num_client_requests();

  MDRequestRef request_start(const cref_t<MClientRequest>& req);
  MDRequestRef request_start_peer(metareqid_t rid, __u32 attempt, const cref_t<Message> &m);
  MDRequestRef request_start_internal(int op);
  bool have_request(metareqid_t rid) {
    return active_requests.count(rid);
  }
  MDRequestRef request_get(metareqid_t rid);
  void request_finish(const MDRequestRef& mdr);
  void request_forward(const MDRequestRef& mdr, mds_rank_t mds, int port=0);
  void dispatch_request(const MDRequestRef& mdr);
  void request_drop_foreign_locks(const MDRequestRef& mdr);
  void request_drop_non_rdlocks(const MDRequestRef& r);
  void request_drop_locks(const MDRequestRef& r);
  void request_cleanup(const MDRequestRef& r);
  
  void request_kill(const MDRequestRef& r);  // called when session closes

  // journal/snap helpers
  CInode *pick_inode_snap(CInode *in, snapid_t follows);
  CInode *cow_inode(CInode *in, snapid_t last);
  void journal_cow_dentry(MutationImpl *mut, EMetaBlob *metablob, CDentry *dn,
                          snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0, CDentry::linkage_t *dnl=0);
  void journal_dirty_inode(MutationImpl *mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP);

  void project_rstat_inode_to_frag(const MutationRef& mut,
				   CInode *cur, CDir *parent, snapid_t first,
				   int linkunlink, SnapRealm *prealm);
  void _project_rstat_inode_to_frag(const CInode::mempool_inode* inode, snapid_t ofirst, snapid_t last,
				    CDir *parent, int linkunlink, bool update_inode);
  void project_rstat_frag_to_inode(const nest_info_t& rstat, const nest_info_t& accounted_rstat,
				   snapid_t ofirst, snapid_t last, CInode *pin, bool cow_head);
  void broadcast_quota_to_client(CInode *in, client_t exclude_ct = -1, bool quota_change = false);
  void predirty_journal_parents(MutationRef mut, EMetaBlob *blob,
				CInode *in, CDir *parent,
				int flags, int linkunlink=0,
				snapid_t follows=CEPH_NOSNAP);

  // peers
  void add_uncommitted_leader(metareqid_t reqid, LogSegment *ls, std::set<mds_rank_t> &peers, bool safe=false) {
    uncommitted_leaders[reqid].ls = ls;
    uncommitted_leaders[reqid].peers = peers;
    uncommitted_leaders[reqid].safe = safe;
  }
  void wait_for_uncommitted_leader(metareqid_t reqid, MDSContext *c) {
    uncommitted_leaders[reqid].waiters.push_back(c);
  }
  bool have_uncommitted_leader(metareqid_t reqid, mds_rank_t from) {
    auto p = uncommitted_leaders.find(reqid);
    return p != uncommitted_leaders.end() && p->second.peers.count(from) > 0;
  }
  void log_leader_commit(metareqid_t reqid);
  void logged_leader_update(metareqid_t reqid);
  void _logged_leader_commit(metareqid_t reqid);
  void committed_leader_peer(metareqid_t r, mds_rank_t from);
  void finish_committed_leaders();

  void add_uncommitted_peer(metareqid_t reqid, LogSegment*, mds_rank_t, MDPeerUpdate *su=nullptr);
  void wait_for_uncommitted_peer(metareqid_t reqid, MDSContext *c) {
    uncommitted_peers.at(reqid).waiters.push_back(c);
  }
  void finish_uncommitted_peer(metareqid_t reqid, bool assert_exist=true);
  MDPeerUpdate* get_uncommitted_peer(metareqid_t reqid, mds_rank_t leader);
  void _logged_peer_commit(mds_rank_t from, metareqid_t reqid);

  void set_recovery_set(std::set<mds_rank_t>& s);
  void handle_mds_failure(mds_rank_t who);
  void handle_mds_recovery(mds_rank_t who);

  void recalc_auth_bits(bool replay);
  void remove_inode_recursive(CInode *in);

  bool is_ambiguous_peer_update(metareqid_t reqid, mds_rank_t leader) {
    auto p = ambiguous_peer_updates.find(leader);
    return p != ambiguous_peer_updates.end() && p->second.count(reqid);
  }
  void add_ambiguous_peer_update(metareqid_t reqid, mds_rank_t leader) {
    ambiguous_peer_updates[leader].insert(reqid);
  }
  void remove_ambiguous_peer_update(metareqid_t reqid, mds_rank_t leader) {
    auto p = ambiguous_peer_updates.find(leader);
    auto q = p->second.find(reqid);
    ceph_assert(q != p->second.end());
    p->second.erase(q);
    if (p->second.empty())
      ambiguous_peer_updates.erase(p);
  }

  void add_rollback(metareqid_t reqid, mds_rank_t leader) {
    resolve_need_rollback[reqid] = leader;
  }
  void finish_rollback(metareqid_t reqid, const MDRequestRef& mdr);

  // ambiguous imports
  void add_ambiguous_import(dirfrag_t base, const std::vector<dirfrag_t>& bounds);
  void add_ambiguous_import(CDir *base, const std::set<CDir*>& bounds);
  bool have_ambiguous_import(dirfrag_t base) {
    return my_ambiguous_imports.count(base);
  }
  void get_ambiguous_import_bounds(dirfrag_t base, std::vector<dirfrag_t>& bounds) {
    ceph_assert(my_ambiguous_imports.count(base));
    bounds = my_ambiguous_imports[base];
  }
  void cancel_ambiguous_import(CDir *);
  void finish_ambiguous_import(dirfrag_t dirino);
  void resolve_start(MDSContext *resolve_done_);
  void send_resolves();
  void maybe_send_pending_resolves() {
    if (resolves_pending)
      send_subtree_resolves();
  }
  
  void _move_subtree_map_bound(dirfrag_t df, dirfrag_t oldparent, dirfrag_t newparent,
			       std::map<dirfrag_t,std::vector<dirfrag_t> >& subtrees);
  ESubtreeMap *create_subtree_map();

  class QuiesceStatistics {
public:
    void inc_inodes() {
      inodes++;
    }
    void inc_inodes_quiesced() {
      inodes_quiesced++;
    }
    uint64_t inc_heartbeat_count() {
      return ++heartbeat_count;
    }
    void inc_inodes_blocked() {
      inodes_blocked++;
    }
    uint64_t get_inodes() const {
      return inodes;
    }
    uint64_t get_inodes_quiesced() const {
      return inodes_quiesced;
    }
    uint64_t get_inodes_blocked() const {
      return inodes_blocked;
    }
    void add_failed(const MDRequestRef& mdr, int rc) {
      failed[mdr] = rc;
    }
    int get_failed(const MDRequestRef& mdr) const {
      auto it = failed.find(mdr);
      return it == failed.end() ? 0 : it->second;
    }
    const auto& get_failed() const {
      return failed;
    }
    void dump(Formatter* f) const {
      f->dump_unsigned("inodes", inodes);
      f->dump_unsigned("inodes_quiesced", inodes_quiesced);
      f->dump_unsigned("inodes_blocked", inodes_blocked);
      f->open_array_section("failed");
      for (auto& [mdr, rc] : failed) {
        f->open_object_section("failure");
        f->dump_object("request", *mdr);
        f->dump_int("result", rc);
        f->close_section();
      }
      f->close_section();
    }
private:
    uint64_t heartbeat_count = 0;
    uint64_t inodes = 0;
    uint64_t inodes_quiesced = 0;
    uint64_t inodes_blocked = 0;
    std::map<MDRequestRef, int> failed;
  };
  class C_MDS_QuiescePath : public MDSInternalContext {
  public:
    C_MDS_QuiescePath(MDCache *c, Context* _finisher=nullptr) :
      MDSInternalContext(c->mds), cache(c), finisher(_finisher) {}
    ~C_MDS_QuiescePath() {
      if (finisher) {
        finisher->complete(-CEPHFS_ECANCELED);
        finisher = nullptr;
      }
    }
    void set_req(const MDRequestRef& _mdr) {
      mdr = _mdr;
    }
    void finish(int r) override {
      if (finisher) {
        finisher->complete(r);
        finisher = nullptr;
      }
    }
    std::shared_ptr<QuiesceStatistics> qs = std::make_shared<QuiesceStatistics>();
    std::chrono::milliseconds delay = 0ms;
    bool splitauth = false;
    MDCache *cache;
    MDRequestRef mdr;
    Context* finisher = nullptr;
  };
  MDRequestRef quiesce_path(filepath p, C_MDS_QuiescePath* c, Formatter *f = nullptr, std::chrono::milliseconds delay = 0ms);

  void clean_open_file_lists();
  void dump_openfiles(Formatter *f);
  bool dump_inode(Formatter *f, uint64_t number);
  void dump_dir(Formatter *f, CDir *dir, bool dentry_dump=false);

  void rejoin_start(MDSContext *rejoin_done_);
  void rejoin_gather_finish();
  void rejoin_send_rejoins();
  void rejoin_export_caps(inodeno_t ino, client_t client, const cap_reconnect_t& icr,
			  int target=-1, bool drop_path=false) {
    auto& ex = cap_exports[ino];
    ex.first = target;
    auto &_icr = ex.second[client] = icr;
    if (drop_path)
      _icr.path.clear();
  }
  void rejoin_recovered_caps(inodeno_t ino, client_t client, const cap_reconnect_t& icr, 
			     mds_rank_t frommds=MDS_RANK_NONE, bool drop_path=false) {
    auto &_icr = cap_imports[ino][client][frommds] = icr;
    if (drop_path)
      _icr.path.clear();
  }
  void rejoin_recovered_client(client_t client, const entity_inst_t& inst) {
    rejoin_client_map.emplace(client, inst);
  }
  bool rejoin_has_cap_reconnect(inodeno_t ino) const {
    return cap_imports.count(ino);
  }
  void add_replay_ino_alloc(inodeno_t ino) {
    cap_imports_missing.insert(ino); // avoid opening ino during cache rejoin
  }
  const cap_reconnect_t *get_replay_cap_reconnect(inodeno_t ino, client_t client) {
    if (cap_imports.count(ino) &&
	cap_imports[ino].count(client) &&
	cap_imports[ino][client].count(MDS_RANK_NONE)) {
      return &cap_imports[ino][client][MDS_RANK_NONE];
    }
    return NULL;
  }
  void remove_replay_cap_reconnect(inodeno_t ino, client_t client) {
    ceph_assert(cap_imports[ino].size() == 1);
    ceph_assert(cap_imports[ino][client].size() == 1);
    cap_imports.erase(ino);
  }
  void wait_replay_cap_reconnect(inodeno_t ino, MDSContext *c) {
    cap_reconnect_waiters[ino].push_back(c);
  }

  void add_reconnected_cap(client_t client, inodeno_t ino, const cap_reconnect_t& icr) {
    reconnected_cap_info_t &info = reconnected_caps[ino][client];
    info.realm_ino = inodeno_t(icr.capinfo.snaprealm);
    info.snap_follows = icr.snap_follows;
  }
  void set_reconnected_dirty_caps(client_t client, inodeno_t ino, int dirty, bool snapflush) {
    reconnected_cap_info_t &info = reconnected_caps[ino][client];
    info.dirty_caps |= dirty;
    if (snapflush)
      info.snapflush = snapflush;
  }
  void add_reconnected_snaprealm(client_t client, inodeno_t ino, snapid_t seq) {
    reconnected_snaprealms[ino][client] = seq;
  }

  void rejoin_open_ino_finish(inodeno_t ino, int ret);
  void rejoin_prefetch_ino_finish(inodeno_t ino, int ret);
  void rejoin_open_sessions_finish(std::map<client_t,std::pair<Session*,uint64_t> >& session_map);
  bool process_imported_caps();
  void choose_lock_states_and_reconnect_caps();
  void prepare_realm_split(SnapRealm *realm, client_t client, inodeno_t ino,
			   std::map<client_t,ref_t<MClientSnap>>& splits);
  void prepare_realm_merge(SnapRealm *realm, SnapRealm *parent_realm, std::map<client_t,ref_t<MClientSnap>>& splits);
  void send_snaps(std::map<client_t,ref_t<MClientSnap>>& splits);
  Capability* rejoin_import_cap(CInode *in, client_t client, const cap_reconnect_t& icr, mds_rank_t frommds);
  void finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq,
				  std::map<client_t,ref_t<MClientSnap>>& updates);
  Capability* try_reconnect_cap(CInode *in, Session *session);
  void export_remaining_imported_caps();

  void do_cap_import(Session *session, CInode *in, Capability *cap,
		     uint64_t p_cap_id, ceph_seq_t p_seq, ceph_seq_t p_mseq,
		     int peer, int p_flags);
  void do_delayed_cap_imports();
  void rebuild_need_snapflush(CInode *head_in, SnapRealm *realm, client_t client,
			      snapid_t snap_follows);
  void open_snaprealms();

  bool open_undef_inodes_dirfrags();
  void opened_undef_inode(CInode *in);
  void opened_undef_dirfrag(CDir *dir) {
    rejoin_undef_dirfrags.erase(dir);
  }

  void reissue_all_caps();

  void start_files_to_recover();
  void do_file_recover();
  void queue_file_recover(CInode *in);
  void _queued_file_recover_cow(CInode *in, MutationRef& mut);

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);
  
  // debug
  void log_stat();

  // root inode
  CInode *get_root() { return root; }
  CInode *get_myin() { return myin; }

  size_t get_cache_size() { return lru.lru_get_size(); }

  // trimming
  std::pair<bool, uint64_t> trim(uint64_t count=0);

  bool trim_non_auth_subtree(CDir *directory);
  void standby_trim_segment(LogSegment *ls);
  void try_trim_non_auth_subtree(CDir *dir);
  bool can_trim_non_auth_dirfrag(CDir *dir) {
    return my_ambiguous_imports.count((dir)->dirfrag()) == 0 &&
	   uncommitted_peer_rename_olddir.count(dir->inode) == 0;
  }

  /**
   * For all unreferenced inodes, dirs, dentries below an inode, compose
   * expiry messages.  This is used when giving up all replicas of entities
   * for an MDS peer in the 'stopping' state, such that the peer can
   * empty its cache and finish shutting down.
   *
   * We have to make sure we're only expiring un-referenced items to
   * avoid interfering with ongoing stray-movement (we can't distinguish
   * between the "moving my strays" and "waiting for my cache to empty"
   * phases within 'stopping')
   *
   * @return false if we completed cleanly, true if caller should stop
   *         expiring because we hit something with refs.
   */
  bool expire_recursive(CInode *in, expiremap& expiremap);

  void trim_client_leases();
  void check_memory_usage();

  void shutdown_start();
  void shutdown_check();
  bool shutdown_pass();
  bool shutdown();                    // clear cache (ie at shutodwn)
  bool shutdown_export_strays();
  void shutdown_export_stray_finish(inodeno_t ino) {
    if (shutdown_exporting_strays.erase(ino))
      shutdown_export_strays();
  }

  // inode_map
  bool have_inode(vinodeno_t vino) {
    if (vino.snapid == CEPH_NOSNAP)
      return inode_map.count(vino.ino) ? true : false;
    else
      return snap_inode_map.count(vino) ? true : false;
  }
  bool have_inode(inodeno_t ino, snapid_t snap=CEPH_NOSNAP) {
    return have_inode(vinodeno_t(ino, snap));
  }
  CInode* get_inode(vinodeno_t vino) {
    if (vino.snapid == CEPH_NOSNAP) {
      auto p = inode_map.find(vino.ino);
      if (p != inode_map.end())
	return p->second;
    } else {
      auto p = snap_inode_map.find(vino);
      if (p != snap_inode_map.end())
	return p->second;
    }
    return NULL;
  }
  CInode* get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }
  CInode* lookup_snap_inode(vinodeno_t vino) {
    auto p = snap_inode_map.lower_bound(vino);
    if (p != snap_inode_map.end() &&
	p->second->ino() == vino.ino && p->second->first <= vino.snapid)
      return p->second;
    return NULL;
  }

  CDir* get_dirfrag(dirfrag_t df) {
    CInode *in = get_inode(df.ino);
    if (!in)
      return NULL;
    return in->get_dirfrag(df.frag);
  }
  CDir* get_dirfrag(inodeno_t ino, std::string_view dn) {
    CInode *in = get_inode(ino);
    if (!in)
      return NULL;
    frag_t fg = in->pick_dirfrag(dn);
    return in->get_dirfrag(fg);
  }
  CDir* get_force_dirfrag(dirfrag_t df, bool replay) {
    CInode *diri = get_inode(df.ino);
    if (!diri)
      return NULL;
    CDir *dir = force_dir_fragment(diri, df.frag, replay);
    if (!dir)
      dir = diri->get_dirfrag(df.frag);
    return dir;
  }

  MDSCacheObject *get_object(const MDSCacheObjectInfo &info);

  void add_inode(CInode *in);

  void remove_inode(CInode *in);

  void touch_dentry(CDentry *dn) {
    if (dn->state_test(CDentry::STATE_BOTTOMLRU)) {
      bottom_lru.lru_midtouch(dn);
    } else {
      if (dn->is_auth())
	lru.lru_touch(dn);
      else
	lru.lru_midtouch(dn);
    }
  }
  void touch_dentry_bottom(CDentry *dn) {
    if (dn->state_test(CDentry::STATE_BOTTOMLRU))
      return;
    lru.lru_bottouch(dn);
  }

  // truncate
  void truncate_inode(CInode *in, LogSegment *ls);
  void _truncate_inode(CInode *in, LogSegment *ls);
  void truncate_inode_finish(CInode *in, LogSegment *ls);
  void truncate_inode_write_finish(CInode *in, LogSegment *ls,
                                   uint32_t block_size);
  void truncate_inode_logged(CInode *in, MutationRef& mut);

  void add_recovered_truncate(CInode *in, LogSegment *ls);
  void remove_recovered_truncate(CInode *in, LogSegment *ls);
  void start_recovered_truncates();

  // purge unsafe inodes
  void start_purge_inodes();
  void purge_inodes(const interval_set<inodeno_t>& i, LogSegment *ls);

  CDir *get_auth_container(CDir *in);
  CDir *get_export_container(CDir *dir);
  void find_nested_exports(CDir *dir, std::set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, std::set<CDir*>& s);

  void init_layouts();
  void create_unlinked_system_inode(CInode *in, inodeno_t ino,
                                    int mode) const;
  CInode *create_system_inode(inodeno_t ino, int mode);
  CInode *create_root_inode();

  void create_empty_hierarchy(MDSGather *gather);
  void create_mydir_hierarchy(MDSGather *gather);

  bool is_open() { return open; }
  void wait_for_open(MDSContext *c) {
    waiting_for_open.push_back(c);
  }

  void open_root_inode(MDSContext *c);
  void open_root();
  void open_mydir_inode(MDSContext *c);
  void open_mydir_frag(MDSContext *c);
  void populate_mydir();

  void _create_system_file(CDir *dir, std::string_view name, CInode *in, MDSContext *fin);
  void _create_system_file_finish(MutationRef& mut, CDentry *dn,
                                  version_t dpv, MDSContext *fin);

  void open_foreign_mdsdir(inodeno_t ino, MDSContext *c);
  CDir *get_stray_dir(CInode *in);

  /**
   * Find the given dentry (and whether it exists or not), its ancestors,
   * and get them all into memory and usable on this MDS. This function
   * makes a best-effort attempt to load everything; if it needs to
   * go away and do something then it will put the request on a waitlist.
   * It prefers the mdr, then the req, then the fin. (At least one of these
   * must be non-null.)
   *
   * At least one of the params mdr, req, and fin must be non-null.
   *
   * @param mdr The MDRequest associated with the path. Can be null.
   * @param cf A MDSContextFactory for waiter building.
   * @param path The path to traverse to.
   *
   * @param flags Specifies different lookup behaviors.
   * By default, path_traverse() forwards the request to the auth MDS if that
   * is appropriate (ie, if it doesn't know the contents of a directory).
   * MDS_TRAVERSE_DISCOVER: Instead of forwarding request, path_traverse()
   * attempts to look up the path from a different MDS (and bring them into
   * its cache as replicas).
   * MDS_TRAVERSE_PATH_LOCKED: path_traverse() will procceed when xlocked
   * dentry is encountered.
   * MDS_TRAVERSE_WANT_DENTRY: Caller wants tail dentry. Add a null dentry if
   * tail dentry does not exist. return 0 even tail dentry is null.
   * MDS_TRAVERSE_WANT_INODE: Caller only wants target inode if it exists, or
   * wants tail dentry if target inode does not exist and MDS_TRAVERSE_WANT_DENTRY
   * is also set.
   * MDS_TRAVERSE_WANT_AUTH: Always forward request to auth MDS of target inode
   * or auth MDS of tail dentry (MDS_TRAVERSE_WANT_DENTRY is set).
   * MDS_TRAVERSE_XLOCK_DENTRY: Caller wants to xlock tail dentry if MDS_TRAVERSE_WANT_INODE
   * is not set or (MDS_TRAVERSE_WANT_INODE is set but target inode does not exist)
   *
   * @param pdnvec Data return parameter -- on success, contains a
   * vector of dentries. On failure, is either empty or contains the
   * full trace of traversable dentries.
   * @param pin Data return parameter -- if successful, points to the inode
   * associated with filepath. If unsuccessful, is null.
   *
   * @returns 0 on success, 1 on "not done yet", 2 on "forwarding", -errno otherwise.
   * If it returns 1, the requester associated with this call has been placed
   * on the appropriate waitlist, and it should unwind itself and back out.
   * If it returns 2 the request has been forwarded, and again the requester
   * should unwind itself and back out.
   */
  int path_traverse(const MDRequestRef& mdr, MDSContextFactory& cf,
		    const filepath& path, int flags,
		    std::vector<CDentry*> *pdnvec, CInode **pin=nullptr);

  int maybe_request_forward_to_auth(const MDRequestRef& mdr, MDSContextFactory& cf,
				    MDSCacheObject *p);

  CInode *cache_traverse(const filepath& path);

  void open_remote_dirfrag(CInode *diri, frag_t fg, MDSContext *fin);
  CInode *get_dentry_inode(CDentry *dn, const MDRequestRef& mdr, bool projected=false);

  bool parallel_fetch(std::map<inodeno_t,filepath>& pathmap, std::set<inodeno_t>& missing);
  bool parallel_fetch_traverse_dir(inodeno_t ino, filepath& path, 
				   std::set<CDir*>& fetch_queue, std::set<inodeno_t>& missing,
				   C_GatherBuilder &gather_bld);

  void open_remote_dentry(CDentry *dn, bool projected, MDSContext *fin,
			  bool want_xlocked=false);
  void _open_remote_dentry_finish(CDentry *dn, inodeno_t ino, MDSContext *fin,
				  bool want_xlocked, int r);

  void make_trace(std::vector<CDentry*>& trace, CInode *in);

  void open_ino(inodeno_t ino, int64_t pool, MDSContext *fin,
		bool want_replica=true, bool want_xlocked=false,
		std::vector<inode_backpointer_t> *ancestors_hint=nullptr,
		mds_rank_t auth_hint=MDS_RANK_NONE);
  void open_ino_batch_start();
  void open_ino_batch_submit();
  void kick_open_ino_peers(mds_rank_t who);

  void find_ino_peers(inodeno_t ino, MDSContext *c,
		      mds_rank_t hint=MDS_RANK_NONE, bool path_locked=false);
  void _do_find_ino_peer(find_ino_peer_info_t& fip);
  void handle_find_ino(const cref_t<MMDSFindIno> &m);
  void handle_find_ino_reply(const cref_t<MMDSFindInoReply> &m);
  void kick_find_ino_peers(mds_rank_t who);

  SnapRealm *get_global_snaprealm() const { return global_snaprealm; }
  void create_global_snaprealm();
  void do_realm_invalidate_and_update_notify(CInode *in, int snapop, bool notify_clients=true);
  void send_snap_update(CInode *in, version_t stid, int snap_op);
  void handle_snap_update(const cref_t<MMDSSnapUpdate> &m);
  void notify_global_snaprealm_update(int snap_op);

  // -- stray --
  void fetch_backtrace(inodeno_t ino, int64_t pool, bufferlist& bl, Context *fin);
  uint64_t get_num_strays() const { return stray_manager.get_num_strays(); }

  // == messages ==
  void dispatch(const cref_t<Message> &m);

  void encode_replica_dir(CDir *dir, mds_rank_t to, bufferlist& bl);
  void encode_replica_dentry(CDentry *dn, mds_rank_t to, bufferlist& bl);
  void encode_replica_inode(CInode *in, mds_rank_t to, bufferlist& bl,
		       uint64_t features);
  
  void decode_replica_dir(CDir *&dir, bufferlist::const_iterator& p, CInode *diri, mds_rank_t from, MDSContext::vec& finished);
  void decode_replica_dentry(CDentry *&dn, bufferlist::const_iterator& p, CDir *dir, MDSContext::vec& finished);
  void decode_replica_inode(CInode *&in, bufferlist::const_iterator& p, CDentry *dn, MDSContext::vec& finished);

  void encode_replica_stray(CDentry *straydn, mds_rank_t who, bufferlist& bl);
  void decode_replica_stray(CDentry *&straydn, CInode **in, const bufferlist &bl, mds_rank_t from);

  // -- namespace --
  void encode_remote_dentry_link(CDentry::linkage_t *dnl, bufferlist& bl);
  void decode_remote_dentry_link(CDir *dir, CDentry *dn, bufferlist::const_iterator& p);
  void send_dentry_link(CDentry *dn, const MDRequestRef& mdr);
  void send_dentry_unlink(CDentry *dn, CDentry *straydn, const MDRequestRef& mdr);

  void wait_for_uncommitted_fragment(dirfrag_t dirfrag, MDSContext *c) {
    uncommitted_fragments.at(dirfrag).waiters.push_back(c);
  }
  bool is_any_uncommitted_fragment() const {
    return !uncommitted_fragments.empty();
  }
  void wait_for_uncommitted_fragments(MDSContext* finisher);
  void rollback_uncommitted_fragments();

  void split_dir(CDir *dir, int byn);
  void merge_dir(CInode *diri, frag_t fg);

  void find_stale_fragment_freeze();
  void fragment_freeze_inc_num_waiters(CDir *dir);
  bool fragment_are_all_frozen(CDir *dir);
  int get_num_fragmenting_dirs() { return fragments.size(); }

  // -- updates --
  //int send_inode_updates(CInode *in);
  //void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, bool bcast=false);
  void handle_dir_update(const cref_t<MDirUpdate> &m);

  // -- cache expiration --
  void handle_cache_expire(const cref_t<MCacheExpire> &m);
  void process_delayed_expire(CDir *dir);
  void discard_delayed_expire(CDir *dir);

  // -- mdsmap --
  void handle_mdsmap(const MDSMap &mdsmap, const MDSMap &oldmap);

  int dump_cache() { return dump_cache({}, nullptr, 0); }
  int dump_cache(std::string_view filename, double timeout);
  int dump_cache(Formatter *f, double timeout);
  void dump_tree(CInode *in, const int cur_depth, const int max_depth, Formatter *f);

  void cache_status(Formatter *f);

  void dump_resolve_status(Formatter *f) const;
  void dump_rejoin_status(Formatter *f) const;

  // == crap fns ==
  void show_cache();
  void show_subtrees(int dbl=10, bool force_print=false);

  CInode *hack_pick_random_inode() {
    ceph_assert(!inode_map.empty());
    int n = rand() % inode_map.size();
    auto p = inode_map.begin();
    while (n--) ++p;
    return p->second;
  }

  void flush_dentry(std::string_view path, Context *fin);
  /**
   * Create and start an OP_ENQUEUE_SCRUB
   */
  void enqueue_scrub(std::string_view path, std::string_view tag,
                     bool force, bool recursive, bool repair,
                     bool scrub_mdsdir, Formatter *f, Context *fin);
  void repair_inode_stats(CInode *diri);
  void repair_dirfrag_stats(CDir *dir);
  void rdlock_dirfrags_stats(CInode *diri, MDSInternalContext *fin);

  // my leader
  MDSRank *mds;

  // -- my cache --
  LRU lru;   // dentry lru for expiring items from cache
  LRU bottom_lru; // dentries that should be trimmed ASAP

  DecayRate decayrate;

  int num_shadow_inodes = 0;

  int num_inodes_with_caps = 0;

  unsigned max_dir_commit_size;

  file_layout_t default_file_layout;
  file_layout_t default_log_layout;

  // -- client leases --
  static constexpr std::size_t client_lease_pools = 3;
  std::array<float, client_lease_pools> client_lease_durations{5.0, 30.0, 300.0};

  // -- client caps --
  uint64_t last_cap_id = 0;

  std::map<ceph_tid_t, discover_info_t> discovers;
  ceph_tid_t discover_last_tid = 0;

  // waiters
  std::map<int, std::map<inodeno_t, MDSContext::vec > > waiting_for_base_ino;

  std::map<inodeno_t,std::map<client_t, reconnected_cap_info_t> > reconnected_caps;   // inode -> client -> snap_follows,realmino
  std::map<inodeno_t,std::map<client_t, snapid_t> > reconnected_snaprealms;  // realmino -> client -> realmseq

  //  realm inodes
  std::set<CInode*> rejoin_pending_snaprealms;
  // cap imports.  delayed snap parent opens.
  std::map<client_t,std::set<CInode*> > delayed_imported_caps;

  // subsystems
  std::unique_ptr<Migrator> migrator;

  bool did_shutdown_log_cap = false;

  std::map<ceph_tid_t, find_ino_peer_info_t> find_ino_peer;
  ceph_tid_t find_ino_peer_last_tid = 0;

  // delayed cache expire
  std::map<CDir*, expiremap> delayed_expire; // subtree root -> expire msg

  /* Because exports may fail, this set lets us keep track of inodes that need exporting. */
  std::set<CInode *> export_pin_queue;
  std::set<CInode *> export_pin_delayed_queue;
  std::set<CInode *> export_ephemeral_pins;

  OpenFileTable open_file_table;

  double export_ephemeral_random_max = 0.0;

 protected:
  // track leader requests whose peers haven't acknowledged commit
  struct uleader {
    uleader() {}
    std::set<mds_rank_t> peers;
    LogSegment *ls = nullptr;
    MDSContext::vec waiters;
    bool safe = false;
    bool committing = false;
    bool recovering = false;
  };

  struct upeer {
    upeer() {}
    mds_rank_t leader;
    LogSegment *ls = nullptr;
    MDPeerUpdate *su = nullptr;
    MDSContext::vec waiters;
  };

  struct open_ino_info_t {
    open_ino_info_t() {}
    std::vector<inode_backpointer_t> ancestors;
    std::set<mds_rank_t> checked;
    mds_rank_t checking = MDS_RANK_NONE;
    mds_rank_t auth_hint = MDS_RANK_NONE;
    bool check_peers = true;
    bool fetch_backtrace = true;
    bool discover = false;
    bool want_replica = false;
    bool want_xlocked = false;
    version_t tid = 0;
    int64_t pool = -1;
    int last_err = 0;
    MDSContext::vec waiters;
  };

  ceph_tid_t open_ino_last_tid = 0;
  std::map<inodeno_t,open_ino_info_t> opening_inodes;

  bool open_ino_batch = false;
  std::map<CDir*, std::pair<std::vector<std::string>, MDSContext::vec> > open_ino_batched_fetch;

  friend struct C_MDC_OpenInoTraverseDir;
  friend struct C_MDC_OpenInoParentOpened;
  friend struct C_MDC_RetryScanStray;

  friend class C_IO_MDC_OpenInoBacktraceFetched;
  friend class C_MDC_Join;
  friend class C_MDC_RespondInternalRequest;

  friend class EPeerUpdate;
  friend class ECommitted;

  void set_readonly() { readonly = true; }

  void handle_resolve(const cref_t<MMDSResolve> &m);
  void handle_resolve_ack(const cref_t<MMDSResolveAck> &m);
  void process_delayed_resolve();
  void discard_delayed_resolve(mds_rank_t who);
  void maybe_resolve_finish();
  void disambiguate_my_imports();
  void disambiguate_other_imports();
  void trim_unlinked_inodes();

  void send_peer_resolves();
  void send_subtree_resolves();
  void maybe_finish_peer_resolve();

  void rejoin_walk(CDir *dir, const ref_t<MMDSCacheRejoin> &rejoin);
  void handle_cache_rejoin(const cref_t<MMDSCacheRejoin> &m);
  void handle_cache_rejoin_weak(const cref_t<MMDSCacheRejoin> &m);
  CInode* rejoin_invent_inode(inodeno_t ino, snapid_t last);
  CDir* rejoin_invent_dirfrag(dirfrag_t df);
  void handle_cache_rejoin_strong(const cref_t<MMDSCacheRejoin> &m);
  void rejoin_scour_survivor_replicas(mds_rank_t from, const cref_t<MMDSCacheRejoin> &ack,
				      std::set<vinodeno_t>& acked_inodes,
				      std::set<SimpleLock *>& gather_locks);
  void handle_cache_rejoin_ack(const cref_t<MMDSCacheRejoin> &m);
  void rejoin_send_acks();
  void rejoin_trim_undef_inodes();
  void maybe_send_pending_rejoins() {
    if (rejoins_pending)
      rejoin_send_rejoins();
  }

  void touch_inode(CInode *in) {
    if (in->get_parent_dn())
      touch_dentry(in->get_projected_parent_dn());
  }

  void inode_remove_replica(CInode *in, mds_rank_t rep, bool rejoin,
			    std::set<SimpleLock *>& gather_locks);
  void dentry_remove_replica(CDentry *dn, mds_rank_t rep, std::set<SimpleLock *>& gather_locks);

  void rename_file(CDentry *srcdn, CDentry *destdn);

  void _open_ino_backtrace_fetched(inodeno_t ino, bufferlist& bl, int err);
  void _open_ino_parent_opened(inodeno_t ino, int ret);
  void _open_ino_traverse_dir(inodeno_t ino, open_ino_info_t& info, int err);
  void _open_ino_fetch_dir(inodeno_t ino, const cref_t<MMDSOpenIno> &m, bool parent,
			   CDir *dir, std::string_view dname);
  int open_ino_traverse_dir(inodeno_t ino, const cref_t<MMDSOpenIno> &m,
			    const std::vector<inode_backpointer_t>& ancestors,
			    bool discover, bool want_xlocked, mds_rank_t *hint);
  void open_ino_finish(inodeno_t ino, open_ino_info_t& info, int err);
  void do_open_ino(inodeno_t ino, open_ino_info_t& info, int err);
  void do_open_ino_peer(inodeno_t ino, open_ino_info_t& info);
  void handle_open_ino(const cref_t<MMDSOpenIno> &m, int err=0);
  void handle_open_ino_reply(const cref_t<MMDSOpenInoReply> &m);

  void scan_stray_dir(dirfrag_t next=dirfrag_t());
  // -- replicas --
  void handle_discover(const cref_t<MDiscover> &dis);
  void handle_discover_reply(const cref_t<MDiscoverReply> &m);
  void handle_dentry_link(const cref_t<MDentryLink> &m);
  void handle_dentry_unlink(const cref_t<MDentryUnlink> &m);

  int dump_cache(std::string_view fn, Formatter *f, double timeout);

  void flush_dentry_work(const MDRequestRef& mdr);
  /**
   * Resolve path to a dentry and pass it onto the ScrubStack.
   *
   * TODO: return enough information to the original mdr formatter
   * and completion that they can subsequeuntly check the progress of
   * this scrub (we won't block them on a whole scrub as it can take a very
   * long time)
   */
  void enqueue_scrub_work(const MDRequestRef& mdr);
  void repair_inode_stats_work(const MDRequestRef& mdr);
  void repair_dirfrag_stats_work(const MDRequestRef& mdr);
  void rdlock_dirfrags_stats_work(const MDRequestRef& mdr);

  ceph::unordered_map<inodeno_t,CInode*> inode_map;  // map of head inodes by ino
  std::map<vinodeno_t, CInode*> snap_inode_map;  // map of snap inodes by ino
  CInode *root = nullptr; // root inode
  CInode *myin = nullptr; // .ceph/mds%d dir

  bool readonly = false;

  int stray_index = 0;
  int stray_fragmenting_index = -1;

  std::set<CInode*> base_inodes;

  std::unique_ptr<PerfCounters> logger;

  Filer filer;
  std::array<xlist<ClientLease*>, client_lease_pools> client_leases{};

  /* subtree keys and each tree's non-recursive nested subtrees (the "bounds") */
  std::map<CDir*,std::set<CDir*> > subtrees;
  std::map<CInode*,std::list<std::pair<CDir*,CDir*> > > projected_subtree_renames;  // renamed ino -> target dir

  // -- requests --
  ceph::unordered_map<metareqid_t, MDRequestRef> active_requests;

  // -- recovery --
  std::set<mds_rank_t> recovery_set;

  // [resolve]
  // from EImportStart w/o EImportFinish during journal replay
  std::map<dirfrag_t, std::vector<dirfrag_t> > my_ambiguous_imports;
  // from MMDSResolves
  std::map<mds_rank_t, std::map<dirfrag_t, std::vector<dirfrag_t> > > other_ambiguous_imports;

  std::map<CInode*, int> uncommitted_peer_rename_olddir;  // peer: preserve the non-auth dir until seeing commit.
  std::map<CInode*, int> uncommitted_peer_unlink;  // peer: preserve the unlinked inode until seeing commit.

  std::map<metareqid_t, uleader> uncommitted_leaders;         // leader: req -> peer set
  std::map<metareqid_t, upeer> uncommitted_peers;  // peer: preserve the peer req until seeing commit.

  std::set<metareqid_t> pending_leaders;
  std::map<int, std::set<metareqid_t> > ambiguous_peer_updates;

  bool resolves_pending = false;
  std::set<mds_rank_t> resolve_gather;	// nodes i need resolves from
  std::set<mds_rank_t> resolve_ack_gather;	// nodes i need a resolve_ack from
  std::set<version_t> resolve_snapclient_commits;
  std::map<metareqid_t, mds_rank_t> resolve_need_rollback;  // rollbacks i'm writing to the journal
  std::map<mds_rank_t, cref_t<MMDSResolve>> delayed_resolve;

  // [rejoin]
  bool rejoins_pending = false;
  std::set<mds_rank_t> rejoin_gather;      // nodes from whom i need a rejoin
  std::set<mds_rank_t> rejoin_sent;        // nodes i sent a rejoin to
  std::set<mds_rank_t> rejoin_ack_sent;    // nodes i sent a rejoin to
  std::set<mds_rank_t> rejoin_ack_gather;  // nodes from whom i need a rejoin ack
  std::map<mds_rank_t,std::map<inodeno_t,std::map<client_t,Capability::Import> > > rejoin_imported_caps;
  std::map<inodeno_t,std::pair<mds_rank_t,std::map<client_t,Capability::Export> > > rejoin_peer_exports;

  std::map<client_t,entity_inst_t> rejoin_client_map;
  std::map<client_t,client_metadata_t> rejoin_client_metadata_map;
  std::map<client_t,std::pair<Session*,uint64_t> > rejoin_session_map;

  std::map<inodeno_t,std::pair<mds_rank_t,std::map<client_t,cap_reconnect_t> > > cap_exports; // ino -> target, client -> capex

  std::map<inodeno_t,std::map<client_t,std::map<mds_rank_t,cap_reconnect_t> > > cap_imports;  // ino -> client -> frommds -> capex
  std::set<inodeno_t> cap_imports_missing;
  std::map<inodeno_t, MDSContext::vec > cap_reconnect_waiters;
  int cap_imports_num_opening = 0;

  std::set<CInode*> rejoin_undef_inodes;
  std::set<CInode*> rejoin_potential_updated_scatterlocks;
  std::set<CDir*>   rejoin_undef_dirfrags;
  std::map<mds_rank_t, std::set<CInode*> > rejoin_unlinked_inodes;

  std::vector<CInode*> rejoin_recover_q, rejoin_check_q;
  std::list<SimpleLock*> rejoin_eval_locks;
  MDSContext::vec rejoin_waiters;

  std::unique_ptr<MDSContext> rejoin_done;
  std::unique_ptr<MDSContext> resolve_done;

  StrayManager stray_manager;

 private:
  std::set<inodeno_t> replay_taken_inos; // the inos have been taken when replaying

  // -- fragmenting --
  struct ufragment {
    ufragment() {}
    int bits = 0;
    bool committed = false;
    LogSegment *ls = nullptr;
    MDSContext::vec waiters;
    frag_vec_t old_frags;
    bufferlist rollback;
  };

  struct fragment_info_t {
    fragment_info_t() {}
    bool is_fragmenting() { return !resultfrags.empty(); }
    uint64_t get_tid() { return mdr ? mdr->reqid.tid : 0; }
    int bits;
    std::vector<CDir*> dirs;
    std::vector<CDir*> resultfrags;
    MDRequestRef mdr;
    std::set<mds_rank_t> notify_ack_waiting;
    bool finishing = false;

    // for deadlock detection
    bool all_frozen = false;
    utime_t last_cum_auth_pins_change;
    int last_cum_auth_pins = 0;
    int num_remote_waiters = 0;	// number of remote authpin waiters
  };

  enum KILL_SHUTDOWN_AT {
    SHUTDOWN_NULL,
    SHUTDOWN_START,
    SHUTDOWN_POSTTRIM,
    SHUTDOWN_POSTONEEXPORT,
    SHUTDOWN_POSTALLEXPORTS,
    SHUTDOWN_SESSIONTERMINATE,
    SHUTDOWN_SUBTREEMAP,
    SHUTDOWN_TRIMALL,
    SHUTDOWN_STRAYPUT,
    SHUTDOWN_LOGCAP,
    SHUTDOWN_EMPTYSUBTREES,
    SHUTDOWN_MYINREMOVAL,
    SHUTDOWN_GLOBALSNAPREALMREMOVAL,
    SHUTDOWN_UNUSED
  };

  typedef std::map<dirfrag_t,fragment_info_t>::iterator fragment_info_iterator;

  friend class EFragment;
  friend class C_MDC_FragmentFrozen;
  friend class C_MDC_FragmentMarking;
  friend class C_MDC_FragmentPrep;
  friend class C_MDC_FragmentStore;
  friend class C_MDC_FragmentCommit;
  friend class C_MDC_FragmentRollback;
  friend class C_IO_MDC_FragmentPurgeOld;

  // -- subtrees --
  static const unsigned int SUBTREES_COUNT_THRESHOLD = 5;
  static const unsigned int SUBTREES_DEPTH_THRESHOLD = 5;

  CInode *get_stray() {
    return strays[stray_index];
  }

  void identify_files_to_recover();

  std::pair<bool, uint64_t> trim_lru(uint64_t count, expiremap& expiremap);
  bool trim_dentry(CDentry *dn, expiremap& expiremap);
  void trim_dirfrag(CDir *dir, CDir *con, expiremap& expiremap);
  bool trim_inode(CDentry *dn, CInode *in, CDir *con, expiremap&);
  void send_expire_messages(expiremap& expiremap);
  void trim_non_auth();      // trim out trimmable non-auth items

  void adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
			    std::vector<CDir*>* frags, MDSContext::vec& waiters, bool replay);
  void adjust_dir_fragments(CInode *diri,
			    const std::vector<CDir*>& srcfrags,
			    frag_t basefrag, int bits,
			    std::vector<CDir*>* resultfrags,
			    MDSContext::vec& waiters,
			    bool replay);
  CDir *force_dir_fragment(CInode *diri, frag_t fg, bool replay=true);
  void get_force_dirfrag_bound_set(const std::vector<dirfrag_t>& dfs, std::set<CDir*>& bounds);

  bool can_fragment(CInode *diri, const std::vector<CDir*>& dirs);
  void fragment_freeze_dirs(const std::vector<CDir*>& dirs);
  void fragment_mark_and_complete(const MDRequestRef& mdr);
  void fragment_frozen(const MDRequestRef& mdr, int r);
  void fragment_unmark_unfreeze_dirs(const std::vector<CDir*>& dirs);
  void fragment_drop_locks(fragment_info_t &info);
  void fragment_maybe_finish(const fragment_info_iterator& it);
  void dispatch_fragment_dir(const MDRequestRef& mdr);
  void _fragment_logged(const MDRequestRef& mdr);
  void _fragment_stored(const MDRequestRef& mdr);
  void _fragment_committed(dirfrag_t f, const MDRequestRef& mdr);
  void _fragment_old_purged(dirfrag_t f, int bits, const MDRequestRef& mdr);

  void handle_fragment_notify(const cref_t<MMDSFragmentNotify> &m);
  void handle_fragment_notify_ack(const cref_t<MMDSFragmentNotifyAck> &m);

  void add_uncommitted_fragment(dirfrag_t basedirfrag, int bits, const frag_vec_t& old_frag,
				LogSegment *ls, bufferlist *rollback=NULL);
  void finish_uncommitted_fragment(dirfrag_t basedirfrag, int op);
  void rollback_uncommitted_fragment(dirfrag_t basedirfrag, frag_vec_t&& old_frags);

  void dispatch_quiesce_path(const MDRequestRef& mdr);
  void dispatch_quiesce_inode(const MDRequestRef& mdr);

  void upkeep_main(void);

  uint64_t cache_memory_limit;
  double cache_reservation;
  double cache_health_threshold;
  std::array<CInode *, NUM_STRAY> strays{}; // my stray dir

  bool export_ephemeral_distributed_config;
  bool export_ephemeral_random_config;
  unsigned export_ephemeral_dist_frag_bits;

  // Stores the symlink target on the file object's head
  bool symlink_recovery;

  // File size recovery
  RecoveryQueue recovery_queue;

  // shutdown
  std::set<inodeno_t> shutdown_exporting_strays;
  std::pair<dirfrag_t, std::string> shutdown_export_next;

  bool opening_root = false, open = false;
  MDSContext::vec waiting_for_open;

  // -- snaprealms --
  SnapRealm *global_snaprealm = nullptr;

  std::map<dirfrag_t, ufragment> uncommitted_fragments;

  std::map<dirfrag_t,fragment_info_t> fragments;

  DecayCounter trim_counter;

  std::thread upkeeper;
  ceph::mutex upkeep_mutex = ceph::make_mutex("MDCache::upkeep_mutex");
  ceph::condition_variable upkeep_cvar;
  time upkeep_last_trim = clock::zero();
  time upkeep_last_release = clock::zero();
  std::atomic<bool> upkeep_trim_shutdown{false};

  uint64_t kill_shutdown_at = 0;

  std::map<inodeno_t, MDRequestRef> quiesced_subvolumes;
  DecayCounter quiesce_counter;
  uint64_t quiesce_threshold;
  std::chrono::milliseconds quiesce_sleep;
};

class C_MDS_RetryRequest : public MDSInternalContext {
  MDCache *cache;
  MDRequestRef mdr;
 public:
  C_MDS_RetryRequest(MDCache *c, const MDRequestRef& r) :
    MDSInternalContext(c->mds), cache(c), mdr(r) {}
  void finish(int r) override;
};

class CF_MDS_RetryRequestFactory : public MDSContextFactory {
public:
  CF_MDS_RetryRequestFactory(MDCache *cache, const MDRequestRef& mdr, bool dl) :
    mdcache(cache), mdr(mdr), drop_locks(dl) {}
  MDSContext *build() override;
private:
  MDCache *mdcache;
  MDRequestRef mdr;
  bool drop_locks;
};

/**
 * Only for contexts called back from an I/O completion
 *
 * Note: duplication of members wrt MDCacheContext, because
 * it'ls the lesser of two evils compared with introducing
 * yet another piece of (multiple) inheritance.
 */
class MDCacheIOContext : public virtual MDSIOContextBase {
protected:
  MDCache *mdcache;
  MDSRank *get_mds() override
  {
    ceph_assert(mdcache != NULL);
    return mdcache->mds;
  }
public:
  explicit MDCacheIOContext(MDCache *mdc_, bool track=true) :
    MDSIOContextBase(track), mdcache(mdc_) {}
};

#endif
