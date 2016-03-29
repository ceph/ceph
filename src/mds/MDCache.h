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

#include "include/types.h"
#include "include/filepath.h"
#include "include/elist.h"

#include "osdc/Filer.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "include/Context.h"
#include "events/EMetaBlob.h"
#include "RecoveryQueue.h"
#include "StrayManager.h"
#include "MDSContext.h"
#include "MDSMap.h"

#include "messages/MClientRequest.h"
#include "messages/MMDSSlaveRequest.h"

class PerfCounters;

class MDSRank;
class Session;
class Migrator;

class Message;
class Session;

class MMDSResolve;
class MMDSResolveAck;
class MMDSCacheRejoin;
class MMDSCacheRejoinAck;
class MJoin;
class MJoinAck;
class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryLink;
class MDentryUnlink;
class MLock;
struct MMDSFindIno;
struct MMDSFindInoReply;
struct MMDSOpenIno;
struct MMDSOpenInoReply;

class Message;
class MClientRequest;
class MMDSSlaveRequest;
struct MClientSnap;

class MMDSFragmentNotify;

class ESubtreeMap;

struct MDRequestImpl;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;
struct MDSlaveUpdate;

enum {
  l_mdc_first = 3000,
  // How many inodes currently in stray dentries
  l_mdc_num_strays,
  // How many stray dentries are currently being purged
  l_mdc_num_strays_purging,
  // How many stray dentries are currently delayed for purge due to refs
  l_mdc_num_strays_delayed,
  // How many purge RADOS ops might currently be in flight?
  l_mdc_num_purge_ops,
  // How many dentries have ever been added to stray dir
  l_mdc_strays_created,
  // How many dentries have ever finished purging from stray dir
  l_mdc_strays_purged,
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

  l_mdc_last,
};


// flags for predirty_journal_parents()
static const int PREDIRTY_PRIMARY = 1; // primary dn, adjust nested accounting
static const int PREDIRTY_DIR = 2;     // update parent dir mtime/size
static const int PREDIRTY_SHALLOW = 4; // only go to immediate parent (for easier rollback)

class MDCache {
 public:
  // my master
  MDSRank *mds;

  // -- my cache --
  LRU lru;   // dentry lru for expiring items from cache
 protected:
  ceph::unordered_map<vinodeno_t,CInode*> inode_map;  // map of inodes by ino
  CInode *root;                            // root inode
  CInode *myin;                            // .ceph/mds%d dir

  bool readonly;
  void set_readonly() { readonly = true; }

  CInode *strays[NUM_STRAY];         // my stray dir
  int stray_index;

  CInode *get_stray() {
    return strays[stray_index];
  }

  set<CInode*> base_inodes;

  PerfCounters *logger;

  Filer filer;

public:
  void advance_stray() {
    stray_index = (stray_index+1)%NUM_STRAY;
  }

  /**
   * Call this when you know that a CDentry is ready to be passed
   * on to StrayManager (i.e. this is a stray you've just created)
   */
  void notify_stray(CDentry *dn) {
    assert(dn->get_dir()->get_inode()->is_stray());
    stray_manager.eval_stray(dn);
  }

  void handle_conf_change(const struct md_config_t *conf,
                          const std::set <std::string> &changed);

  void maybe_eval_stray(CInode *in, bool delay=false);
  bool is_readonly() { return readonly; }
  void force_readonly();

  DecayRate decayrate;

  int num_inodes_with_caps;
  int num_caps;

  unsigned max_dir_commit_size;

  static file_layout_t gen_default_file_layout(const MDSMap &mdsmap);
  static file_layout_t gen_default_log_layout(const MDSMap &mdsmap);

  file_layout_t default_file_layout;
  file_layout_t default_log_layout;

  void register_perfcounters();

  // -- client leases --
public:
  static const int client_lease_pools = 3;
  float client_lease_durations[client_lease_pools];
protected:
  xlist<ClientLease*> client_leases[client_lease_pools];
public:
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

  // -- client caps --
  uint64_t              last_cap_id;
  


  // -- discover --
  struct discover_info_t {
    ceph_tid_t tid;
    mds_rank_t mds;
    inodeno_t ino;
    frag_t frag;
    snapid_t snap;
    filepath want_path;
    bool want_base_dir;
    bool want_xlocked;

    discover_info_t() : tid(0), mds(-1), snap(CEPH_NOSNAP), want_base_dir(false), want_xlocked(false) {}
  };

  map<ceph_tid_t, discover_info_t> discovers;
  ceph_tid_t discover_last_tid;

  void _send_discover(discover_info_t& dis);
  discover_info_t& _create_discover(mds_rank_t mds) {
    ceph_tid_t t = ++discover_last_tid;
    discover_info_t& d = discovers[t];
    d.tid = t;
    d.mds = mds;
    return d;
  }

  // waiters
  map<int, map<inodeno_t, list<MDSInternalContextBase*> > > waiting_for_base_ino;

  void discover_base_ino(inodeno_t want_ino, MDSInternalContextBase *onfinish, mds_rank_t from=MDS_RANK_NONE);
  void discover_dir_frag(CInode *base, frag_t approx_fg, MDSInternalContextBase *onfinish,
			 mds_rank_t from=MDS_RANK_NONE);
  void discover_path(CInode *base, snapid_t snap, filepath want_path, MDSInternalContextBase *onfinish,
		     bool want_xlocked=false, mds_rank_t from=MDS_RANK_NONE);
  void discover_path(CDir *base, snapid_t snap, filepath want_path, MDSInternalContextBase *onfinish,
		     bool want_xlocked=false);
  void kick_discovers(mds_rank_t who);  // after a failure.


public:
  int get_num_inodes() { return inode_map.size(); }
  int get_num_dentries() { return lru.lru_get_size(); }


  // -- subtrees --
protected:
  map<CDir*,set<CDir*> > subtrees;   // nested bounds on subtrees.
  map<CInode*,list<pair<CDir*,CDir*> > > projected_subtree_renames;  // renamed ino -> target dir
  
  // adjust subtree auth specification
  //  dir->dir_auth
  //  imports/exports/nested_exports
  //  join/split subtrees as appropriate
public:
  bool is_subtrees() { return !subtrees.empty(); }
  void list_subtrees(list<CDir*>& ls);
  void adjust_subtree_auth(CDir *root, mds_authority_t auth, bool do_eval=true);
  void adjust_subtree_auth(CDir *root, mds_rank_t a, mds_rank_t b=CDIR_AUTH_UNKNOWN, bool do_eval=true) {
    adjust_subtree_auth(root, mds_authority_t(a,b), do_eval); 
  }
  void adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, mds_authority_t auth);
  void adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, mds_rank_t a) {
    adjust_bounded_subtree_auth(dir, bounds, mds_authority_t(a, CDIR_AUTH_UNKNOWN));
  }
  void adjust_bounded_subtree_auth(CDir *dir, vector<dirfrag_t>& bounds, mds_authority_t auth);
  void adjust_bounded_subtree_auth(CDir *dir, vector<dirfrag_t>& bounds, mds_rank_t a) {
    adjust_bounded_subtree_auth(dir, bounds, mds_authority_t(a, CDIR_AUTH_UNKNOWN));
  }
  void map_dirfrag_set(list<dirfrag_t>& dfs, set<CDir*>& result);
  void try_subtree_merge(CDir *root);
  void try_subtree_merge_at(CDir *root, bool do_eval=true);
  void subtree_merge_writebehind_finish(CInode *in, MutationRef& mut);
  void eval_subtree_root(CInode *diri);
  CDir *get_subtree_root(CDir *dir);
  CDir *get_projected_subtree_root(CDir *dir);
  bool is_leaf_subtree(CDir *dir) {
    assert(subtrees.count(dir));
    return subtrees[dir].empty();
  }
  void remove_subtree(CDir *dir);
  bool is_subtree(CDir *root) {
    return subtrees.count(root);
  }
  void get_subtree_bounds(CDir *root, set<CDir*>& bounds);
  void get_wouldbe_subtree_bounds(CDir *root, set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const list<dirfrag_t>& bounds);

  void project_subtree_rename(CInode *diri, CDir *olddir, CDir *newdir);
  void adjust_subtree_after_rename(CInode *diri, CDir *olddir,
                                   bool pop, bool imported = false);

  void get_auth_subtrees(set<CDir*>& s);
  void get_fullauth_subtrees(set<CDir*>& s);

  int num_subtrees();
  int num_subtrees_fullauth();
  int num_subtrees_fullnonauth();

  
protected:
  // delayed cache expire
  map<CDir*, map<mds_rank_t, MCacheExpire*> > delayed_expire; // subtree root -> expire msg


  // -- requests --
protected:
  ceph::unordered_map<metareqid_t, MDRequestRef> active_requests;

public:
  int get_num_client_requests();

  MDRequestRef request_start(MClientRequest *req);
  MDRequestRef request_start_slave(metareqid_t rid, __u32 attempt, Message *m);
  MDRequestRef request_start_internal(int op);
  bool have_request(metareqid_t rid) {
    return active_requests.count(rid);
  }
  MDRequestRef request_get(metareqid_t rid);
  void request_pin_ref(MDRequestRef& r, CInode *ref, vector<CDentry*>& trace);
  void request_finish(MDRequestRef& mdr);
  void request_forward(MDRequestRef& mdr, mds_rank_t mds, int port=0);
  void dispatch_request(MDRequestRef& mdr);
  void request_drop_foreign_locks(MDRequestRef& mdr);
  void request_drop_non_rdlocks(MDRequestRef& r);
  void request_drop_locks(MDRequestRef& r);
  void request_cleanup(MDRequestRef& r);
  
  void request_kill(MDRequestRef& r);  // called when session closes

  // journal/snap helpers
  CInode *pick_inode_snap(CInode *in, snapid_t follows);
  CInode *cow_inode(CInode *in, snapid_t last);
  void journal_cow_dentry(MutationImpl *mut, EMetaBlob *metablob, CDentry *dn,
                          snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0, CDentry::linkage_t *dnl=0);
  void journal_cow_inode(MutationRef& mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0);
  void journal_dirty_inode(MutationImpl *mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP);

  void project_rstat_inode_to_frag(CInode *cur, CDir *parent, snapid_t first,
				   int linkunlink, SnapRealm *prealm);
  void _project_rstat_inode_to_frag(inode_t& inode, snapid_t ofirst, snapid_t last,
				    CDir *parent, int linkunlink=0);
  void project_rstat_frag_to_inode(nest_info_t& rstat, nest_info_t& accounted_rstat,
				   snapid_t ofirst, snapid_t last, 
				   CInode *pin, bool cow_head);
  void broadcast_quota_to_client(CInode *in);
  void predirty_journal_parents(MutationRef mut, EMetaBlob *blob,
				CInode *in, CDir *parent,
				int flags, int linkunlink=0,
				snapid_t follows=CEPH_NOSNAP);

  // slaves
  void add_uncommitted_master(metareqid_t reqid, LogSegment *ls, set<mds_rank_t> &slaves, bool safe=false) {
    uncommitted_masters[reqid].ls = ls;
    uncommitted_masters[reqid].slaves = slaves;
    uncommitted_masters[reqid].safe = safe;
  }
  void wait_for_uncommitted_master(metareqid_t reqid, MDSInternalContextBase *c) {
    uncommitted_masters[reqid].waiters.push_back(c);
  }
  void log_master_commit(metareqid_t reqid);
  void logged_master_update(metareqid_t reqid);
  void _logged_master_commit(metareqid_t reqid);
  void committed_master_slave(metareqid_t r, mds_rank_t from);
  void finish_committed_masters();

  void _logged_slave_commit(mds_rank_t from, metareqid_t reqid);

  // -- recovery --
protected:
  set<mds_rank_t> recovery_set;

public:
  void set_recovery_set(set<mds_rank_t>& s);
  void handle_mds_failure(mds_rank_t who);
  void handle_mds_recovery(mds_rank_t who);

protected:
  // [resolve]
  // from EImportStart w/o EImportFinish during journal replay
  map<dirfrag_t, vector<dirfrag_t> >            my_ambiguous_imports;  
  // from MMDSResolves
  map<mds_rank_t, map<dirfrag_t, vector<dirfrag_t> > > other_ambiguous_imports;  

  map<mds_rank_t, map<metareqid_t, MDSlaveUpdate*> > uncommitted_slave_updates;  // slave: for replay.
  map<CInode*, int> uncommitted_slave_rename_olddir;  // slave: preserve the non-auth dir until seeing commit.
  map<CInode*, int> uncommitted_slave_unlink;  // slave: preserve the unlinked inode until seeing commit.

  // track master requests whose slaves haven't acknowledged commit
  struct umaster {
    set<mds_rank_t> slaves;
    LogSegment *ls;
    list<MDSInternalContextBase*> waiters;
    bool safe;
    bool committing;
    bool recovering;
    umaster() : ls(NULL), safe(false), committing(false), recovering(false) {}
  };
  map<metareqid_t, umaster>                 uncommitted_masters;         // master: req -> slave set

  set<metareqid_t>		pending_masters;
  map<int, set<metareqid_t> >	ambiguous_slave_updates;

  friend class ESlaveUpdate;
  friend class ECommitted;

  bool resolves_pending;
  set<mds_rank_t> resolve_gather;	// nodes i need resolves from
  set<mds_rank_t> resolve_ack_gather;	// nodes i need a resolve_ack from
  map<metareqid_t, mds_rank_t> need_resolve_rollback;  // rollbacks i'm writing to the journal
  map<mds_rank_t, MMDSResolve*> delayed_resolve;
  
  void handle_resolve(MMDSResolve *m);
  void handle_resolve_ack(MMDSResolveAck *m);
  void process_delayed_resolve();
  void discard_delayed_resolve(mds_rank_t who);
  void maybe_resolve_finish();
  void disambiguate_imports();
  void trim_unlinked_inodes();
  void add_uncommitted_slave_update(metareqid_t reqid, mds_rank_t master, MDSlaveUpdate*);
  void finish_uncommitted_slave_update(metareqid_t reqid, mds_rank_t master);
  MDSlaveUpdate* get_uncommitted_slave_update(metareqid_t reqid, mds_rank_t master);
public:
  void recalc_auth_bits(bool replay);
  void remove_inode_recursive(CInode *in);

  bool is_ambiguous_slave_update(metareqid_t reqid, mds_rank_t master) {
    return ambiguous_slave_updates.count(master) &&
	   ambiguous_slave_updates[master].count(reqid);
  }
  void add_ambiguous_slave_update(metareqid_t reqid, mds_rank_t master) {
    ambiguous_slave_updates[master].insert(reqid);
  }
  void remove_ambiguous_slave_update(metareqid_t reqid, mds_rank_t master) {
    assert(ambiguous_slave_updates[master].count(reqid));
    ambiguous_slave_updates[master].erase(reqid);
    if (ambiguous_slave_updates[master].empty())
      ambiguous_slave_updates.erase(master);
  }

  void add_rollback(metareqid_t reqid, mds_rank_t master) {
    need_resolve_rollback[reqid] = master;
  }
  void finish_rollback(metareqid_t reqid);

  // ambiguous imports
  void add_ambiguous_import(dirfrag_t base, const vector<dirfrag_t>& bounds);
  void add_ambiguous_import(CDir *base, const set<CDir*>& bounds);
  bool have_ambiguous_import(dirfrag_t base) {
    return my_ambiguous_imports.count(base);
  }
  void get_ambiguous_import_bounds(dirfrag_t base, vector<dirfrag_t>& bounds) {
    assert(my_ambiguous_imports.count(base));
    bounds = my_ambiguous_imports[base];
  }
  void cancel_ambiguous_import(CDir *);
  void finish_ambiguous_import(dirfrag_t dirino);
  void resolve_start(MDSInternalContext *resolve_done_);
  void send_resolves();
  void send_slave_resolves();
  void send_subtree_resolves();
  void maybe_send_pending_resolves() {
    if (resolves_pending)
      send_subtree_resolves();
  }
  
  void _move_subtree_map_bound(dirfrag_t df, dirfrag_t oldparent, dirfrag_t newparent,
			       map<dirfrag_t,vector<dirfrag_t> >& subtrees);
  ESubtreeMap *create_subtree_map();


  void clean_open_file_lists();

protected:
  // [rejoin]
  bool rejoins_pending;
  set<mds_rank_t> rejoin_gather;      // nodes from whom i need a rejoin
  set<mds_rank_t> rejoin_sent;        // nodes i sent a rejoin to
  set<mds_rank_t> rejoin_ack_gather;  // nodes from whom i need a rejoin ack
  map<mds_rank_t,map<inodeno_t,map<client_t,Capability::Import> > > rejoin_imported_caps;
  map<inodeno_t,pair<mds_rank_t,map<client_t,Capability::Export> > > rejoin_slave_exports;
  map<client_t,entity_inst_t> rejoin_client_map;

  map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> > cap_exports; // ino -> client -> capex
  map<inodeno_t,mds_rank_t> cap_export_targets; // ino -> auth mds

  map<inodeno_t,map<client_t,map<mds_rank_t,ceph_mds_cap_reconnect> > > cap_imports;  // ino -> client -> frommds -> capex
  map<inodeno_t,int> cap_imports_dirty;
  set<inodeno_t> cap_imports_missing;
  map<inodeno_t, list<MDSInternalContextBase*> > cap_reconnect_waiters;
  int cap_imports_num_opening;
  
  set<CInode*> rejoin_undef_inodes;
  set<CInode*> rejoin_potential_updated_scatterlocks;
  set<CDir*>   rejoin_undef_dirfrags;
  map<mds_rank_t, set<CInode*> > rejoin_unlinked_inodes;

  vector<CInode*> rejoin_recover_q, rejoin_check_q;
  list<SimpleLock*> rejoin_eval_locks;
  list<MDSInternalContextBase*> rejoin_waiters;

  void rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_weak(MMDSCacheRejoin *m);
  CInode* rejoin_invent_inode(inodeno_t ino, snapid_t last);
  CDir* rejoin_invent_dirfrag(dirfrag_t df);
  void handle_cache_rejoin_strong(MMDSCacheRejoin *m);
  void rejoin_scour_survivor_replicas(mds_rank_t from, MMDSCacheRejoin *ack,
				      set<vinodeno_t>& acked_inodes,
				      set<SimpleLock *>& gather_locks);
  void handle_cache_rejoin_ack(MMDSCacheRejoin *m);
  void rejoin_send_acks();
  void rejoin_trim_undef_inodes();
  void maybe_send_pending_rejoins() {
    if (rejoins_pending)
      rejoin_send_rejoins();
  }
  MDSInternalContext *rejoin_done;
  MDSInternalContext *resolve_done;
public:
  void rejoin_start(MDSInternalContext *rejoin_done_);
  void rejoin_gather_finish();
  void rejoin_send_rejoins();
  void rejoin_export_caps(inodeno_t ino, client_t client, ceph_mds_cap_reconnect& capinfo,
			  int target=-1) {
    cap_exports[ino][client] = capinfo;
    cap_export_targets[ino] = target;
  }
  void rejoin_recovered_caps(inodeno_t ino, client_t client, cap_reconnect_t& icr, 
			     mds_rank_t frommds=MDS_RANK_NONE) {
    cap_imports[ino][client][frommds] = icr.capinfo;
  }
  ceph_mds_cap_reconnect *get_replay_cap_reconnect(inodeno_t ino, client_t client) {
    if (cap_imports.count(ino) &&
	cap_imports[ino].count(client) &&
	cap_imports[ino][client].count(MDS_RANK_NONE)) {
      return &cap_imports[ino][client][MDS_RANK_NONE];
    }
    return NULL;
  }
  void remove_replay_cap_reconnect(inodeno_t ino, client_t client) {
    assert(cap_imports[ino].size() == 1);
    assert(cap_imports[ino][client].size() == 1);
    cap_imports.erase(ino);
  }
  void set_reconnect_dirty_caps(inodeno_t ino, int dirty) {
    cap_imports_dirty[ino] |= dirty;
  }
  void wait_replay_cap_reconnect(inodeno_t ino, MDSInternalContextBase *c) {
    cap_reconnect_waiters[ino].push_back(c);
  }

  // [reconnect/rejoin caps]
  map<CInode*,map<client_t, inodeno_t> >  reconnected_caps;   // inode -> client -> realmino
  map<inodeno_t,map<client_t, snapid_t> > reconnected_snaprealms;  // realmino -> client -> realmseq

  void add_reconnected_cap(CInode *in, client_t client, inodeno_t realm) {
    reconnected_caps[in][client] = realm;
  }
  void add_reconnected_snaprealm(client_t client, inodeno_t ino, snapid_t seq) {
    reconnected_snaprealms[ino][client] = seq;
  }

  friend class C_MDC_RejoinOpenInoFinish;
  friend class C_MDC_RejoinSessionsOpened;
  void rejoin_open_ino_finish(inodeno_t ino, int ret);
  void rejoin_open_sessions_finish(map<client_t,entity_inst_t> client_map,
				   map<client_t,uint64_t>& sseqmap);
  bool process_imported_caps();
  void choose_lock_states_and_reconnect_caps();
  void prepare_realm_split(SnapRealm *realm, client_t client, inodeno_t ino,
			   map<client_t,MClientSnap*>& splits);
  void do_realm_invalidate_and_update_notify(CInode *in, int snapop, bool nosend=false);
  void send_snaps(map<client_t,MClientSnap*>& splits);
  Capability* rejoin_import_cap(CInode *in, client_t client, ceph_mds_cap_reconnect& icr, mds_rank_t frommds);
  void finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq);
  void try_reconnect_cap(CInode *in, Session *session);
  void export_remaining_imported_caps();

  // cap imports.  delayed snap parent opens.
  //  realm inode -> client -> cap inodes needing to split to this realm
  map<CInode*,map<client_t, set<inodeno_t> > > missing_snap_parents; 
  map<client_t,set<CInode*> > delayed_imported_caps;

  void do_cap_import(Session *session, CInode *in, Capability *cap,
		     uint64_t p_cap_id, ceph_seq_t p_seq, ceph_seq_t p_mseq,
		     int peer, int p_flags);
  void do_delayed_cap_imports();
  void check_realm_past_parents(SnapRealm *realm);
  void open_snap_parents();

  bool open_undef_inodes_dirfrags();
  void opened_undef_inode(CInode *in);
  void opened_undef_dirfrag(CDir *dir) {
    rejoin_undef_dirfrags.erase(dir);
  }

  void reissue_all_caps();
  

  friend class Locker;
  friend class Migrator;
  friend class MDBalancer;

  // StrayManager needs to be able to remove_inode() from us
  // when it is done purging
  friend class StrayManager;

  // File size recovery
private:
  RecoveryQueue recovery_queue;
  void identify_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q);
  void start_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q);
public:
  void do_file_recover();
  void queue_file_recover(CInode *in);
  void _queued_file_recover_cow(CInode *in, MutationRef& mut);

  // subsystems
  Migrator *migrator;

 public:
  explicit MDCache(MDSRank *m);
  ~MDCache();
  
  // debug
  void log_stat();

  // root inode
  CInode *get_root() { return root; }
  CInode *get_myin() { return myin; }

  // cache
  void set_cache_size(size_t max) { lru.lru_set_max(max); }
  size_t get_cache_size() { return lru.lru_get_size(); }

  // trimming
  bool trim(int max=-1, int count=-1);   // trim cache
  bool trim_dentry(CDentry *dn, map<mds_rank_t, MCacheExpire*>& expiremap);
  void trim_dirfrag(CDir *dir, CDir *con,
		    map<mds_rank_t, MCacheExpire*>& expiremap);
  bool trim_inode(CDentry *dn, CInode *in, CDir *con,
		  map<mds_rank_t,class MCacheExpire*>& expiremap);
  void send_expire_messages(map<mds_rank_t, MCacheExpire*>& expiremap);
  void trim_non_auth();      // trim out trimmable non-auth items
  bool trim_non_auth_subtree(CDir *directory);
  void standby_trim_segment(LogSegment *ls);
  void try_trim_non_auth_subtree(CDir *dir);
  bool can_trim_non_auth_dirfrag(CDir *dir) {
    return my_ambiguous_imports.count((dir)->dirfrag()) == 0 &&
	   uncommitted_slave_rename_olddir.count(dir->inode) == 0;
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
  bool expire_recursive(
    CInode *in,
    std::map<mds_rank_t, MCacheExpire*>& expiremap,
    CDir *subtree);

  void trim_client_leases();
  void check_memory_usage();

  // shutdown
  void shutdown_start();
  void shutdown_check();
  bool shutdown_pass();
  bool shutdown_export_strays();
  bool shutdown_export_caps();
  bool shutdown();                    // clear cache (ie at shutodwn)

  bool did_shutdown_log_cap;

  // inode_map
  bool have_inode(vinodeno_t vino) {
    return inode_map.count(vino) ? true:false;
  }
  bool have_inode(inodeno_t ino, snapid_t snap=CEPH_NOSNAP) {
    return have_inode(vinodeno_t(ino, snap));
  }
  CInode* get_inode(vinodeno_t vino) {
    if (have_inode(vino))
      return inode_map[vino];
    return NULL;
  }
  CInode* get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }

  CDir* get_dirfrag(dirfrag_t df) {
    CInode *in = get_inode(df.ino);
    if (!in)
      return NULL;
    return in->get_dirfrag(df.frag);
  }
  CDir* get_dirfrag(inodeno_t ino, const string& dn) {
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

  MDSCacheObject *get_object(MDSCacheObjectInfo &info);

  

 public:
  void add_inode(CInode *in);

  void remove_inode(CInode *in);
 protected:
  void touch_inode(CInode *in) {
    if (in->get_parent_dn())
      touch_dentry(in->get_projected_parent_dn());
  }
public:
  void touch_dentry(CDentry *dn) {
    // touch ancestors
    if (dn->get_dir()->get_inode()->get_projected_parent_dn())
      touch_dentry(dn->get_dir()->get_inode()->get_projected_parent_dn());
    
    // touch me
    if (dn->is_auth())
      lru.lru_touch(dn);
    else
      lru.lru_midtouch(dn);
  }
  void touch_dentry_bottom(CDentry *dn) {
    lru.lru_bottouch(dn);
    if (dn->get_projected_linkage()->is_primary() &&
	dn->get_dir()->inode->is_stray()) {
      CInode *in = dn->get_projected_linkage()->get_inode();
      if (in->has_dirfrags()) {
	list<CDir*> ls;
	in->get_dirfrags(ls);
	for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p)
	  (*p)->touch_dentries_bottom();
      }
    }
  }
protected:

  void inode_remove_replica(CInode *in, mds_rank_t rep, bool rejoin,
			    set<SimpleLock *>& gather_locks);
  void dentry_remove_replica(CDentry *dn, mds_rank_t rep, set<SimpleLock *>& gather_locks);

  void rename_file(CDentry *srcdn, CDentry *destdn);

 public:
  // truncate
  void truncate_inode(CInode *in, LogSegment *ls);
  void _truncate_inode(CInode *in, LogSegment *ls);
  void truncate_inode_finish(CInode *in, LogSegment *ls);
  void truncate_inode_logged(CInode *in, MutationRef& mut);

  void add_recovered_truncate(CInode *in, LogSegment *ls);
  void remove_recovered_truncate(CInode *in, LogSegment *ls);
  void start_recovered_truncates();


 public:
  CDir *get_auth_container(CDir *in);
  CDir *get_export_container(CDir *dir);
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);


private:
  bool opening_root, open;
  list<MDSInternalContextBase*> waiting_for_open;

public:
  void init_layouts();
  void create_unlinked_system_inode(CInode *in, inodeno_t ino,
                                    int mode) const;
  CInode *create_system_inode(inodeno_t ino, int mode);
  CInode *create_root_inode();

  void create_empty_hierarchy(MDSGather *gather);
  void create_mydir_hierarchy(MDSGather *gather);

  bool is_open() { return open; }
  void wait_for_open(MDSInternalContextBase *c) {
    waiting_for_open.push_back(c);
  }

  void open_root_inode(MDSInternalContextBase *c);
  void open_root();
  void open_mydir_inode(MDSInternalContextBase *c);
  void populate_mydir();

  void _create_system_file(CDir *dir, const char *name, CInode *in, MDSInternalContextBase *fin);
  void _create_system_file_finish(MutationRef& mut, CDentry *dn,
                                  version_t dpv, MDSInternalContextBase *fin);

  void open_foreign_mdsdir(inodeno_t ino, MDSInternalContextBase *c);
  CDentry *get_or_create_stray_dentry(CInode *in);

  MDSInternalContextBase *_get_waiter(MDRequestRef& mdr, Message *req, MDSInternalContextBase *fin);

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
   * @param req The Message associated with the path. Can be null.
   * @param fin The Context associated with the path. Can be null.
   * @param path The path to traverse to.
   * @param pdnvec Data return parameter -- on success, contains a
   * vector of dentries. On failure, is either empty or contains the
   * full trace of traversable dentries.
   * @param pin Data return parameter -- if successful, points to the inode
   * associated with filepath. If unsuccessful, is null.
   * @param onfail Specifies different lookup failure behaviors. If set to
   * MDS_TRAVERSE_DISCOVERXLOCK, path_traverse will succeed on null
   * dentries (instead of returning -ENOENT). If set to
   * MDS_TRAVERSE_FORWARD, it will forward the request to the auth
   * MDS if that becomes appropriate (ie, if it doesn't know the contents
   * of a directory). If set to MDS_TRAVERSE_DISCOVER, it
   * will attempt to look up the path from a different MDS (and bring them
   * into its cache as replicas).
   *
   * @returns 0 on success, 1 on "not done yet", 2 on "forwarding", -errno otherwise.
   * If it returns 1, the requester associated with this call has been placed
   * on the appropriate waitlist, and it should unwind itself and back out.
   * If it returns 2 the request has been forwarded, and again the requester
   * should unwind itself and back out.
   */
  int path_traverse(MDRequestRef& mdr, Message *req, MDSInternalContextBase *fin, const filepath& path,
		    vector<CDentry*> *pdnvec, CInode **pin, int onfail);
  bool path_is_mine(filepath& path);
  bool path_is_mine(string& p) {
    filepath path(p, 1);
    return path_is_mine(path);
  }

  CInode *cache_traverse(const filepath& path);

  void open_remote_dirfrag(CInode *diri, frag_t fg, MDSInternalContextBase *fin);
  CInode *get_dentry_inode(CDentry *dn, MDRequestRef& mdr, bool projected=false);

  bool parallel_fetch(map<inodeno_t,filepath>& pathmap, set<inodeno_t>& missing);
  bool parallel_fetch_traverse_dir(inodeno_t ino, filepath& path, 
				   set<CDir*>& fetch_queue, set<inodeno_t>& missing,
				   C_GatherBuilder &gather_bld);

  void open_remote_dentry(CDentry *dn, bool projected, MDSInternalContextBase *fin,
			  bool want_xlocked=false);
  void _open_remote_dentry_finish(CDentry *dn, inodeno_t ino, MDSInternalContextBase *fin,
				  bool want_xlocked, int r);

  void make_trace(vector<CDentry*>& trace, CInode *in);

protected:
  struct open_ino_info_t {
    vector<inode_backpointer_t> ancestors;
    set<mds_rank_t> checked;
    mds_rank_t checking;
    mds_rank_t auth_hint;
    bool check_peers;
    bool fetch_backtrace;
    bool discover;
    bool want_replica;
    bool want_xlocked;
    version_t tid;
    int64_t pool;
    list<MDSInternalContextBase*> waiters;
    open_ino_info_t() : checking(MDS_RANK_NONE), auth_hint(MDS_RANK_NONE),
      check_peers(true), fetch_backtrace(true), discover(false),
      want_replica(false), want_xlocked(false), tid(0), pool(-1) {}
  };
  ceph_tid_t open_ino_last_tid;
  map<inodeno_t,open_ino_info_t> opening_inodes;

  void _open_ino_backtrace_fetched(inodeno_t ino, bufferlist& bl, int err);
  void _open_ino_parent_opened(inodeno_t ino, int ret);
  void _open_ino_traverse_dir(inodeno_t ino, open_ino_info_t& info, int err);
  void _open_ino_fetch_dir(inodeno_t ino, MMDSOpenIno *m, CDir *dir);
  MDSInternalContextBase* _open_ino_get_waiter(inodeno_t ino, MMDSOpenIno *m);
  int open_ino_traverse_dir(inodeno_t ino, MMDSOpenIno *m,
			    vector<inode_backpointer_t>& ancestors,
			    bool discover, bool want_xlocked, mds_rank_t *hint);
  void open_ino_finish(inodeno_t ino, open_ino_info_t& info, int err);
  void do_open_ino(inodeno_t ino, open_ino_info_t& info, int err);
  void do_open_ino_peer(inodeno_t ino, open_ino_info_t& info);
  void handle_open_ino(MMDSOpenIno *m);
  void handle_open_ino_reply(MMDSOpenInoReply *m);
  friend class C_IO_MDC_OpenInoBacktraceFetched;
  friend struct C_MDC_OpenInoTraverseDir;
  friend struct C_MDC_OpenInoParentOpened;

public:
  void kick_open_ino_peers(mds_rank_t who);
  void open_ino(inodeno_t ino, int64_t pool, MDSInternalContextBase *fin,
		bool want_replica=true, bool want_xlocked=false);
  
  // -- find_ino_peer --
  struct find_ino_peer_info_t {
    inodeno_t ino;
    ceph_tid_t tid;
    MDSInternalContextBase *fin;
    mds_rank_t hint;
    mds_rank_t checking;
    set<mds_rank_t> checked;

    find_ino_peer_info_t() : tid(0), fin(NULL), hint(MDS_RANK_NONE), checking(MDS_RANK_NONE) {}
  };

  map<ceph_tid_t, find_ino_peer_info_t> find_ino_peer;
  ceph_tid_t find_ino_peer_last_tid;

  void find_ino_peers(inodeno_t ino, MDSInternalContextBase *c, mds_rank_t hint=MDS_RANK_NONE);
  void _do_find_ino_peer(find_ino_peer_info_t& fip);
  void handle_find_ino(MMDSFindIno *m);
  void handle_find_ino_reply(MMDSFindInoReply *m);
  void kick_find_ino_peers(mds_rank_t who);

  // -- snaprealms --
public:
  void snaprealm_create(MDRequestRef& mdr, CInode *in);
  void _snaprealm_create_finish(MDRequestRef& mdr, MutationRef& mut, CInode *in);

  // -- stray --
public:
  void eval_remote(CDentry *dn);
  void fetch_backtrace(inodeno_t ino, int64_t pool, bufferlist& bl, Context *fin);

protected:
  void scan_stray_dir(dirfrag_t next=dirfrag_t());
  StrayManager stray_manager;
  friend struct C_MDC_RetryScanStray;
  friend class C_IO_MDC_FetchedBacktrace;

  // == messages ==
 public:
  void dispatch(Message *m);

 protected:
  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);
  friend class C_MDC_Join;

public:
  void replicate_dir(CDir *dir, mds_rank_t to, bufferlist& bl) {
    dirfrag_t df = dir->dirfrag();
    ::encode(df, bl);
    dir->encode_replica(to, bl);
  }
  void replicate_dentry(CDentry *dn, mds_rank_t to, bufferlist& bl) {
    ::encode(dn->name, bl);
    ::encode(dn->last, bl);
    dn->encode_replica(to, bl);
  }
  void replicate_inode(CInode *in, mds_rank_t to, bufferlist& bl,
		       uint64_t features) {
    ::encode(in->inode.ino, bl);  // bleh, minor assymetry here
    ::encode(in->last, bl);
    in->encode_replica(to, bl, features);
  }
  
  CDir* add_replica_dir(bufferlist::iterator& p, CInode *diri, mds_rank_t from, list<MDSInternalContextBase*>& finished);
  CDir* forge_replica_dir(CInode *diri, frag_t fg, mds_rank_t from);
  CDentry *add_replica_dentry(bufferlist::iterator& p, CDir *dir, list<MDSInternalContextBase*>& finished);
  CInode *add_replica_inode(bufferlist::iterator& p, CDentry *dn, list<MDSInternalContextBase*>& finished);

  void replicate_stray(CDentry *straydn, mds_rank_t who, bufferlist& bl);
  CDentry *add_replica_stray(bufferlist &bl, mds_rank_t from);

  // -- namespace --
public:
  void send_dentry_link(CDentry *dn, MDRequestRef& mdr);
  void send_dentry_unlink(CDentry *dn, CDentry *straydn, MDRequestRef& mdr);
protected:
  void handle_dentry_link(MDentryLink *m);
  void handle_dentry_unlink(MDentryUnlink *m);


  // -- fragmenting --
private:
  struct ufragment {
    int bits;
    bool committed;
    LogSegment *ls;
    list<MDSInternalContextBase*> waiters;
    list<frag_t> old_frags;
    bufferlist rollback;
    ufragment() : bits(0), committed(false), ls(NULL) {}
  };
  map<dirfrag_t, ufragment> uncommitted_fragments;

  struct fragment_info_t {
    int bits;
    list<CDir*> dirs;
    list<CDir*> resultfrags;
    MDRequestRef mdr;
    // for deadlock detection
    bool all_frozen;
    utime_t last_cum_auth_pins_change;
    int last_cum_auth_pins;
    int num_remote_waiters;	// number of remote authpin waiters
    fragment_info_t() : bits(0), all_frozen(false), last_cum_auth_pins(0), num_remote_waiters(0) {}
    bool is_fragmenting() { return !resultfrags.empty(); }
  };
  map<dirfrag_t,fragment_info_t> fragments;

  void adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
			    list<CDir*>& frags, list<MDSInternalContextBase*>& waiters, bool replay);
  void adjust_dir_fragments(CInode *diri,
			    list<CDir*>& srcfrags,
			    frag_t basefrag, int bits,
			    list<CDir*>& resultfrags, 
			    list<MDSInternalContextBase*>& waiters,
			    bool replay);
  CDir *force_dir_fragment(CInode *diri, frag_t fg, bool replay=true);
  void get_force_dirfrag_bound_set(vector<dirfrag_t>& dfs, set<CDir*>& bounds);

  bool can_fragment(CInode *diri, list<CDir*>& dirs);
  void fragment_freeze_dirs(list<CDir*>& dirs);
  void fragment_mark_and_complete(MDRequestRef& mdr);
  void fragment_frozen(MDRequestRef& mdr, int r);
  void fragment_unmark_unfreeze_dirs(list<CDir*>& dirs);
  void dispatch_fragment_dir(MDRequestRef& mdr);
  void _fragment_logged(MDRequestRef& mdr);
  void _fragment_stored(MDRequestRef& mdr);
  void _fragment_committed(dirfrag_t f, list<CDir*>& resultfrags);
  void _fragment_finish(dirfrag_t f, list<CDir*>& resultfrags);

  friend class EFragment;
  friend class C_MDC_FragmentFrozen;
  friend class C_MDC_FragmentMarking;
  friend class C_MDC_FragmentPrep;
  friend class C_MDC_FragmentStore;
  friend class C_MDC_FragmentCommit;
  friend class C_IO_MDC_FragmentFinish;

  void handle_fragment_notify(MMDSFragmentNotify *m);

  void add_uncommitted_fragment(dirfrag_t basedirfrag, int bits, list<frag_t>& old_frag,
				LogSegment *ls, bufferlist *rollback=NULL);
  void finish_uncommitted_fragment(dirfrag_t basedirfrag, int op);
  void rollback_uncommitted_fragment(dirfrag_t basedirfrag, list<frag_t>& old_frags);
public:
  void wait_for_uncommitted_fragment(dirfrag_t dirfrag, MDSInternalContextBase *c) {
    assert(uncommitted_fragments.count(dirfrag));
    uncommitted_fragments[dirfrag].waiters.push_back(c);
  }
  void split_dir(CDir *dir, int byn);
  void merge_dir(CInode *diri, frag_t fg);
  void rollback_uncommitted_fragments();

  void find_stale_fragment_freeze();
  void fragment_freeze_inc_num_waiters(CDir *dir);
  bool fragment_are_all_frozen(CDir *dir);
  int get_num_fragmenting_dirs() { return fragments.size(); }

  // -- updates --
  //int send_inode_updates(CInode *in);
  //void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, bool bcast=false);
  void handle_dir_update(MDirUpdate *m);

  // -- cache expiration --
  void handle_cache_expire(MCacheExpire *m);
  void process_delayed_expire(CDir *dir);
  void discard_delayed_expire(CDir *dir);

  void notify_mdsmap_changed();
  void notify_osdmap_changed();

protected:
  void dump_cache(const char *fn, Formatter *f);
public:
  void dump_cache() {dump_cache(NULL, NULL);}
  void dump_cache(const std::string &filename);
  void dump_cache(Formatter *f);

  void dump_resolve_status(Formatter *f) const;
  void dump_rejoin_status(Formatter *f) const;

  // == crap fns ==
 public:
  void show_cache();
  void show_subtrees(int dbl=10);

  CInode *hack_pick_random_inode() {
    assert(!inode_map.empty());
    int n = rand() % inode_map.size();
    ceph::unordered_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
    while (n--) ++p;
    return p->second;
  }

protected:
  void flush_dentry_work(MDRequestRef& mdr);
  /**
   * Resolve path to a dentry and pass it onto the ScrubStack.
   *
   * TODO: return enough information to the original mdr formatter
   * and completion that they can subsequeuntly check the progress of
   * this scrub (we won't block them on a whole scrub as it can take a very
   * long time)
   */
  void enqueue_scrub_work(MDRequestRef& mdr);
  void repair_inode_stats_work(MDRequestRef& mdr);
  void repair_dirfrag_stats_work(MDRequestRef& mdr);
  friend class C_MDC_RepairDirfragStats;
public:
  void flush_dentry(const string& path, Context *fin);
  /**
   * Create and start an OP_ENQUEUE_SCRUB
   */
  void enqueue_scrub(const string& path, const std::string &tag,
                     bool force, bool recursive, bool repair,
		     Formatter *f, Context *fin);
  void repair_inode_stats(CInode *diri);
  void repair_dirfrag_stats(CDir *dir);
};

class C_MDS_RetryRequest : public MDSInternalContext {
  MDCache *cache;
  MDRequestRef mdr;
 public:
  C_MDS_RetryRequest(MDCache *c, MDRequestRef& r);
  virtual void finish(int r);
};

#endif
