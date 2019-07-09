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


#ifndef CEPH_CDIR_H
#define CEPH_CDIR_H

#include <iosfwd>
#include <list>
#include <map>
#include <set>
#include <string>
#include <string_view>

#include "common/bloom_filter.hpp"
#include "common/config.h"
#include "include/buffer_fwd.h"
#include "include/counter.h"
#include "include/types.h"

#include "CInode.h"
#include "MDSCacheObject.h"
#include "MDSContext.h"
#include "cephfs_features.h"
#include "SessionMap.h"
#include "messages/MClientReply.h"

class CDentry;
class MDCache;

struct ObjectOperation;

std::ostream& operator<<(std::ostream& out, const class CDir& dir);

class CDir : public MDSCacheObject, public Counter<CDir> {
public:
  MEMPOOL_CLASS_HELPERS();

  typedef mempool::mds_co::map<dentry_key_t, CDentry*> dentry_key_map;
  typedef mempool::mds_co::set<dentry_key_t> dentry_key_set;

  using fnode_ptr = std::shared_ptr<fnode_t>;
  using fnode_const_ptr = std::shared_ptr<const fnode_t>;

  template <typename ...Args>
  static fnode_ptr allocate_fnode(Args && ...args) {
    static mempool::mds_co::pool_allocator<fnode_t> allocator;
    return std::allocate_shared<fnode_t>(allocator, std::forward<Args>(args)...);
  }

  // -- freezing --
  struct freeze_tree_state_t {
    CDir *dir; // freezing/frozen tree root
    int auth_pins = 0;
    bool frozen = false;
    freeze_tree_state_t(CDir *d) : dir(d) {}
  };

  class scrub_info_t {
  public:
    MEMPOOL_CLASS_HELPERS();
    struct scrub_stamps {
      version_t version;
      utime_t time;
      scrub_stamps() : version(0) {}
      void operator=(const scrub_stamps &o) {
        version = o.version;
        time = o.time;
      }
    };

    scrub_info_t() :
      directory_scrubbing(false),
      need_scrub_local(false),
      last_scrub_dirty(false),
      pending_scrub_error(false) {}

    /// inodes we contain with dirty scrub stamps
    dentry_key_map dirty_scrub_stamps; // TODO: make use of this!

    scrub_stamps recursive_start; // when we last started a recursive scrub
    scrub_stamps last_recursive; // when we last finished a recursive scrub
    scrub_stamps last_local; // when we last did a local scrub

    bool directory_scrubbing; /// safety check
    bool need_scrub_local;
    bool last_scrub_dirty; /// is scrub info dirty or is it flushed to fnode?
    bool pending_scrub_error;

    /// these are lists of children in each stage of scrubbing
    dentry_key_set directories_to_scrub;
    dentry_key_set directories_scrubbing;
    dentry_key_set directories_scrubbed;
    dentry_key_set others_to_scrub;
    dentry_key_set others_scrubbing;
    dentry_key_set others_scrubbed;

    ScrubHeaderRefConst header;
  };

  // -- pins --
  static const int PIN_DNWAITER =     1;
  static const int PIN_INOWAITER =    2;
  static const int PIN_CHILD =        3;
  static const int PIN_FROZEN =       4;
  static const int PIN_SUBTREE =      5;
  static const int PIN_IMPORTING =    7;
  static const int PIN_IMPORTBOUND =  9;
  static const int PIN_EXPORTBOUND = 10;
  static const int PIN_STICKY =      11;
  static const int PIN_SUBTREETEMP = 12;  // used by MDCache::trim_non_auth()

  // -- state --
  static const unsigned STATE_COMPLETE =      (1<< 0);  // the complete contents are in cache
  static const unsigned STATE_FROZENTREE =    (1<< 1);  // root of tree (bounded by exports)
  static const unsigned STATE_FREEZINGTREE =  (1<< 2);  // in process of freezing
  static const unsigned STATE_FROZENDIR =     (1<< 3);
  static const unsigned STATE_FREEZINGDIR =   (1<< 4);
  static const unsigned STATE_COMMITTING =    (1<< 5);  // mid-commit
  static const unsigned STATE_FETCHING =      (1<< 6);  // currenting fetching
  static const unsigned STATE_CREATING =      (1<< 7);
  static const unsigned STATE_IMPORTBOUND =   (1<< 8);
  static const unsigned STATE_EXPORTBOUND =   (1<< 9);
  static const unsigned STATE_EXPORTING =     (1<<10);
  static const unsigned STATE_IMPORTING =     (1<<11);
  static const unsigned STATE_FRAGMENTING =   (1<<12);
  static const unsigned STATE_STICKY =        (1<<13);  // sticky pin due to inode stickydirs
  static const unsigned STATE_DNPINNEDFRAG =  (1<<14);  // dir is refragmenting
  static const unsigned STATE_ASSIMRSTAT =    (1<<15);  // assimilating inode->frag rstats
  static const unsigned STATE_DIRTYDFT =      (1<<16);  // dirty dirfragtree
  static const unsigned STATE_BADFRAG =       (1<<17);  // bad dirfrag
  static const unsigned STATE_TRACKEDBYOFT =  (1<<18);  // tracked by open file table
  static const unsigned STATE_AUXSUBTREE =    (1<<19);  // no subtree merge

  // common states
  static const unsigned STATE_CLEAN =  0;

  // these state bits are preserved by an import/export
  // ...except if the directory is hashed, in which case none of them are!
  static const unsigned MASK_STATE_EXPORTED = 
  (STATE_COMPLETE|STATE_DIRTY|STATE_DIRTYDFT|STATE_BADFRAG);
  static const unsigned MASK_STATE_IMPORT_KEPT = 
  (						  
   STATE_IMPORTING |
   STATE_IMPORTBOUND |
   STATE_EXPORTBOUND |
   STATE_FROZENTREE |
   STATE_STICKY |
   STATE_TRACKEDBYOFT);
  static const unsigned MASK_STATE_EXPORT_KEPT = 
  (STATE_EXPORTING |
   STATE_IMPORTBOUND |
   STATE_EXPORTBOUND |
   STATE_FROZENTREE |
   STATE_FROZENDIR |
   STATE_STICKY |
   STATE_TRACKEDBYOFT);
  static const unsigned MASK_STATE_FRAGMENT_KEPT = 
  (STATE_DIRTY |
   STATE_EXPORTBOUND |
   STATE_IMPORTBOUND |
   STATE_AUXSUBTREE |
   STATE_REJOINUNDEF);

  // -- rep spec --
  static const int REP_NONE =     0;
  static const int REP_ALL =      1;
  static const int REP_LIST =     2;

  static const unsigned EXPORT_NONCE  = 1;

  // -- wait masks --
  static const uint64_t WAIT_DENTRY       = (1<<0);  // wait for item to be in cache
  static const uint64_t WAIT_COMPLETE     = (1<<1);  // wait for complete dir contents
  static const uint64_t WAIT_FROZEN       = (1<<2);  // auth pins removed
  static const uint64_t WAIT_CREATED	  = (1<<3);  // new dirfrag is logged

  static const int WAIT_DNLOCK_OFFSET = 4;

  static const uint64_t WAIT_ANY_MASK = (uint64_t)(-1);
  static const uint64_t WAIT_ATSUBTREEROOT = (WAIT_SINGLEAUTH);

  // -- dump flags --
  static const int DUMP_PATH             = (1 << 0);
  static const int DUMP_DIRFRAG          = (1 << 1);
  static const int DUMP_SNAPID_FIRST     = (1 << 2);
  static const int DUMP_VERSIONS         = (1 << 3);
  static const int DUMP_REP              = (1 << 4);
  static const int DUMP_DIR_AUTH         = (1 << 5);
  static const int DUMP_STATES           = (1 << 6);
  static const int DUMP_MDS_CACHE_OBJECT = (1 << 7);
  static const int DUMP_ITEMS            = (1 << 8);
  static const int DUMP_ALL              = (-1);
  static const int DUMP_DEFAULT          = DUMP_ALL & (~DUMP_ITEMS);

  CDir(CInode *in, frag_t fg, MDCache *mdcache, bool auth);

  std::string_view pin_name(int p) const override {
    switch (p) {
    case PIN_DNWAITER: return "dnwaiter";
    case PIN_INOWAITER: return "inowaiter";
    case PIN_CHILD: return "child";
    case PIN_FROZEN: return "frozen";
    case PIN_SUBTREE: return "subtree";
    case PIN_IMPORTING: return "importing";
    case PIN_IMPORTBOUND: return "importbound";
    case PIN_EXPORTBOUND: return "exportbound";
    case PIN_STICKY: return "sticky";
    case PIN_SUBTREETEMP: return "subtreetemp";
    default: return generic_pin_name(p);
    }
  }

  bool is_lt(const MDSCacheObject *r) const override {
    return dirfrag() < (static_cast<const CDir*>(r))->dirfrag();
  }

  void resync_accounted_fragstat();
  void resync_accounted_rstat();
  void assimilate_dirty_rstat_inodes(MutationRef& mut);
  void assimilate_dirty_rstat_inodes_finish(EMetaBlob *blob);

  void mark_exporting() {
    state_set(CDir::STATE_EXPORTING);
    inode->num_exporting_dirs++;
  }
  void clear_exporting() {
    state_clear(CDir::STATE_EXPORTING);
    inode->num_exporting_dirs--;
  }

  version_t get_version() const { return fnode->version; }
  void update_projected_version() {
    ceph_assert(projected_fnode.empty());
    projected_version = fnode->version;
  }
  version_t get_projected_version() const { return projected_version; }

  void reset_fnode(fnode_const_ptr&& ptr) {
    fnode = std::move(ptr);
  }

  const fnode_const_ptr& get_fnode() const {
    return fnode;
  }

  // only used for updating newly allocated CDir
  fnode_t* _get_fnode() {
    if (fnode == empty_fnode)
      reset_fnode(allocate_fnode());
    return const_cast<fnode_t*>(fnode.get());
  }

  const fnode_const_ptr& get_projected_fnode() const {
    if (projected_fnode.empty())
      return fnode;
    else
      return projected_fnode.back();
  }

  // fnode should have already been projected in caller's context
  fnode_t* _get_projected_fnode() {
    ceph_assert(!projected_fnode.empty());
    return const_cast<fnode_t*>(projected_fnode.back().get());
  }

  fnode_ptr project_fnode(const MutationRef& mut);

  void pop_and_dirty_projected_fnode(LogSegment *ls, const MutationRef& mut);
  bool is_projected() const { return !projected_fnode.empty(); }
  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void _set_dirty_flag() {
    if (!state_test(STATE_DIRTY)) {
      state_set(STATE_DIRTY);
      get(PIN_DIRTY);
    }
  }
  void mark_dirty(LogSegment *ls, version_t pv=0);
  void mark_clean();

  bool is_new() { return item_new.is_on_list(); }
  void mark_new(LogSegment *ls);

  bool is_bad() { return state_test(STATE_BADFRAG); }

  /**
   * Call to start this CDir on a new scrub.
   * @pre It is not currently scrubbing
   * @pre The CDir is marked complete.
   * @post It has set up its internal scrubbing state.
   */
  void scrub_initialize(const ScrubHeaderRefConst& header);
  /**
   * Get the next dentry to scrub. Gives you a CDentry* and its meaning. This
   * function will give you all directory-representing dentries before any
   * others.
   * 0: success, you should scrub this CDentry right now
   * EAGAIN: is currently fetching the next CDentry into memory for you.
   *   It will activate your callback when done; try again when it does!
   * ENOENT: there are no remaining dentries to scrub
   * <0: There was an unexpected error
   *
   * @param cb An MDSContext which will be activated only if
   *   we return EAGAIN via rcode, or else ignored
   * @param dnout CDentry * which you should next scrub, or NULL
   * @returns a value as described above
   */
  int scrub_dentry_next(MDSContext *cb, CDentry **dnout);
  /**
   * Get the currently scrubbing dentries. When returned, the passed-in
   * list will be filled with all CDentry * which have been returned
   * from scrub_dentry_next() but not sent back via scrub_dentry_finished().
   */
  std::vector<CDentry*> scrub_dentries_scrubbing();
  /**
   * Report to the CDir that a CDentry has been scrubbed. Call this
   * for every CDentry returned from scrub_dentry_next().
   * @param dn The CDentry which has been scrubbed.
   */
  void scrub_dentry_finished(CDentry *dn);
  /**
   * Call this once all CDentries have been scrubbed, according to
   * scrub_dentry_next's listing. It finalizes the scrub statistics.
   */
  void scrub_finished();
  /**
   * Tell the CDir to do a local scrub of itself.
   * @pre The CDir is_complete().
   * @returns true if the rstats and directory contents match, false otherwise.
   */
  bool scrub_local();

  const scrub_info_t *scrub_info() const {
    if (!scrub_infop) {
      scrub_info_create();
    }
    return scrub_infop.get();
  }

  // -- accessors --
  inodeno_t ino()     const { return inode->ino(); }          // deprecate me?
  frag_t    get_frag()    const { return frag; }
  dirfrag_t dirfrag() const { return dirfrag_t(inode->ino(), frag); }

  CInode *get_inode()    { return inode; }
  const CInode *get_inode() const { return inode; }
  CDir *get_parent_dir() { return inode->get_parent_dir(); }

  dentry_key_map::iterator begin() { return items.begin(); }
  dentry_key_map::iterator end() { return items.end(); }
  dentry_key_map::iterator lower_bound(dentry_key_t key) { return items.lower_bound(key); }

  unsigned get_num_head_items() const { return num_head_items; }
  unsigned get_num_head_null() const { return num_head_null; }
  unsigned get_num_snap_items() const { return num_snap_items; }
  unsigned get_num_snap_null() const { return num_snap_null; }
  unsigned get_num_any() const { return num_head_items + num_head_null + num_snap_items + num_snap_null; }
  
  bool check_rstats(bool scrub=false);

  void inc_num_dirty() { num_dirty++; }
  void dec_num_dirty() { 
    ceph_assert(num_dirty > 0);
    num_dirty--; 
  }
  int get_num_dirty() const {
    return num_dirty;
  }

  void adjust_num_inodes_with_caps(int d);

  int64_t get_frag_size() const {
    return get_projected_fnode()->fragstat.size();
  }

  // -- dentries and inodes --
  CDentry* lookup_exact_snap(std::string_view dname, snapid_t last);
  CDentry* lookup(std::string_view n, snapid_t snap=CEPH_NOSNAP);

  CDentry* add_null_dentry(std::string_view dname,
			   snapid_t first=2, snapid_t last=CEPH_NOSNAP);
  CDentry* add_primary_dentry(std::string_view dname, CInode *in,
			      snapid_t first=2, snapid_t last=CEPH_NOSNAP);
  CDentry* add_remote_dentry(std::string_view dname, inodeno_t ino, unsigned char d_type,
			     snapid_t first=2, snapid_t last=CEPH_NOSNAP);
  void remove_dentry( CDentry *dn );         // delete dentry
  void link_remote_inode( CDentry *dn, inodeno_t ino, unsigned char d_type);
  void link_remote_inode( CDentry *dn, CInode *in );
  void link_primary_inode( CDentry *dn, CInode *in );
  void unlink_inode(CDentry *dn, bool adjust_lru=true);
  void try_remove_unlinked_dn(CDentry *dn);

  void add_to_bloom(CDentry *dn);
  bool is_in_bloom(std::string_view name);
  bool has_bloom() { return (bloom ? true : false); }
  void remove_bloom() {
    bloom.reset();
  }

  void try_remove_dentries_for_stray();
  bool try_trim_snap_dentry(CDentry *dn, const std::set<snapid_t>& snaps);

  void split(int bits, std::vector<CDir*>* subs, MDSContext::vec& waiters, bool replay);
  void merge(const std::vector<CDir*>& subs, MDSContext::vec& waiters, bool replay);

  bool should_split() const {
    return g_conf()->mds_bal_split_size > 0 &&
           (int)get_frag_size() > g_conf()->mds_bal_split_size;
  }
  bool should_split_fast() const;
  bool should_merge() const {
    return get_frag() != frag_t() &&
	   (int)get_frag_size() < g_conf()->mds_bal_merge_size;
  }

  mds_authority_t authority() const override;
  mds_authority_t get_dir_auth() const { return dir_auth; }
  void set_dir_auth(const mds_authority_t &a);
  void set_dir_auth(mds_rank_t a) { set_dir_auth(mds_authority_t(a, CDIR_AUTH_UNKNOWN)); }
  bool is_ambiguous_dir_auth() const {
    return dir_auth.second != CDIR_AUTH_UNKNOWN;
  }
  bool is_full_dir_auth() const {
    return is_auth() && !is_ambiguous_dir_auth();
  }
  bool is_full_dir_nonauth() const {
    return !is_auth() && !is_ambiguous_dir_auth();
  }
  
  bool is_subtree_root() const {
    return dir_auth != CDIR_AUTH_DEFAULT;
  }

  bool contains(CDir *x);  // true if we are x or an ancestor of x 

  // for giving to clients
  void get_dist_spec(std::set<mds_rank_t>& ls, mds_rank_t auth) {
    if (is_rep()) {
      list_replicas(ls);
      if (!ls.empty()) 
	ls.insert(auth);
    }
  }

  static void encode_dirstat(ceph::buffer::list& bl, const session_info_t& info, const DirStat& ds);

  void _encode_base(ceph::buffer::list& bl) {
    ENCODE_START(1, 1, bl);
    encode(first, bl);
    encode(*fnode, bl);
    encode(dir_rep, bl);
    encode(dir_rep_by, bl);
    ENCODE_FINISH(bl);
  }
  void _decode_base(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(first, p);
    {
      auto _fnode = allocate_fnode();
      decode(*_fnode, p);
      reset_fnode(std::move(_fnode));
    }
    decode(dir_rep, p);
    decode(dir_rep_by, p);
    DECODE_FINISH(p);
  }

  // -- state --
  bool is_complete() { return state & STATE_COMPLETE; }
  bool is_exporting() { return state & STATE_EXPORTING; }
  bool is_importing() { return state & STATE_IMPORTING; }
  bool is_dirty_dft() { return state & STATE_DIRTYDFT; }

  int get_dir_rep() const { return dir_rep; }
  bool is_rep() const { 
    if (dir_rep == REP_NONE) return false;
    return true;
  }
 
  // -- fetch --
  object_t get_ondisk_object() { 
    return file_object_t(ino(), frag);
  }
  void fetch(MDSContext *c, bool ignore_authpinnability=false);
  void fetch(MDSContext *c, std::string_view want_dn, bool ignore_authpinnability=false);
  void fetch(MDSContext *c, const std::set<dentry_key_t>& keys);

#if 0  // unused?
  void wait_for_commit(Context *c, version_t v=0);
#endif
  void commit_to(version_t want);
  void commit(version_t want, MDSContext *c,
	      bool ignore_authpinnability=false, int op_prio=-1);

  // -- dirtyness --
  version_t get_committing_version() const { return committing_version; }
  version_t get_committed_version() const { return committed_version; }
  void set_committed_version(version_t v) { committed_version = v; }

  void mark_complete();

  // -- reference counting --
  void first_get() override;
  void last_put() override;

  bool is_waiting_for_dentry(std::string_view dname, snapid_t snap) {
    return waiting_on_dentry.count(string_snap_t(dname, snap));
  }
  void add_dentry_waiter(std::string_view dentry, snapid_t snap, MDSContext *c);
  void take_dentry_waiting(std::string_view dentry, snapid_t first, snapid_t last, MDSContext::vec& ls);
  void take_sub_waiting(MDSContext::vec& ls);  // dentry or ino

  void add_waiter(uint64_t mask, MDSContext *c) override;
  void take_waiting(uint64_t mask, MDSContext::vec& ls) override;  // may include dentry waiters
  void finish_waiting(uint64_t mask, int result = 0);    // ditto

  // -- import/export --
  void encode_export(ceph::buffer::list& bl);
  void finish_export();
  void abort_export() {
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(ceph::buffer::list::const_iterator& blp, LogSegment *ls);
  void abort_import();

  // -- auth pins --
  bool can_auth_pin(int *err_ret=nullptr) const override;
  int get_auth_pins() const { return auth_pins; }
  int get_dir_auth_pins() const { return dir_auth_pins; }
  void auth_pin(void *who) override;
  void auth_unpin(void *who) override;

  void adjust_nested_auth_pins(int dirinc, void *by);
  void verify_fragstat();

  void _walk_tree(std::function<bool(CDir*)> cb);

  bool freeze_tree();
  void _freeze_tree();
  void unfreeze_tree();
  void adjust_freeze_after_rename(CDir *dir);

  bool freeze_dir();
  void _freeze_dir();
  void unfreeze_dir();

  void maybe_finish_freeze();

  std::pair<bool,bool> is_freezing_or_frozen_tree() const {
    if (freeze_tree_state) {
      if (freeze_tree_state->frozen)
	return std::make_pair(false, true);
      return std::make_pair(true, false);
    }
    return std::make_pair(false, false);
  }

  bool is_freezing() const override { return is_freezing_dir() || is_freezing_tree(); }
  bool is_freezing_tree() const {
    if (!num_freezing_trees)
      return false;
    return is_freezing_or_frozen_tree().first;
  }
  bool is_freezing_tree_root() const { return state & STATE_FREEZINGTREE; }
  bool is_freezing_dir() const { return state & STATE_FREEZINGDIR; }

  bool is_frozen() const override { return is_frozen_dir() || is_frozen_tree(); }
  bool is_frozen_tree() const {
    if (!num_frozen_trees)
      return false;
    return is_freezing_or_frozen_tree().second;
  }
  bool is_frozen_tree_root() const { return state & STATE_FROZENTREE; }
  bool is_frozen_dir() const { return state & STATE_FROZENDIR; }

  bool is_freezeable(bool freezing=false) const {
    // no nested auth pins.
    if (auth_pins - (freezing ? 1 : 0) > 0 ||
	(freeze_tree_state && freeze_tree_state->auth_pins != auth_pins))
      return false;

    // inode must not be frozen.
    if (!is_subtree_root() && inode->is_frozen())
      return false;

    return true;
  }

  bool is_freezeable_dir(bool freezing=false) const {
    if ((auth_pins - freezing) > 0 || dir_auth_pins > 0)
      return false;

    // if not subtree root, inode must not be frozen (tree--frozen_dir is okay).
    if (!is_subtree_root() && inode->is_frozen() && !inode->is_frozen_dir())
      return false;

    return true;
  }

  bool is_any_freezing_or_frozen_inode() const {
    return num_frozen_inodes || !freezing_inodes.empty();
  }
  bool is_auth_pinned_by_lock_cache() const {
    return frozen_inode_suppressed;
  }
  void disable_frozen_inode() {
    ceph_assert(num_frozen_inodes == 0);
    frozen_inode_suppressed++;
  }
  void enable_frozen_inode();

  std::ostream& print_db_line_prefix(std::ostream& out) override;
  void print(std::ostream& out) override;
  void dump(ceph::Formatter *f, int flags = DUMP_DEFAULT) const;
  void dump_load(ceph::Formatter *f);

  // context
  MDCache *cache;

  CInode *inode;  // my inode
  frag_t frag;   // my frag

  snapid_t first = 2;
  mempool::mds_co::compact_map<snapid_t,old_rstat_t> dirty_old_rstat;  // [value.first,key]

  // my inodes with dirty rstat data
  elist<CInode*> dirty_rstat_inodes;

  elist<CDentry*> dirty_dentries;
  elist<CDir*>::item item_dirty, item_new;

  // lock caches that auth-pin me
  elist<MDLockCache::DirItem*> lock_caches_with_auth_pins;

  // all dirfrags within freezing/frozen tree reference the 'state'
  std::shared_ptr<freeze_tree_state_t> freeze_tree_state;

protected:
  // friends
  friend class Migrator;
  friend class CInode;
  friend class MDCache;
  friend class MDiscover;
  friend class MDBalancer;

  friend class CDirDiscover;
  friend class CDirExport;
  friend class C_IO_Dir_TMAP_Fetched;
  friend class C_IO_Dir_OMAP_Fetched;
  friend class C_IO_Dir_OMAP_FetchedMore;
  friend class C_IO_Dir_Committed;

  void _omap_fetch(MDSContext *fin, const std::set<dentry_key_t>& keys);
  void _omap_fetch_more(
    ceph::buffer::list& hdrbl, std::map<std::string, ceph::buffer::list>& omap,
    MDSContext *fin);
  CDentry *_load_dentry(
      std::string_view key,
      std::string_view dname,
      snapid_t last,
      ceph::buffer::list &bl,
      int pos,
      const std::set<snapid_t> *snaps,
      double rand_threshold,
      bool *force_dirty);

  /**
   * Go bad due to a damaged dentry (register with damagetable and go BADFRAG)
   */
  void go_bad_dentry(snapid_t last, std::string_view dname);

  /**
   * Go bad due to a damaged header (register with damagetable and go BADFRAG)
   */
  void go_bad(bool complete);

  void _omap_fetched(ceph::buffer::list& hdrbl, std::map<std::string, ceph::buffer::list>& omap,
		     bool complete, int r);

  // -- commit --
  void _commit(version_t want, int op_prio);
  void _omap_commit(int op_prio);
  void _encode_dentry(CDentry *dn, ceph::buffer::list& bl, const std::set<snapid_t> *snaps);
  void _committed(int r, version_t v);

  static fnode_const_ptr empty_fnode;
  // fnode is a pointer to constant fnode_t, the constant fnode_t can be shared
  // by CDir and log events. To update fnode, read-copy-update should be used.

  fnode_const_ptr fnode = empty_fnode;

  version_t projected_version = 0;
  mempool::mds_co::list<fnode_const_ptr> projected_fnode;

  std::unique_ptr<scrub_info_t> scrub_infop;

  // contents of this directory
  dentry_key_map items;       // non-null AND null
  unsigned num_head_items = 0;
  unsigned num_head_null = 0;
  unsigned num_snap_items = 0;
  unsigned num_snap_null = 0;

  int num_dirty = 0;

  int num_inodes_with_caps = 0;

  // state
  version_t committing_version = 0;
  version_t committed_version = 0;

  mempool::mds_co::compact_set<mempool::mds_co::string> stale_items;

  // lock nesting, freeze
  static int num_frozen_trees;
  static int num_freezing_trees;

  // freezing/frozen inodes in this dirfrag
  int num_frozen_inodes = 0;
  int frozen_inode_suppressed = 0;
  elist<CInode*> freezing_inodes;

  int dir_auth_pins = 0;

  // cache control  (defined for authority; hints for replicas)
  __s32      dir_rep;
  mempool::mds_co::compact_set<__s32> dir_rep_by;      // if dir_rep == REP_LIST

  // popularity
  dirfrag_load_vec_t pop_me;
  dirfrag_load_vec_t pop_nested;
  dirfrag_load_vec_t pop_auth_subtree;
  dirfrag_load_vec_t pop_auth_subtree_nested;

  ceph::coarse_mono_time last_popularity_sample = ceph::coarse_mono_clock::zero();

  load_spread_t pop_spread;

  elist<CInode*> pop_lru_subdirs;

  std::unique_ptr<bloom_filter> bloom; // XXX not part of mempool::mds_co
  /* If you set up the bloom filter, you must keep it accurate!
   * It's deleted when you mark_complete() and is deliberately not serialized.*/

  mempool::mds_co::compact_set<mempool::mds_co::string> wanted_items;
  mempool::mds_co::compact_map<version_t, MDSContext::vec_alloc<mempool::mds_co::pool_allocator> > waiting_for_commit;

  // -- waiters --
  mempool::mds_co::compact_map< string_snap_t, MDSContext::vec_alloc<mempool::mds_co::pool_allocator> > waiting_on_dentry; // FIXME string_snap_t not in mempool

private:
  friend std::ostream& operator<<(std::ostream& out, const class CDir& dir);

  void log_mark_dirty();

  /**
   * Create a scrub_info_t struct for the scrub_infop pointer.
   */
  void scrub_info_create() const;
  /**
   * Delete the scrub_infop if it's not got any useful data.
   */
  void scrub_maybe_delete_info();
  /**
   * Check the given set (presumably one of those in scrub_info_t) for the
   * next key to scrub and look it up (or fail!).
   */
  int _next_dentry_on_set(dentry_key_set &dns, bool missing_okay,
                          MDSContext *cb, CDentry **dnout);

  void link_inode_work( CDentry *dn, CInode *in );
  void unlink_inode_work( CDentry *dn );
  void remove_null_dentries();
  void purge_stale_snap_data(const std::set<snapid_t>& snaps);

  void prepare_new_fragment(bool replay);
  void prepare_old_fragment(std::map<string_snap_t, MDSContext::vec >& dentry_waiters, bool replay);
  void steal_dentry(CDentry *dn);  // from another dir.  used by merge/split.
  void finish_old_fragment(MDSContext::vec& waiters, bool replay);
  void init_fragment_pins();
  std::string get_path() const;

  // -- authority --
  /*
   *     normal: <parent,unknown>   !subtree_root
   * delegation: <mds,unknown>       subtree_root
   *  ambiguous: <mds1,mds2>         subtree_root
   *             <parent,mds2>       subtree_root
   */
  mds_authority_t dir_auth;
};

#endif
