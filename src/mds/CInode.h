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

#ifndef CEPH_CINODE_H
#define CEPH_CINODE_H

#include <list>
#include <map>
#include <set>
#include <string_view>

#include "common/config.h"
#include "common/RefCountedObj.h"
#include "include/compat.h"
#include "include/counter.h"
#include "include/elist.h"
#include "include/types.h"
#include "include/lru.h"
#include "include/compact_set.h"

#include "MDSCacheObject.h"
#include "MDSContext.h"
#include "flock.h"

#include "BatchOp.h"
#include "CDentry.h"
#include "SimpleLock.h"
#include "ScatterLock.h"
#include "LocalLockC.h"
#include "Capability.h"
#include "SnapRealm.h"
#include "Mutation.h"

#include "messages/MClientCaps.h"

#define dout_context g_ceph_context

class Context;
class CDir;
class CInode;
class MDCache;
class LogSegment;
struct SnapRealm;
class Session;
struct ObjectOperation;
class EMetaBlob;

struct cinode_lock_info_t {
  int lock;
  int wr_caps;
};

struct CInodeCommitOperation {
public:
  CInodeCommitOperation(int prio, int64_t po)
    : pool(po), priority(prio) {
  }
  CInodeCommitOperation(int prio, int64_t po, file_layout_t l, uint64_t f, std::string_view s)
    : pool(po), priority(prio), _layout(l), _features(f), _symlink(s) {
      update_layout_symlink = true;
  }

  void update(ObjectOperation &op, inode_backtrace_t &bt);
  int64_t get_pool() { return pool; }

private:
  int64_t pool;     ///< pool id
  int priority;
  bool update_layout_symlink = false;
  file_layout_t _layout;
  uint64_t _features;
  std::string_view _symlink;
};

struct CInodeCommitOperations {
  std::vector<CInodeCommitOperation> ops_vec;
  inode_backtrace_t bt;
  version_t version;
  CInode *in;
};

/**
 * Base class for CInode, containing the backing store data and
 * serialization methods.  This exists so that we can read and
 * handle CInodes from the backing store without hitting all
 * the business logic in CInode proper.
 */
class InodeStoreBase {
public:
  using mempool_inode = inode_t<mempool::mds_co::pool_allocator>;
  using inode_ptr = std::shared_ptr<mempool_inode>;
  using inode_const_ptr = std::shared_ptr<const mempool_inode>;

  template <typename ...Args>
  static inode_ptr allocate_inode(Args && ...args) {
    static mempool::mds_co::pool_allocator<mempool_inode> allocator;
    return std::allocate_shared<mempool_inode>(allocator, std::forward<Args>(args)...);
  }
  
  using mempool_xattr_map = xattr_map<mempool::mds_co::pool_allocator>; // FIXME bufferptr not in mempool
  using xattr_map_ptr = std::shared_ptr<mempool_xattr_map>;
  using xattr_map_const_ptr = std::shared_ptr<const mempool_xattr_map>;

  template <typename ...Args>
  static xattr_map_ptr allocate_xattr_map(Args && ...args) {
    static mempool::mds_co::pool_allocator<mempool_xattr_map> allocator;
    return std::allocate_shared<mempool_xattr_map>(allocator, std::forward<Args>(args)...);
  }

  using mempool_old_inode = old_inode_t<mempool::mds_co::pool_allocator>;
  using mempool_old_inode_map = mempool::mds_co::map<snapid_t, mempool_old_inode>;
  using old_inode_map_ptr = std::shared_ptr<mempool_old_inode_map>;
  using old_inode_map_const_ptr = std::shared_ptr<const mempool_old_inode_map>;

  template <typename ...Args>
  static old_inode_map_ptr allocate_old_inode_map(Args && ...args) {
    static mempool::mds_co::pool_allocator<mempool_old_inode_map> allocator;
    return std::allocate_shared<mempool_old_inode_map>(allocator, std::forward<Args>(args)...);
  }

  void reset_inode(inode_const_ptr&& ptr) {
    inode = std::move(ptr);
  }

  void reset_xattrs(xattr_map_const_ptr&& ptr) {
    xattrs = std::move(ptr);
  }

  void reset_old_inodes(old_inode_map_const_ptr&& ptr) {
    old_inodes = std::move(ptr);
  }

  void encode_xattrs(bufferlist &bl) const;
  void decode_xattrs(bufferlist::const_iterator &p);
  void encode_old_inodes(bufferlist &bl, uint64_t features) const;
  void decode_old_inodes(bufferlist::const_iterator &p);

  /* Helpers */
  static object_t get_object_name(inodeno_t ino, frag_t fg, std::string_view suffix);

  /* Full serialization for use in ".inode" root inode objects */
  void encode(ceph::buffer::list &bl, uint64_t features, const ceph::buffer::list *snap_blob=NULL) const;
  void decode(ceph::buffer::list::const_iterator &bl, ceph::buffer::list& snap_blob);

  /* Serialization without ENCODE_START/FINISH blocks for use embedded in dentry */
  void encode_bare(ceph::buffer::list &bl, uint64_t features, const ceph::buffer::list *snap_blob=NULL) const;
  void decode_bare(ceph::buffer::list::const_iterator &bl, ceph::buffer::list &snap_blob, __u8 struct_v=5);

  /* For test/debug output */
  void dump(ceph::Formatter *f) const;

  void decode_json(JSONObj *obj);
  static void xattrs_cb(InodeStoreBase::mempool_xattr_map& c, JSONObj *obj);
  static void old_indoes_cb(InodeStoreBase::mempool_old_inode_map& c, JSONObj *obj);
  
  /* For use by offline tools */
  __u32 hash_dentry_name(std::string_view dn);
  frag_t pick_dirfrag(std::string_view dn);

  mempool::mds_co::string	symlink;      // symlink dest, if symlink
  fragtree_t			dirfragtree;  // dir frag tree, if any.  always consistent with our dirfrag map.
  snapid_t 			oldest_snap = CEPH_NOSNAP;
  damage_flags_t 		damage_flags = 0;

protected:
  static inode_const_ptr	empty_inode;

  // Following members are pointers to constant data, the constant data can
  // be shared by CInode and log events. To update these members in CInode,
  // read-copy-update should be used.
  inode_const_ptr		inode = empty_inode;
  xattr_map_const_ptr		xattrs;
  old_inode_map_const_ptr	old_inodes;   // key = last, value.first = first
};

inline void decode_noshare(InodeStoreBase::mempool_xattr_map& xattrs,
                          ceph::buffer::list::const_iterator &p)
{
  decode_noshare<mempool::mds_co::pool_allocator>(xattrs, p);
}

class InodeStore : public InodeStoreBase {
public:
  mempool_inode* get_inode() {
    if (inode == empty_inode)
      reset_inode(allocate_inode());
    return const_cast<mempool_inode*>(inode.get());
  }
  mempool_xattr_map* get_xattrs() { return const_cast<mempool_xattr_map*>(xattrs.get()); }

  void encode(ceph::buffer::list &bl, uint64_t features) const {
    InodeStoreBase::encode(bl, features, &snap_blob);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    InodeStoreBase::decode(bl, snap_blob);
  }
  void encode_bare(ceph::buffer::list &bl, uint64_t features) const {
    InodeStoreBase::encode_bare(bl, features, &snap_blob);
  }
  void decode_bare(ceph::buffer::list::const_iterator &bl) {
    InodeStoreBase::decode_bare(bl, snap_blob);
  }

  static void generate_test_instances(std::list<InodeStore*>& ls);

  using InodeStoreBase::inode;
  using InodeStoreBase::xattrs;
  using InodeStoreBase::old_inodes;

  // FIXME bufferlist not part of mempool
  ceph::buffer::list snap_blob;  // Encoded copy of SnapRealm, because we can't
			 // rehydrate it without full MDCache
};
WRITE_CLASS_ENCODER_FEATURES(InodeStore)

// just for ceph-dencoder
class InodeStoreBare : public InodeStore {
public:
  void encode(ceph::buffer::list &bl, uint64_t features) const {
    InodeStore::encode_bare(bl, features);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    InodeStore::decode_bare(bl);
  }
  static void generate_test_instances(std::list<InodeStoreBare*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(InodeStoreBare)

// cached inode wrapper
class CInode : public MDSCacheObject, public InodeStoreBase, public Counter<CInode> {
 public:
  MEMPOOL_CLASS_HELPERS();

  using mempool_cap_map = mempool::mds_co::map<client_t, Capability>;
  /**
   * @defgroup Scrubbing and fsck
   */

  /**
   * Report the results of validation against a particular inode.
   * Each member is a pair of bools.
   * <member>.first represents if validation was performed against the member.
   * <member.second represents if the member passed validation.
   * performed_validation is set to true if the validation was actually
   * run. It might not be run if, for instance, the inode is marked as dirty.
   * passed_validation is set to true if everything that was checked
   * passed its validation.
   */
  struct validated_data {
    template<typename T>struct member_status {
      bool checked = false;
      bool passed = false;
      bool repaired = false;
      int ondisk_read_retval = 0;
      T ondisk_value;
      T memory_value;
      std::stringstream error_str;
    };

    struct raw_stats_t {
      frag_info_t dirstat;
      nest_info_t rstat;
    };

    validated_data() {}

    void dump(ceph::Formatter *f) const;

    bool all_damage_repaired() const;

    bool performed_validation = false;
    bool passed_validation = false;

    member_status<inode_backtrace_t> backtrace;
    member_status<mempool_inode> inode; // XXX should not be in mempool; wait for pmr
    member_status<raw_stats_t> raw_stats;
  };

  // friends
  friend class Server;
  friend class Locker;
  friend class Migrator;
  friend class MDCache;
  friend class StrayManager;
  friend class CDir;
  friend std::ostream& operator<<(std::ostream&, const CInode&);

  class scrub_info_t {
  public:
    scrub_info_t() {}

    version_t last_scrub_version = 0;
    utime_t last_scrub_stamp;

    bool last_scrub_dirty = false; /// are our stamps dirty with respect to disk state?
    bool scrub_in_progress = false; /// are we currently scrubbing?

    fragset_t queued_frags;

    ScrubHeaderRef header;
  };

  // -- pins --
  static const int PIN_DIRFRAG =         -1; 
  static const int PIN_CAPS =             2;  // client caps
  static const int PIN_IMPORTING =       -4;  // importing
  static const int PIN_OPENINGDIR =       7;
  static const int PIN_REMOTEPARENT =     8;
  static const int PIN_BATCHOPENJOURNAL = 9;
  static const int PIN_SCATTERED =        10;
  static const int PIN_STICKYDIRS =       11;
  //static const int PIN_PURGING =         -12;	
  static const int PIN_FREEZING =         13;
  static const int PIN_FROZEN =           14;
  static const int PIN_IMPORTINGCAPS =   -15;
  static const int PIN_PASTSNAPPARENT =  -16;
  static const int PIN_OPENINGSNAPPARENTS = 17;
  static const int PIN_TRUNCATING =       18;
  static const int PIN_STRAY =            19;  // we pin our stray inode while active
  static const int PIN_NEEDSNAPFLUSH =    20;
  static const int PIN_DIRTYRSTAT =       21;
  static const int PIN_EXPORTINGCAPS =    22;
  static const int PIN_DIRTYPARENT =      23;
  static const int PIN_DIRWAITER =        24;

  // -- dump flags --
  static const int DUMP_INODE_STORE_BASE = (1 << 0);
  static const int DUMP_MDS_CACHE_OBJECT = (1 << 1);
  static const int DUMP_LOCKS =            (1 << 2);
  static const int DUMP_STATE =            (1 << 3);
  static const int DUMP_CAPS =             (1 << 4);
  static const int DUMP_PATH =             (1 << 5);
  static const int DUMP_DIRFRAGS =         (1 << 6);
  static const int DUMP_ALL =              (-1);
  static const int DUMP_DEFAULT = DUMP_ALL & (~DUMP_PATH) & (~DUMP_DIRFRAGS);

  // -- state --
  static const int STATE_EXPORTING 		= (1<<0);   // on nonauth bystander.
  static const int STATE_OPENINGDIR		= (1<<1);
  static const int STATE_FREEZING		= (1<<2);
  static const int STATE_FROZEN			= (1<<3);
  static const int STATE_AMBIGUOUSAUTH		= (1<<4);
  static const int STATE_EXPORTINGCAPS		= (1<<5);
  static const int STATE_NEEDSRECOVER		= (1<<6);
  static const int STATE_RECOVERING		= (1<<7);
  static const int STATE_PURGING		= (1<<8);
  static const int STATE_DIRTYPARENT		= (1<<9);
  static const int STATE_DIRTYRSTAT		= (1<<10);
  static const int STATE_STRAYPINNED		= (1<<11);
  static const int STATE_FROZENAUTHPIN		= (1<<12);
  static const int STATE_DIRTYPOOL		= (1<<13);
  static const int STATE_REPAIRSTATS		= (1<<14);
  static const int STATE_MISSINGOBJS		= (1<<15);
  static const int STATE_EVALSTALECAPS		= (1<<16);
  static const int STATE_QUEUEDEXPORTPIN	= (1<<17);
  static const int STATE_TRACKEDBYOFT		= (1<<18);  // tracked by open file table
  static const int STATE_DELAYEDEXPORTPIN	= (1<<19);
  static const int STATE_DISTEPHEMERALPIN       = (1<<20);
  static const int STATE_RANDEPHEMERALPIN       = (1<<21);
  static const int STATE_CLIENTWRITEABLE	= (1<<22);

  // orphan inode needs notification of releasing reference
  static const int STATE_ORPHAN =	STATE_NOTIFYREF;

  static const int MASK_STATE_EXPORTED =
    (STATE_DIRTY|STATE_NEEDSRECOVER|STATE_DIRTYPARENT|STATE_DIRTYPOOL|
    STATE_DISTEPHEMERALPIN|STATE_RANDEPHEMERALPIN);
  static const int MASK_STATE_EXPORT_KEPT =
    (STATE_FROZEN|STATE_AMBIGUOUSAUTH|STATE_EXPORTINGCAPS|
     STATE_QUEUEDEXPORTPIN|STATE_TRACKEDBYOFT|STATE_DELAYEDEXPORTPIN|
     STATE_DISTEPHEMERALPIN|STATE_RANDEPHEMERALPIN);

  /* These are for "permanent" state markers that are passed around between
   * MDS. Nothing protects/updates it like a typical MDS lock.
   *
   * Currently, we just use this for REPLICATED inodes. The reason we need to
   * replicate the random epin state is because the directory inode is still
   * under the authority of the parent subtree. So it's not exported normally
   * and we can't pass around the state that way. The importer of the dirfrags
   * still needs to know that the inode is random pinned though otherwise it
   * doesn't know that the dirfrags are pinned.
   */
  static const int MASK_STATE_REPLICATED = STATE_RANDEPHEMERALPIN;

  // -- waiters --
  static const uint64_t WAIT_DIR         = (1<<0);
  static const uint64_t WAIT_FROZEN      = (1<<1);
  static const uint64_t WAIT_TRUNC       = (1<<2);
  static const uint64_t WAIT_FLOCK       = (1<<3);
  
  static const uint64_t WAIT_ANY_MASK	= (uint64_t)(-1);

  // misc
  static const unsigned EXPORT_NONCE = 1; // nonce given to replicas created by export

  // ---------------------------
  CInode() = delete;
  CInode(MDCache *c, bool auth=true, snapid_t f=2, snapid_t l=CEPH_NOSNAP);
  ~CInode() override {
    close_dirfrags();
    close_snaprealm();
    clear_file_locks();
    ceph_assert(num_projected_srnodes == 0);
    ceph_assert(num_caps_notable == 0);
    ceph_assert(num_subtree_roots == 0);
    ceph_assert(num_exporting_dirs == 0);
    ceph_assert(batch_ops.empty());
  }

  std::map<int, std::unique_ptr<BatchOp>> batch_ops;

  std::string_view pin_name(int p) const override;

  std::ostream& print_db_line_prefix(std::ostream& out) const override;

  const scrub_info_t *scrub_info() const {
    if (!scrub_infop)
      scrub_info_create();
    return scrub_infop.get();
  }

  const ScrubHeaderRef& get_scrub_header() {
    static const ScrubHeaderRef nullref;
    return scrub_infop ? scrub_infop->header : nullref;
  }

  bool scrub_is_in_progress() const {
    return (scrub_infop && scrub_infop->scrub_in_progress);
  }
  /**
   * Start scrubbing on this inode. That could be very short if it's
   * a file, or take a long time if we're recursively scrubbing a directory.
   * @pre It is not currently scrubbing
   * @post it has set up internal scrubbing state
   * @param scrub_version What version are we scrubbing at (usually, parent
   * directory's get_projected_version())
   */
  void scrub_initialize(ScrubHeaderRef& header);
  /**
   * Call this once the scrub has been completed, whether it's a full
   * recursive scrub on a directory or simply the data on a file (or
   * anything in between).
   * @param c An out param which is filled in with a Context* that must
   * be complete()ed.
   */
  void scrub_finished();

  void scrub_aborted();

  fragset_t& scrub_queued_frags() {
    ceph_assert(scrub_infop);
    return scrub_infop->queued_frags;
  }

  bool is_multiversion() const {
    return snaprealm ||  // other snaprealms will link to me
      get_inode()->is_dir() ||  // links to me in other snaps
      get_inode()->nlink > 1 || // there are remote links, possibly snapped, that will need to find me
      is_any_old_inodes(); // once multiversion, always multiversion.  until old_inodes gets cleaned out.
  }
  snapid_t get_oldest_snap();

  bool is_dirty_rstat() {
    return state_test(STATE_DIRTYRSTAT);
  }
  void mark_dirty_rstat();
  void clear_dirty_rstat();

  //bool hack_accessed = false;
  //utime_t hack_load_stamp;

  /**
   * Projection methods, used to store inode changes until they have been journaled,
   * at which point they are popped.
   * Usage:
   * project_inode as needed. If you're changing xattrs or sr_t, then pass true
   * as needed then change the xattrs/snapnode member as needed. (Dirty
   * exception: project_past_snaprealm_parent allows you to project the
   * snapnode after doing project_inode (i.e. you don't need to pass
   * snap=true).
   *
   * Then, journal. Once journaling is done, pop_and_dirty_projected_inode.
   * This function will take care of the inode itself, the xattrs, and the snaprealm.
   */

  struct projected_inode {
    static sr_t* const UNDEF_SRNODE;

    inode_ptr const inode;
    xattr_map_ptr const xattrs;
    sr_t* const snapnode;

    projected_inode() = delete;
    explicit projected_inode(inode_ptr&& i, xattr_map_ptr&& x, sr_t *s=nullptr) :
      inode(std::move(i)), xattrs(std::move(x)), snapnode(s) {}
  };
  projected_inode project_inode(const MutationRef& mut,
				bool xattr = false, bool snap = false);

  void pop_and_dirty_projected_inode(LogSegment *ls, const MutationRef& mut);

  version_t get_projected_version() const {
    if (projected_nodes.empty())
      return get_inode()->version;
    else
      return projected_nodes.back().inode->version;
  }
  bool is_projected() const {
    return !projected_nodes.empty();
  }

  const inode_const_ptr& get_projected_inode() const {
    if (projected_nodes.empty())
      return get_inode();
    else
      return projected_nodes.back().inode;
  }
  // inode should have already been projected in caller's context
  mempool_inode* _get_projected_inode() {
    ceph_assert(!projected_nodes.empty());
    return const_cast<mempool_inode*>(projected_nodes.back().inode.get());
  }
  const inode_const_ptr& get_previous_projected_inode() const {
    ceph_assert(!projected_nodes.empty());
    auto it = projected_nodes.rbegin();
    ++it;
    if (it != projected_nodes.rend())
      return it->inode;
    else
      return get_inode();
  }

  const xattr_map_const_ptr& get_projected_xattrs() {
    if (projected_nodes.empty())
      return xattrs;
    else
      return projected_nodes.back().xattrs;
  }
  const xattr_map_const_ptr& get_previous_projected_xattrs() {
    ceph_assert(!projected_nodes.empty());
    auto it = projected_nodes.rbegin();
    ++it;
    if (it != projected_nodes.rend())
      return it->xattrs;
    else
      return xattrs;
  }

  sr_t *prepare_new_srnode(snapid_t snapid);
  void project_snaprealm(sr_t *new_srnode);
  sr_t *project_snaprealm(snapid_t snapid=0) {
    sr_t* new_srnode = prepare_new_srnode(snapid);
    project_snaprealm(new_srnode);
    return new_srnode;
  }
  const sr_t *get_projected_srnode() const;

  void mark_snaprealm_global(sr_t *new_srnode);
  void clear_snaprealm_global(sr_t *new_srnode);
  bool is_projected_snaprealm_global() const;

  void record_snaprealm_past_parent(sr_t *new_snap, SnapRealm *newparent);
  void record_snaprealm_parent_dentry(sr_t *new_snap, SnapRealm *newparent,
				      CDentry *dn, bool primary_dn);
  void project_snaprealm_past_parent(SnapRealm *newparent);
  void early_pop_projected_snaprealm();

  const mempool_old_inode& cow_old_inode(snapid_t follows, bool cow_head);
  void split_old_inode(snapid_t snap);
  snapid_t pick_old_inode(snapid_t last) const;
  void pre_cow_old_inode();
  bool has_snap_data(snapid_t s);
  void purge_stale_snap_data(const std::set<snapid_t>& snaps);

  size_t get_num_dirfrags() const { return dirfrags.size(); }
  CDir* get_dirfrag(frag_t fg) {
    auto pi = dirfrags.find(fg);
    if (pi != dirfrags.end()) {
      //assert(g_conf()->debug_mds < 2 || dirfragtree.is_leaf(fg)); // performance hack FIXME
      return pi->second;
    } 
    return NULL;
  }
  std::pair<bool, std::vector<CDir*>> get_dirfrags_under(frag_t fg);
  CDir* get_approx_dirfrag(frag_t fg);

  template<typename Container>
  void get_dirfrags(Container& ls) const {
    // all dirfrags
    if constexpr (std::is_same_v<Container, std::vector<CDir*>>)
      ls.reserve(ls.size() + dirfrags.size());
    for (const auto &p : dirfrags)
      ls.push_back(p.second);
  }

  auto get_dirfrags() const {
    std::vector<CDir*> result;
    get_dirfrags(result);
    return result;
  }

  void get_nested_dirfrags(std::vector<CDir*>&) const;
  std::vector<CDir*> get_nested_dirfrags() const {
    std::vector<CDir*> v;
    get_nested_dirfrags(v);
    return v;
  }
  void get_subtree_dirfrags(std::vector<CDir*>&) const;
  std::vector<CDir*> get_subtree_dirfrags() const {
    std::vector<CDir*> v;
    get_subtree_dirfrags(v);
    return v;
  }
  int get_num_subtree_roots() const {
    return num_subtree_roots;
  }

  CDir *get_or_open_dirfrag(MDCache *mdcache, frag_t fg);
  CDir *add_dirfrag(CDir *dir);
  void close_dirfrag(frag_t fg);
  void close_dirfrags();
  bool has_subtree_root_dirfrag(int auth=-1);
  bool has_subtree_or_exporting_dirfrag();

  void force_dirfrags();
  void verify_dirfrags();

  void get_stickydirs();
  void put_stickydirs();

  void add_need_snapflush(CInode *snapin, snapid_t snapid, client_t client);
  void remove_need_snapflush(CInode *snapin, snapid_t snapid, client_t client);
  std::pair<bool,bool> split_need_snapflush(CInode *cowin, CInode *in);

  // -- accessors --

  inodeno_t ino() const { return get_inode()->ino; }
  vinodeno_t vino() const { return vinodeno_t(ino(), last); }
  int d_type() const { return IFTODT(get_inode()->mode); }
  bool is_root() const { return ino() == CEPH_INO_ROOT; }
  bool is_stray() const { return MDS_INO_IS_STRAY(ino()); }
  mds_rank_t get_stray_owner() const {
    return (mds_rank_t)MDS_INO_STRAY_OWNER(ino());
  }
  bool is_mdsdir() const { return MDS_INO_IS_MDSDIR(ino()); }
  bool is_base() const { return MDS_INO_IS_BASE(ino()); }
  bool is_system() const { return ino() < MDS_INO_SYSTEM_BASE; }
  bool is_lost_and_found() const { return ino() == CEPH_INO_LOST_AND_FOUND; }
  bool is_normal() const { return !(is_base() || is_system() || is_stray()); }
  bool is_file() const    { return get_inode()->is_file(); }
  bool is_symlink() const { return get_inode()->is_symlink(); }
  bool is_dir() const     { return get_inode()->is_dir(); }

  bool is_head() const { return last == CEPH_NOSNAP; }

  // note: this overloads MDSCacheObject
  bool is_ambiguous_auth() const {
    return state_test(STATE_AMBIGUOUSAUTH) ||
      MDSCacheObject::is_ambiguous_auth();
  }
  void set_ambiguous_auth() {
    state_set(STATE_AMBIGUOUSAUTH);
  }
  void clear_ambiguous_auth(MDSContext::vec& finished);
  void clear_ambiguous_auth();

  const inode_const_ptr& get_inode() const {
    return inode;
  }

  // only used for updating newly allocated CInode
  mempool_inode* _get_inode() {
    if (inode == empty_inode)
      reset_inode(allocate_inode());
    return const_cast<mempool_inode*>(inode.get());
  }

  const xattr_map_const_ptr& get_xattrs() const { return xattrs; }

  bool is_any_old_inodes() const { return old_inodes && !old_inodes->empty(); }
  const old_inode_map_const_ptr& get_old_inodes() const { return old_inodes; }

  CDentry* get_parent_dn() { return parent; }
  const CDentry* get_parent_dn() const { return parent; }
  CDentry* get_projected_parent_dn() { return !projected_parent.empty() ? projected_parent.back() : parent; }
  const CDentry* get_projected_parent_dn() const { return !projected_parent.empty() ? projected_parent.back() : parent; }
  const CDentry* get_oldest_parent_dn() const {
    if (parent)
      return parent;
    return !projected_parent.empty() ? projected_parent.front(): NULL;
  }
  CDir *get_parent_dir();
  const CDir *get_projected_parent_dir() const;
  CDir *get_projected_parent_dir();
  CInode *get_parent_inode();
  
  bool is_lt(const MDSCacheObject *r) const override {
    const CInode *o = static_cast<const CInode*>(r);
    return ino() < o->ino() ||
      (ino() == o->ino() && last < o->last);
  }

  // -- misc -- 
  bool is_ancestor_of(const CInode *other) const;
  bool is_projected_ancestor_of(const CInode *other) const;

  void make_path_string(std::string& s, bool projected=false, const CDentry *use_parent=NULL) const;
  void make_path(filepath& s, bool projected=false) const;
  void name_stray_dentry(std::string& dname);
  
  // -- dirtyness --
  version_t get_version() const { return get_inode()->version; }

  version_t pre_dirty();
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(LogSegment *ls);
  void mark_clean();

  void store(MDSContext *fin);
  void _stored(int r, version_t cv, Context *fin);
  /**
   * Flush a CInode to disk. This includes the backtrace, the parent
   * directory's link, and the Inode object itself (if a base directory).
   * @pre is_auth() on both the inode and its containing directory
   * @pre can_auth_pin()
   * @param fin The Context to call when the flush is completed.
   */
  void flush(MDSContext *fin);
  void fetch(MDSContext *fin);
  void _fetched(ceph::buffer::list& bl, ceph::buffer::list& bl2, Context *fin);  

  void _commit_ops(int r, C_GatherBuilder &gather_bld,
                   std::vector<CInodeCommitOperation> &ops_vec,
                   inode_backtrace_t &bt);
  void build_backtrace(int64_t pool, inode_backtrace_t& bt);
  void _store_backtrace(std::vector<CInodeCommitOperation> &ops_vec,
                        inode_backtrace_t &bt, int op_prio);
  void store_backtrace(CInodeCommitOperations &op, int op_prio);
  void store_backtrace(MDSContext *fin, int op_prio=-1);
  void _stored_backtrace(int r, version_t v, Context *fin);
  void fetch_backtrace(Context *fin, ceph::buffer::list *backtrace);

  void mark_dirty_parent(LogSegment *ls, bool dirty_pool=false);
  void clear_dirty_parent();
  void verify_diri_backtrace(ceph::buffer::list &bl, int err);
  bool is_dirty_parent() { return state_test(STATE_DIRTYPARENT); }
  bool is_dirty_pool() { return state_test(STATE_DIRTYPOOL); }

  void encode_snap_blob(ceph::buffer::list &bl);
  void decode_snap_blob(const ceph::buffer::list &bl);
  void encode_store(ceph::buffer::list& bl, uint64_t features);
  void decode_store(ceph::buffer::list::const_iterator& bl);

  void add_dir_waiter(frag_t fg, MDSContext *c);
  void take_dir_waiting(frag_t fg, MDSContext::vec& ls);
  bool is_waiting_for_dir(frag_t fg) {
    return waiting_on_dir.count(fg);
  }
  void add_waiter(uint64_t tag, MDSContext *c) override;
  void take_waiting(uint64_t tag, MDSContext::vec& ls) override;

  // -- encode/decode helpers --
  void _encode_base(ceph::buffer::list& bl, uint64_t features);
  void _decode_base(ceph::buffer::list::const_iterator& p);
  void _encode_locks_full(ceph::buffer::list& bl);
  void _decode_locks_full(ceph::buffer::list::const_iterator& p);
  void _encode_locks_state_for_replica(ceph::buffer::list& bl, bool need_recover);
  void _encode_locks_state_for_rejoin(ceph::buffer::list& bl, int rep);
  void _decode_locks_state_for_replica(ceph::buffer::list::const_iterator& p, bool is_new);
  void _decode_locks_rejoin(ceph::buffer::list::const_iterator& p, MDSContext::vec& waiters,
			    std::list<SimpleLock*>& eval_locks, bool survivor);

  // -- import/export --
  void encode_export(ceph::buffer::list& bl);
  void finish_export();
  void abort_export() {
    put(PIN_TEMPEXPORTING);
    ceph_assert(state_test(STATE_EXPORTINGCAPS));
    state_clear(STATE_EXPORTINGCAPS);
    put(PIN_EXPORTINGCAPS);
  }
  void decode_import(ceph::buffer::list::const_iterator& p, LogSegment *ls);
  
  // for giving to clients
  int encode_inodestat(ceph::buffer::list& bl, Session *session, SnapRealm *realm,
		       snapid_t snapid=CEPH_NOSNAP, unsigned max_bytes=0,
		       int getattr_wants=0);
  void encode_cap_message(const ceph::ref_t<MClientCaps> &m, Capability *cap);

  SimpleLock* get_lock(int type) override;

  void set_object_info(MDSCacheObjectInfo &info) override;

  void encode_lock_state(int type, ceph::buffer::list& bl) override;
  void decode_lock_state(int type, const ceph::buffer::list& bl) override;
  void encode_lock_iauth(ceph::buffer::list& bl);
  void decode_lock_iauth(ceph::buffer::list::const_iterator& p);
  void encode_lock_ilink(ceph::buffer::list& bl);
  void decode_lock_ilink(ceph::buffer::list::const_iterator& p);
  void encode_lock_idft(ceph::buffer::list& bl);
  void decode_lock_idft(ceph::buffer::list::const_iterator& p);
  void encode_lock_ifile(ceph::buffer::list& bl);
  void decode_lock_ifile(ceph::buffer::list::const_iterator& p);
  void encode_lock_inest(ceph::buffer::list& bl);
  void decode_lock_inest(ceph::buffer::list::const_iterator& p);
  void encode_lock_ixattr(ceph::buffer::list& bl);
  void decode_lock_ixattr(ceph::buffer::list::const_iterator& p);
  void encode_lock_isnap(ceph::buffer::list& bl);
  void decode_lock_isnap(ceph::buffer::list::const_iterator& p);
  void encode_lock_iflock(ceph::buffer::list& bl);
  void decode_lock_iflock(ceph::buffer::list::const_iterator& p);
  void encode_lock_ipolicy(ceph::buffer::list& bl);
  void decode_lock_ipolicy(ceph::buffer::list::const_iterator& p);

  void _finish_frag_update(CDir *dir, MutationRef& mut);

  void clear_dirty_scattered(int type) override;
  bool is_dirty_scattered();
  void clear_scatter_dirty();  // on rejoin ack

  void start_scatter(ScatterLock *lock);
  void finish_scatter_update(ScatterLock *lock, CDir *dir,
			     version_t inode_version, version_t dir_accounted_version);
  void finish_scatter_gather_update(int type, MutationRef& mut);
  void finish_scatter_gather_update_accounted(int type, EMetaBlob *metablob);

  // -- snap --
  void open_snaprealm(bool no_split=false);
  void close_snaprealm(bool no_join=false);
  SnapRealm *find_snaprealm() const;
  void encode_snap(ceph::buffer::list& bl);
  void decode_snap(ceph::buffer::list::const_iterator& p);

  client_t get_loner() const { return loner_cap; }
  client_t get_wanted_loner() const { return want_loner_cap; }

  // this is the loner state our locks should aim for
  client_t get_target_loner() const {
    if (loner_cap == want_loner_cap)
      return loner_cap;
    else
      return -1;
  }

  client_t calc_ideal_loner();
  void set_loner_cap(client_t l);
  bool choose_ideal_loner();
  bool try_set_loner();
  bool try_drop_loner();

  // choose new lock state during recovery, based on issued caps
  void choose_lock_state(SimpleLock *lock, int allissued);
  void choose_lock_states(int dirty_caps);

  int count_nonstale_caps();
  bool multiple_nonstale_caps();

  bool is_any_caps() { return !client_caps.empty(); }
  bool is_any_nonstale_caps() { return count_nonstale_caps(); }

  const mempool::mds_co::compact_map<int32_t,int32_t>& get_mds_caps_wanted() const { return mds_caps_wanted; }
  void set_mds_caps_wanted(mempool::mds_co::compact_map<int32_t,int32_t>& m);
  void set_mds_caps_wanted(mds_rank_t mds, int32_t wanted);

  const mempool_cap_map& get_client_caps() const { return client_caps; }
  Capability *get_client_cap(client_t client) {
    auto client_caps_entry = client_caps.find(client);
    if (client_caps_entry != client_caps.end())
      return &client_caps_entry->second;
    return 0;
  }
  int get_client_cap_pending(client_t client) const {
    auto client_caps_entry = client_caps.find(client);
    if (client_caps_entry != client_caps.end()) {
      return client_caps_entry->second.pending();
    } else {
      return 0;
    }
  }

  int get_num_caps_notable() const { return num_caps_notable; }
  void adjust_num_caps_notable(int d);

  Capability *add_client_cap(client_t client, Session *session,
			     SnapRealm *conrealm=nullptr, bool new_inode=false);
  void remove_client_cap(client_t client);
  void move_to_realm(SnapRealm *realm);

  Capability *reconnect_cap(client_t client, const cap_reconnect_t& icr, Session *session);
  void clear_client_caps_after_export();
  void export_client_caps(std::map<client_t,Capability::Export>& cl);

  // caps allowed
  int get_caps_liked() const;
  int get_caps_allowed_ever() const;
  int get_caps_allowed_by_type(int type) const;
  int get_caps_careful() const;
  int get_xlocker_mask(client_t client) const;
  int get_caps_allowed_for_client(Session *s, Capability *cap,
				  const mempool_inode *file_i) const;

  // caps issued, wanted
  int get_caps_issued(int *ploner = 0, int *pother = 0, int *pxlocker = 0,
		      int shift = 0, int mask = -1);
  bool is_any_caps_wanted() const;
  int get_caps_wanted(int *ploner = 0, int *pother = 0, int shift = 0, int mask = -1) const;
  bool issued_caps_need_gather(SimpleLock *lock);

  // client writeable
  bool is_clientwriteable() const { return state & STATE_CLIENTWRITEABLE; }
  void mark_clientwriteable();
  void clear_clientwriteable();

  // -- authority --
  mds_authority_t authority() const override;

  // -- auth pins --
  bool can_auth_pin(int *err_ret=nullptr) const override;
  void auth_pin(void *by) override;
  void auth_unpin(void *by) override;

  // -- freeze --
  bool is_freezing_inode() const { return state_test(STATE_FREEZING); }
  bool is_frozen_inode() const { return state_test(STATE_FROZEN); }
  bool is_frozen_auth_pin() const { return state_test(STATE_FROZENAUTHPIN); }
  bool is_frozen() const override;
  bool is_frozen_dir() const;
  bool is_freezing() const override;

  /* Freeze the inode. auth_pin_allowance lets the caller account for any
   * auth_pins it is itself holding/responsible for. */
  bool freeze_inode(int auth_pin_allowance=0);
  void unfreeze_inode(MDSContext::vec& finished);
  void unfreeze_inode();

  void freeze_auth_pin();
  void unfreeze_auth_pin();

  // -- reference counting --
  void bad_put(int by) override {
    generic_dout(0) << " bad put " << *this << " by " << by << " " << pin_name(by) << " was " << ref
#ifdef MDS_REF_SET
		    << " (" << ref_map << ")"
#endif
		    << dendl;
#ifdef MDS_REF_SET
    ceph_assert(ref_map[by] > 0);
#endif
    ceph_assert(ref > 0);
  }
  void bad_get(int by) override {
    generic_dout(0) << " bad get " << *this << " by " << by << " " << pin_name(by) << " was " << ref
#ifdef MDS_REF_SET
		    << " (" << ref_map << ")"
#endif
		    << dendl;
#ifdef MDS_REF_SET
    ceph_assert(ref_map[by] >= 0);
#endif
  }
  void first_get() override;
  void last_put() override;
  void _put() override;

  // -- hierarchy stuff --
  void set_primary_parent(CDentry *p) {
    ceph_assert(parent == 0 ||
	   g_conf().get_val<bool>("mds_hack_allow_loading_invalid_metadata"));
    parent = p;
  }
  void remove_primary_parent(CDentry *dn) {
    ceph_assert(dn == parent);
    parent = 0;
  }
  void add_remote_parent(CDentry *p);
  void remove_remote_parent(CDentry *p);
  int num_remote_parents() {
    return remote_parents.size(); 
  }

  void push_projected_parent(CDentry *dn) {
    projected_parent.push_back(dn);
  }
  void pop_projected_parent() {
    ceph_assert(projected_parent.size());
    parent = projected_parent.front();
    projected_parent.pop_front();
  }
  bool is_parent_projected() const {
    return !projected_parent.empty();
  }

  mds_rank_t get_export_pin(bool inherit=true) const;
  void check_pin_policy(mds_rank_t target);
  void set_export_pin(mds_rank_t rank);
  void queue_export_pin(mds_rank_t target);
  void maybe_export_pin(bool update=false);

  void set_ephemeral_pin(bool dist, bool rand);
  void clear_ephemeral_pin(bool dist, bool rand);

  void setxattr_ephemeral_dist(bool val=false);
  bool is_ephemeral_dist() const {
    return state_test(STATE_DISTEPHEMERALPIN);
  }

  double get_ephemeral_rand() const;
  void maybe_ephemeral_rand(double threshold=-1.0);
  void setxattr_ephemeral_rand(double prob=0.0);
  bool is_ephemeral_rand() const {
    return state_test(STATE_RANDEPHEMERALPIN);
  }

  bool has_ephemeral_policy() const {
    return get_inode()->export_ephemeral_random_pin > 0.0 ||
           get_inode()->export_ephemeral_distributed_pin;
  }
  bool is_ephemerally_pinned() const {
    return state_test(STATE_DISTEPHEMERALPIN) ||
           state_test(STATE_RANDEPHEMERALPIN);
  }

  void print(std::ostream& out) const override;
  void dump(ceph::Formatter *f, int flags = DUMP_DEFAULT) const;

  /**
   * Validate that the on-disk state of an inode matches what
   * we expect from our memory state. Currently this checks that:
   * 1) The backtrace associated with the file data exists and is correct
   * 2) For directories, the actual inode metadata matches our memory state,
   * 3) For directories, the rstats match
   *
   * @param results A freshly-created validated_data struct, with values set
   * as described in the struct documentation.
   * @param mdr The request to be responeded upon the completion of the
   * validation (or NULL)
   * @param fin Context to call back on completion (or NULL)
   */
  void validate_disk_state(validated_data *results,
                           MDSContext *fin);
  static void dump_validation_results(const validated_data& results,
                                      ceph::Formatter *f);

  //bool hack_accessed = false;
  //utime_t hack_load_stamp;

  MDCache *mdcache;

  SnapRealm        *snaprealm = nullptr;
  SnapRealm        *containing_realm = nullptr;
  snapid_t          first, last;
  mempool::mds_co::compact_set<snapid_t> dirty_old_rstats;

  uint64_t last_journaled = 0;       // log offset for the last time i was journaled
  //loff_t last_open_journaled;  // log offset for the last journaled EOpen
  utime_t last_dirstat_prop;

  // list item node for when we have unpropagated rstat data
  elist<CInode*>::item dirty_rstat_item;

  mempool::mds_co::set<client_t> client_snap_caps;
  mempool::mds_co::compact_map<snapid_t, mempool::mds_co::set<client_t> > client_need_snapflush;

  // LogSegment lists i (may) belong to
  elist<CInode*>::item item_dirty;
  elist<CInode*>::item item_open_file;
  elist<CInode*>::item item_dirty_parent;
  elist<CInode*>::item item_dirty_dirfrag_dir;
  elist<CInode*>::item item_dirty_dirfrag_nest;
  elist<CInode*>::item item_dirty_dirfrag_dirfragtree;

  // Either SnapRealm::inodes_with_caps or Locker::need_snapflush_inodes (but not both!)
  elist<CInode*>::item item_caps;
  mutable elist<CInode*>::item item_realm;

  // also update RecoveryQueue::RecoveryQueue() if you change this
  elist<CInode*>::item& item_recover_queue = item_dirty_dirfrag_dir;
  elist<CInode*>::item& item_recover_queue_front = item_dirty_dirfrag_nest;

  inode_load_vec_t pop;
  elist<CInode*>::item item_pop_lru;

  // -- locks --
  static const LockType versionlock_type;
  static const LockType authlock_type;
  static const LockType linklock_type;
  static const LockType dirfragtreelock_type;
  static const LockType filelock_type;
  static const LockType xattrlock_type;
  static const LockType snaplock_type;
  static const LockType nestlock_type;
  static const LockType flocklock_type;
  static const LockType policylock_type;
  static const LockType quiescelock_type;

  /* Please consult doc/dev/mds_internals/quiesce.rst for information about the
   * quiescelock.
   */

  SimpleLock quiescelock; // FIXME not part of mempool
  LocalLockC versionlock; // FIXME not part of mempool
  SimpleLock authlock; // FIXME not part of mempool
  SimpleLock linklock; // FIXME not part of mempool
  ScatterLock dirfragtreelock; // FIXME not part of mempool
  ScatterLock filelock; // FIXME not part of mempool
  SimpleLock xattrlock; // FIXME not part of mempool
  SimpleLock snaplock; // FIXME not part of mempool
  ScatterLock nestlock; // FIXME not part of mempool
  SimpleLock flocklock; // FIXME not part of mempool
  SimpleLock policylock; // FIXME not part of mempool

  // -- caps -- (new)
  // client caps
  client_t loner_cap = -1, want_loner_cap = -1;

protected:
  ceph_lock_state_t *get_fcntl_lock_state() {
    if (!fcntl_locks)
      fcntl_locks = new ceph_lock_state_t(g_ceph_context, CEPH_LOCK_FCNTL);
    return fcntl_locks;
  }
  void clear_fcntl_lock_state() {
    delete fcntl_locks;
    fcntl_locks = NULL;
  }
  ceph_lock_state_t *get_flock_lock_state() {
    if (!flock_locks)
      flock_locks = new ceph_lock_state_t(g_ceph_context, CEPH_LOCK_FLOCK);
    return flock_locks;
  }
  void clear_flock_lock_state() {
    delete flock_locks;
    flock_locks = NULL;
  }
  void clear_file_locks() {
    clear_fcntl_lock_state();
    clear_flock_lock_state();
  }
  void _encode_file_locks(ceph::buffer::list& bl) const {
    using ceph::encode;
    bool has_fcntl_locks = fcntl_locks && !fcntl_locks->empty();
    encode(has_fcntl_locks, bl);
    if (has_fcntl_locks)
      encode(*fcntl_locks, bl);
    bool has_flock_locks = flock_locks && !flock_locks->empty();
    encode(has_flock_locks, bl);
    if (has_flock_locks)
      encode(*flock_locks, bl);
  }
  void _decode_file_locks(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    bool has_fcntl_locks;
    decode(has_fcntl_locks, p);
    if (has_fcntl_locks)
      decode(*get_fcntl_lock_state(), p);
    else
      clear_fcntl_lock_state();
    bool has_flock_locks;
    decode(has_flock_locks, p);
    if (has_flock_locks)
      decode(*get_flock_lock_state(), p);
    else
      clear_flock_lock_state();
  }

  /**
   * Return the pool ID where we currently write backtraces for
   * this inode (in addition to inode.old_pools)
   *
   * @returns a pool ID >=0
   */
  int64_t get_backtrace_pool() const;

  // parent dentries in cache
  CDentry         *parent = nullptr;             // primary link
  mempool::mds_co::compact_set<CDentry*>    remote_parents;     // if hard linked

  mempool::mds_co::list<CDentry*>   projected_parent;   // for in-progress rename, (un)link, etc.

  mds_authority_t inode_auth = CDIR_AUTH_DEFAULT;

  // -- distributed state --
  // file capabilities
  mempool_cap_map client_caps; // client -> caps
  mempool::mds_co::compact_map<int32_t, int32_t> mds_caps_wanted;     // [auth] mds -> caps wanted
  int replica_caps_wanted = 0; // [replica] what i've requested from auth
  int num_caps_notable = 0;

  ceph_lock_state_t *fcntl_locks = nullptr;
  ceph_lock_state_t *flock_locks = nullptr;

  // -- waiting --
  mempool::mds_co::compact_map<frag_t, MDSContext::vec > waiting_on_dir;


  // -- freezing inode --
  int auth_pin_freeze_allowance = 0;
  elist<CInode*>::item item_freezing_inode;
  void maybe_finish_freeze_inode();
private:

  friend class ValidationContinuation;

  /**
   * Create a scrub_info_t struct for the scrub_infop pointer.
   */
  void scrub_info_create() const;
  /**
   * Delete the scrub_info_t struct if it's not got any useful data
   */
  void scrub_maybe_delete_info();

  void pop_projected_snaprealm(sr_t *next_snaprealm, bool early);

  bool _validate_disk_state(class ValidationContinuation *c,
                            int rval, int stage);

  struct projected_const_node {
    inode_const_ptr inode;
    xattr_map_const_ptr xattrs;
    sr_t *snapnode;

    projected_const_node() = delete;
    projected_const_node(projected_const_node&&) = default;
    explicit projected_const_node(const inode_const_ptr& i, const xattr_map_const_ptr& x, sr_t *s) :
      inode(i), xattrs(x), snapnode(s) {}
  };

  mempool::mds_co::list<projected_const_node> projected_nodes;   // projected values (only defined while dirty)
  size_t num_projected_srnodes = 0;

  // -- cache infrastructure --
  mempool::mds_co::compact_map<frag_t,CDir*> dirfrags; // cached dir fragments under this Inode

  //for the purpose of quickly determining whether there's a subtree root or exporting dir
  int num_subtree_roots = 0;
  int num_exporting_dirs = 0;

  int stickydir_ref = 0;
  std::unique_ptr<scrub_info_t> scrub_infop;
  /** @} Scrubbing and fsck */
};

std::ostream& operator<<(std::ostream& out, const CInode& in);

extern cinode_lock_info_t cinode_lock_info[];
extern int num_cinode_locks;
#undef dout_context
#endif
