#ifndef CEPH_CINODE_H
#define CEPH_CINODE_H

#include "mds/mdstypes.h"
#include "CObject.h"

#include "SimpleLock.h"
#include "ScatterLock.h"
#include "LocalLock.h"
#include "Capability.h"

#include "include/elist.h"

class LogSegment;
class EMetaBlob;
class Session;
class MDCache;
class MClientCaps;

typedef std::map<string, bufferptr> xattr_map_t;

class CInode : public CObject {
public:
  MDCache* const mdcache;
protected:

  inode_t inode;
  std::string symlink;
  xattr_map_t xattrs;

  struct projected_inode_t {
    inode_t inode;
    xattr_map_t xattrs;
    bool xattrs_projected;
    projected_inode_t(const inode_t &i, const xattr_map_t *px)
      : inode(i), xattrs_projected(false) { 
      if (px) {
	xattrs = *px;
	xattrs_projected = true;
      }
    }
  };
  list<projected_inode_t> projected_nodes;

  CDentry *parent;
  list<CDentry*> projected_parent;

  map<frag_t,CDir*> dirfrags;

  void first_get();
  void last_put();
public:
  // pins
  static const int PIN_DIRFRAG =		-1;
  static const int PIN_CAPS =			2;  // client caps
  static const int PIN_DIRTYRSTAT =		21;
  // states
  static const unsigned STATE_FREEING =		(1<<0);
  static const unsigned STATE_CHECKINGMAXSIZE =	(1<<1);
  static const unsigned STATE_DIRTYRSTAT =	(1<<15);

  CInode(MDCache *_mdcache);

  inodeno_t ino() const { return inode.ino; }
  vinodeno_t vino() const { return vinodeno_t(inode.ino, CEPH_NOSNAP); }
  uint8_t d_type() const { return IFTODT(inode.mode); }

  bool is_head() const { return true; }
  bool is_lt(const CObject *r) const {
    const CInode *o = static_cast<const CInode*>(r);
    return vino() < o->vino();
  }

  bool is_root() const { return ino() == MDS_INO_ROOT; }
  bool is_stray() const { return MDS_INO_IS_STRAY(ino()); }
  bool is_mdsdir() const { return MDS_INO_IS_MDSDIR(ino()); }
  bool is_base() const { return is_root() || is_mdsdir(); }
  bool is_system() const { return ino() < MDS_INO_SYSTEM_BASE; }
  mds_rank_t get_stray_owner() const {
    return (mds_rank_t)MDS_INO_STRAY_OWNER(ino());
  }

  bool is_dir() const { return inode.is_dir(); }
  bool is_file() const { return inode.is_file(); }
  bool is_symlink() const { return inode.is_symlink(); }

  void make_string(std::string& s) const;
  void name_stray_dentry(string& dname) const;

  inode_t* __get_inode() {  return &inode; }
  const inode_t* get_inode() const {  return &inode; }
  inode_t* __get_projected_inode() {
    mutex_assert_locked_by_me();
    if (projected_nodes.empty())
      return &inode;
    else
      return &projected_nodes.back().inode;
  }
  const inode_t* get_projected_inode() const {
    mutex_assert_locked_by_me();
    if (projected_nodes.empty())
      return &inode;
    else
      return &projected_nodes.back().inode;
  }
  xattr_map_t* __get_xattrs() { return &xattrs; }
  const xattr_map_t* get_xattrs() const { return &xattrs; }
  const xattr_map_t* get_projected_xattrs() const {
    mutex_assert_locked_by_me();
    for (auto p = projected_nodes.rbegin(); p != projected_nodes.rend(); ++p)
      if (p->xattrs_projected)
	return &p->xattrs;
    return &xattrs;
  }
  version_t get_version() const { return inode.version; }
  version_t get_projected_version() const {
    if (projected_nodes.empty())
      return inode.version;
    else
      return projected_nodes.back().inode.version;
  }
  void __set_symlink(const std::string& s) { symlink = s; }
  const std::string& get_symlink() const { return symlink; }

  bool is_projected() const { return !projected_nodes.empty(); }
  inode_t *project_inode(xattr_map_t **ppx=0);
  void pop_and_dirty_projected_inode(LogSegment *ls);

  version_t pre_dirty();
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();

  void set_primary_parent(CDentry *p) {
    assert(parent == NULL);
    parent = p;
  }
  void remove_primary_parent(CDentry *dn) {
    assert(dn == parent);
    parent = NULL;
  }

  CDentry* get_parent_dn() const { return parent; }
  CDentry* get_projected_parent_dn() const {
    mutex_assert_locked_by_me();
    if (projected_parent.empty())
      return parent;
    return  projected_parent.back();
  }

  void push_projected_parent(CDentry *dn) {
    mutex_assert_locked_by_me();
    projected_parent.push_back(dn);
  }
  CDentry* pop_projected_parent() {
    mutex_assert_locked_by_me();
    assert(!projected_parent.empty());
    CDentry *dn = projected_parent.front();
    projected_parent.pop_front();
    return dn;
  }

  CDentryRef get_lock_parent_dn();
  CDentryRef get_lock_projected_parent_dn();
  bool is_projected_ancestor_of(CInode *other) const;

  frag_t pick_dirfrag(const string &dn) const {
    return frag_t();
  }
  CDirRef get_dirfrag(frag_t fg) {
    mutex_assert_locked_by_me();
    CDirRef ref;
    auto p = dirfrags.find(fg);
    if (p != dirfrags.end())
      ref = p->second;
    return ref;
  }
  CDirRef get_or_open_dirfrag(frag_t fg);
  void close_dirfrag(frag_t fg);
  void get_dirfrags(list<CDir*>& ls);

  int encode_inodestat(bufferlist& bl, Session *session,
		       unsigned max_bytes=0, int getattr_wants=0);
  void encode_cap_message(MClientCaps *m, Capability *cap);

protected:
  static LockType versionlock_type;
  static LockType authlock_type;
  static LockType linklock_type;
  static LockType dirfragtreelock_type;
  static LockType filelock_type;
  static LockType xattrlock_type;
  static LockType snaplock_type;
  static LockType nestlock_type;
  static LockType flocklock_type;
  static LockType policylock_type;

public:
  LocalLock  versionlock;
  SimpleLock authlock;
  SimpleLock linklock;
  ScatterLock dirfragtreelock;
  ScatterLock filelock;
  SimpleLock xattrlock;
  SimpleLock snaplock;
  ScatterLock nestlock;
  SimpleLock flocklock;
  SimpleLock policylock;

  SimpleLock* get_lock(int type) {
    switch (type) {
    case CEPH_LOCK_IFILE: return &filelock;
    case CEPH_LOCK_IAUTH: return &authlock;
    case CEPH_LOCK_ILINK: return &linklock;
    case CEPH_LOCK_IDFT: return &dirfragtreelock;
    case CEPH_LOCK_IXATTR: return &xattrlock;
    case CEPH_LOCK_ISNAP: return &snaplock;
    case CEPH_LOCK_INEST: return &nestlock;
    case CEPH_LOCK_IFLOCK: return &flocklock;
    case CEPH_LOCK_IPOLICY: return &policylock;
    }
    return 0;
  }

protected:
  std::map<client_t, Capability*> client_caps;         // client -> caps
  client_t loner_cap, want_loner_cap;
  uint64_t want_max_size;

public:
  bool is_any_caps() { return !client_caps.empty(); }

  const std::map<client_t,Capability*>& get_client_caps() const { return client_caps; }
  Capability *get_client_cap(client_t client) const {
    auto p = client_caps.find(client);
    if (p != client_caps.end())
      return p->second;
    return NULL;
  }
  int get_client_cap_pending(client_t client) const {
    Capability *cap = get_client_cap(client);
    return cap ? cap->pending() : 0;
  }

  Capability *add_client_cap(Session *session);
  void remove_client_cap(Session *session);

  // caps issued, wanted
  int get_caps_issued(int *ploner = 0, int *pother = 0, int *pxlocker = 0,
                      int shift = 0, int mask = -1);
  int get_caps_wanted(int *ploner = 0, int *pother = 0, int shift = 0, int mask = -1) const;
  bool is_any_caps_wanted() const;
  bool issued_caps_need_gather(SimpleLock *lock);

  // caps allowed
  int get_caps_liked() const;
  int get_caps_allowed_ever() const;
  int get_caps_allowed_by_type(int type) const;
  int get_caps_careful() const;
  int get_xlocker_mask(client_t client) const;
  int get_caps_allowed_for_client(Session *s, const inode_t *file_i) const;


  client_t get_loner() const { return loner_cap; };
  client_t get_wanted_loner() const { return want_loner_cap; };
  // this is the loner state our locks should aim for
  client_t get_target_loner() const {
    if (loner_cap == want_loner_cap)
      return loner_cap;
    return -1;
  }
  client_t calc_ideal_loner();
  client_t choose_ideal_loner();
  void set_loner_cap(client_t l);
  bool try_set_loner();
  bool try_drop_loner();

  void set_wanted_max_size(uint64_t size) {
    if (size > want_max_size)
      want_max_size = size;
  }
  uint64_t get_clear_wanted_max_size() { 
    uint64_t size = want_max_size;
    want_max_size = 0;
    return size; 
  }

protected:
  void finish_scatter_update(ScatterLock *lock, CDir *dir);
  void __frag_update_finish(CDir *dir, const MutationRef& mut);
  friend class C_Inode_FragUpdate;
public:
  void start_scatter(ScatterLock *lock);
  void finish_scatter_gather_update(int type, const MutationRef& mut);
  void finish_scatter_gather_update_accounted(int type, const MutationRef& mut,
		  			      EMetaBlob *metablob);
  void clear_dirty_scattered(int type);

  bool is_dirty_rstat() { return state_test(STATE_DIRTYRSTAT); }
  void mark_dirty_rstat();
  void clear_dirty_rstat();

public:
  elist<CInode*>::item dirty_rstat_item;
  elist<CInode*>::item item_dirty;
  elist<CInode*>::item item_dirty_dirfrag_dir;
  elist<CInode*>::item item_dirty_dirfrag_nest;

public: // crap
  static snapid_t first, last;
  static snapid_t oldest_snap;
  static compact_map<snapid_t, old_inode_t> old_inodes;
  static fragtree_t dirfragtree;

  uint64_t last_journaled;

};

ostream& operator<<(ostream& out, const CInode& in);
#endif
