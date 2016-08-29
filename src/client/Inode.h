// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_INODE_H
#define CEPH_CLIENT_INODE_H

#include "include/types.h"
#include "include/xlist.h"

#include "mds/mdstypes.h" // hrm

#include "osdc/ObjectCacher.h"
#include "include/assert.h"

#include "InodeRef.h"

class Client;
struct MetaSession;
class Dentry;
class Dir;
struct SnapRealm;
struct Inode;
class ceph_lock_state_t;
class MetaRequest;
class UserGroups;
class filepath;

struct Cap {
  MetaSession *session;
  Inode *inode;
  xlist<Cap*>::item cap_item;

  uint64_t cap_id;
  unsigned issued;
  unsigned implemented;
  unsigned wanted;   // as known to mds.
  uint64_t seq, issue_seq;
  __u32 mseq;  // migration seq
  __u32 gen;

  Cap() : session(NULL), inode(NULL), cap_item(this), cap_id(0), issued(0),
	       implemented(0), wanted(0), seq(0), issue_seq(0), mseq(0), gen(0) {}

  void dump(Formatter *f) const;
};

struct CapSnap {
  //snapid_t follows;  // map key
  InodeRef in;
  SnapContext context;
  int issued, dirty;

  uint64_t size;
  utime_t ctime, btime, mtime, atime;
  version_t time_warp_seq;
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;
  map<string,bufferptr> xattrs;
  version_t xattr_version;

  bufferlist inline_data;
  version_t inline_version;

  bool writing, dirty_data;
  uint64_t flush_tid;

  explicit CapSnap(Inode *i)
    : in(i), issued(0), dirty(0),
      size(0), time_warp_seq(0), mode(0), uid(0), gid(0), xattr_version(0),
      inline_version(0), writing(false), dirty_data(false), flush_tid(0)
  {}

  void dump(Formatter *f) const;
};

// inode flags
#define I_COMPLETE	1
#define I_DIR_ORDERED	2
#define I_CAP_DROPPED	4
#define I_SNAPDIR_OPEN	8

struct Inode {
  Client *client;

  // -- the actual inode --
  inodeno_t ino; // ORDER DEPENDENCY: oset
  snapid_t  snapid;
  ino_t faked_ino;

  uint32_t   rdev;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time
  utime_t    btime;   // birth time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;

  // file (data access)
  ceph_dir_layout dir_layout;
  file_layout_t layout;
  uint64_t   size;        // on directory, # dentries
  uint32_t   truncate_seq;
  uint64_t   truncate_size;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())

  uint64_t max_size;  // max size we can write to

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat;

  // special stuff
  version_t version;           // auth only
  version_t xattr_version;

  // inline data
  version_t  inline_version;
  bufferlist inline_data;

  bool is_root()    const { return ino == MDS_INO_ROOT; }
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool has_dir_layout() const {
    return layout != file_layout_t();
  }

  __u32 hash_dentry_name(const string &dn) {
    int which = dir_layout.dl_dir_hash;
    if (!which)
      which = CEPH_STR_HASH_LINUX;
    assert(ceph_str_hash_valid(which));
    return ceph_str_hash(which, dn.data(), dn.length());
  }

  unsigned flags;

  quota_info_t quota;

  bool is_complete_and_ordered() {
    static const unsigned wants = I_COMPLETE | I_DIR_ORDERED;
    return (flags & wants) == wants;
  }

  // about the dir (if this is one!)
  Dir       *dir;     // if i'm a dir.
  fragtree_t dirfragtree;
  set<int>  dir_contacts;
  uint64_t dir_release_count, dir_ordered_count;
  bool dir_hashed, dir_replicated;

  // per-mds caps
  map<mds_rank_t, Cap*> caps;            // mds -> Cap
  Cap *auth_cap;
  int64_t cap_dirtier_uid;
  int64_t cap_dirtier_gid;
  unsigned dirty_caps, flushing_caps;
  std::map<ceph_tid_t, int> flushing_cap_tids;
  int shared_gen, cache_gen;
  int snap_caps, snap_cap_refs;
  utime_t hold_caps_until;
  xlist<Inode*>::item cap_item, flushing_cap_item;

  SnapRealm *snaprealm;
  xlist<Inode*>::item snaprealm_item;
  InodeRef snapdir_parent;  // only if we are a snapdir inode
  map<snapid_t,CapSnap*> cap_snaps;   // pending flush to mds

  //int open_by_mode[CEPH_FILE_MODE_NUM];
  map<int,int> open_by_mode;
  map<int,int> cap_refs;

  ObjectCacher::ObjectSet oset; // ORDER DEPENDENCY: ino

  uint64_t     reported_size, wanted_max_size, requested_max_size;

  int       _ref;      // ref count. 1 for each dentry, fh that links to me.
  int       ll_ref;   // separate ref count for ll client
  set<Dentry*> dn_set;      // if i'm linked to a dentry.
  string    symlink;  // symlink content, if it's a symlink
  map<string,bufferptr> xattrs;
  map<frag_t,int> fragmap;  // known frag -> mds mappings

  list<Cond*>       waitfor_caps;
  list<Cond*>       waitfor_commit;

  Dentry *get_first_parent() {
    assert(!dn_set.empty());
    return *dn_set.begin();
  }

  void make_long_path(filepath& p);
  void make_nosnap_relative_path(filepath& p);

  void get();
  int _put(int n=1);

  int get_num_ref() {
    return _ref;
  }

  void ll_get() {
    ll_ref++;
  }
  void ll_put(int n=1) {
    assert(ll_ref >= n);
    ll_ref -= n;
  }

  // file locks
  ceph_lock_state_t *fcntl_locks;
  ceph_lock_state_t *flock_locks;

  xlist<MetaRequest*> unsafe_ops;

  Inode(Client *c, vinodeno_t vino, file_layout_t *newlayout)
    : client(c), ino(vino.ino), snapid(vino.snapid), faked_ino(0),
      rdev(0), mode(0), uid(0), gid(0), nlink(0),
      size(0), truncate_seq(1), truncate_size(-1),
      time_warp_seq(0), max_size(0), version(0), xattr_version(0),
      inline_version(0), flags(0),
      dir(0), dir_release_count(1), dir_ordered_count(1),
      dir_hashed(false), dir_replicated(false), auth_cap(NULL),
      cap_dirtier_uid(-1), cap_dirtier_gid(-1),
      dirty_caps(0), flushing_caps(0), shared_gen(0), cache_gen(0),
      snap_caps(0), snap_cap_refs(0),
      cap_item(this), flushing_cap_item(this),
      snaprealm(0), snaprealm_item(this),
      oset((void *)this, newlayout->pool_id, this->ino),
      reported_size(0), wanted_max_size(0), requested_max_size(0),
      _ref(0), ll_ref(0), dn_set(),
      fcntl_locks(NULL), flock_locks(NULL),
      async_err(0)
  {
    memset(&dir_layout, 0, sizeof(dir_layout));
    memset(&quota, 0, sizeof(quota));
  }
  ~Inode();

  vinodeno_t vino() const { return vinodeno_t(ino, snapid); }

  struct Compare {
    bool operator() (Inode* const & left, Inode* const & right) {
      if (left->ino.val < right->ino.val) {
	return (left->snapid.val < right->snapid.val);
      }
      return false;
    }
  };

  bool check_mode(uid_t uid, UserGroups& groups, unsigned want);

  // CAPS --------
  void get_open_ref(int mode);
  bool put_open_ref(int mode);

  void get_cap_ref(int cap);
  int put_cap_ref(int cap);
  bool is_any_caps();
  bool cap_is_valid(Cap* cap) const;
  int caps_issued(int *implemented = 0) const;
  void touch_cap(Cap *cap);
  void try_touch_cap(mds_rank_t mds);
  bool caps_issued_mask(unsigned mask);
  int caps_used();
  int caps_file_wanted();
  int caps_wanted();
  int caps_mds_wanted();
  int caps_dirty();

  bool have_valid_size();
  Dir *open_dir();

  // Record errors to be exposed in fclose/fflush
  int async_err;

  void dump(Formatter *f) const;
};

ostream& operator<<(ostream &out, const Inode &in);

#endif
