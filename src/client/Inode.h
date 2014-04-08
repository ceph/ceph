// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_INODE_H
#define CEPH_CLIENT_INODE_H

#include "include/types.h"
#include "include/xlist.h"
#include "include/filepath.h"

#include "mds/mdstypes.h" // hrm

#include "osdc/ObjectCacher.h"
#include "include/assert.h"

struct MetaSession;
class Dentry;
class Dir;
struct SnapRealm;
class Inode;

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
  Inode *in;
  SnapContext context;
  int issued, dirty;

  uint64_t size;
  utime_t ctime, mtime, atime;
  version_t time_warp_seq;
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;
  map<string,bufferptr> xattrs;
  version_t xattr_version;

  bool writing, dirty_data;
  uint64_t flush_tid;
  xlist<CapSnap*>::item flushing_item;

  CapSnap(Inode *i)
    : in(i), issued(0), dirty(0),
      size(0), time_warp_seq(0), mode(0), uid(0), gid(0), xattr_version(0),
      writing(false), dirty_data(false), flush_tid(0),
      flushing_item(this)
  {}

  void dump(Formatter *f) const;
};


// inode flags
#define I_COMPLETE 1

class Inode {
 public:
  CephContext *cct;

  // -- the actual inode --
  inodeno_t ino;
  snapid_t  snapid;
  uint32_t   rdev;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;

  // file (data access)
  ceph_dir_layout dir_layout;
  ceph_file_layout layout;
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

  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool has_dir_layout() const {
    for (unsigned c = 0; c < sizeof(layout); c++)
      if (*((const char *)&layout + c))
	return true;
    return false;
  }

  __u32 hash_dentry_name(const string &dn) {
    int which = dir_layout.dl_dir_hash;
    if (!which)
      which = CEPH_STR_HASH_LINUX;
    return ceph_str_hash(which, dn.data(), dn.length());
  }

  unsigned flags;

  // about the dir (if this is one!)
  set<int>  dir_contacts;
  bool      dir_hashed, dir_replicated;

  // per-mds caps
  map<int,Cap*> caps;            // mds -> Cap
  Cap *auth_cap;
  unsigned dirty_caps, flushing_caps;
  uint64_t flushing_cap_seq;
  __u16 flushing_cap_tid[CEPH_CAP_BITS];
  int shared_gen, cache_gen;
  int snap_caps, snap_cap_refs;
  utime_t hold_caps_until;
  xlist<Inode*>::item cap_item, flushing_cap_item;
  ceph_tid_t last_flush_tid;

  SnapRealm *snaprealm;
  xlist<Inode*>::item snaprealm_item;
  Inode *snapdir_parent;  // only if we are a snapdir inode
  map<snapid_t,CapSnap*> cap_snaps;   // pending flush to mds

  //int open_by_mode[CEPH_FILE_MODE_NUM];
  map<int,int> open_by_mode;
  map<int,int> cap_refs;

  ObjectCacher::ObjectSet oset;

  uint64_t     reported_size, wanted_max_size, requested_max_size;

  int       _ref;      // ref count. 1 for each dentry, fh that links to me.
  int       ll_ref;   // separate ref count for ll client
  Dir       *dir;     // if i'm a dir.
  set<Dentry*> dn_set;      // if i'm linked to a dentry.
  string    symlink;  // symlink content, if it's a symlink
  fragtree_t dirfragtree;
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

  void get() {
    _ref++;
    lsubdout(cct, mds, 15) << "inode.get on " << this << " " <<  ino << '.' << snapid
                           << " now " << _ref << dendl;
  }
  /// private method to put a reference; see Client::put_inode()
  int _put(int n=1) {
    _ref -= n; 
    lsubdout(cct, mds, 15) << "inode.put on " << this << " " << ino << '.' << snapid
                           << " now " << _ref << dendl;
    assert(_ref >= 0);
    return _ref;
  }

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

  Inode(CephContext *cct_, vinodeno_t vino, ceph_file_layout *newlayout)
    : cct(cct_), ino(vino.ino), snapid(vino.snapid),
      rdev(0), mode(0), uid(0), gid(0), nlink(0),
      size(0), truncate_seq(1), truncate_size(-1),
      time_warp_seq(0), max_size(0), version(0), xattr_version(0),
      inline_version(0),
      flags(0),
      dir_hashed(false), dir_replicated(false), auth_cap(NULL),
      dirty_caps(0), flushing_caps(0), flushing_cap_seq(0), shared_gen(0), cache_gen(0),
      snap_caps(0), snap_cap_refs(0),
      cap_item(this), flushing_cap_item(this), last_flush_tid(0),
      snaprealm(0), snaprealm_item(this), snapdir_parent(0),
      oset((void *)this, newlayout->fl_pg_pool, ino),
      reported_size(0), wanted_max_size(0), requested_max_size(0),
      _ref(0), ll_ref(0), 
      dir(0), dn_set()
  {
    memset(&dir_layout, 0, sizeof(dir_layout));
    memset(&layout, 0, sizeof(layout));
    memset(&flushing_cap_tid, 0, sizeof(__u16)*CEPH_CAP_BITS);
  }
  ~Inode() { }

  vinodeno_t vino() { return vinodeno_t(ino, snapid); }

  struct Compare {
    bool operator() (Inode* const & left, Inode* const & right) {
      if (left->ino.val < right->ino.val) {
	return (left->snapid.val < right->snapid.val);
      }
      return false;
    }
  };

  bool check_mode(uid_t uid, gid_t gid, gid_t *sgids, int sgid_count, uint32_t flags);

  // CAPS --------
  void get_open_ref(int mode);
  bool put_open_ref(int mode);

  void get_cap_ref(int cap);
  int put_cap_ref(int cap);
  bool is_any_caps();
  bool cap_is_valid(Cap* cap);
  int caps_issued(int *implemented = 0);
  void touch_cap(Cap *cap);
  void try_touch_cap(int mds);
  bool caps_issued_mask(unsigned mask);
  int caps_used();
  int caps_file_wanted();
  int caps_wanted();
  int caps_dirty();

  bool have_valid_size();
  Dir *open_dir();

  void dump(Formatter *f) const;
};

ostream& operator<<(ostream &out, Inode &in);

#endif
