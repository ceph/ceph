// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_INODE_H
#define CEPH_CLIENT_INODE_H

#include <numeric>

#include "include/compat.h"
#include "include/ceph_assert.h"
#include "include/types.h"
#include "include/xlist.h"

#include "mds/flock.h"
#include "mds/mdstypes.h" // hrm
#include "include/cephfs/types.h"

#include "osdc/ObjectCacher.h"

#include "InodeRef.h"
#include "MetaSession.h"
#include "UserPerm.h"
#include "Delegation.h"

class Client;
class Dentry;
class Dir;
struct SnapRealm;
struct Inode;
class MetaRequest;
class filepath;
class Fh;

class Cap {
public:
  Cap() = delete;
  Cap(Inode &i, MetaSession *s) : inode(i),
                                  session(s),
                                  gen(s->cap_gen),
                                  cap_item(this)
  {
    s->caps.push_back(&cap_item);
  }
  ~Cap() {
    cap_item.remove_myself();
  }

  void touch(void) {
    // move to back of LRU
    session->caps.push_back(&cap_item);
  }

  void dump(Formatter *f) const;

  Inode &inode;
  MetaSession *session;
  uint64_t cap_id = 0;
  unsigned issued = 0;
  unsigned implemented = 0;
  unsigned wanted = 0;   // as known to mds.
  uint64_t seq = 0;
  uint64_t issue_seq = 0;
  __u32 mseq = 0;  // migration seq
  __u32 gen;
  UserPerm latest_perms;

private:
  /* Note that this Cap will not move (see Inode::caps):
   *
   * Section 23.1.2#8
   * The insert members shall not affect the validity of iterators and
   * references to the container, and the erase members shall invalidate only
   * iterators and references to the erased elements.
   */
  xlist<Cap *>::item cap_item;
};

struct CapSnap {
  //snapid_t follows;  // map key
  InodeRef in;
  SnapContext context;
  int issued = 0, dirty = 0;

  uint64_t size = 0;
  utime_t ctime, btime, mtime, atime;
  version_t time_warp_seq = 0;
  uint64_t change_attr = 0;
  uint32_t   mode = 0;
  uid_t      uid = 0;
  gid_t      gid = 0;
  std::map<std::string,bufferptr> xattrs;
  version_t xattr_version = 0;

  bufferlist inline_data;
  version_t inline_version = 0;

  bool writing = false, dirty_data = false;
  uint64_t flush_tid = 0;

  explicit CapSnap(Inode *i)
    : in(i)
  {}

  void dump(Formatter *f) const;
};

// inode flags
#define I_COMPLETE		(1 << 0)
#define I_DIR_ORDERED		(1 << 1)
#define I_SNAPDIR_OPEN		(1 << 2)
#define I_KICK_FLUSH		(1 << 3)
#define I_CAP_DROPPED		(1 << 4)
#define I_ERROR_FILELOCK	(1 << 5)

struct Inode : RefCountedObject {
  ceph::coarse_mono_time hold_caps_until;
  Client *client;

  // -- the actual inode --
  inodeno_t ino; // ORDER DEPENDENCY: oset
  snapid_t  snapid;
  ino_t faked_ino = 0;

  uint32_t   rdev = 0;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time
  utime_t    btime;   // birth time

  // perm (namespace permissions)
  uint32_t   mode = 0;
  uid_t      uid = 0;
  gid_t      gid = 0;

  // nlink
  int32_t    nlink = 0;

  // file (data access)
  ceph_dir_layout dir_layout{};
  file_layout_t layout;
  uint64_t   size = 0;        // on directory, # dentries
  uint32_t   truncate_seq = 1;
  uint64_t   truncate_size = -1;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq = 0;  // count of (potential) mtime/atime timewarps (i.e., utimes())
  uint64_t   change_attr = 0;

  uint64_t max_size = 0;  // max size we can write to

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat;

  // special stuff
  version_t version = 0;           // auth only
  version_t xattr_version = 0;
  utime_t   snap_btime;        // snapshot creation (birth) time
  std::map<std::string, std::string> snap_metadata;

  // inline data
  version_t  inline_version = 0;
  bufferlist inline_data;

  std::vector<uint8_t> fscrypt_auth;
  std::vector<uint8_t> fscrypt_file;
  bool is_fscrypt_enabled() {
    return !!fscrypt_auth.size();
  }

  bool is_root()    const { return ino == CEPH_INO_ROOT; }
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool has_dir_layout() const {
    return layout != file_layout_t();
  }

  __u32 hash_dentry_name(const std::string &dn) {
    int which = dir_layout.dl_dir_hash;
    if (!which)
      which = CEPH_STR_HASH_LINUX;
    ceph_assert(ceph_str_hash_valid(which));
    return ceph_str_hash(which, dn.data(), dn.length());
  }

  unsigned flags = 0;

  quota_info_t quota;

  bool is_complete_and_ordered() {
    static const unsigned wants = I_COMPLETE | I_DIR_ORDERED;
    return (flags & wants) == wants;
  }

  // about the dir (if this is one!)
  Dir       *dir = 0;     // if i'm a dir.
  fragtree_t dirfragtree;
  uint64_t dir_release_count = 1;
  uint64_t dir_ordered_count = 1;
  bool dir_hashed = false;
  bool dir_replicated = false;

  // per-mds caps
  std::map<mds_rank_t, Cap> caps;            // mds -> Cap
  Cap *auth_cap = 0;
  int64_t cap_dirtier_uid = -1;
  int64_t cap_dirtier_gid = -1;
  unsigned dirty_caps = 0;
  unsigned flushing_caps = 0;
  std::map<ceph_tid_t, int> flushing_cap_tids;
  int shared_gen = 0;
  int cache_gen = 0;
  int snap_caps = 0;
  int snap_cap_refs = 0;
  xlist<Inode*>::item delay_cap_item, dirty_cap_item, flushing_cap_item;

  SnapRealm *snaprealm = 0;
  xlist<Inode*>::item snaprealm_item;
  InodeRef snapdir_parent;  // only if we are a snapdir inode
  std::map<snapid_t,CapSnap> cap_snaps;   // pending flush to mds

  //int open_by_mode[CEPH_FILE_MODE_NUM];
  std::map<int,int> open_by_mode;
  std::map<int,int> cap_refs;

  ObjectCacher::ObjectSet oset; // ORDER DEPENDENCY: ino

  uint64_t reported_size = 0;
  uint64_t wanted_max_size = 0;
  uint64_t requested_max_size = 0;

  uint64_t  ll_ref = 0;   // separate ref count for ll client
  xlist<Dentry *> dentries; // if i'm linked to a dentry.
  std::string    symlink;  // symlink content, if it's a symlink
  std::map<std::string,bufferptr> xattrs;
  std::map<frag_t,int> fragmap;  // known frag -> mds mappings
  std::map<frag_t, std::vector<mds_rank_t>> frag_repmap; // non-auth mds mappings

  std::list<Context*> waitfor_caps;
  std::list<Context*> waitfor_caps_pending;
  std::list<Context*> waitfor_commit;
  std::list<ceph::condition_variable*> waitfor_deleg;

  Dentry *get_first_parent() {
    ceph_assert(!dentries.empty());
    return *dentries.begin();
  }

  void make_long_path(filepath& p);
  void make_short_path(filepath& p);
  bool make_path_string(std::string& s);
  void make_nosnap_relative_path(filepath& p);

  // The ref count. 1 for each dentry, fh, inode_map,
  // cwd that links to me.
  void iget() { get(); }
  void iput(int n=1) { ceph_assert(n >= 0); while (n--) put(); }

  void ll_get() {
    ll_ref++;
  }
  void ll_put(uint64_t n=1) {
    ceph_assert(ll_ref >= n);
    ll_ref -= n;
  }

  // file locks
  std::unique_ptr<ceph_lock_state_t> fcntl_locks;
  std::unique_ptr<ceph_lock_state_t> flock_locks;

  bool has_any_filelocks() {
    return
      (fcntl_locks && !fcntl_locks->empty()) ||
      (flock_locks && !flock_locks->empty());
  }

  std::list<Delegation> delegations;

  xlist<MetaRequest*> unsafe_ops;

  std::set<Fh*> fhs;

  mds_rank_t dir_pin = MDS_RANK_NONE;

  Inode() = delete;
  Inode(Client *c, vinodeno_t vino, file_layout_t *newlayout)
    : client(c), ino(vino.ino), snapid(vino.snapid), delay_cap_item(this),
      dirty_cap_item(this), flushing_cap_item(this), snaprealm_item(this),
      oset((void *)this, newlayout->pool_id, this->ino) {}
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

  bool check_mode(const UserPerm& perms, unsigned want);

  // CAPS --------
  void get_open_ref(int mode);
  bool put_open_ref(int mode);

  void get_cap_ref(int cap);
  int put_cap_ref(int cap);
  bool is_any_caps();
  bool cap_is_valid(const Cap &cap) const;
  int caps_issued(int *implemented = 0) const;
  void try_touch_cap(mds_rank_t mds);
  bool caps_issued_mask(unsigned mask, bool allow_impl=false);
  int caps_used();
  int caps_file_wanted();
  int caps_wanted();
  int caps_mds_wanted();
  int caps_dirty();
  const UserPerm *get_best_perms();

  bool have_valid_size();
  Dir *open_dir();

  void add_fh(Fh *f) {fhs.insert(f);}
  void rm_fh(Fh *f) {fhs.erase(f);}
  void set_async_err(int r);
  void dump(Formatter *f) const;

  void break_all_delegs() { break_deleg(false); };

  void recall_deleg(bool skip_read);
  bool has_recalled_deleg();
  int set_deleg(Fh *fh, unsigned type, ceph_deleg_cb_t cb, void *priv);
  void unset_deleg(Fh *fh);

  void mark_caps_dirty(int caps);
  void mark_caps_clean();
private:
  // how many opens for write on this Inode?
  long open_count_for_write()
  {
    return (long)(open_by_mode[CEPH_FILE_MODE_RDWR] +
		  open_by_mode[CEPH_FILE_MODE_WR]);
  };

  // how many opens of any sort on this inode?
  long open_count()
  {
    return (long) std::accumulate(open_by_mode.begin(), open_by_mode.end(), 0,
				  [] (int value, const std::map<int, int>::value_type& p)
                   { return value + p.second; });
  };

  void break_deleg(bool skip_read);
  bool delegations_broken(bool skip_read);

};

std::ostream& operator<<(std::ostream &out, const Inode &in);

#endif
