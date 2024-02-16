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


#ifndef CEPH_CLIENT_H
#define CEPH_CLIENT_H

#include "common/CommandTable.h"
#include "common/Finisher.h"
#include "common/Timer.h"
#include "common/ceph_mutex.h"
#include "common/cmdparse.h"
#include "common/compiler_extensions.h"
#include "include/common_fwd.h"
#include "include/cephfs/ceph_ll_client.h"
#include "include/filepath.h"
#include "include/interval_set.h"
#include "include/lru.h"
#include "include/types.h"
#include "include/unordered_map.h"
#include "include/unordered_set.h"
#include "include/cephfs/metrics/Types.h"
#include "mds/mdstypes.h"
#include "include/cephfs/types.h"
#include "msg/Dispatcher.h"
#include "msg/MessageRef.h"
#include "msg/Messenger.h"
#include "osdc/ObjectCacher.h"

#include "RWRef.h"
#include "InodeRef.h"
#include "MetaSession.h"
#include "UserPerm.h"

#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>

using std::set;
using std::map;
using std::fstream;

class FSMap;
class FSMapUser;
class MonClient;


struct DirStat;
struct LeaseStat;
struct InodeStat;

class Filer;
class Objecter;
class WritebackHandler;

class MDSMap;
class Message;
class destructive_lock_ref_t;

enum {
  l_c_first = 20000,
  l_c_reply,
  l_c_lat,
  l_c_wrlat,
  l_c_read,
  l_c_fsync,
  l_c_md_avg,
  l_c_md_sqsum,
  l_c_md_ops,
  l_c_rd_avg,
  l_c_rd_sqsum,
  l_c_rd_ops,
  l_c_wr_avg,
  l_c_wr_sqsum,
  l_c_wr_ops,
  l_c_last,
};


class MDSCommandOp : public CommandOp
{
  public:
  mds_gid_t     mds_gid;

  explicit MDSCommandOp(ceph_tid_t t) : CommandOp(t) {}
};

/* error code for ceph_fuse */
#define CEPH_FUSE_NO_MDS_UP    -((1<<16)+0) /* no mds up deteced in ceph_fuse */
#define CEPH_FUSE_LAST         -((1<<16)+1) /* (unused) */

// ============================================
// types for my local metadata cache
/* basic structure:
   
 - Dentries live in an LRU loop.  they get expired based on last access.
      see include/lru.h.  items can be bumped to "mid" or "top" of list, etc.
 - Inode has ref count for each Fh, Dir, or Dentry that points to it.
 - when Inode ref goes to 0, it's expired.
 - when Dir is empty, it's removed (and it's Inode ref--)
 
*/

/* getdir result */
struct DirEntry {
  explicit DirEntry(const std::string &s) : d_name(s), stmask(0) {}
  DirEntry(const std::string &n, struct stat& s, int stm)
    : d_name(n), st(s), stmask(stm) {}

  std::string d_name;
  struct stat st;
  int stmask;
};

struct Cap;
class Dir;
class Dentry;
struct SnapRealm;
struct Fh;
struct CapSnap;

struct MetaRequest;
class ceph_lock_state_t;

// ========================================================
// client interface

struct dir_result_t {
  static const int SHIFT = 28;
  static const int64_t MASK = (1 << SHIFT) - 1;
  static const int64_t HASH = 0xFFULL << (SHIFT + 24); // impossible frag bits
  static const loff_t END = 1ULL << (SHIFT + 32);

  struct dentry {
    int64_t offset;
    std::string name;
    std::string alternate_name;
    InodeRef inode;
    explicit dentry(int64_t o) : offset(o) {}
    dentry(int64_t o, std::string n, std::string an, InodeRef in) :
      offset(o), name(std::move(n)), alternate_name(std::move(an)), inode(std::move(in)) {}
  };
  struct dentry_off_lt {
    bool operator()(const dentry& d, int64_t off) const {
      return dir_result_t::fpos_cmp(d.offset, off) < 0;
    }
  };


  explicit dir_result_t(Inode *in, const UserPerm& perms, int fd);


  static uint64_t make_fpos(unsigned h, unsigned l, bool hash) {
    uint64_t v =  ((uint64_t)h<< SHIFT) | (uint64_t)l;
    if (hash)
      v |= HASH;
    else
      ceph_assert((v & HASH) != HASH);
    return v;
  }
  static unsigned fpos_high(uint64_t p) {
    unsigned v = (p & (END-1)) >> SHIFT;
    if ((p & HASH) == HASH)
      return ceph_frag_value(v);
    return v;
  }
  static unsigned fpos_low(uint64_t p) {
    return p & MASK;
  }
  static int fpos_cmp(uint64_t l, uint64_t r) {
    int c = ceph_frag_compare(fpos_high(l), fpos_high(r));
    if (c)
      return c;
    if (fpos_low(l) == fpos_low(r))
      return 0;
    return fpos_low(l) < fpos_low(r) ? -1 : 1;
  }

  unsigned offset_high() { return fpos_high(offset); }
  unsigned offset_low() { return fpos_low(offset); }

  void set_end() { offset |= END; }
  bool at_end() { return (offset & END); }

  void set_hash_order() { offset |= HASH; }
  bool hash_order() { return (offset & HASH) == HASH; }

  bool is_cached() {
    if (buffer.empty())
      return false;
    if (hash_order()) {
      return buffer_frag.contains(offset_high());
    } else {
      return buffer_frag == frag_t(offset_high());
    }
  }

  void reset() {
    last_name.clear();
    next_offset = 2;
    offset = 0;
    ordered_count = 0;
    cache_index = 0;
    buffer.clear();
  }

  InodeRef inode;
  int64_t offset;        // hash order:
			 //   (0xff << 52) | ((24 bits hash) << 28) |
			 //   (the nth entry has hash collision);
			 // frag+name order;
			 //   ((frag value) << 28) | (the nth entry in frag);

  unsigned next_offset;  // offset of next chunk (last_name's + 1)
  std::string last_name;      // last entry in previous chunk

  uint64_t release_count;
  uint64_t ordered_count;
  unsigned cache_index;
  int start_shared_gen;  // dir shared_gen at start of readdir
  UserPerm perms;

  frag_t buffer_frag;

  std::vector<dentry> buffer;
  struct dirent de;

  int fd;                // fd attached using fdopendir (-1 if none)
};

class Client : public Dispatcher, public md_config_obs_t {
public:
  friend class C_Block_Sync; // Calls block map and protected helpers
  friend class C_Client_CacheInvalidate;  // calls ino_invalidate_cb
  friend class C_Client_DentryInvalidate;  // calls dentry_invalidate_cb
  friend class C_Client_FlushComplete; // calls put_inode()
  friend class C_Client_Remount;
  friend class C_Client_RequestInterrupt;
  friend class C_Deleg_Timeout; // Asserts on client_lock, called when a delegation is unreturned
  friend class C_Client_CacheRelease; // Asserts on client_lock
  friend class SyntheticClient;
  friend void intrusive_ptr_release(Inode *in);
  template <typename T> friend struct RWRefState;
  template <typename T> friend class RWRef;

  using Dispatcher::cct;
  using clock = ceph::coarse_mono_clock;

  typedef int (*add_dirent_cb_t)(void *p, struct dirent *de, struct ceph_statx *stx, off_t off, Inode *in);

  struct walk_dentry_result {
    InodeRef in;
    std::string alternate_name;
  };

  class CommandHook : public AdminSocketHook {
  public:
    explicit CommandHook(Client *client);
    int call(std::string_view command, const cmdmap_t& cmdmap,
	     const bufferlist&,
	     Formatter *f,
	     std::ostream& errss,
	     bufferlist& out) override;
  private:
    Client *m_client;
  };

  // snapshot info returned via get_snap_info(). nothing to do
  // with SnapInfo on the MDS.
  struct SnapInfo {
    snapid_t id;
    std::map<std::string, std::string> metadata;
  };

  Client(Messenger *m, MonClient *mc, Objecter *objecter_);
  Client(const Client&) = delete;
  Client(const Client&&) = delete;
  virtual ~Client() override;

  static UserPerm pick_my_perms(CephContext *c) {
    uid_t uid = c->_conf->client_mount_uid >= 0 ? c->_conf->client_mount_uid : -1;
    gid_t gid = c->_conf->client_mount_gid >= 0 ? c->_conf->client_mount_gid : -1;
    return UserPerm(uid, gid);
  }
  UserPerm pick_my_perms() {
    uid_t uid = user_id >= 0 ? user_id : -1;
    gid_t gid = group_id >= 0 ? group_id : -1;
    return UserPerm(uid, gid);
  }

  int mount(const std::string &mount_root, const UserPerm& perms,
	    bool require_mds=false, const std::string &fs_name="");
  void unmount();
  bool is_unmounting() const {
    return mount_state.check_current_state(CLIENT_UNMOUNTING);
  }
  bool is_mounted() const {
    return mount_state.check_current_state(CLIENT_MOUNTED);
  }
  bool is_mounting() const {
    return mount_state.check_current_state(CLIENT_MOUNTING);
  }
  bool is_initialized() const {
    return initialize_state.check_current_state(CLIENT_INITIALIZED);
  }
  void abort_conn();

  void set_uuid(const std::string& uuid);
  void set_session_timeout(unsigned timeout);
  int start_reclaim(const std::string& uuid, unsigned flags,
		    const std::string& fs_name);
  void finish_reclaim();

  fs_cluster_id_t get_fs_cid() {
    return fscid;
  }

  int mds_command(
    const std::string &mds_spec,
    const std::vector<std::string>& cmd,
    const bufferlist& inbl,
    bufferlist *poutbl, std::string *prs, Context *onfinish);

  // these should (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statvfs *stbuf, const UserPerm& perms);

  // crap
  int chdir(const char *s, std::string &new_cwd, const UserPerm& perms);
  void _getcwd(std::string& cwd, const UserPerm& perms);
  void getcwd(std::string& cwd, const UserPerm& perms);

  // namespace ops
  int opendir(const char *name, dir_result_t **dirpp, const UserPerm& perms);
  int fdopendir(int dirfd, dir_result_t **dirpp, const UserPerm& perms);
  int closedir(dir_result_t *dirp);

  /**
   * Fill a directory listing from dirp, invoking cb for each entry
   * with the given pointer, the dirent, the struct stat, the stmask,
   * and the offset.
   *
   * Returns 0 if it reached the end of the directory.
   * If @a cb returns a negative error code, stop and return that.
   */
  int readdir_r_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p,
		   unsigned want=0, unsigned flags=AT_STATX_DONT_SYNC,
		   bool getref=false);

  struct dirent * readdir(dir_result_t *d);
  int readdir_r(dir_result_t *dirp, struct dirent *de);
  int readdirplus_r(dir_result_t *dirp, struct dirent *de, struct ceph_statx *stx, unsigned want, unsigned flags, Inode **out);

  /*
   * Get the next snapshot delta entry.
   *
   */
  int readdir_snapdiff(dir_result_t* dir1, snapid_t snap2,
    struct dirent* out_de, snapid_t* out_snap);

  int getdir(const char *relpath, std::list<std::string>& names,
	     const UserPerm& perms);  // get the whole dir at once.

  /**
   * Returns the length of the buffer that got filled in, or -errno.
   * If it returns -CEPHFS_ERANGE you just need to increase the size of the
   * buffer and try again.
   */
  int _getdents(dir_result_t *dirp, char *buf, int buflen, bool ful);  // get a bunch of dentries at once
  int getdents(dir_result_t *dirp, char *buf, int buflen) {
    return _getdents(dirp, buf, buflen, true);
  }
  int getdnames(dir_result_t *dirp, char *buf, int buflen) {
    return _getdents(dirp, buf, buflen, false);
  }

  void rewinddir(dir_result_t *dirp);
  loff_t telldir(dir_result_t *dirp);
  void seekdir(dir_result_t *dirp, loff_t offset);

  int may_delete(const char *relpath, const UserPerm& perms);
  int link(const char *existing, const char *newname, const UserPerm& perm, std::string alternate_name="");
  int unlink(const char *path, const UserPerm& perm);
  int unlinkat(int dirfd, const char *relpath, int flags, const UserPerm& perm);
  int rename(const char *from, const char *to, const UserPerm& perm, std::string alternate_name="");

  // dirs
  int mkdir(const char *path, mode_t mode, const UserPerm& perm, std::string alternate_name="");
  int mkdirat(int dirfd, const char *relpath, mode_t mode, const UserPerm& perm,
              std::string alternate_name="");
  int mkdirs(const char *path, mode_t mode, const UserPerm& perms);
  int rmdir(const char *path, const UserPerm& perms);

  // symlinks
  int readlink(const char *path, char *buf, loff_t size, const UserPerm& perms);
  int readlinkat(int dirfd, const char *relpath, char *buf, loff_t size, const UserPerm& perms);

  int symlink(const char *existing, const char *newname, const UserPerm& perms, std::string alternate_name="");
  int symlinkat(const char *target, int dirfd, const char *relpath, const UserPerm& perms,
                std::string alternate_name="");

  // path traversal for high-level interface
  int walk(std::string_view path, struct walk_dentry_result* result, const UserPerm& perms, bool followsym=true);

  // inode stuff
  unsigned statx_to_mask(unsigned int flags, unsigned int want);
  int stat(const char *path, struct stat *stbuf, const UserPerm& perms,
	   frag_info_t *dirstat=0, int mask=CEPH_STAT_CAP_INODE_ALL);
  int statx(const char *path, struct ceph_statx *stx,
	    const UserPerm& perms,
	    unsigned int want, unsigned int flags);
  int lstat(const char *path, struct stat *stbuf, const UserPerm& perms,
	    frag_info_t *dirstat=0, int mask=CEPH_STAT_CAP_INODE_ALL);

  int setattr(const char *relpath, struct stat *attr, int mask,
	      const UserPerm& perms);
  int setattrx(const char *relpath, struct ceph_statx *stx, int mask,
	       const UserPerm& perms, int flags=0);
  int fsetattr(int fd, struct stat *attr, int mask, const UserPerm& perms);
  int fsetattrx(int fd, struct ceph_statx *stx, int mask, const UserPerm& perms);
  int chmod(const char *path, mode_t mode, const UserPerm& perms);
  int fchmod(int fd, mode_t mode, const UserPerm& perms);
  int chmodat(int dirfd, const char *relpath, mode_t mode, int flags, const UserPerm& perms);
  int lchmod(const char *path, mode_t mode, const UserPerm& perms);
  int chown(const char *path, uid_t new_uid, gid_t new_gid,
	    const UserPerm& perms);
  int fchown(int fd, uid_t new_uid, gid_t new_gid, const UserPerm& perms);
  int lchown(const char *path, uid_t new_uid, gid_t new_gid,
	     const UserPerm& perms);
  int chownat(int dirfd, const char *relpath, uid_t new_uid, gid_t new_gid,
              int flags, const UserPerm& perms);
  int utime(const char *path, struct utimbuf *buf, const UserPerm& perms);
  int lutime(const char *path, struct utimbuf *buf, const UserPerm& perms);
  int futime(int fd, struct utimbuf *buf, const UserPerm& perms);
  int utimes(const char *relpath, struct timeval times[2], const UserPerm& perms);
  int lutimes(const char *relpath, struct timeval times[2], const UserPerm& perms);
  int futimes(int fd, struct timeval times[2], const UserPerm& perms);
  int futimens(int fd, struct timespec times[2], const UserPerm& perms);
  int utimensat(int dirfd, const char *relpath, struct timespec times[2], int flags,
                const UserPerm& perms);
  int flock(int fd, int operation, uint64_t owner);
  int truncate(const char *path, loff_t size, const UserPerm& perms);

  // file ops
  int mknod(const char *path, mode_t mode, const UserPerm& perms, dev_t rdev=0);

  int create_and_open(int dirfd, const char *relpath, int flags, const UserPerm& perms,
                      mode_t mode, int stripe_unit, int stripe_count, int object_size,
                      const char *data_pool, std::string alternate_name);
  int open(const char *path, int flags, const UserPerm& perms, mode_t mode=0, std::string alternate_name="") {
    return open(path, flags, perms, mode, 0, 0, 0, NULL, alternate_name);
  }
  int open(const char *path, int flags, const UserPerm& perms,
	   mode_t mode, int stripe_unit, int stripe_count, int object_size,
	   const char *data_pool, std::string alternate_name="");
  int openat(int dirfd, const char *relpath, int flags, const UserPerm& perms,
             mode_t mode, int stripe_unit, int stripe_count,
             int object_size, const char *data_pool, std::string alternate_name);
  int openat(int dirfd, const char *path, int flags, const UserPerm& perms, mode_t mode=0,
             std::string alternate_name="") {
    return openat(dirfd, path, flags, perms, mode, 0, 0, 0, NULL, alternate_name);
  }

  int lookup_hash(inodeno_t ino, inodeno_t dirino, const char *name,
		  const UserPerm& perms);
  int lookup_ino(inodeno_t ino, const UserPerm& perms, Inode **inode=NULL);
  int lookup_name(Inode *in, Inode *parent, const UserPerm& perms);
  int _close(int fd);
  int close(int fd);
  loff_t lseek(int fd, loff_t offset, int whence);
  int read(int fd, char *buf, loff_t size, loff_t offset=-1);
  int preadv(int fd, const struct iovec *iov, int iovcnt, loff_t offset=-1);
  int write(int fd, const char *buf, loff_t size, loff_t offset=-1);
  int pwritev(int fd, const struct iovec *iov, int iovcnt, loff_t offset=-1);
  int fake_write_size(int fd, loff_t size);
  int ftruncate(int fd, loff_t size, const UserPerm& perms);
  int fsync(int fd, bool syncdataonly);
  int fstat(int fd, struct stat *stbuf, const UserPerm& perms,
	    int mask=CEPH_STAT_CAP_INODE_ALL);
  int fstatx(int fd, struct ceph_statx *stx, const UserPerm& perms,
	     unsigned int want, unsigned int flags);
  int statxat(int dirfd, const char *relpath,
              struct ceph_statx *stx, const UserPerm& perms,
              unsigned int want, unsigned int flags);
  int fallocate(int fd, int mode, loff_t offset, loff_t length);

  // full path xattr ops
  int getxattr(const char *path, const char *name, void *value, size_t size,
	       const UserPerm& perms);
  int lgetxattr(const char *path, const char *name, void *value, size_t size,
		const UserPerm& perms);
  int fgetxattr(int fd, const char *name, void *value, size_t size,
		const UserPerm& perms);
  int listxattr(const char *path, char *list, size_t size, const UserPerm& perms);
  int llistxattr(const char *path, char *list, size_t size, const UserPerm& perms);
  int flistxattr(int fd, char *list, size_t size, const UserPerm& perms);
  int removexattr(const char *path, const char *name, const UserPerm& perms);
  int lremovexattr(const char *path, const char *name, const UserPerm& perms);
  int fremovexattr(int fd, const char *name, const UserPerm& perms);
  int setxattr(const char *path, const char *name, const void *value,
	       size_t size, int flags, const UserPerm& perms);
  int lsetxattr(const char *path, const char *name, const void *value,
		size_t size, int flags, const UserPerm& perms);
  int fsetxattr(int fd, const char *name, const void *value, size_t size,
		int flags, const UserPerm& perms);

  int sync_fs();
  int64_t drop_caches();

  int get_snap_info(const char *path, const UserPerm &perms, SnapInfo *snap_info);

  // hpc lazyio
  int lazyio(int fd, int enable);
  int lazyio_propagate(int fd, loff_t offset, size_t count);
  int lazyio_synchronize(int fd, loff_t offset, size_t count);

  // expose file layout
  int describe_layout(const char *path, file_layout_t* layout,
		      const UserPerm& perms);
  int fdescribe_layout(int fd, file_layout_t* layout);
  int get_file_stripe_address(int fd, loff_t offset, std::vector<entity_addr_t>& address);
  int get_file_extent_osds(int fd, loff_t off, loff_t *len, std::vector<int>& osds);
  int get_osd_addr(int osd, entity_addr_t& addr);

  // expose mdsmap
  int64_t get_default_pool_id();

  // expose osdmap
  int get_local_osd();
  int get_pool_replication(int64_t pool);
  int64_t get_pool_id(const char *pool_name);
  std::string get_pool_name(int64_t pool);
  int get_osd_crush_location(int id, std::vector<std::pair<std::string, std::string> >& path);

  int enumerate_layout(int fd, std::vector<ObjectExtent>& result,
		       loff_t length, loff_t offset);

  int mksnap(const char *path, const char *name, const UserPerm& perm,
             mode_t mode=0, const std::map<std::string, std::string> &metadata={});
  int rmsnap(const char *path, const char *name, const UserPerm& perm, bool check_perms=false);

  // Inode permission checking
  int inode_permission(Inode *in, const UserPerm& perms, unsigned want);

  // expose caps
  int get_caps_issued(int fd);
  int get_caps_issued(const char *path, const UserPerm& perms);

  snapid_t ll_get_snapid(Inode *in);
  vinodeno_t ll_get_vino(Inode *in) {
    std::lock_guard lock(client_lock);
    return _get_vino(in);
  }
  // get inode from faked ino
  Inode *ll_get_inode(ino_t ino);
  Inode *ll_get_inode(vinodeno_t vino);
  int ll_lookup(Inode *parent, const char *name, struct stat *attr,
		Inode **out, const UserPerm& perms);
  int ll_lookup_inode(struct inodeno_t ino, const UserPerm& perms, Inode **inode);
  int ll_lookup_vino(vinodeno_t vino, const UserPerm& perms, Inode **inode);
  int ll_lookupx(Inode *parent, const char *name, Inode **out,
			struct ceph_statx *stx, unsigned want, unsigned flags,
			const UserPerm& perms);
  bool ll_forget(Inode *in, uint64_t count);
  bool ll_put(Inode *in);
  int ll_get_snap_ref(snapid_t snap);

  int ll_getattr(Inode *in, struct stat *st, const UserPerm& perms);
  int ll_getattrx(Inode *in, struct ceph_statx *stx, unsigned int want,
		  unsigned int flags, const UserPerm& perms);
  int ll_setattrx(Inode *in, struct ceph_statx *stx, int mask,
		  const UserPerm& perms);
  int ll_setattr(Inode *in, struct stat *st, int mask,
		 const UserPerm& perms);
  int ll_getxattr(Inode *in, const char *name, void *value, size_t size,
		  const UserPerm& perms);
  int ll_setxattr(Inode *in, const char *name, const void *value, size_t size,
		  int flags, const UserPerm& perms);
  int ll_removexattr(Inode *in, const char *name, const UserPerm& perms);
  int ll_listxattr(Inode *in, char *list, size_t size, const UserPerm& perms);
  int ll_opendir(Inode *in, int flags, dir_result_t **dirpp,
		 const UserPerm& perms);
  int ll_releasedir(dir_result_t* dirp);
  int ll_fsyncdir(dir_result_t* dirp);
  int ll_readlink(Inode *in, char *buf, size_t bufsize, const UserPerm& perms);
  int ll_mknod(Inode *in, const char *name, mode_t mode, dev_t rdev,
	       struct stat *attr, Inode **out, const UserPerm& perms);
  int ll_mknodx(Inode *parent, const char *name, mode_t mode, dev_t rdev,
	        Inode **out, struct ceph_statx *stx, unsigned want,
		unsigned flags, const UserPerm& perms);
  int ll_mkdir(Inode *in, const char *name, mode_t mode, struct stat *attr,
	       Inode **out, const UserPerm& perm);
  int ll_mkdirx(Inode *parent, const char *name, mode_t mode, Inode **out,
		struct ceph_statx *stx, unsigned want, unsigned flags,
		const UserPerm& perms);
  int ll_symlink(Inode *in, const char *name, const char *value,
		 struct stat *attr, Inode **out, const UserPerm& perms);
  int ll_symlinkx(Inode *parent, const char *name, const char *value,
		  Inode **out, struct ceph_statx *stx, unsigned want,
		  unsigned flags, const UserPerm& perms);
  int ll_unlink(Inode *in, const char *name, const UserPerm& perm);
  int ll_rmdir(Inode *in, const char *name, const UserPerm& perms);
  int ll_rename(Inode *parent, const char *name, Inode *newparent,
		const char *newname, const UserPerm& perm);
  int ll_link(Inode *in, Inode *newparent, const char *newname,
	      const UserPerm& perm);
  int ll_open(Inode *in, int flags, Fh **fh, const UserPerm& perms);
  int _ll_create(Inode *parent, const char *name, mode_t mode,
	      int flags, InodeRef *in, int caps, Fh **fhp,
	      const UserPerm& perms);
  int ll_create(Inode *parent, const char *name, mode_t mode, int flags,
		struct stat *attr, Inode **out, Fh **fhp,
		const UserPerm& perms);
  int ll_createx(Inode *parent, const char *name, mode_t mode,
		int oflags, Inode **outp, Fh **fhp,
		struct ceph_statx *stx, unsigned want, unsigned lflags,
		const UserPerm& perms);
  int ll_read_block(Inode *in, uint64_t blockid, char *buf,  uint64_t offset,
		    uint64_t length, file_layout_t* layout);

  int ll_write_block(Inode *in, uint64_t blockid,
		     char* buf, uint64_t offset,
		     uint64_t length, file_layout_t* layout,
		     uint64_t snapseq, uint32_t sync);
  int ll_commit_blocks(Inode *in, uint64_t offset, uint64_t length);

  int ll_statfs(Inode *in, struct statvfs *stbuf, const UserPerm& perms);
  int ll_walk(const char* name, Inode **i, struct ceph_statx *stx,
	       unsigned int want, unsigned int flags, const UserPerm& perms);
  uint32_t ll_stripe_unit(Inode *in);
  int ll_file_layout(Inode *in, file_layout_t *layout);
  uint64_t ll_snap_seq(Inode *in);

  int ll_read(Fh *fh, loff_t off, loff_t len, bufferlist *bl);
  int ll_write(Fh *fh, loff_t off, loff_t len, const char *data);
  int64_t ll_readv(struct Fh *fh, const struct iovec *iov, int iovcnt, int64_t off);
  int64_t ll_writev(struct Fh *fh, const struct iovec *iov, int iovcnt, int64_t off);
  loff_t ll_lseek(Fh *fh, loff_t offset, int whence);
  int ll_flush(Fh *fh);
  int ll_fsync(Fh *fh, bool syncdataonly);
  int ll_sync_inode(Inode *in, bool syncdataonly);
  int ll_fallocate(Fh *fh, int mode, int64_t offset, int64_t length);
  int ll_release(Fh *fh);
  int ll_getlk(Fh *fh, struct flock *fl, uint64_t owner);
  int ll_setlk(Fh *fh, struct flock *fl, uint64_t owner, int sleep);
  int ll_flock(Fh *fh, int cmd, uint64_t owner);
  int ll_lazyio(Fh *fh, int enable);
  int ll_file_layout(Fh *fh, file_layout_t *layout);
  void ll_interrupt(void *d);
  bool ll_handle_umask() {
    return acl_type != NO_ACL;
  }

  int ll_get_stripe_osd(struct Inode *in, uint64_t blockno,
			file_layout_t* layout);
  uint64_t ll_get_internal_offset(struct Inode *in, uint64_t blockno);

  int ll_num_osds(void);
  int ll_osdaddr(int osd, uint32_t *addr);
  int ll_osdaddr(int osd, char* buf, size_t size);

  void _ll_register_callbacks(struct ceph_client_callback_args *args);
  void ll_register_callbacks(struct ceph_client_callback_args *args); // deprecated
  int ll_register_callbacks2(struct ceph_client_callback_args *args);
  std::pair<int, bool> test_dentry_handling(bool can_invalidate);

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
	                          const std::set <std::string> &changed) override;
  uint32_t get_deleg_timeout() { return deleg_timeout; }
  int set_deleg_timeout(uint32_t timeout);
  int ll_delegation(Fh *fh, unsigned cmd, ceph_deleg_cb_t cb, void *priv);

  entity_name_t get_myname() { return messenger->get_myname(); }
  void wait_on_list(std::list<ceph::condition_variable*>& ls);
  void signal_cond_list(std::list<ceph::condition_variable*>& ls);

  void set_filer_flags(int flags);
  void clear_filer_flags(int flags);

  void tear_down_cache();

  void update_metadata(std::string const &k, std::string const &v);

  client_t get_nodeid() { return whoami; }

  inodeno_t get_root_ino();
  Inode *get_root();

  virtual int init();
  virtual void shutdown();

  // messaging
  void cancel_commands(const MDSMap& newmap);
  void handle_mds_map(const MConstRef<MMDSMap>& m);
  void handle_fs_map(const MConstRef<MFSMap>& m);
  void handle_fs_map_user(const MConstRef<MFSMapUser>& m);
  void handle_osd_map(const MConstRef<MOSDMap>& m);

  void handle_lease(const MConstRef<MClientLease>& m);

  // inline data
  int uninline_data(Inode *in, Context *onfinish);

  // file caps
  void check_cap_issue(Inode *in, unsigned issued);
  void add_update_cap(Inode *in, MetaSession *session, uint64_t cap_id,
		      unsigned issued, unsigned wanted, unsigned seq, unsigned mseq,
		      inodeno_t realm, int flags, const UserPerm& perms);
  void remove_cap(Cap *cap, bool queue_release);
  void remove_all_caps(Inode *in);
  void remove_session_caps(MetaSession *session, int err);
  int mark_caps_flushing(Inode *in, ceph_tid_t *ptid);
  void adjust_session_flushing_caps(Inode *in, MetaSession *old_s, MetaSession *new_s);
  void flush_caps_sync();
  void kick_flushing_caps(Inode *in, MetaSession *session);
  void kick_flushing_caps(MetaSession *session);
  void early_kick_flushing_caps(MetaSession *session);
  int get_caps(Fh *fh, int need, int want, int *have, loff_t endoff);
  int get_caps_used(Inode *in);

  void maybe_update_snaprealm(SnapRealm *realm, snapid_t snap_created, snapid_t snap_highwater,
			      std::vector<snapid_t>& snaps);

  void handle_quota(const MConstRef<MClientQuota>& m);
  void handle_snap(const MConstRef<MClientSnap>& m);
  void handle_caps(const MConstRef<MClientCaps>& m);
  void handle_cap_import(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m);
  void handle_cap_export(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m);
  void handle_cap_trunc(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m);
  void handle_cap_flush_ack(MetaSession *session, Inode *in, Cap *cap, const MConstRef<MClientCaps>& m);
  void handle_cap_flushsnap_ack(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m);
  void handle_cap_grant(MetaSession *session, Inode *in, Cap *cap, const MConstRef<MClientCaps>& m);
  void cap_delay_requeue(Inode *in);

  void send_cap(Inode *in, MetaSession *session, Cap *cap, int flags,
		int used, int want, int retain, int flush,
		ceph_tid_t flush_tid);

  void send_flush_snap(Inode *in, MetaSession *session, snapid_t follows, CapSnap& capsnap);

  void flush_snaps(Inode *in);
  void get_cap_ref(Inode *in, int cap);
  void put_cap_ref(Inode *in, int cap);
  void wait_sync_caps(Inode *in, ceph_tid_t want);
  void wait_sync_caps(ceph_tid_t want);
  void queue_cap_snap(Inode *in, SnapContext &old_snapc);
  void finish_cap_snap(Inode *in, CapSnap &capsnap, int used);

  void _schedule_invalidate_dentry_callback(Dentry *dn, bool del);
  void _async_dentry_invalidate(vinodeno_t dirino, vinodeno_t ino, std::string& name);
  void _try_to_trim_inode(Inode *in, bool sched_inval);

  void _schedule_invalidate_callback(Inode *in, int64_t off, int64_t len);
  void _invalidate_inode_cache(Inode *in);
  void _invalidate_inode_cache(Inode *in, int64_t off, int64_t len);
  void _async_invalidate(vinodeno_t ino, int64_t off, int64_t len);

  void _schedule_ino_release_callback(Inode *in);
  void _async_inode_release(vinodeno_t ino);

  bool _release(Inode *in);

  /**
   * Initiate a flush of the data associated with the given inode.
   * If you specify a Context, you are responsible for holding an inode
   * reference for the duration of the flush. If not, _flush() will
   * take the reference for you.
   * @param in The Inode whose data you wish to flush.
   * @param c The Context you wish us to complete once the data is
   * flushed. If already flushed, this will be called in-line.
   *
   * @returns true if the data was already flushed, false otherwise.
   */
  bool _flush(Inode *in, Context *c);
  void _flush_range(Inode *in, int64_t off, uint64_t size);
  void _flushed(Inode *in);
  void flush_set_callback(ObjectCacher::ObjectSet *oset);

  void close_release(Inode *in);
  void close_safe(Inode *in);

  void lock_fh_pos(Fh *f);
  void unlock_fh_pos(Fh *f);

  // metadata cache
  void update_dir_dist(Inode *in, DirStat *st, mds_rank_t from);

  void clear_dir_complete_and_ordered(Inode *diri, bool complete);
  void insert_readdir_results(MetaRequest *request, MetaSession *session,
                              Inode *diri, Inode *diri_other);
  Inode* insert_trace(MetaRequest *request, MetaSession *session);
  void update_inode_file_size(Inode *in, int issued, uint64_t size,
			      uint64_t truncate_seq, uint64_t truncate_size);
  void update_inode_file_time(Inode *in, int issued, uint64_t time_warp_seq,
			      utime_t ctime, utime_t mtime, utime_t atime);

  Inode *add_update_inode(InodeStat *st, utime_t ttl, MetaSession *session,
			  const UserPerm& request_perms);
  Dentry *insert_dentry_inode(Dir *dir, const std::string& dname, LeaseStat *dlease,
			      Inode *in, utime_t from, MetaSession *session,
			      Dentry *old_dentry = NULL);
  void update_dentry_lease(Dentry *dn, LeaseStat *dlease, utime_t from, MetaSession *session);

  bool use_faked_inos() { return _use_faked_inos; }
  vinodeno_t map_faked_ino(ino_t ino);

  //notify the mds to flush the mdlog
  void flush_mdlog_sync(Inode *in);
  void flush_mdlog_sync();
  void flush_mdlog(MetaSession *session);

  void renew_caps();
  void renew_caps(MetaSession *session);
  void flush_cap_releases();
  void renew_and_flush_cap_releases();
  void tick();
  void start_tick_thread();

  void update_read_io_size(size_t size) {
    total_read_ops++;
    total_read_size += size;
  }

  void update_write_io_size(size_t size) {
    total_write_ops++;
    total_write_size += size;
  }

  void inc_dentry_nr() {
    ++dentry_nr;
  }
  void dec_dentry_nr() {
    --dentry_nr;
  }
  void dlease_hit() {
    ++dlease_hits;
  }
  void dlease_miss() {
    ++dlease_misses;
  }
  std::tuple<uint64_t, uint64_t, uint64_t> get_dlease_hit_rates() {
    return std::make_tuple(dlease_hits, dlease_misses, dentry_nr);
  }

  void cap_hit() {
    ++cap_hits;
  }
  void cap_miss() {
    ++cap_misses;
  }
  std::pair<uint64_t, uint64_t> get_cap_hit_rates() {
    return std::make_pair(cap_hits, cap_misses);
  }

  void inc_opened_files() {
    ++opened_files;
  }
  void dec_opened_files() {
    --opened_files;
  }
  std::pair<uint64_t, uint64_t> get_opened_files_rates() {
    return std::make_pair(opened_files, inode_map.size());
  }

  void inc_pinned_icaps() {
    ++pinned_icaps;
  }
  void dec_pinned_icaps(uint64_t nr=1) {
    pinned_icaps -= nr;
  }
  std::pair<uint64_t, uint64_t> get_pinned_icaps_rates() {
    return std::make_pair(pinned_icaps, inode_map.size());
  }

  void inc_opened_inodes() {
    ++opened_inodes;
  }
  void dec_opened_inodes() {
    --opened_inodes;
  }
  std::pair<uint64_t, uint64_t> get_opened_inodes_rates() {
    return std::make_pair(opened_inodes, inode_map.size());
  }

  /* timer_lock for 'timer' */
  ceph::mutex timer_lock = ceph::make_mutex("Client::timer_lock");
  SafeTimer timer;

  /* tick thread */
  std::thread upkeeper;
  ceph::condition_variable upkeep_cond;
  bool tick_thread_stopped = false;

  std::unique_ptr<PerfCounters> logger;
  std::unique_ptr<MDSMap> mdsmap;

  bool fuse_default_permissions;
  bool _collect_and_send_global_metrics;

protected:
  std::list<ceph::condition_variable*> waiting_for_reclaim;
  /* Flags for check_caps() */
  static const unsigned CHECK_CAPS_NODELAY = 0x1;
  static const unsigned CHECK_CAPS_SYNCHRONOUS = 0x2;

  void check_caps(Inode *in, unsigned flags);

  void set_cap_epoch_barrier(epoch_t e);

  void handle_command_reply(const MConstRef<MCommandReply>& m);
  int fetch_fsmap(bool user);
  int resolve_mds(
      const std::string &mds_spec,
      std::vector<mds_gid_t> *targets);

  void get_session_metadata(std::map<std::string, std::string> *meta) const;
  bool have_open_session(mds_rank_t mds);
  void got_mds_push(MetaSession *s);
  MetaSessionRef _get_mds_session(mds_rank_t mds, Connection *con);  ///< return session for mds *and* con; null otherwise
  MetaSessionRef _get_or_open_mds_session(mds_rank_t mds);
  MetaSessionRef _open_mds_session(mds_rank_t mds);
  void _close_mds_session(MetaSession *s);
  void _closed_mds_session(MetaSession *s, int err=0, bool rejected=false);
  bool _any_stale_sessions() const;
  void _kick_stale_sessions();
  void handle_client_session(const MConstRef<MClientSession>& m);
  void send_reconnect(MetaSession *s);
  void resend_unsafe_requests(MetaSession *s);
  void wait_unsafe_requests();

  void dump_mds_requests(Formatter *f);
  void dump_mds_sessions(Formatter *f, bool cap_dump=false);

  int make_request(MetaRequest *req, const UserPerm& perms,
                   InodeRef *ptarget = 0, bool *pcreated = 0,
                   mds_rank_t use_mds=-1, bufferlist *pdirbl=0,
                   size_t feature_needed=ULONG_MAX);
  void put_request(MetaRequest *request);
  void unregister_request(MetaRequest *request);

  int verify_reply_trace(int r, MetaSession *session, MetaRequest *request,
			 const MConstRef<MClientReply>& reply,
			 InodeRef *ptarget, bool *pcreated,
			 const UserPerm& perms);
  void encode_cap_releases(MetaRequest *request, mds_rank_t mds);
  int encode_inode_release(Inode *in, MetaRequest *req,
			   mds_rank_t mds, int drop,
			   int unless,int force=0);
  void encode_dentry_release(Dentry *dn, MetaRequest *req,
			     mds_rank_t mds, int drop, int unless);
  mds_rank_t choose_target_mds(MetaRequest *req, Inode** phash_diri=NULL);
  void connect_mds_targets(mds_rank_t mds);
  void send_request(MetaRequest *request, MetaSession *session,
		    bool drop_cap_releases=false);
  MRef<MClientRequest> build_client_request(MetaRequest *request, mds_rank_t mds);
  void kick_requests(MetaSession *session);
  void kick_requests_closed(MetaSession *session);
  void handle_client_request_forward(const MConstRef<MClientRequestForward>& reply);
  void handle_client_reply(const MConstRef<MClientReply>& reply);
  bool is_dir_operation(MetaRequest *request);

  int path_walk(const filepath& fp, struct walk_dentry_result* result, const UserPerm& perms, bool followsym=true, int mask=0,
                InodeRef dirinode=nullptr);
  int path_walk(const filepath& fp, InodeRef *end, const UserPerm& perms,
                bool followsym=true, int mask=0, InodeRef dirinode=nullptr);

  // fake inode number for 32-bits ino_t
  void _assign_faked_ino(Inode *in);
  void _assign_faked_root(Inode *in);
  void _release_faked_ino(Inode *in);
  void _reset_faked_inos();
  vinodeno_t _map_faked_ino(ino_t ino);

  // Optional extra metadata about me to send to the MDS
  void populate_metadata(const std::string &mount_root);

  SnapRealm *get_snap_realm(inodeno_t r);
  SnapRealm *get_snap_realm_maybe(inodeno_t r);
  void put_snap_realm(SnapRealm *realm);
  bool adjust_realm_parent(SnapRealm *realm, inodeno_t parent);
  void update_snap_trace(MetaSession *session, const bufferlist& bl, SnapRealm **realm_ret, bool must_flush=true);
  void invalidate_snaprealm_and_children(SnapRealm *realm);

  void refresh_snapdir_attrs(Inode *in, Inode *diri);
  Inode *open_snapdir(Inode *diri);

  int get_fd() {
    int fd = free_fd_set.range_start();
    free_fd_set.erase(fd, 1);
    return fd;
  }
  void put_fd(int fd) {
    free_fd_set.insert(fd, 1);
  }

  /*
   * Resolve file descriptor, or return NULL.
   */
  Fh *get_filehandle(int fd) {
    auto it = fd_map.find(fd);
    if (it == fd_map.end())
      return NULL;
    return it->second;
  }
  int get_fd_inode(int fd, InodeRef *in);

  // helpers
  void wake_up_session_caps(MetaSession *s, bool reconnect);

  void wait_on_context_list(std::list<Context*>& ls);
  void signal_context_list(std::list<Context*>& ls);

  // -- metadata cache stuff

  // decrease inode ref.  delete if dangling.
  void _put_inode(Inode *in, int n);
  void delay_put_inodes(bool wakeup=false);
  void put_inode(Inode *in, int n=1);
  void close_dir(Dir *dir);

  int subscribe_mdsmap(const std::string &fs_name="");

  void _abort_mds_sessions(int err);

  // same as unmount() but for when the client_lock is already held
  void _unmount(bool abort);

  //int get_cache_size() { return lru.lru_get_size(); }

  /**
   * Don't call this with in==NULL, use get_or_create for that
   * leave dn set to default NULL unless you're trying to add
   * a new inode to a pre-created Dentry
   */
  Dentry* link(Dir *dir, const std::string& name, Inode *in, Dentry *dn);
  void unlink(Dentry *dn, bool keepdir, bool keepdentry);

  int fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat=0, nest_info_t *rstat=0);
  int fill_stat(InodeRef& in, struct stat *st, frag_info_t *dirstat=0, nest_info_t *rstat=0) {
    return fill_stat(in.get(), st, dirstat, rstat);
  }

  void fill_statx(Inode *in, unsigned int mask, struct ceph_statx *stx);
  void fill_statx(InodeRef& in, unsigned int mask, struct ceph_statx *stx) {
    return fill_statx(in.get(), mask, stx);
  }

  void touch_dn(Dentry *dn);

  // trim cache.
  void trim_cache(bool trim_kernel_dcache=false);
  void trim_cache_for_reconnect(MetaSession *s);
  void trim_dentry(Dentry *dn);
  void trim_caps(MetaSession *s, uint64_t max);
  void _invalidate_kernel_dcache();
  void _trim_negative_child_dentries(InodeRef& in);

  void dump_inode(Formatter *f, Inode *in, set<Inode*>& did, bool disconnected);
  void dump_cache(Formatter *f);  // debug

  // force read-only
  void force_session_readonly(MetaSession *s);

  void dump_status(Formatter *f);  // debug

  bool ms_dispatch2(const MessageRef& m) override;

  void ms_handle_connect(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;

  int authenticate();

  Inode* get_quota_root(Inode *in, const UserPerm& perms, quota_max_t type=QUOTA_ANY);
  bool check_quota_condition(Inode *in, const UserPerm& perms,
			     std::function<bool (const Inode &)> test);
  bool is_quota_files_exceeded(Inode *in, const UserPerm& perms);
  bool is_quota_bytes_exceeded(Inode *in, int64_t new_bytes,
			       const UserPerm& perms);
  bool is_quota_bytes_approaching(Inode *in, const UserPerm& perms);

  int check_pool_perm(Inode *in, int need);

  void handle_client_reclaim_reply(const MConstRef<MClientReclaimReply>& reply);

  /**
   * Call this when an OSDMap is seen with a full flag (global or per pool)
   * set.
   *
   * @param pool the pool ID affected, or -1 if all.
   */
  void _handle_full_flag(int64_t pool);

  void _close_sessions();

  void _pre_init();

  /**
   * The basic housekeeping parts of init (perf counters, admin socket)
   * that is independent of how objecters/monclient/messengers are
   * being set up.
   */
  void _finish_init();

  // global client lock
  //  - protects Client and buffer cache both!
  ceph::mutex client_lock = ceph::make_mutex("Client::client_lock");

  std::map<snapid_t, int> ll_snap_ref;

  InodeRef               root = nullptr;
  map<Inode*, InodeRef>  root_parents;
  Inode*                 root_ancestor = nullptr;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  InodeRef cwd;

  std::unique_ptr<Filer>             filer;
  std::unique_ptr<ObjectCacher>      objectcacher;
  std::unique_ptr<WritebackHandler>  writeback_handler;

  Messenger *messenger;
  MonClient *monclient;
  Objecter  *objecter;

  client_t whoami;

  /* The state migration mechanism */
  enum _state {
    /* For the initialize_state */
    CLIENT_NEW, // The initial state for the initialize_state or after Client::shutdown()
    CLIENT_INITIALIZING, // At the beginning of the Client::init()
    CLIENT_INITIALIZED, // At the end of CLient::init()

    /* For the mount_state */
    CLIENT_UNMOUNTED, // The initial state for the mount_state or after unmounted
    CLIENT_MOUNTING, // At the beginning of Client::mount()
    CLIENT_MOUNTED, // At the end of Client::mount()
    CLIENT_UNMOUNTING, // At the beginning of the Client::_unmout()
  };

  typedef enum _state state_t;
  using RWRef_t = RWRef<state_t>;

  struct mount_state_t : public RWRefState<state_t> {
    public:
      bool is_valid_state(state_t state) const override {
        switch (state) {
	  case Client::CLIENT_MOUNTING:
	  case Client::CLIENT_MOUNTED:
	  case Client::CLIENT_UNMOUNTING:
	  case Client::CLIENT_UNMOUNTED:
            return true;
          default:
            return false;
        }
      }

      int check_reader_state(state_t require) const override {
        if (require == Client::CLIENT_MOUNTING &&
            (state == Client::CLIENT_MOUNTING || state == Client::CLIENT_MOUNTED))
          return true;
        else
          return false;
      }

      /* The state migration check */
      int check_writer_state(state_t require) const override {
        if (require == Client::CLIENT_MOUNTING &&
            state == Client::CLIENT_UNMOUNTED)
          return true;
	else if (require == Client::CLIENT_MOUNTED &&
            state == Client::CLIENT_MOUNTING)
          return true;
	else if (require == Client::CLIENT_UNMOUNTING &&
            state == Client::CLIENT_MOUNTED)
          return true;
	else if (require == Client::CLIENT_UNMOUNTED &&
            state == Client::CLIENT_UNMOUNTING)
          return true;
        else
          return false;
      }

      mount_state_t(state_t state, const char *lockname, uint64_t reader_cnt=0)
        : RWRefState (state, lockname, reader_cnt) {}
      ~mount_state_t() {}
  };

  struct initialize_state_t : public RWRefState<state_t> {
    public:
      bool is_valid_state(state_t state) const override {
        switch (state) {
	  case Client::CLIENT_NEW:
          case Client::CLIENT_INITIALIZING:
	  case Client::CLIENT_INITIALIZED:
            return true;
          default:
            return false;
        }
      }

      int check_reader_state(state_t require) const override {
        if (require == Client::CLIENT_INITIALIZED &&
            state >= Client::CLIENT_INITIALIZED)
          return true;
        else
          return false;
      }

      /* The state migration check */
      int check_writer_state(state_t require) const override {
        if (require == Client::CLIENT_INITIALIZING &&
            (state == Client::CLIENT_NEW))
          return true;
	else if (require == Client::CLIENT_INITIALIZED &&
            (state == Client::CLIENT_INITIALIZING))
          return true;
	else if (require == Client::CLIENT_NEW &&
            (state == Client::CLIENT_INITIALIZED))
          return true;
        else
          return false;
      }

      initialize_state_t(state_t state, const char *lockname, uint64_t reader_cnt=0)
        : RWRefState (state, lockname, reader_cnt) {}
      ~initialize_state_t() {}
  };

  struct mount_state_t mount_state;
  struct initialize_state_t initialize_state;

private:
  struct C_Readahead : public Context {
    C_Readahead(Client *c, Fh *f);
    ~C_Readahead() override;
    void finish(int r) override;

    Client *client;
    Fh *f;
  };

  /*
   * These define virtual xattrs exposing the recursive directory
   * statistics and layout metadata.
   */
  struct VXattr {
    const std::string name;
    size_t (Client::*getxattr_cb)(Inode *in, char *val, size_t size);
    int (Client::*setxattr_cb)(Inode *in, const void *val, size_t size,
			       const UserPerm& perms);
    bool readonly;
    bool (Client::*exists_cb)(Inode *in);
    unsigned int flags;
  };

  enum {
    NO_ACL = 0,
    POSIX_ACL,
  };

  enum {
    MAY_EXEC = 1,
    MAY_WRITE = 2,
    MAY_READ = 4,
  };

  typedef std::function<void(dir_result_t*, MetaRequest*, InodeRef&, frag_t)> fill_readdir_args_cb_t;

  std::unique_ptr<CephContext, std::function<void(CephContext*)>> cct_deleter;

  /* Flags for VXattr */
  static const unsigned VXATTR_RSTAT = 0x1;
  static const unsigned VXATTR_DIRSTAT = 0x2;

  static const VXattr _dir_vxattrs[];
  static const VXattr _file_vxattrs[];
  static const VXattr _common_vxattrs[];


  bool is_reserved_vino(vinodeno_t &vino);

  void fill_dirent(struct dirent *de, const char *name, int type, uint64_t ino, loff_t next_off);

  int _opendir(Inode *in, dir_result_t **dirpp, const UserPerm& perms, int fd = -1);
  void _readdir_drop_dirp_buffer(dir_result_t *dirp);
  bool _readdir_have_frag(dir_result_t *dirp);
  void _readdir_next_frag(dir_result_t *dirp);
  void _readdir_rechoose_frag(dir_result_t *dirp);
  int _readdir_get_frag(int op, dir_result_t *dirp,
    fill_readdir_args_cb_t fill_req_cb);
  int _readdir_cache_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p, int caps, bool getref);
  int _readdir_r_cb(int op,
    dir_result_t* d,
    add_dirent_cb_t cb,
    fill_readdir_args_cb_t fill_cb,
    void* p,
    unsigned want,
    unsigned flags,
    bool getref,
    bool bypass_cache);

  void _closedir(dir_result_t *dirp);

  // other helpers
  void _fragmap_remove_non_leaves(Inode *in);
  void _fragmap_remove_stopped_mds(Inode *in, mds_rank_t mds);

  void _ll_get(Inode *in);
  int _ll_put(Inode *in, uint64_t num);
  void _ll_drop_pins();

  Fh *_create_fh(Inode *in, int flags, int cmode, const UserPerm& perms);
  int _release_fh(Fh *fh);
  void _put_fh(Fh *fh);

  std::pair<int, bool> _do_remount(bool retry_on_error);

  int _read_sync(Fh *f, uint64_t off, uint64_t len, bufferlist *bl, bool *checkeof);
  int _read_async(Fh *f, uint64_t off, uint64_t len, bufferlist *bl);

  bool _dentry_valid(const Dentry *dn);

  // internal interface
  //   call these with client_lock held!
  int _do_lookup(Inode *dir, const std::string& name, int mask, InodeRef *target,
		 const UserPerm& perms);

  int _lookup(Inode *dir, const std::string& dname, int mask, InodeRef *target,
	      const UserPerm& perm, std::string* alternate_name=nullptr,
              bool is_rename=false);

  int _link(Inode *in, Inode *dir, const char *name, const UserPerm& perm, std::string alternate_name,
	    InodeRef *inp = 0);
  int _unlink(Inode *dir, const char *name, const UserPerm& perm);
  int _rename(Inode *olddir, const char *oname, Inode *ndir, const char *nname, const UserPerm& perm, std::string alternate_name);
  int _mkdir(Inode *dir, const char *name, mode_t mode, const UserPerm& perm,
	     InodeRef *inp = 0, const std::map<std::string, std::string> &metadata={},
             std::string alternate_name="");
  int _rmdir(Inode *dir, const char *name, const UserPerm& perms);
  int _symlink(Inode *dir, const char *name, const char *target,
	       const UserPerm& perms, std::string alternate_name, InodeRef *inp = 0);
  int _mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev,
	     const UserPerm& perms, InodeRef *inp = 0);
  int _do_setattr(Inode *in, struct ceph_statx *stx, int mask,
		  const UserPerm& perms, InodeRef *inp,
		  std::vector<uint8_t>* aux=nullptr);
  void stat_to_statx(struct stat *st, struct ceph_statx *stx);
  int __setattrx(Inode *in, struct ceph_statx *stx, int mask,
		 const UserPerm& perms, InodeRef *inp = 0);
  int _setattrx(InodeRef &in, struct ceph_statx *stx, int mask,
		const UserPerm& perms);
  int _setattr(InodeRef &in, struct stat *attr, int mask,
	       const UserPerm& perms);
  int _ll_setattrx(Inode *in, struct ceph_statx *stx, int mask,
		   const UserPerm& perms, InodeRef *inp = 0);
  int _getattr(Inode *in, int mask, const UserPerm& perms, bool force=false);
  int _getattr(InodeRef &in, int mask, const UserPerm& perms, bool force=false) {
    return _getattr(in.get(), mask, perms, force);
  }
  int _readlink(Inode *in, char *buf, size_t size);
  int _getxattr(Inode *in, const char *name, void *value, size_t len,
		const UserPerm& perms);
  int _getxattr(InodeRef &in, const char *name, void *value, size_t len,
		const UserPerm& perms);
  int _getvxattr(Inode *in, const UserPerm& perms, const char *attr_name,
                 ssize_t size, void *value, mds_rank_t rank);
  int _listxattr(Inode *in, char *names, size_t len, const UserPerm& perms);
  int _do_setxattr(Inode *in, const char *name, const void *value, size_t len,
		   int flags, const UserPerm& perms);
  int _setxattr(Inode *in, const char *name, const void *value, size_t len,
		int flags, const UserPerm& perms);
  int _setxattr(InodeRef &in, const char *name, const void *value, size_t len,
		int flags, const UserPerm& perms);
  int _setxattr_check_data_pool(std::string& name, std::string& value, const OSDMap *osdmap);
  void _setxattr_maybe_wait_for_osdmap(const char *name, const void *value, size_t len);
  int _removexattr(Inode *in, const char *nm, const UserPerm& perms);
  int _removexattr(InodeRef &in, const char *nm, const UserPerm& perms);
  int _open(Inode *in, int flags, mode_t mode, Fh **fhp,
	    const UserPerm& perms);
  int _renew_caps(Inode *in);
  int _create(Inode *in, const char *name, int flags, mode_t mode, InodeRef *inp,
	      Fh **fhp, int stripe_unit, int stripe_count, int object_size,
	      const char *data_pool, bool *created, const UserPerm &perms,
              std::string alternate_name);

  loff_t _lseek(Fh *fh, loff_t offset, int whence);
  int64_t _read(Fh *fh, int64_t offset, uint64_t size, bufferlist *bl);
  int64_t _write(Fh *fh, int64_t offset, uint64_t size, const char *buf,
          const struct iovec *iov, int iovcnt);
  int64_t _preadv_pwritev_locked(Fh *fh, const struct iovec *iov,
                                 unsigned iovcnt, int64_t offset,
                                 bool write, bool clamp_to_int);
  int _preadv_pwritev(int fd, const struct iovec *iov, unsigned iovcnt,
                      int64_t offset, bool write);
  int _flush(Fh *fh);
  int _fsync(Fh *fh, bool syncdataonly);
  int _fsync(Inode *in, bool syncdataonly);
  int _sync_fs();
  int clear_suid_sgid(Inode *in, const UserPerm& perms, bool defer=false);
  int _fallocate(Fh *fh, int mode, int64_t offset, int64_t length);
  int _getlk(Fh *fh, struct flock *fl, uint64_t owner);
  int _setlk(Fh *fh, struct flock *fl, uint64_t owner, int sleep);
  int _flock(Fh *fh, int cmd, uint64_t owner);
  int _lazyio(Fh *fh, int enable);

  Dentry *get_or_create(Inode *dir, const char* name);

  int xattr_permission(Inode *in, const char *name, unsigned want,
		       const UserPerm& perms);
  int may_setattr(Inode *in, struct ceph_statx *stx, int mask,
		  const UserPerm& perms);
  int may_open(Inode *in, int flags, const UserPerm& perms);
  int may_lookup(Inode *dir, const UserPerm& perms);
  int may_create(Inode *dir, const UserPerm& perms);
  int may_delete(Inode *dir, const char *name, const UserPerm& perms);
  int may_hardlink(Inode *in, const UserPerm& perms);

  int _getattr_for_perm(Inode *in, const UserPerm& perms);

  vinodeno_t _get_vino(Inode *in);

  bool _vxattrcb_fscrypt_auth_exists(Inode *in);
  size_t _vxattrcb_fscrypt_auth(Inode *in, char *val, size_t size);
  int _vxattrcb_fscrypt_auth_set(Inode *in, const void *val, size_t size, const UserPerm& perms);
  bool _vxattrcb_fscrypt_file_exists(Inode *in);
  size_t _vxattrcb_fscrypt_file(Inode *in, char *val, size_t size);
  int _vxattrcb_fscrypt_file_set(Inode *in, const void *val, size_t size, const UserPerm& perms);
  bool _vxattrcb_quota_exists(Inode *in);
  size_t _vxattrcb_quota(Inode *in, char *val, size_t size);
  size_t _vxattrcb_quota_max_bytes(Inode *in, char *val, size_t size);
  size_t _vxattrcb_quota_max_files(Inode *in, char *val, size_t size);

  bool _vxattrcb_layout_exists(Inode *in);
  size_t _vxattrcb_layout(Inode *in, char *val, size_t size);
  size_t _vxattrcb_layout_stripe_unit(Inode *in, char *val, size_t size);
  size_t _vxattrcb_layout_stripe_count(Inode *in, char *val, size_t size);
  size_t _vxattrcb_layout_object_size(Inode *in, char *val, size_t size);
  size_t _vxattrcb_layout_pool(Inode *in, char *val, size_t size);
  size_t _vxattrcb_layout_pool_namespace(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_entries(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_files(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_subdirs(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_rentries(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_rfiles(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_rsubdirs(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_rsnaps(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_rbytes(Inode *in, char *val, size_t size);
  size_t _vxattrcb_dir_rctime(Inode *in, char *val, size_t size);

  bool _vxattrcb_dir_pin_exists(Inode *in);
  size_t _vxattrcb_dir_pin(Inode *in, char *val, size_t size);

  bool _vxattrcb_snap_btime_exists(Inode *in);
  size_t _vxattrcb_snap_btime(Inode *in, char *val, size_t size);

  size_t _vxattrcb_caps(Inode *in, char *val, size_t size);

  bool _vxattrcb_mirror_info_exists(Inode *in);
  size_t _vxattrcb_mirror_info(Inode *in, char *val, size_t size);

  size_t _vxattrcb_cluster_fsid(Inode *in, char *val, size_t size);
  size_t _vxattrcb_client_id(Inode *in, char *val, size_t size);

  static const VXattr *_get_vxattrs(Inode *in);
  static const VXattr *_match_vxattr(Inode *in, const char *name);

  int _do_filelock(Inode *in, Fh *fh, int lock_type, int op, int sleep,
		   struct flock *fl, uint64_t owner, bool removing=false);
  int _interrupt_filelock(MetaRequest *req);
  void _encode_filelocks(Inode *in, bufferlist& bl);
  void _release_filelocks(Fh *fh);
  void _update_lock_state(struct flock *fl, uint64_t owner, ceph_lock_state_t *lock_state);

  int _posix_acl_create(Inode *dir, mode_t *mode, bufferlist& xattrs_bl,
			const UserPerm& perms);
  int _posix_acl_chmod(Inode *in, mode_t mode, const UserPerm& perms);
  int _posix_acl_permission(Inode *in, const UserPerm& perms, unsigned want);

  mds_rank_t _get_random_up_mds() const;

  int _ll_getattr(Inode *in, int caps, const UserPerm& perms);
  int _lookup_parent(Inode *in, const UserPerm& perms, Inode **parent=NULL);
  int _lookup_name(Inode *in, Inode *parent, const UserPerm& perms);
  int _lookup_vino(vinodeno_t ino, const UserPerm& perms, Inode **inode=NULL);
  bool _ll_forget(Inode *in, uint64_t count);

  void collect_and_send_metrics();
  void collect_and_send_global_metrics();

  void update_io_stat_metadata(utime_t latency);
  void update_io_stat_read(utime_t latency);
  void update_io_stat_write(utime_t latency);

  uint32_t deleg_timeout = 0;

  client_switch_interrupt_callback_t switch_interrupt_cb = nullptr;
  client_remount_callback_t remount_cb = nullptr;
  client_ino_callback_t ino_invalidate_cb = nullptr;
  client_dentry_callback_t dentry_invalidate_cb = nullptr;
  client_umask_callback_t umask_cb = nullptr;
  client_ino_release_t ino_release_cb = nullptr;
  void *callback_handle = nullptr;
  bool can_invalidate_dentries = false;

  Finisher async_ino_invalidator;
  Finisher async_dentry_invalidator;
  Finisher interrupt_finisher;
  Finisher remount_finisher;
  Finisher async_ino_releasor;
  Finisher objecter_finisher;

  ceph::coarse_mono_time last_cap_renew;

  CommandHook m_command_hook;

  int user_id, group_id;
  int acl_type = NO_ACL;

  epoch_t cap_epoch_barrier = 0;

  // mds sessions
  map<mds_rank_t, MetaSessionRef> mds_sessions;  // mds -> push seq
  std::set<mds_rank_t> mds_ranks_closing;  // mds ranks currently tearing down sessions
  std::list<ceph::condition_variable*> waiting_for_mdsmap;

  // FSMap, for when using mds_command
  std::list<ceph::condition_variable*> waiting_for_fsmap;
  std::unique_ptr<FSMap> fsmap;
  std::unique_ptr<FSMapUser> fsmap_user;

  // This mutex only protects command_table
  ceph::mutex command_lock = ceph::make_mutex("Client::command_lock");
  // MDS command state
  CommandTable<MDSCommandOp> command_table;

  bool _use_faked_inos;

  // Cluster fsid
  fs_cluster_id_t fscid;

  // file handles, etc.
  interval_set<int> free_fd_set;  // unused fds
  ceph::unordered_map<int, Fh*> fd_map;
  set<Fh*> ll_unclosed_fh_set;
  ceph::unordered_set<dir_result_t*> opened_dirs;
  uint64_t fd_gen = 1;

  bool   mount_aborted = false;
  bool   blocklisted = false;

  ceph::unordered_map<vinodeno_t, Inode*> inode_map;
  ceph::unordered_map<ino_t, vinodeno_t> faked_ino_map;
  interval_set<ino_t> free_faked_inos;
  ino_t last_used_faked_ino;
  ino_t last_used_faked_root;

  int local_osd = -CEPHFS_ENXIO;
  epoch_t local_osd_epoch = 0;

  // mds requests
  ceph_tid_t last_tid = 0;
  ceph_tid_t oldest_tid = 0; // oldest incomplete mds request, excluding setfilelock requests
  map<ceph_tid_t, MetaRequest*> mds_requests;

  // cap flushing
  ceph_tid_t last_flush_tid = 1;

  xlist<Inode*> delayed_list;
  int num_flushing_caps = 0;
  ceph::unordered_map<inodeno_t,SnapRealm*> snap_realms;
  std::map<std::string, std::string> metadata;

  ceph::coarse_mono_time last_auto_reconnect;
  std::chrono::seconds caps_release_delay, mount_timeout;
  // trace generation
  std::ofstream traceout;

  ceph::condition_variable mount_cond, sync_cond;

  std::map<std::pair<int64_t,std::string>, int> pool_perms;
  std::list<ceph::condition_variable*> waiting_for_pool_perm;

  std::list<ceph::condition_variable*> waiting_for_rename;

  uint64_t retries_on_invalidate = 0;

  // state reclaim
  int reclaim_errno = 0;
  epoch_t reclaim_osd_epoch = 0;
  entity_addrvec_t reclaim_target_addrs;

  // dentry lease metrics
  uint64_t dentry_nr = 0;
  uint64_t dlease_hits = 0;
  uint64_t dlease_misses = 0;

  uint64_t cap_hits = 0;
  uint64_t cap_misses = 0;

  uint64_t opened_files = 0;
  uint64_t pinned_icaps = 0;
  uint64_t opened_inodes = 0;

  uint64_t total_read_ops = 0;
  uint64_t total_read_size = 0;

  uint64_t total_write_ops = 0;
  uint64_t total_write_size = 0;

  ceph::spinlock delay_i_lock;
  std::map<Inode*,int> delay_i_release;

  uint64_t nr_metadata_request = 0;
  uint64_t nr_read_request = 0;
  uint64_t nr_write_request = 0;
};

/**
 * Specialization of Client that manages its own Objecter instance
 * and handles init/shutdown of messenger/monclient
 */
class StandaloneClient : public Client
{
public:
  StandaloneClient(Messenger *m, MonClient *mc, boost::asio::io_context& ictx);

  ~StandaloneClient() override;

  int init() override;
  void shutdown() override;
};

#endif
