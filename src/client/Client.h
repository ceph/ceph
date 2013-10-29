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

#include "include/types.h"

// stl
#include <string>
#include <set>
#include <map>
#include <fstream>
using std::set;
using std::map;
using std::fstream;

#include "include/unordered_map.h"

#include "include/filepath.h"
#include "include/interval_set.h"
#include "include/lru.h"

#include "mds/mdstypes.h"

#include "msg/Message.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/Finisher.h"

#include "common/compiler_extensions.h"
#include "common/cmdparse.h"

#include "osdc/ObjectCacher.h"

class MDSMap;
class OSDMap;
class MonClient;

class CephContext;
class MClientReply;
class MClientRequest;
class MClientSession;
class MClientRequest;
class MClientRequestForward;
struct MClientLease;
class MClientCaps;
class MClientCapRelease;

struct DirStat;
struct LeaseStat;
struct InodeStat;

class Filer;
class Objecter;
class WritebackHandler;

class PerfCounters;

enum {
  l_c_first = 20000,
  l_c_reply,
  l_c_lat,
  l_c_owrlat,
  l_c_ordlat,
  l_c_wrlat,
  l_c_last,
};



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
  string d_name;
  struct stat st;
  int stmask;
  DirEntry(const string &s) : d_name(s), stmask(0) {}
  DirEntry(const string &n, struct stat& s, int stm) : d_name(n), st(s), stmask(stm) {}
};

class Inode;
struct Cap;
class Dir;
class Dentry;
struct SnapRealm;
struct Fh;
struct CapSnap;

struct MetaSession;
struct MetaRequest;


typedef void (*client_ino_callback_t)(void *handle, vinodeno_t ino, int64_t off, int64_t len);

typedef void (*client_dentry_callback_t)(void *handle, vinodeno_t dirino,
					 vinodeno_t ino, string& name);

typedef int (*client_getgroups_callback_t)(void *handle, uid_t uid, gid_t **sgids);

// ========================================================
// client interface

struct dir_result_t {
  static const int SHIFT = 28;
  static const int64_t MASK = (1 << SHIFT) - 1;
  static const loff_t END = 1ULL << (SHIFT + 32);

  static uint64_t make_fpos(unsigned frag, unsigned off) {
    return ((uint64_t)frag << SHIFT) | (uint64_t)off;
  }
  static unsigned fpos_frag(uint64_t p) {
    return (p & ~END) >> SHIFT;
  }
  static unsigned fpos_off(uint64_t p) {
    return p & MASK;
  }


  Inode *inode;

  int64_t offset;        // high bits: frag_t, low bits: an offset

  uint64_t this_offset;  // offset of last chunk, adjusted for . and ..
  uint64_t next_offset;  // offset of next chunk (last_name's + 1)
  string last_name;      // last entry in previous chunk

  uint64_t release_count;
  int start_shared_gen;  // dir shared_gen at start of readdir

  frag_t buffer_frag;
  vector<pair<string,Inode*> > *buffer;

  string at_cache_name;  // last entry we successfully returned

  dir_result_t(Inode *in);

  frag_t frag() { return frag_t(offset >> SHIFT); }
  unsigned fragpos() { return offset & MASK; }

  void next_frag() {
    frag_t fg = offset >> SHIFT;
    if (fg.is_rightmost())
      set_end();
    else 
      set_frag(fg.next());
  }
  void set_frag(frag_t f) {
    offset = (uint64_t)f << SHIFT;
    assert(sizeof(offset) == 8);
  }
  void set_end() { offset |= END; }
  bool at_end() { return (offset & END); }

  void reset() {
    last_name.clear();
    at_cache_name.clear();
    next_offset = 2;
    this_offset = 0;
    offset = 0;
    delete buffer;
    buffer = 0;
  }
};

class Client : public Dispatcher {
 public:
  CephContext *cct;

  PerfCounters *logger;

  class CommandHook : public AdminSocketHook {
    Client *m_client;
  public:
    CommandHook(Client *client);
    bool call(std::string command, cmdmap_t &cmdmap, std::string format,
	      bufferlist& out);
  };
  CommandHook m_command_hook;

  // cluster descriptors
  MDSMap *mdsmap; 
  OSDMap *osdmap;

  SafeTimer timer;

  client_ino_callback_t ino_invalidate_cb;
  void *ino_invalidate_cb_handle;

  client_dentry_callback_t dentry_invalidate_cb;
  void *dentry_invalidate_cb_handle;

  client_getgroups_callback_t getgroups_cb;
  void *getgroups_cb_handle;

  Finisher async_ino_invalidator;
  Finisher async_dentry_invalidator;

  Context *tick_event;
  utime_t last_cap_renew;
  void renew_caps();
  void renew_caps(MetaSession *session);
  void flush_cap_releases();
public:
  void tick();

 protected:
  MonClient *monclient;
  Messenger *messenger;  
  client_t whoami;

  // mds sessions
  map<int, MetaSession*> mds_sessions;  // mds -> push seq
  list<Cond*> waiting_for_mdsmap;

  bool have_open_session(int mds);
  void got_mds_push(MetaSession *s);
  MetaSession *_get_mds_session(int mds, Connection *con);  ///< return session for mds *and* con; null otherwise
  MetaSession *_get_or_open_mds_session(int mds);
  MetaSession *_open_mds_session(int mds);
  void _close_mds_session(MetaSession *s);
  void _closed_mds_session(MetaSession *s);
  void handle_client_session(MClientSession *m);
  void send_reconnect(MetaSession *s);
  void resend_unsafe_requests(MetaSession *s);

  // mds requests
  tid_t last_tid, last_flush_seq;
  map<tid_t, MetaRequest*> mds_requests;
  set<int>                 failed_mds;

  void dump_mds_requests(Formatter *f);
  void dump_mds_sessions(Formatter *f);

  int make_request(MetaRequest *req, int uid, int gid,
		   //MClientRequest *req, int uid, int gid,
		   Inode **ptarget = 0, bool *pcreated = 0,
		   int use_mds=-1, bufferlist *pdirbl=0);
  void put_request(MetaRequest *request);

  int verify_reply_trace(int r, MetaRequest *request, MClientReply *reply,
			 Inode **ptarget, bool *pcreated, int uid, int gid);
  void encode_cap_releases(MetaRequest *request, int mds);
  int encode_inode_release(Inode *in, MetaRequest *req,
			   int mds, int drop,
			   int unless,int force=0);
  void encode_dentry_release(Dentry *dn, MetaRequest *req,
			     int mds, int drop, int unless);
  int choose_target_mds(MetaRequest *req);
  void connect_mds_targets(int mds);
  void send_request(MetaRequest *request, MetaSession *session);
  MClientRequest *build_client_request(MetaRequest *request);
  void kick_requests(MetaSession *session);
  void kick_requests_closed(MetaSession *session);
  void handle_client_request_forward(MClientRequestForward *reply);
  void handle_client_reply(MClientReply *reply);

  bool   initialized;
  bool   mounted;
  bool   unmounting;

  int local_osd;
  epoch_t local_osd_epoch;

  int unsafe_sync_write;

public:
  entity_name_t get_myname() { return messenger->get_myname(); } 
  void sync_write_commit(Inode *in);

protected:
  Filer                 *filer;     
  ObjectCacher          *objectcacher;
  Objecter              *objecter;     // (non-blocking) osd interface
  WritebackHandler      *writeback_handler;

  // cache
  ceph::unordered_map<vinodeno_t, Inode*> inode_map;
  Inode*                 root;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  // all inodes with caps sit on either cap_list or delayed_caps.
  xlist<Inode*> delayed_caps, cap_list;
  int num_flushing_caps;
  ceph::unordered_map<inodeno_t,SnapRealm*> snap_realms;

  SnapRealm *get_snap_realm(inodeno_t r);
  SnapRealm *get_snap_realm_maybe(inodeno_t r);
  void put_snap_realm(SnapRealm *realm);
  bool adjust_realm_parent(SnapRealm *realm, inodeno_t parent);
  inodeno_t update_snap_trace(bufferlist& bl, bool must_flush=true);
  inodeno_t _update_snap_trace(vector<SnapRealmInfo>& trace);
  void invalidate_snaprealm_and_children(SnapRealm *realm);

  Inode *open_snapdir(Inode *diri);


  // file handles, etc.
  interval_set<int> free_fd_set;  // unused fds
  ceph::unordered_map<int, Fh*> fd_map;
  
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
    ceph::unordered_map<int, Fh*>::iterator p = fd_map.find(fd);
    if (p == fd_map.end())
      return NULL;
    return p->second;
  }

  // global client lock
  //  - protects Client and buffer cache both!
  Mutex                  client_lock;

  // helpers
  void wake_inode_waiters(MetaSession *s);
  void wait_on_list(list<Cond*>& ls);
  void signal_cond_list(list<Cond*>& ls);

  void wait_on_context_list(list<Context*>& ls);
  void signal_context_list(list<Context*>& ls);

  // -- metadata cache stuff

  // decrease inode ref.  delete if dangling.
  void put_inode(Inode *in, int n=1);
  void close_dir(Dir *dir);

  friend class C_Client_PutInode; // calls put_inode()
  friend class C_Client_CacheInvalidate;  // calls ino_invalidate_cb
  friend class C_Client_DentryInvalidate;  // calls dentry_invalidate_cb

  //int get_cache_size() { return lru.lru_get_size(); }
  //void set_cache_size(int m) { lru.lru_set_max(m); }

  /**
   * Don't call this with in==NULL, use get_or_create for that
   * leave dn set to default NULL unless you're trying to add
   * a new inode to a pre-created Dentry
   */
  Dentry* link(Dir *dir, const string& name, Inode *in, Dentry *dn);
  void unlink(Dentry *dn, bool keepdir);

  // path traversal for high-level interface
  Inode *cwd;
  int path_walk(const filepath& fp, Inode **end, bool followsym=true);
  int fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat=0, nest_info_t *rstat=0);
  void touch_dn(Dentry *dn);

  // trim cache.
  void trim_cache();
  void trim_dentry(Dentry *dn);
  void trim_caps(MetaSession *s, int max);
  
  void dump_inode(Formatter *f, Inode *in, set<Inode*>& did, bool disconnected);
  void dump_cache(Formatter *f);  // debug
  
  // trace generation
  ofstream traceout;


  Cond mount_cond, sync_cond;


  // friends
  friend class SyntheticClient;
  bool ms_dispatch(Message *m);

  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);


 public:
  void set_filer_flags(int flags);
  void clear_filer_flags(int flags);

  Client(Messenger *m, MonClient *mc);
  ~Client();
  void tear_down_cache();

  client_t get_nodeid() { return whoami; }

  inodeno_t get_root_ino();

  int init()  WARN_UNUSED_RESULT;
  void shutdown();

  // messaging
  void handle_mds_map(class MMDSMap *m);

  void handle_lease(MClientLease *m);

  // file caps
  void check_cap_issue(Inode *in, Cap *cap, unsigned issued);
  void add_update_cap(Inode *in, MetaSession *session, uint64_t cap_id,
		      unsigned issued, unsigned seq, unsigned mseq, inodeno_t realm,
		      int flags);
  void remove_cap(Cap *cap);
  void remove_all_caps(Inode *in);
  void remove_session_caps(MetaSession *session);
  void mark_caps_dirty(Inode *in, int caps);
  int mark_caps_flushing(Inode *in);
  void flush_caps();
  void flush_caps(Inode *in, MetaSession *session);
  void kick_flushing_caps(MetaSession *session);
  void kick_maxsize_requests(MetaSession *session);
  int get_caps(Inode *in, int need, int want, int *have, loff_t endoff);

  void maybe_update_snaprealm(SnapRealm *realm, snapid_t snap_created, snapid_t snap_highwater, 
			      vector<snapid_t>& snaps);

  void handle_snap(struct MClientSnap *m);
  void handle_caps(class MClientCaps *m);
  void handle_cap_import(MetaSession *session, Inode *in, class MClientCaps *m);
  void handle_cap_export(MetaSession *session, Inode *in, class MClientCaps *m);
  void handle_cap_trunc(MetaSession *session, Inode *in, class MClientCaps *m);
  void handle_cap_flush_ack(MetaSession *session, Inode *in, Cap *cap, class MClientCaps *m);
  void handle_cap_flushsnap_ack(MetaSession *session, Inode *in, class MClientCaps *m);
  void handle_cap_grant(MetaSession *session, Inode *in, Cap *cap, class MClientCaps *m);
  void cap_delay_requeue(Inode *in);
  void send_cap(Inode *in, MetaSession *session, Cap *cap,
		int used, int want, int retain, int flush);
  void check_caps(Inode *in, bool is_delayed);
  void get_cap_ref(Inode *in, int cap);
  void put_cap_ref(Inode *in, int cap);
  void flush_snaps(Inode *in, bool all_again=false, CapSnap *again=0);
  void wait_sync_caps(uint64_t want);
  void queue_cap_snap(Inode *in, snapid_t seq=0);
  void finish_cap_snap(Inode *in, CapSnap *capsnap, int used);
  void _flushed_cap_snap(Inode *in, snapid_t seq);

  void _schedule_invalidate_dentry_callback(Dentry *dn);
  void _async_dentry_invalidate(vinodeno_t dirino, vinodeno_t ino, string& name);
  void _invalidate_inode_parents(Inode *in);

  void _schedule_invalidate_callback(Inode *in, int64_t off, int64_t len, bool keep_caps);
  void _invalidate_inode_cache(Inode *in, bool keep_caps);
  void _invalidate_inode_cache(Inode *in, int64_t off, int64_t len, bool keep_caps);
  void _async_invalidate(Inode *in, int64_t off, int64_t len, bool keep_caps);
  void _release(Inode *in);
  
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
  bool _flush(Inode *in, Context *c=NULL);
  void _flush_range(Inode *in, int64_t off, uint64_t size);
  void _flushed(Inode *in);
  void flush_set_callback(ObjectCacher::ObjectSet *oset);

  void close_release(Inode *in);
  void close_safe(Inode *in);

  void lock_fh_pos(Fh *f);
  void unlock_fh_pos(Fh *f);
  
  // metadata cache
  void update_dir_dist(Inode *in, DirStat *st);

  void insert_readdir_results(MetaRequest *request, MetaSession *session, Inode *diri);
  Inode* insert_trace(MetaRequest *request, MetaSession *session);
  void update_inode_file_bits(Inode *in,
			      uint64_t truncate_seq, uint64_t truncate_size, uint64_t size,
			      uint64_t time_warp_seq, utime_t ctime, utime_t mtime, utime_t atime,
			      int issued);
  Inode *add_update_inode(InodeStat *st, utime_t ttl, MetaSession *session);
  Dentry *insert_dentry_inode(Dir *dir, const string& dname, LeaseStat *dlease, 
			      Inode *in, utime_t from, MetaSession *session, bool set_offset,
			      Dentry *old_dentry = NULL);
  void update_dentry_lease(Dentry *dn, LeaseStat *dlease, utime_t from, MetaSession *session);


  // ----------------------
  // fs ops.
private:

  void fill_dirent(struct dirent *de, const char *name, int type, uint64_t ino, loff_t next_off);

  // some readdir helpers
  typedef int (*add_dirent_cb_t)(void *p, struct dirent *de, struct stat *st, int stmask, off_t off);

  int _opendir(Inode *in, dir_result_t **dirpp, int uid=-1, int gid=-1);
  void _readdir_drop_dirp_buffer(dir_result_t *dirp);
  bool _readdir_have_frag(dir_result_t *dirp);
  void _readdir_next_frag(dir_result_t *dirp);
  void _readdir_rechoose_frag(dir_result_t *dirp);
  int _readdir_get_frag(dir_result_t *dirp);
  int _readdir_cache_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p);
  void _closedir(dir_result_t *dirp);

  // other helpers
  void _ll_get(Inode *in);
  int _ll_put(Inode *in, int num);
  void _ll_drop_pins();

  Fh *_create_fh(Inode *in, int flags, int cmode);
  int _release_fh(Fh *fh);

  int _read_sync(Fh *f, uint64_t off, uint64_t len, bufferlist *bl);
  int _read_async(Fh *f, uint64_t off, uint64_t len, bufferlist *bl);

  // internal interface
  //   call these with client_lock held!
  int _do_lookup(Inode *dir, const string& name, Inode **target);
  int _lookup(Inode *dir, const string& dname, Inode **target);

  int _link(Inode *in, Inode *dir, const char *name, int uid=-1, int gid=-1, Inode **inp = 0);
  int _unlink(Inode *dir, const char *name, int uid=-1, int gid=-1);
  int _rename(Inode *olddir, const char *oname, Inode *ndir, const char *nname, int uid=-1, int gid=-1);
  int _mkdir(Inode *dir, const char *name, mode_t mode, int uid=-1, int gid=-1, Inode **inp = 0);
  int _rmdir(Inode *dir, const char *name, int uid=-1, int gid=-1);
  int _symlink(Inode *dir, const char *name, const char *target, int uid=-1, int gid=-1, Inode **inp = 0);
  int _mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev, int uid=-1, int gid=-1, Inode **inp = 0);
  int _setattr(Inode *in, struct stat *attr, int mask, int uid=-1, int gid=-1, Inode **inp = 0);
  int _getattr(Inode *in, int mask, int uid=-1, int gid=-1, bool force=false);
  int _getxattr(Inode *in, const char *name, void *value, size_t len, int uid=-1, int gid=-1);
  int _listxattr(Inode *in, char *names, size_t len, int uid=-1, int gid=-1);
  int _setxattr(Inode *in, const char *name, const void *value, size_t len, int flags, int uid=-1, int gid=-1);
  int _removexattr(Inode *in, const char *nm, int uid=-1, int gid=-1);
  int _open(Inode *in, int flags, mode_t mode, Fh **fhp, int uid=-1, int gid=-1);
  int _create(Inode *in, const char *name, int flags, mode_t mode, Inode **inp, Fh **fhp,
              int stripe_unit, int stripe_count, int object_size, const char *data_pool,
	      bool *created = NULL, int uid=-1, int gid=-1);
  loff_t _lseek(Fh *fh, loff_t offset, int whence);
  int _read(Fh *fh, int64_t offset, uint64_t size, bufferlist *bl);
  int _write(Fh *fh, int64_t offset, uint64_t size, const char *buf);
  int _flush(Fh *fh);
  int _fsync(Fh *fh, bool syncdataonly);
  int _sync_fs();
  int _fallocate(Fh *fh, int mode, int64_t offset, int64_t length);

  int get_or_create(Inode *dir, const char* name,
		    Dentry **pdn, bool expect_null=false);

  int check_permissions(Inode *in, int flags, int uid, int gid);

public:
  int mount(const std::string &mount_root);
  void unmount();

  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statvfs *stbuf);

  // crap
  int chdir(const char *s);
  void getcwd(std::string& cwd);

  // namespace ops
  int opendir(const char *name, dir_result_t **dirpp);
  int closedir(dir_result_t *dirp);

  /**
   * Fill a directory listing from dirp, invoking cb for each entry
   * with the given pointer, the dirent, the struct stat, the stmask,
   * and the offset.
   *
   * Returns 0 if it reached the end of the directory.
   * If @a cb returns a negative error code, stop and return that.
   */
  int readdir_r_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p);

  struct dirent * readdir(dir_result_t *d);
  int readdir_r(dir_result_t *dirp, struct dirent *de);
  int readdirplus_r(dir_result_t *dirp, struct dirent *de, struct stat *st, int *stmask);

  int getdir(const char *relpath, list<string>& names);  // get the whole dir at once.

  /**
   * Returns the length of the buffer that got filled in, or -errno.
   * If it returns -ERANGE you just need to increase the size of the
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

  int link(const char *existing, const char *newname);
  int unlink(const char *path);
  int rename(const char *from, const char *to);

  // dirs
  int mkdir(const char *path, mode_t mode);
  int mkdirs(const char *path, mode_t mode);
  int rmdir(const char *path);

  // symlinks
  int readlink(const char *path, char *buf, loff_t size);
  int symlink(const char *existing, const char *newname);

  // inode stuff
  int stat(const char *path, struct stat *stbuf, frag_info_t *dirstat=0, int mask=CEPH_STAT_CAP_INODE_ALL);
  int lstat(const char *path, struct stat *stbuf, frag_info_t *dirstat=0, int mask=CEPH_STAT_CAP_INODE_ALL);
  int lstatlite(const char *path, struct statlite *buf);

  int setattr(const char *relpath, struct stat *attr, int mask);
  int fsetattr(int fd, struct stat *attr, int mask);
  int chmod(const char *path, mode_t mode);
  int fchmod(int fd, mode_t mode);
  int lchmod(const char *path, mode_t mode);
  int chown(const char *path, int uid, int gid);
  int fchown(int fd, int uid, int gid);
  int lchown(const char *path, int uid, int gid);
  int utime(const char *path, struct utimbuf *buf);
  int lutime(const char *path, struct utimbuf *buf);
  int truncate(const char *path, loff_t size);

  // file ops
  int mknod(const char *path, mode_t mode, dev_t rdev=0);
  int open(const char *path, int flags, mode_t mode=0);
  int open(const char *path, int flags, mode_t mode, int stripe_unit, int stripe_count, int object_size, const char *data_pool);
  int lookup_hash(inodeno_t ino, inodeno_t dirino, const char *name);
  int lookup_ino(inodeno_t ino);
  int close(int fd);
  loff_t lseek(int fd, loff_t offset, int whence);
  int read(int fd, char *buf, loff_t size, loff_t offset=-1);
  int write(int fd, const char *buf, loff_t size, loff_t offset=-1);
  int fake_write_size(int fd, loff_t size);
  int ftruncate(int fd, loff_t size);
  int fsync(int fd, bool syncdataonly);
  int fstat(int fd, struct stat *stbuf);
  int fallocate(int fd, int mode, loff_t offset, loff_t length);

  // full path xattr ops
  int getxattr(const char *path, const char *name, void *value, size_t size);
  int lgetxattr(const char *path, const char *name, void *value, size_t size);
  int listxattr(const char *path, char *list, size_t size);
  int llistxattr(const char *path, char *list, size_t size);
  int removexattr(const char *path, const char *name);
  int lremovexattr(const char *path, const char *name);
  int setxattr(const char *path, const char *name, const void *value, size_t size, int flags);
  int lsetxattr(const char *path, const char *name, const void *value, size_t size, int flags);

  int sync_fs();
  int64_t drop_caches();

  // hpc lazyio
  int lazyio_propogate(int fd, loff_t offset, size_t count);
  int lazyio_synchronize(int fd, loff_t offset, size_t count);

  // expose file layout
  int describe_layout(const char *path, ceph_file_layout* layout);
  int fdescribe_layout(int fd, ceph_file_layout* layout);
  int get_file_stripe_address(int fd, loff_t offset, vector<entity_addr_t>& address);
  int get_file_extent_osds(int fd, loff_t off, loff_t *len, vector<int>& osds);
  int get_osd_addr(int osd, entity_addr_t& addr);

  // expose osdmap 
  int get_local_osd();
  int get_pool_replication(int64_t pool);
  int64_t get_pool_id(const char *pool_name);
  string get_pool_name(int64_t pool);
  int get_osd_crush_location(int id, vector<pair<string, string> >& path);

  int enumerate_layout(int fd, vector<ObjectExtent>& result,
		       loff_t length, loff_t offset);

  int mksnap(const char *path, const char *name);
  int rmsnap(const char *path, const char *name);

  // expose caps
  int get_caps_issued(int fd);
  int get_caps_issued(const char *path);

  // low-level interface
  int ll_lookup(vinodeno_t parent, const char *name, struct stat *attr, int uid = -1, int gid = -1);
  bool ll_forget(vinodeno_t vino, int count);
  Inode *_ll_get_inode(vinodeno_t vino);
  int ll_getattr(vinodeno_t vino, struct stat *st, int uid = -1, int gid = -1);
  int ll_setattr(vinodeno_t vino, struct stat *st, int mask, int uid = -1, int gid = -1);
  int ll_getxattr(vinodeno_t vino, const char *name, void *value, size_t size, int uid=-1, int gid=-1);
  int ll_setxattr(vinodeno_t vino, const char *name, const void *value, size_t size, int flags, int uid=-1, int gid=-1);
  int ll_removexattr(vinodeno_t vino, const char *name, int uid=-1, int gid=-1);
  int ll_listxattr(vinodeno_t vino, char *list, size_t size, int uid=-1, int gid=-1);
  int ll_opendir(vinodeno_t vino, void **dirpp, int uid = -1, int gid = -1);
  void ll_releasedir(void *dirp);
  int ll_readlink(vinodeno_t vino, const char **value, int uid = -1, int gid = -1);
  int ll_mknod(vinodeno_t vino, const char *name, mode_t mode, dev_t rdev, struct stat *attr, int uid = -1, int gid = -1);
  int ll_mkdir(vinodeno_t vino, const char *name, mode_t mode, struct stat *attr, int uid = -1, int gid = -1);
  int ll_symlink(vinodeno_t vino, const char *name, const char *value, struct stat *attr, int uid = -1, int gid = -1);
  int ll_unlink(vinodeno_t vino, const char *name, int uid = -1, int gid = -1);
  int ll_rmdir(vinodeno_t vino, const char *name, int uid = -1, int gid = -1);
  int ll_rename(vinodeno_t parent, const char *name, vinodeno_t newparent, const char *newname, int uid = -1, int gid = -1);
  int ll_link(vinodeno_t vino, vinodeno_t newparent, const char *newname, struct stat *attr, int uid = -1, int gid = -1);
  int ll_describe_layout(Fh *fh, ceph_file_layout* layout);
  int ll_open(vinodeno_t vino, int flags, Fh **fh, int uid = -1, int gid = -1);
  int ll_create(vinodeno_t parent, const char *name, mode_t mode, int flags, struct stat *attr, Fh **fh, int uid = -1, int gid = -1);
  int ll_read(Fh *fh, loff_t off, loff_t len, bufferlist *bl);
  int ll_write(Fh *fh, loff_t off, loff_t len, const char *data);
  int ll_flush(Fh *fh);
  int ll_fsync(Fh *fh, bool syncdataonly);
  int ll_fallocate(Fh *fh, int mode, loff_t offset, loff_t length);
  int ll_release(Fh *fh);
  int ll_statfs(vinodeno_t vino, struct statvfs *stbuf);

  void ll_register_ino_invalidate_cb(client_ino_callback_t cb, void *handle);

  void ll_register_dentry_invalidate_cb(client_dentry_callback_t cb, void *handle);

  void ll_register_getgroups_cb(client_getgroups_callback_t cb, void *handle);
};

#endif
