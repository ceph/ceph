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


#ifndef __CLIENT_H
#define __CLIENT_H


#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "msg/Message.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "messages/MClientReply.h"

#include "include/types.h"
#include "include/lru.h"
#include "include/filepath.h"
#include "include/interval_set.h"

#include "common/Mutex.h"
#include "common/Timer.h"

#include "FileCache.h"


// stl
#include <set>
#include <map>
#include <fstream>
using std::set;
using std::map;
using std::fstream;

#include <ext/hash_map>
using namespace __gnu_cxx;



class MStatfsReply;
class MClientSession;
class MClientRequest;
class MClientRequestForward;
class MMonMap;

class Filer;
class Objecter;
class ObjectCacher;

extern class LogType client_logtype;
extern class Logger  *client_logger;



// ============================================
// types for my local metadata cache
/* basic structure:
   
 - Dentries live in an LRU loop.  they get expired based on last access.
      see include/lru.h.  items can be bumped to "mid" or "top" of list, etc.
 - Inode has ref count for each Fh, Dir, or Dentry that points to it.
 - when Inode ref goes to 0, it's expired.
 - when Dir is empty, it's removed (and it's Inode ref--)
 
*/

class Dir;
class Inode;

class Dentry : public LRUObject {
 public:
  string  name;                      // sort of lame
  //const char *name;
  Dir     *dir;
  Inode   *inode;
  int     ref;                       // 1 if there's a dir beneath me.
  
  void get() { assert(ref == 0); ref++; lru_pin(); }
  void put() { assert(ref == 1); ref--; lru_unpin(); }
  
  Dentry() : dir(0), inode(0), ref(0) { }

  /*Dentry() : name(0), dir(0), inode(0), ref(0) { }
  Dentry(string& n) : name(0), dir(0), inode(0), ref(0) { 
    name = new char[n.length()+1];
    strcpy((char*)name, n.c_str());
  }
  ~Dentry() {
    delete[] name;
    }*/
};

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  //hash_map<const char*, Dentry*, hash<const char*>, eqstr> dentries;
  hash_map<string, Dentry*> dentries;

  Dir(Inode* in) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};


class InodeCap {
 public:
  int  caps;
  long seq;
  InodeCap() : caps(0), seq(0) {}
};


class Inode {
 public:
  inode_t   inode;    // the actual inode
  utime_t   valid_until;
  int mask;

  // about the dir (if this is one!)
  int       dir_auth;
  set<int>  dir_contacts;
  bool      dir_hashed, dir_replicated;

  // per-mds caps
  map<int,InodeCap> caps;            // mds -> InodeCap
  map<int,InodeCap> stale_caps;      // mds -> cap .. stale

  utime_t   file_wr_mtime;   // [writers] time of last write
  off_t     file_wr_size;    // [writers] largest offset we've written to
  int       num_open_rd, num_open_wr, num_open_lazy;  // num readers, writers

  int       ref;      // ref count. 1 for each dentry, fh that links to me.
  int       ll_ref;   // separate ref count for ll client
  Dir       *dir;     // if i'm a dir.
  Dentry    *dn;      // if i'm linked to a dentry.
  string    *symlink; // symlink content, if it's a symlink
  fragtree_t dirfragtree;
  map<frag_t,int> fragmap;  // known frag -> mds mappings

  // for caching i/o mode
  FileCache fc;

  // for sync i/o mode
  int       sync_reads;   // sync reads in progress
  int       sync_writes;  // sync writes in progress

  list<Cond*>       waitfor_write;
  list<Cond*>       waitfor_read;
  list<Cond*>       waitfor_lazy;
  list<Context*>    waitfor_no_read, waitfor_no_write;

  // <hack>
  bool hack_balance_reads;
  // </hack>

  void make_path(string& p) {
    if (dn) {
      if (dn->dir && dn->dir->parent_inode)
	dn->dir->parent_inode->make_path(p);
      p += "/";
      p += dn->name;
    }
  }

  void get() { 
    ref++; 
    //cout << "inode.get on " << this << " " << hex << inode.ino << dec << " now " << ref << endl;
  }
  void put(int n=1) { 
    ref -= n; assert(ref >= 0); 
    //cout << "inode.put on " << this << " " << hex << inode.ino << dec << " now " << ref << endl;
  }

  void ll_get() {
    ll_ref++;
  }
  void ll_put(int n=1) {
    assert(ll_ref >= n);
    ll_ref -= n;
  }

  Inode(inode_t _inode, ObjectCacher *_oc) : 
    inode(_inode),
    valid_until(0, 0),
    dir_auth(-1), dir_hashed(false), dir_replicated(false), 
    file_wr_mtime(0, 0), file_wr_size(0), 
    num_open_rd(0), num_open_wr(0), num_open_lazy(0),
    ref(0), ll_ref(0), 
    dir(0), dn(0), symlink(0),
    fc(_oc, _inode),
    sync_reads(0), sync_writes(0),
    hack_balance_reads(false)
  { }
  ~Inode() {
    if (symlink) { delete symlink; symlink = 0; }
  }

  inodeno_t ino() { return inode.ino; }

  bool is_dir() { return inode.is_dir(); }

  int file_caps() {
    int c = 0;
    for (map<int,InodeCap>::iterator it = caps.begin();
         it != caps.end();
         it++)
      c |= it->second.caps;
    for (map<int,InodeCap>::iterator it = stale_caps.begin();
         it != stale_caps.end();
         it++)
      c |= it->second.caps;
    return c;
  }

  int file_caps_wanted() {
    int w = 0;
    if (num_open_rd) w |= CEPH_CAP_RD|CEPH_CAP_RDCACHE;
    if (num_open_wr) w |= CEPH_CAP_WR|CEPH_CAP_WRBUFFER;
    if (num_open_lazy) w |= CEPH_CAP_LAZYIO;
    if (fc.is_dirty()) w |= CEPH_CAP_WRBUFFER;
    if (fc.is_cached()) w |= CEPH_CAP_RDCACHE;
    return w;
  }

  void add_open(int cmode) {
    if (cmode & FILE_MODE_R) num_open_rd++;
    if (cmode & FILE_MODE_W) num_open_wr++;
    if (cmode & FILE_MODE_LAZY) num_open_lazy++;
  }
  void sub_open(int cmode) {
    if (cmode & FILE_MODE_R) num_open_rd--;
    if (cmode & FILE_MODE_W) num_open_wr--;
    if (cmode & FILE_MODE_LAZY) num_open_lazy--;
  }
  
  int authority(const string& dname) {
    if (!dirfragtree.empty()) {
      __gnu_cxx::hash<string> H;
      frag_t fg = dirfragtree[H(dname)];
      while (fg != frag_t()) {
	if (fragmap.count(fg) &&
	    fragmap[fg] >= 0) {
	  //cout << "picked frag ino " << inode.ino << " dname " << dname << " fg " << fg << " mds" << fragmap[fg] << std::endl;
	  return fragmap[fg];
	}
	fg = frag_t(fg.value(), fg.bits()-1); // try more general...
      }
    }
    return authority();
  }

  int authority() {
    if (dir_auth >= 0)
      return dir_auth;

    assert(dn);
    return dn->dir->parent_inode->authority(dn->name);
  }


  int pick_replica(MDSMap *mdsmap) {
    // replicas?
    /* fixme
    if (//ino() > 1ULL && 
	dir_contacts.size()) {
      set<int>::iterator it = dir_contacts.begin();
      if (dir_contacts.size() == 1)
        return *it;
      else {
	//cout << "dir_contacts on " << inode.ino << " is " << dir_contacts << std::endl;
	int r = 1 + (rand() % dir_contacts.size());
	int a = authority();
	while (r--) {
	  it++;
	  if (mdsmap->is_down(*it)) it++;
	  if (it == dir_contacts.end()) it = dir_contacts.begin();
	  if (*it == a) it++;  // skip the authority
	  if (it == dir_contacts.end()) it = dir_contacts.begin();
	}
	return *it;
      }
    }
    */

    if (dir_replicated) {// || ino() == 1) {
      // pick a random mds that isn't the auth
      set<int> s;
      mdsmap->get_in_mds_set(s);
      set<int>::iterator it = s.begin();
      if (s.empty())
	return 0;
      if (s.size() == 1)
        return *it;
      else {
	//cout << "dir_contacts on " << inode.ino << " is " << dir_contacts << std::endl;
	int r = 1 + (rand() % s.size());
	int a = authority();
	while (r--) {
	  it++;
	  if (mdsmap->is_down(*it)) it++;
	  if (it == s.end()) it = s.begin();
	  if (*it == a) it++;  // skip the authority
	  if (it == s.end()) it = s.begin();
	}
	//if (inode.ino == 1) cout << "chose " << *it << " from " << s << std::endl;
	return *it;
      }
      //cout << "num_mds is " << mdcluster->get_num_mds() << endl;
      //return mdsmap->get_random_in_mds();
      //return rand() % mdsmap->get_num_mds();  // huh.. pick a random mds!
    }
    else
      return authority();
  }


  // open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
  Dir *open_dir() {
    if (!dir) {
      if (dn) dn->get();      // pin dentry
      get();                  // pin inode
      dir = new Dir(this);
    }
    return dir;
  }

};




// file handle for any open file state

struct Fh {
  Inode    *inode;
  off_t     pos;
  int       mds;        // have to talk to mds we opened with (for now)
  int       mode;       // the mode i opened the file with

  bool is_lazy() { return mode & O_LAZY; }

  bool pos_locked;           // pos is currently in use
  list<Cond*> pos_waiters;   // waiters for pos

  Fh() : inode(0), pos(0), mds(0), mode(0), pos_locked(false) {}
};





// ========================================================
// client interface

class Client : public Dispatcher {
 public:
  
  /* getdir result */
  struct DirEntry {
    string d_name;
    struct stat st;
    int stmask;
    DirEntry(const string &s) : d_name(s), stmask(0) {}
    DirEntry(const string &n, struct stat& s, int stm) : d_name(n), st(s), stmask(stm) {}
  };

  struct DirResult {
    static const int SHIFT = 28;
    static const int64_t MASK = (1 << SHIFT) - 1;
    static const off_t END = 1ULL << (SHIFT + 32);

    filepath path;
    Inode *inode;
    int64_t offset;   // high bits: frag_t, low bits: an offset
    map<frag_t, vector<DirEntry> > buffer;

    DirResult(const filepath &fp, Inode *in=0) : path(fp), inode(in), offset(0) { 
      if (inode) inode->get();
    }

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
    void set_end() { offset = END; }
    bool at_end() { return (offset == END); }
  };


  // cluster descriptors
  MDSMap *mdsmap; 
  OSDMap *osdmap;

  SafeTimer timer;

  Context *tick_event;
  utime_t last_cap_renew_request;
  utime_t last_cap_renew;
  void renew_caps();
public:
  void tick();

 protected:
  Messenger *messenger;  
  int whoami;
  MonMap *monmap;
  
  // mds sessions
  map<int, version_t> mds_sessions;  // mds -> push seq
  map<int, list<Cond*> > waiting_for_session;
  map<int, int> caps_by_mds;
  list<Cond*> waiting_for_mdsmap;

  void handle_client_session(MClientSession *m);
  void send_reconnect(int mds);

  // mds requests
  struct MetaRequest {
    tid_t tid;
    MClientRequest *request;    
    bufferlist request_payload;  // in case i have to retry

    bool     idempotent;         // is request idempotent?
    set<int> mds;                // who i am asking
    int      resend_mds;         // someone wants you to (re)send the request here
    int      num_fwd;            // # of times i've been forwarded
    int      retry_attempt;

    MClientReply *reply;         // the reply

    Cond  *caller_cond;          // who to take up
    Cond  *dispatch_cond;        // who to kick back

    MetaRequest(MClientRequest *req, tid_t t) : 
      tid(t), request(req), 
      idempotent(false), resend_mds(-1), num_fwd(0), retry_attempt(0),
      reply(0), 
      caller_cond(0), dispatch_cond(0) { }
  };
  tid_t last_tid;
  map<tid_t, MetaRequest*> mds_requests;
  set<int>                 failed_mds;

  struct StatfsRequest {
    tid_t tid;
    MStatfsReply *reply;
    Cond *caller_cond;
    StatfsRequest(tid_t t, Cond *cc) : tid(t), reply(0), caller_cond(cc) {}
  };
  map<tid_t,StatfsRequest*> statfs_requests;
  
  MClientReply *make_request(MClientRequest *req, int use_auth=-1);
  int choose_target_mds(MClientRequest *req);
  void send_request(MetaRequest *request, int mds);
  void kick_requests(int mds);
  void handle_client_request_forward(MClientRequestForward *reply);
  void handle_client_reply(MClientReply *reply);
  void handle_statfs_reply(MStatfsReply *reply);

  bool   mounted;
  int mounters;
  bool   unmounting;
  Cond   mount_cond;  

  int    unsafe_sync_write;
public:
  entity_name_t get_myname() { return messenger->get_myname(); } 
  void hack_sync_write_safe();

protected:
  Filer                 *filer;     
  ObjectCacher          *objectcacher;
  Objecter              *objecter;     // (non-blocking) osd interface
  
  // cache
  hash_map<inodeno_t, Inode*> inode_map;
  Inode*                 root;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  // cap weirdness
  map<inodeno_t, map<int, class MClientFileCaps*> > cap_reap_queue;  // ino -> mds -> msg .. set of (would-be) stale caps to reap


  // file handles, etc.
  string                 cwd;
  interval_set<int> free_fd_set;  // unused fds
  hash_map<int, Fh*> fd_map;
  
  int get_fd() {
    int fd = free_fd_set.start();
    free_fd_set.erase(fd, 1);
    return fd;
  }
  void put_fd(int fd) {
    free_fd_set.insert(fd, 1);
  }

  void mkabspath(const char *rel, string& abs) {
    if (rel[0] == '/') {
      abs = rel;
    } else {
      abs = cwd;
      abs += "/";
      abs += rel;
    }
  }


  // global client lock
  //  - protects Client and buffer cache both!
  Mutex                  client_lock;


  // -- metadata cache stuff

  // decrease inode ref.  delete if dangling.
  void put_inode(Inode *in, int n=1) {
    //cout << "put_inode on " << in << " " << in->inode.ino << endl;
    in->put(n);
    if (in->ref == 0) {
      //cout << "put_inode deleting " << in->inode.ino << endl;
      inode_map.erase(in->inode.ino);
      if (in == root) root = 0;
      delete in;
    }
  }

  void close_dir(Dir *dir) {
    assert(dir->is_empty());
    
    Inode *in = dir->parent_inode;
    if (in->dn) in->dn->put();   // unpin dentry
    
    delete in->dir;
    in->dir = 0;
    put_inode(in);               // unpin inode
  }

  //int get_cache_size() { return lru.lru_get_size(); }
  //void set_cache_size(int m) { lru.lru_set_max(m); }

  Dentry* link(Dir *dir, const string& name, Inode *in) {
    Dentry *dn = new Dentry;
    dn->name = name;
    
    // link to dir
    dn->dir = dir;
    //cout << "link dir " << dir->parent_inode->inode.ino << " '" << name << "' -> inode " << in->inode.ino << endl;
    dir->dentries[dn->name] = dn;

    // link to inode
    dn->inode = in;
    assert(in->dn == 0);
    in->dn = dn;
    in->get();

    if (in->dir) dn->get();  // dir -> dn pin

    lru.lru_insert_mid(dn);    // mid or top?
    return dn;
  }

  void unlink(Dentry *dn) {
    Inode *in = dn->inode;
    assert(in->dn == dn);

    // unlink from inode
    if (dn->inode->dir) dn->put();        // dir -> dn pin
    dn->inode = 0;
    in->dn = 0;
    put_inode(in);
        
    // unlink from dir
    dn->dir->dentries.erase(dn->name);
    if (dn->dir->is_empty()) 
      close_dir(dn->dir);
    dn->dir = 0;

    // delete den
    lru.lru_remove(dn);
    delete dn;
  }

  Dentry *relink(Dir *dir, const string& name, Inode *in) {
    Dentry *olddn = in->dn;
    Dir *olddir = olddn->dir;  // note: might == dir!

    // newdn, attach to inode.  don't touch inode ref.
    Dentry *newdn = new Dentry;
    newdn->dir = dir;
    newdn->name = name;
    newdn->inode = in;
    in->dn = newdn;

    if (in->dir) { // dir -> dn pin
      newdn->get();
      olddn->put();
    }

    // unlink old dn from dir
    olddir->dentries.erase(olddn->name);
    olddn->inode = 0;
    olddn->dir = 0;
    lru.lru_remove(olddn);
    
    // link new dn to dir
    dir->dentries[name] = newdn;
    lru.lru_insert_mid(newdn);
    
    // olddir now empty?  (remember, olddir might == dir)
    if (olddir->is_empty()) 
      close_dir(olddir);

    return newdn;
  }

  // move dentry to top of lru
  void touch_dn(Dentry *dn) { lru.lru_touch(dn); }  

  // trim cache.
  void trim_cache();
  void dump_inode(Inode *in, set<Inode*>& did);
  void dump_cache();  // debug
  
  // find dentry based on filepath
  Dentry *lookup(filepath& path);

  int fill_stat(Inode *in, struct stat *st);

  
  // trace generation
  ofstream traceout;


  // friends
  friend class SyntheticClient;

 public:
  Client(Messenger *m, MonMap *mm);
  ~Client();
  void tear_down_cache();   

  int get_nodeid() { return whoami; }

  void init();
  void shutdown();

  // messaging
  void dispatch(Message *m);

  void handle_mon_map(MMonMap *m);
  void handle_unmount(Message*);
  void handle_mds_map(class MMDSMap *m);

  // file caps
  void handle_file_caps(class MClientFileCaps *m);
  void implemented_caps(class MClientFileCaps *m, Inode *in);
  void release_caps(Inode *in, int retain=0);
  void update_caps_wanted(Inode *in);

  void close_release(Inode *in);
  void close_safe(Inode *in);

  void lock_fh_pos(Fh *f);
  void unlock_fh_pos(Fh *f);
  
  // metadata cache
  Inode* insert_inode(Dir *dir, InodeStat *in_info, const string& dn);
  void update_dir_dist(Inode *in, DirStat *st);
  Inode* insert_trace(MClientReply *reply);

  // ----------------------
  // fs ops.
private:
  void _try_mount();
  void _mount_timeout();
  Context *mount_timeout_event;

  class C_MountTimeout : public Context {
    Client *client;
  public:
    C_MountTimeout(Client *c) : client(c) { }
    void finish(int r) {
      if (r >= 0) client->_mount_timeout();
    }
  };

  // some helpers
  int _do_lstat(const char *path, int mask, Inode **in);
  int _opendir(const char *name, DirResult **dirpp);
  void _readdir_add_dirent(DirResult *dirp, const string& name, Inode *in);
  void _readdir_fill_dirent(struct dirent *de, DirEntry *entry, off_t);
  bool _readdir_have_frag(DirResult *dirp);
  void _readdir_next_frag(DirResult *dirp);
  void _readdir_rechoose_frag(DirResult *dirp);
  int _readdir_get_frag(DirResult *dirp);
  void _closedir(DirResult *dirp);
  void _ll_get(Inode *in);
  int _ll_put(Inode *in, int num);
  void _ll_drop_pins();

  // internal interface
  //   call these with client_lock held!
  int _link(const char *existing, const char *newname);
  int _unlink(const char *path);
  int _rename(const char *from, const char *to);
  int _mkdir(const char *path, mode_t mode);
  int _rmdir(const char *path);
  int _readlink(const char *path, char *buf, off_t size);
  int _symlink(const char *existing, const char *newname);
  int _lstat(const char *path, struct stat *stbuf);
  int _chmod(const char *relpath, mode_t mode);
  int _chown(const char *relpath, uid_t uid, gid_t gid);
  int _utimes(const char *relpath, utime_t mtime, utime_t atime);
  int _mknod(const char *path, mode_t mode, dev_t rdev);
  int _open(const char *path, int flags, mode_t mode, Fh **fhp);
  int _release(Fh *fh);
  int _read(Fh *fh, off_t offset, off_t size, bufferlist *bl);
  int _write(Fh *fh, off_t offset, off_t size, const char *buf);
  int _flush(Fh *fh);
  int _truncate(const char *file, off_t length);
  int _ftruncate(Fh *fh, off_t length);
  int _fsync(Fh *fh, bool syncdataonly);
  int _statfs(struct statvfs *stbuf);


public:
  int mount();
  int unmount();

  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statvfs *stbuf);

  // crap
  int chdir(const char *s);
  const string getcwd() { return cwd; }

  // namespace ops
  int getdir(const char *relpath, list<string>& names);  // get the whole dir at once.

  int opendir(const char *name, DIR **dirpp);
  int closedir(DIR *dirp);
  int readdir_r(DIR *dirp, struct dirent *de);
  int readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask);
  void rewinddir(DIR *dirp); 
  off_t telldir(DIR *dirp);
  void seekdir(DIR *dirp, off_t offset);

  struct dirent_plus *readdirplus(DIR *dirp);
  int readdirplus_r(DIR *dirp, struct dirent_plus *entry, struct dirent_plus **result);
  struct dirent_lite *readdirlite(DIR *dirp);
  int readdirlite_r(DIR *dirp, struct dirent_lite *entry, struct dirent_lite **result);
 
  int link(const char *existing, const char *newname);
  int unlink(const char *path);
  int rename(const char *from, const char *to);

  // dirs
  int mkdir(const char *path, mode_t mode);
  int rmdir(const char *path);

  // symlinks
  int readlink(const char *path, char *buf, off_t size);
  int symlink(const char *existing, const char *newname);

  // inode stuff
  int lstat(const char *path, struct stat *stbuf);
  int lstatlite(const char *path, struct statlite *buf);

  int chmod(const char *path, mode_t mode);
  int chown(const char *path, uid_t uid, gid_t gid);
  int utime(const char *path, struct utimbuf *buf);

  // file ops
  int mknod(const char *path, mode_t mode, dev_t rdev=0);
  int open(const char *path, int flags, mode_t mode=0);
  int close(int fd);
  off_t lseek(int fd, off_t offset, int whence);
  int read(int fd, char *buf, off_t size, off_t offset=-1);
  int write(int fd, const char *buf, off_t size, off_t offset=-1);
  int fake_write_size(int fd, off_t size);
  int truncate(const char *file, off_t size);
  int ftruncate(int fd, off_t size);
  int fsync(int fd, bool syncdataonly);

  // hpc lazyio
  int lazyio_propogate(int fd, off_t offset, size_t count);
  int lazyio_synchronize(int fd, off_t offset, size_t count);

  // expose file layout
  int describe_layout(int fd, FileLayout* layout);
  int get_stripe_unit(int fd);
  int get_stripe_width(int fd);
  int get_stripe_period(int fd);
  int enumerate_layout(int fd, list<ObjectExtent>& result,
		       off_t length, off_t offset);

  // low-level interface
  int ll_lookup(inodeno_t parent, const char *name, struct stat *attr);
  bool ll_forget(inodeno_t ino, int count);
  Inode *_ll_get_inode(inodeno_t ino);
  int ll_getattr(inodeno_t ino, struct stat *st);
  int ll_setattr(inodeno_t ino, struct stat *st, int mask);
  int ll_opendir(inodeno_t ino, void **dirpp);
  void ll_releasedir(void *dirp);
  int ll_readlink(inodeno_t ino, const char **value);
  int ll_mknod(inodeno_t ino, const char *name, mode_t mode, dev_t rdev, struct stat *attr);
  int ll_mkdir(inodeno_t ino, const char *name, mode_t mode, struct stat *attr);
  int ll_symlink(inodeno_t ino, const char *name, const char *value, struct stat *attr);
  int ll_unlink(inodeno_t ino, const char *name);
  int ll_rmdir(inodeno_t ino, const char *name);
  int ll_rename(inodeno_t parent, const char *name, inodeno_t newparent, const char *newname);
  int ll_link(inodeno_t ino, inodeno_t newparent, const char *newname, struct stat *attr);
  int ll_open(inodeno_t ino, int flags, Fh **fh);
  int ll_create(inodeno_t parent, const char *name, mode_t mode, int flags, struct stat *attr, Fh **fh);
  int ll_read(Fh *fh, off_t off, off_t len, bufferlist *bl);
  int ll_write(Fh *fh, off_t off, off_t len, const char *data);
  int ll_flush(Fh *fh);
  int ll_fsync(Fh *fh, bool syncdataonly);
  int ll_release(Fh *fh);
  int ll_statfs(inodeno_t, struct statvfs *stbuf);


  // failure
  void ms_handle_failure(Message*, const entity_inst_t& inst);
  void ms_handle_reset(const entity_addr_t& addr, entity_name_t last);
  void ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t last);
};

#endif
