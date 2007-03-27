// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "msg/SerialMessenger.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MOSDUpdate.h"
#include "messages/MOSDUpdateReply.h"
#include "messages/MClientUpdate.h"
#include "messages/MClientUpdateReply.h"
#include "messages/MClientRenewal.h"
//#include "messages/MClientRenewalReply.h"

//#include "msgthread.h"

#include "include/types.h"
#include "include/lru.h"
#include "include/filepath.h"
#include "include/interval_set.h"

#include "common/Mutex.h"

#include "FileCache.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

#include "crypto/Ticket.h"
#include "crypto/CapGroup.h"
#include "crypto/MerkleTree.h"
#include "crypto/RecentPopularity.h"
//#include "ClientCapCache.h"

// stl
#include <set>
#include <map>
using namespace std;

#include<unistd.h>
#include<sys/types.h>

#include <ext/hash_map>
using namespace __gnu_cxx;

#define O_LAZY 01000000


class Filer;
class Objecter;
class ObjectCacher;
class Ticket;
class ClientCapCache;

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

typedef int fh_t;

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
  time_t    valid_until;

  // about the dir (if this is one!)
  int       dir_auth;
  set<int>    dir_contacts;
  bool      dir_hashed, dir_replicated;

  // per-mds caps
  map<int,InodeCap> caps;            // mds -> InodeCap
  map<int,InodeCap> stale_caps;      // mds -> cap .. stale

  time_t    file_wr_mtime;   // [writers] time of last write
  off_t     file_wr_size;    // [writers] largest offset we've written to
  int       num_open_rd, num_open_wr, num_open_lazy;  // num readers, writers

  int       ref;      // ref count. 1 for each dentry, fh that links to me.
  Dir       *dir;     // if i'm a dir.
  Dentry    *dn;      // if i'm linked to a dentry.
  string    *symlink; // symlink content, if it's a symlink

  // secure caps
  map<uid_t, ExtCap> ext_cap_cache;

  // for caching i/o mode
  FileCache fc;

  // for sync i/o mode
  int       sync_reads;   // sync reads in progress
  int       sync_writes;  // sync writes in progress

  list<Cond*>       waitfor_write;
  list<Cond*>       waitfor_read;
  list<Cond*>       waitfor_lazy;
  list<Context*>    waitfor_no_read, waitfor_no_write;

  void get() { 
    ref++; 
    //cout << "inode.get on " << hex << inode.ino << dec << " now " << ref << endl;
  }
  void put() { 
    ref--; assert(ref >= 0); 
    //cout << "inode.put on " << hex << inode.ino << dec << " now " << ref << endl;
  }

  Inode(inode_t _inode, ObjectCacher *_oc) : 
    inode(_inode),
    valid_until(0),
    dir_auth(-1), dir_hashed(false), dir_replicated(false), 
    file_wr_mtime(0), file_wr_size(0), 
    num_open_rd(0), num_open_wr(0), num_open_lazy(0),
    ref(0), dir(0), dn(0), symlink(0),
    fc(_oc, _inode),
    sync_reads(0), sync_writes(0)
  { }
  ~Inode() {
    if (symlink) { delete symlink; symlink = 0; }
  }

  inodeno_t ino() { return inode.ino; }

  bool is_dir() {
    return (inode.mode & INODE_TYPE_MASK) == INODE_MODE_DIR;
  }

  void set_ext_cap(uid_t user, ExtCap *ecap) { ext_cap_cache[user] = (*ecap); }
  ExtCap* get_ext_cap(uid_t user) {
    if (ext_cap_cache.count(user))
      return &(ext_cap_cache[user]); 
    return 0;
  }

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
    if (num_open_rd) w |= CAP_FILE_RD|CAP_FILE_RDCACHE;
    if (num_open_wr) w |= CAP_FILE_WR|CAP_FILE_WRBUFFER;
    if (num_open_lazy) w |= CAP_FILE_LAZYIO;
    return w;
  }

  int authority(MDSMap *mdsmap) {
    //cout << "authority on " << inode.ino << " .. dir_auth is " << dir_auth<< endl;
    // parent?
    if (dn && dn->dir && dn->dir->parent_inode) {
      // parent hashed?
      if (dn->dir->parent_inode->dir_hashed) {
        // hashed
	assert(0); 
	// fixme
        //return mdcluster->hash_dentry( dn->dir->parent_inode->ino(),
	//dn->name );
      }

      if (dir_auth >= 0)
        return dir_auth;
      else
        return dn->dir->parent_inode->authority(mdsmap);
    }

    if (dir_auth >= 0)
      return dir_auth;

    assert(0);    // !!!
    return 0;
  }
  int dentry_authority(const char *dn,
                       MDSMap *mdsmap) {
    assert(0);
    return 0;
    //return ->hash_dentry( ino(),
    //dn );
  }
  int pick_replica(MDSMap *mdsmap) {
    // replicas?
    if (ino() > 1ULL && dir_contacts.size()) {
      //cout << "dir_contacts if " << dir_contacts << endl;
      set<int>::iterator it = dir_contacts.begin();
      if (dir_contacts.size() == 1)
        return *it;
      else {
        int r = rand() % dir_contacts.size();
        while (r--) it++;
        return *it;
      }
    }

    if (dir_replicated || ino() == 1) {
      //cout << "num_mds is " << mdcluster->get_num_mds() << endl;
      return rand() % mdsmap->get_num_mds();  // huh.. pick a random mds!
    }
    else
      return authority(mdsmap);
  }


  // open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
  Dir *open_dir() {
    if (!dir) {
      if (dn) dn->get();      // pin dentry
      get();
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

  Fh() : inode(0), pos(0), mds(0), mode(0) {}
};





// ========================================================
// client interface

class Client : public Dispatcher {
 public:
  
  /* getdir result */
  struct DirResult {
    string path;
    map<string,inode_t> contents;
    map<string,inode_t>::iterator p;
    int off;
    int size;
    struct dirent_plus dp;
    struct dirent_lite dl;
    DirResult() : p(contents.end()), off(-1), size(0) {}
  };


 protected:
  Messenger *messenger;  
  int whoami;
  MonMap *monmap;
  
  // mds fake RPC
  tid_t last_tid;
  map<tid_t, Cond*>                mds_rpc_cond;
  map<tid_t, class MClientReply*>  mds_rpc_reply;
  map<tid_t, Cond*>                mds_rpc_dispatch_cond;

  // cluster descriptors
  MDSMap *mdsmap; 
  OSDMap *osdmap;

  bool   mounted;
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

  // capability cache
  ClientCapCache *capcache;
  
  // cache
  hash_map<inodeno_t, Inode*> inode_map;
  Inode*                 root;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  // cap weirdness
  map<inodeno_t, map<int, class MClientFileCaps*> > cap_reap_queue;  // ino -> mds -> msg .. set of (would-be) stale caps to reap


  // file handles, etc.
  string                 cwd;
  interval_set<fh_t>     free_fh_set;  // unused fh's
  hash_map<fh_t, Fh*>    fh_map;
  
  fh_t get_fh() {
    fh_t fh = free_fh_set.start();
    free_fh_set.erase(fh, 1);
    return fh;
  }
  void put_fh(fh_t fh) {
    free_fh_set.insert(fh, 1);
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
  void put_inode(Inode *in) {
    in->put();
    if (in->ref == 0) {
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
    put_inode(in);
  }

  int get_cache_size() { return lru.lru_get_size(); }
  void set_cache_size(int m) { lru.lru_set_max(m); }

  Dentry* link(Dir *dir, const string& name, Inode *in) {
    Dentry *dn = new Dentry;
    dn->name = name;
    
    // link to dir
    dn->dir = dir;
    dir->dentries[dn->name] = dn;

    // link to inode
    dn->inode = in;
    in->dn = dn;
    in->get();

    lru.lru_insert_mid(dn);    // mid or top?
    return dn;
  }

  void unlink(Dentry *dn) {
    Inode *in = dn->inode;

    // unlink from inode
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

  Dentry *relink(Dentry *dn, Dir *dir, const string& name) {
    // first link new dn to dir
    /*
    char *oldname = (char*)dn->name;
    dn->name = new char[name.length()+1];
    strcpy((char*)dn->name, name.c_str());
    dir->dentries[dn->name] = dn;
    */
    dir->dentries[name] = dn;

    // unlink from old dir
    dn->dir->dentries.erase(dn->name);
    //delete[] oldname;
    if (dn->dir->is_empty()) 
      close_dir(dn->dir);

    // fix up dn
    dn->name = name;
    dn->dir = dir;

    return dn;
  }

  // move dentry to top of lru
  void touch_dn(Dentry *dn) { lru.lru_touch(dn); }  

  // trim cache.
  void trim_cache();
  void dump_inode(Inode *in, set<Inode*>& did);
  void dump_cache();  // debug
  
  // find dentry based on filepath
  Dentry *lookup(filepath& path);

  // make blocking mds request
  MClientReply *make_request(MClientRequest *req, bool auth_best=false, int use_auth=-1);
  MClientReply* sendrecv(MClientRequest *req, int mds);
  void handle_client_reply(MClientReply *reply);

  void fill_stat(inode_t& inode, struct stat *st);
  void fill_statlite(inode_t& inode, struct statlite *st);

  // security
  map<uid_t,Ticket*> user_ticket;
  map<uid_t,int>     user_ticket_ref;
  map<uid_t,list<Cond*> >   ticket_waiter_cond;
  map<uid_t,esignPub*> user_pub_key;
  map<uid_t,esignPriv*> user_priv_key;

  map<hash_t, CapGroup> groups;
  //map<hash_t, set<entity_inst_t> > update_waiter_osd;
  map<hash_t, set<int> > update_waiter_osd;
  //map<hash_t, list<Cond*> > update_waiter_cond;
  map<uid_t, set<cap_id_t> > caps_in_use;
  // renew caps that are in use (leaves a re-use grace period)
  // expunge caps that are not open, are expired and have no extension

  // prediction
  map<uid_t, string > successor;
  map<uid_t, inodeno_t> successor_inode;
  map<string, inodeno_t> path_map;
  map<uid_t, RecentPopularity> predicter;

  Ticket *get_user_ticket(uid_t uid, gid_t gid);
  void put_user_ticket(Ticket *tk);

  void handle_osd_update(MOSDUpdate *m);
  void handle_client_update_reply(MClientUpdateReply *m);

  // friends
  friend class SyntheticClient;
  friend class ClientCapCache;

 public:
  Client(Messenger *m, MonMap *mm);
  ~Client();
  void tear_down_cache();   

  int get_nodeid() { return whoami; }

  void init();
  void shutdown();

  // messaging
  void dispatch(Message *m);
  void handle_mount_ack(class MClientMountAck*);
  void handle_unmount_ack(Message*);
  void handle_mds_map(class MMDSMap *m);

  // user tickets
  void handle_auth_user_ack(class MClientAuthUserAck *m);

  // file caps
  void handle_file_caps(class MClientFileCaps *m);
  void implemented_caps(class MClientFileCaps *m, Inode *in);
  void release_caps(Inode *in, int retain=0);
  void update_caps_wanted(Inode *in);

  void close_release(Inode *in);
  void close_safe(Inode *in);

  // metadata cache
  Inode* insert_inode(Dir *dir, InodeStat *in_info, const string& dn);
  void update_inode_dist(Inode *in, InodeStat *st);
  Inode* insert_trace(MClientReply *reply);

  // ----------------------
  // fs ops.
  int mount();
  int unmount();

  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statvfs *stbuf,
	     __int64_t uid=-1, __int64_t gid=-1);

  // crap
  int chdir(const char *s, __int64_t uid, __int64_t gid);

  // namespace ops
  int getdir(const char *path, list<string>& contents,
	     __int64_t uid = -1, __int64_t gid = -1);
  int getdir(const char *path, map<string,inode_t>& contents,
	     __int64_t uid = -1, __int64_t gid = -1);

  DIR *opendir(const char *name, __int64_t uid = -1, __int64_t gid = -1);
  int closedir(DIR *dir, __int64_t uid = -1, __int64_t gid = -1);
  struct dirent *readdir(DIR *dir, __int64_t uid = -1, __int64_t gid = -1); 
  void rewinddir(DIR *dir, __int64_t uid = -1, __int64_t gid = -1); 
  off_t telldir(DIR *dir, __int64_t uid = -1, __int64_t gid = -1);
  void seekdir(DIR *dir, off_t offset,
	       __int64_t uid = -1, __int64_t gid = -1);

  struct dirent_plus *readdirplus(DIR *dirp,
				  __int64_t uid = -1, __int64_t gid = -1);
  int readdirplus_r(DIR *dirp, struct dirent_plus *entry, struct dirent_plus **result);
  struct dirent_lite *readdirlite(DIR *dirp, __int64_t uid, __int64_t gid);
  int readdirlite_r(DIR *dirp, struct dirent_lite *entry, struct dirent_lite **result);
 

  int link(const char *existing, const char *newname,
	   __int64_t uid = -1, __int64_t gid = -1);
  int unlink(const char *path, __int64_t uid = -1, __int64_t gid = -1);
  int rename(const char *from, const char *to,
	     __int64_t uid = -1, __int64_t gid = -1);

  // dirs
  int mkdir(const char *path, mode_t mode,
	    __int64_t uid = -1, __int64_t gid = -1);
  int rmdir(const char *path, __int64_t uid = -1, __int64_t gid = -1);

  // symlinks
  int readlink(const char *path, char *buf, off_t size,
	       __int64_t uid = -1, __int64_t gid = -1);
  int symlink(const char *existing, const char *newname,
	      __int64_t uid = -1, __int64_t gid = -1);

  // inode stuff
  int _lstat(const char *path, int mask, Inode **in,
	     __int64_t uid = -1, __int64_t gid = -1);
  int lstat(const char *path, struct stat *stbuf,
	    __int64_t uid = -1, __int64_t gid = -1);
  int lstatlite(const char *path, struct statlite *buf,
		__int64_t uid = -1, __int64_t gid = -1);

  int chmod(const char *path, mode_t mode,
	    __int64_t uid = -1, __int64_t gid = -1);
  //int chown(const char *path, uid_t uid, gid_t gid,
  //    __int64_t uid = -1, __int64_t gid = -1);
  int chown(const char *path, __int64_t uid, __int64_t gid);
  int utime(const char *path, struct utimbuf *buf,
	    __int64_t uid = -1, __int64_t gid = -1);
  
  // file ops
  int mknod(const char *path, mode_t mode,
	    __int64_t uid = -1, __int64_t gid = -1);
  int open(const char *path, int mode,
	   __int64_t uid = -1, __int64_t gid = -1);
  int close(fh_t fh,
	    __int64_t uid = -1, __int64_t gid = -1);
  off_t lseek(fh_t fh, off_t offset, int whence);
  int read(fh_t fh, char *buf, off_t size, off_t offset=-1,
	   __int64_t uid = -1, __int64_t gid = -1);
  int write(fh_t fh, const char *buf, off_t size, off_t offset=-1,
	    __int64_t uid = -1, __int64_t gid = -1);
  int truncate(const char *file, off_t size,
	       __int64_t uid = -1, __int64_t gid = -1);
    //int truncate(fh_t fh, long long size);
  int fsync(fh_t fh, bool syncdataonly,
	    __int64_t uid = -1, __int64_t gid = -1);


  // hpc lazyio
  int lazyio_propogate(int fd, off_t offset, size_t count,
		       __int64_t uid = -1, __int64_t gid = -1);
  int lazyio_synchronize(int fd, off_t offset, size_t count,
			 __int64_t uid = -1, __int64_t gid = -1);

  // expose file layout
  int describe_layout(int fd, FileLayout* layout);
  int get_stripe_unit(int fd);
  int get_stripe_width(int fd);
  int get_stripe_period(int fd);
  int enumerate_layout(int fd, list<ObjectExtent>& result,
		       off_t length, off_t offset);

  // failure
  void ms_handle_failure(Message*, const entity_inst_t& inst);

};

#endif
