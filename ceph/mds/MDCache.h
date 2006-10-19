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



#ifndef __MDCACHE_H
#define __MDCACHE_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <ext/hash_map>

#include "include/types.h"
#include "include/filepath.h"

#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "Lock.h"


class MDS;
class Migrator;

class Message;

class MDiscover;
class MDiscoverReply;
//class MInodeUpdate;
class MCacheExpire;
class MDirUpdate;
class MDentryUnlink;
class MLock;

class MRenameWarning;
class MRenameNotify;
class MRenameNotifyAck;
class MRename;
class MRenamePrep;
class MRenameReq;
class MRenameAck;

class MClientRequest;


// MDCache

typedef hash_map<inodeno_t, CInode*> inode_map_t;


typedef const char* pchar;

/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
typedef struct {
  CInode *ref;                                // reference inode
  set< CInode* >            request_pins;
  set< CDir* >              request_dir_pins;
  map< CDentry*, vector<CDentry*> > traces;   // path pins held
  set< CDentry* >           xlocks;           // xlocks (local)
  set< CDentry* >           foreign_xlocks;   // xlocks on foreign hosts
} active_request_t;


class Message;

namespace __gnu_cxx {
  template<> struct hash<Message*> {
    size_t operator()(const Message *p) const { 
      static hash<unsigned long> H;
      return H((unsigned long)p); 
    }
  };
}

class MDCache {
 protected:
  // the cache
  CInode                       *root;        // root inode
 public:
  LRU                           lru;         // lru for expiring items
 protected:
  inode_map_t                   inode_map;   // map of inodes by ino            
 
  MDS *mds;

  // root
  list<Context*>     waiting_for_root;

  // imports, exports, and hashes.
  set<CDir*>             imports;                // includes root (on mds0)
  set<CDir*>             exports;
  set<CDir*>             hashdirs;
  map<CDir*,set<CDir*> > nested_exports;         // exports nested under imports _or_ hashdirs
  
  // rename fun
  set<inodeno_t>                    stray_rename_warnings; // notifies i haven't seen
  map<inodeno_t, MRenameNotify*>    stray_rename_notifies;



 public:
  Migrator *migrator;

  // active MDS requests
  //map<Message*, active_request_t>   active_requests;
  hash_map<Message*, active_request_t>   active_requests;

  int shutdown_commits;
  bool did_shutdown_exports;
  
  friend class Migrator;
  friend class MDBalancer;

 public:
  MDCache(MDS *m);
  ~MDCache();
  
  // root inode
  CInode *get_root() { return root; }
  void set_root(CInode *r) {
    root = r;
    add_inode(root);
  }

  // cache
  void set_cache_size(size_t max) {
    lru.lru_set_max(max);
  }
  size_t get_cache_size() { return lru.lru_get_size(); }
  bool trim(int max = -1);   // trim cache

  // shutdown
  void shutdown_start();
  void shutdown_check();
  bool shutdown_pass();
  bool shutdown();                    // clear cache (ie at shutodwn)

  // have_inode?
  bool have_inode( inodeno_t ino ) {
    return inode_map.count(ino) ? true:false;
  }

  // return inode* or null
  CInode* get_inode( inodeno_t ino ) {
    if (have_inode(ino))
      return inode_map[ ino ];
    return NULL;
  }
  

  // adding/removing
 public:
  CInode *create_inode();
  void add_inode(CInode *in);
 protected:
  void remove_inode(CInode *in);
  void destroy_inode(CInode *in);

  void touch_inode(CInode *in) {
    // touch parent(s) too
    if (in->get_parent_dir()) touch_inode(in->get_parent_dir()->inode);
    
    // top or mid, depending on whether i'm auth
    if (in->is_auth())
      lru.lru_touch(in);
    else
      lru.lru_midtouch(in);
  }

 public:

 protected:
  // private methods
  CDir *get_auth_container(CDir *in);
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);


  void rename_file(CDentry *srcdn, CDentry *destdn);
  void fix_renamed_dir(CDir *srcdir,
                       CInode *in,
                       CDir *destdir,
                       bool authchanged,   // _inode_ auth changed
                       int dirauth=-1);    // dirauth (for certain cases)

 public:
  int open_root(Context *c);
  int path_traverse(filepath& path, vector<CDentry*>& trace, bool follow_trailing_sym,
                    Message *req, Context *ondelay,
                    int onfail,
                    Context *onfinish=0,
                    bool is_client_req = false);
  void open_remote_dir(CInode *diri, Context *fin);
  void open_remote_ino(inodeno_t ino, Message *req, Context *fin);
  void open_remote_ino_2(inodeno_t ino, Message *req,
                         vector<Anchor*>& anchortrace,
                         Context *onfinish);

  bool path_pin(vector<CDentry*>& trace, Message *m, Context *c);
  void path_unpin(vector<CDentry*>& trace, Message *m);
  void make_trace(vector<CDentry*>& trace, CInode *in);
  
  bool request_start(Message *req,
                     CInode *ref,
                     vector<CDentry*>& trace);
  void request_cleanup(Message *req);
  void request_finish(Message *req);
  void request_forward(Message *req, int mds, int port=0);
  void request_pin_inode(Message *req, CInode *in);
  void request_pin_dir(Message *req, CDir *dir);

  // anchors
  void anchor_inode(CInode *in, Context *onfinish);
  //void unanchor_inode(CInode *in, Context *c);

  void handle_inode_link(class MInodeLink *m);
  void handle_inode_link_ack(class MInodeLinkAck *m);

  // == messages ==
 public:
  int proc_message(Message *m);

 protected:
  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);


  // -- namespace --
  // these handle logging, cache sync themselves.
  // UNLINK
 public:
  void dentry_unlink(CDentry *in, Context *c);
 protected:
  void dentry_unlink_finish(CDentry *in, CDir *dir, Context *c);
  void handle_dentry_unlink(MDentryUnlink *m);
  void handle_inode_unlink(class MInodeUnlink *m);
  void handle_inode_unlink_ack(class MInodeUnlinkAck *m);
  friend class C_MDC_DentryUnlink;

  // RENAME
  // initiator
 public:
  void file_rename(CDentry *srcdn, CDentry *destdn, Context *c);
 protected:
  void handle_rename_ack(MRenameAck *m);              // dest -> init (almost always)
  void file_rename_finish(CDir *srcdir, CInode *in, Context *c);
  friend class C_MDC_RenameAck;

  // src
  void handle_rename_req(MRenameReq *m);              // dest -> src
  void file_rename_foreign_src(CDentry *srcdn, 
                               inodeno_t destdirino, string& destname, string& destpath, int destauth, 
                               int initiator);
  void file_rename_warn(CInode *in, set<int>& notify);
  void handle_rename_notify_ack(MRenameNotifyAck *m); // bystanders -> src
  void file_rename_ack(CInode *in, int initiator);
  friend class C_MDC_RenameNotifyAck;

  // dest
  void handle_rename_prep(MRenamePrep *m);            // init -> dest
  void handle_rename(MRename *m);                     // src -> dest
  void file_rename_notify(CInode *in, 
                          CDir *srcdir, string& srcname, CDir *destdir, string& destname,
                          set<int>& notify, int srcauth);

  // bystander
  void handle_rename_warning(MRenameWarning *m);      // src -> bystanders
  void handle_rename_notify(MRenameNotify *m);        // dest -> bystanders



  // -- misc auth --
  int ino_proxy_auth(inodeno_t ino, 
                     int frommds,
                     map<CDir*, set<inodeno_t> >& inomap);
  void do_ino_proxy(CInode *in, Message *m);
  void do_dir_proxy(CDir *dir, Message *m);




  // -- updates --
  //int send_inode_updates(CInode *in);
  //void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, bool bcast=false);
  void handle_dir_update(MDirUpdate *m);

  void handle_cache_expire(MCacheExpire *m);

  // -- locks --
  // high level interface
 public:
  bool inode_hard_read_try(CInode *in, Context *con);
  bool inode_hard_read_start(CInode *in, MClientRequest *m);
  void inode_hard_read_finish(CInode *in);
  bool inode_hard_write_start(CInode *in, MClientRequest *m);
  void inode_hard_write_finish(CInode *in);
  bool inode_file_read_start(CInode *in, MClientRequest *m);
  void inode_file_read_finish(CInode *in);
  bool inode_file_write_start(CInode *in, MClientRequest *m);
  void inode_file_write_finish(CInode *in);

  void inode_hard_eval(CInode *in);
  void inode_file_eval(CInode *in);

 protected:
  void inode_hard_mode(CInode *in, int mode);
  void inode_file_mode(CInode *in, int mode);

  // low level triggers
  void inode_hard_sync(CInode *in);
  void inode_hard_lock(CInode *in);
  bool inode_file_sync(CInode *in);
  void inode_file_lock(CInode *in);
  void inode_file_mixed(CInode *in);
  void inode_file_loner(CInode *in);

  // messengers
  void handle_lock(MLock *m);
  void handle_lock_inode_hard(MLock *m);
  void handle_lock_inode_file(MLock *m);

  // -- file i/o --
 public:
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, MClientRequest *req);
  bool issue_caps(CInode *in);

 protected:
  void handle_client_file_caps(class MClientFileCaps *m);

  void request_inode_file_caps(CInode *in);
  void handle_inode_file_caps(class MInodeFileCaps *m);



  // dirs
  void handle_lock_dir(MLock *m);

  // dentry locks
 public:
  bool dentry_xlock_start(CDentry *dn, 
                          Message *m, CInode *ref);
  void dentry_xlock_finish(CDentry *dn, bool quiet=false);
  void handle_lock_dn(MLock *m);
  void dentry_xlock_request(CDir *dir, string& dname, bool create,
                            Message *req, Context *onfinish);

  

  // == crap fns ==
 public:
  CInode* hack_get_file(string& fn);
  vector<CInode*> hack_add_file(string& fn, CInode* in);

  void dump() {
    if (root) root->dump();
  }

  void show_imports();
  void show_cache();

};


#endif
