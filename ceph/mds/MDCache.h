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
class Renamer;

class Logger;

class Message;

class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryUnlink;
class MLock;


class MClientRequest;


// MDCache

//typedef const char* pchar;



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
  // my master
  MDS *mds;

  // the cache
  CInode                       *root;        // root inode
  LRU                           lru;         // lru for expiring items
  hash_map<inodeno_t,CInode*>   inode_map;   // map of inodes by ino            
 
  // root
  list<Context*>     waiting_for_root;

  // imports, exports, and hashes.
  set<CDir*>             imports;                // includes root (on mds0)
  set<CDir*>             exports;
  set<CDir*>             hashdirs;
  map<CDir*,set<CDir*> > nested_exports;         // exports nested under imports _or_ hashdirs
  
  // active MDS requests
  hash_map<Message*, active_request_t>   active_requests;

  // shutdown crap
  int shutdown_commits;
  bool did_shutdown_exports;
  friend class C_MDC_ShutdownCommit;

  friend class Locker;
  friend class Migrator;
  friend class Renamer;
  friend class MDBalancer;

 public:
  // subsystems
  Migrator *migrator;
  Renamer *renamer;

 public:
  MDCache(MDS *m);
  ~MDCache();
  
  // debug
  void log_stat(Logger *logger);

  // root inode
  CInode *get_root() { return root; }
  void set_root(CInode *r) {
    root = r;
    add_inode(root);
  }

  // cache
  void set_cache_size(size_t max) { lru.lru_set_max(max); }
  size_t get_cache_size() { return lru.lru_get_size(); }
  bool trim(int max = -1);   // trim cache

  // shutdown
  void shutdown_start();
  void shutdown_check();
  bool shutdown_pass();
  bool shutdown();                    // clear cache (ie at shutodwn)

  // inode_map
  bool have_inode( inodeno_t ino ) { return inode_map.count(ino) ? true:false; }
  CInode* get_inode( inodeno_t ino ) {
    if (have_inode(ino))
      return inode_map[ ino ];
    return NULL;
  }
  
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
  void rename_file(CDentry *srcdn, CDentry *destdn);

 protected:
  // private methods
  CDir *get_auth_container(CDir *in);
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);


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
  void dispatch(Message *m);

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



  // == crap fns ==
 public:
  void dump() {
    if (root) root->dump();
  }

  void show_imports();
  void show_cache();

};


#endif
