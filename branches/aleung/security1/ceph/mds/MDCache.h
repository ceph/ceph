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

class MMDSImportMap;
class MMDSCacheRejoin;
class MMDSCacheRejoinAck;
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
 public:
  // my master
  MDS *mds;

  LRU                           lru;         // dentry lru for expiring items from cache

 protected:
  // the cache
  CInode                       *root;        // root inode
  hash_map<inodeno_t,CInode*>   inode_map;   // map of inodes by ino            
  
  list<CInode*>                 inode_expire_queue;  // inodes to delete


  // root
  list<Context*>     waiting_for_root;

  // imports, exports, and hashes.
  set<CDir*>             imports;                // includes root (on mds0)
  set<CDir*>             exports;
  set<CDir*>             hashdirs;
  map<CDir*,set<CDir*> > nested_exports;         // exports nested under imports _or_ hashdirs
  
  void adjust_export(int to,   CDir *root, set<CDir*>& bounds);
  void adjust_import(int from, CDir *root, set<CDir*>& bounds);
  
  

  // active MDS requests
  hash_map<Message*, active_request_t>   active_requests;
  
  // inode purging
  map<inodeno_t, inode_t>         purging;
  map<inodeno_t, list<Context*> > waiting_for_purge;

  // shutdown crap
  int shutdown_commits;
  bool did_shutdown_exports;
  bool did_shutdown_log_cap;
  friend class C_MDC_ShutdownCommit;
  friend class UserBatch;

  // recovery
protected:
  // from EImportStart w/o EImportFinish during journal replay
  map<inodeno_t, set<inodeno_t> >            my_ambiguous_imports;  
  // from MMDSImportMaps
  map<int, map<inodeno_t, set<inodeno_t> > > other_ambiguous_imports;  

  set<int> recovery_set;
  set<int> wants_import_map;   // nodes i need to send my import map to
  set<int> got_import_map;     // nodes i need to send my import map to (when exports finish)
  set<int> rejoin_ack_gather;  // nodes i need a rejoin ack from
  
  void handle_import_map(MMDSImportMap *m);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_ack(MMDSCacheRejoinAck *m);
  void disambiguate_imports();
  void cache_rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin);
  void send_cache_rejoin_acks();
public:
  void send_import_map(int who);
  void send_import_map_now(int who);
  void send_import_map_later(int who) {
    wants_import_map.insert(who);
  }
  void send_pending_import_maps();  // maybe.
  void send_cache_rejoins();

  void set_recovery_set(set<int>& s) {
    recovery_set = s;
  }

  // ambiguous imports
  void add_ambiguous_import(inodeno_t base, set<inodeno_t>& bounds) {
    my_ambiguous_imports[base].swap(bounds);
  }
  void cancel_ambiguous_import(inodeno_t dirino);
  void finish_ambiguous_import(inodeno_t dirino);
  
  void finish_ambiguous_export(inodeno_t dirino, set<inodeno_t>& bounds);



  

  friend class CInode;
  friend class Locker;
  friend class Migrator;
  friend class Renamer;
  friend class MDBalancer;
  friend class EImportMap;


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
  void set_root(CInode *r);

  int get_num_imports() { return imports.size(); }
  void add_import(CDir *dir);
  void remove_import(CDir *dir);
  void recalc_auth_bits();

  void log_import_map(Context *onsync=0);

 
  // cache
  void set_cache_size(size_t max) { lru.lru_set_max(max); }
  size_t get_cache_size() { return lru.lru_get_size(); }
  bool trim(int max = -1);   // trim cache
  void trim_non_auth();      // trim out trimmable non-auth items

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
  

  int hash_dentry(inodeno_t ino, const string& s) {
    return 0; // fixme
  }
  

 public:
  CInode *create_inode();
  void add_inode(CInode *in);

 protected:
  void remove_inode(CInode *in);
  void destroy_inode(CInode *in);
  void touch_inode(CInode *in) {
    if (in->get_parent_dn())
      touch_dentry(in->get_parent_dn());
  }
  void touch_dentry(CDentry *dn) {
    // touch ancestors
    if (dn->get_dir()->get_inode()->get_parent_dn())
      touch_dentry(dn->get_dir()->get_inode()->get_parent_dn());
    
    // touch me
    if (dn->is_auth())
      lru.lru_touch(dn);
    else
      lru.lru_midtouch(dn);
  }

  void inode_remove_replica(CInode *in, int rep);

  void rename_file(CDentry *srcdn, CDentry *destdn);

 public:
  // inode purging
  void purge_inode(inode_t& inode);
  void purge_inode_finish(inodeno_t ino);
  void purge_inode_finish_2(inodeno_t ino);
  void waitfor_purge(inodeno_t ino, Context *c);
  void start_recovered_purges();


 public:
  CDir *get_auth_container(CDir *in);
  CDir *get_export_container(CDir *dir);
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);


 public:
  CInode *create_root_inode();
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
