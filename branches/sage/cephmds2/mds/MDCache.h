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
#include "include/Context.h"

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

class Message;
class MClientRequest;


// MDCache

//typedef const char* pchar;



/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
struct MDRequest {
  metareqid_t reqid;
  Message *request;  // MClientRequest, or MLock
  
  vector<CDentry*> trace;  // original path traversal.
  CInode *ref;       // reference inode.  if there is only one, and its path is pinned.
  
  // cache pins (so things don't expire)
  set< CInode* >            inode_pins;
  set< CDentry* >           dentry_pins;
  set< CDir* >              dir_pins;
  
  // auth pins
  set< CDir* >              dir_auth_pins;
  set< CInode* >            inode_auth_pins;
  
  // held locks
  set< SimpleLock* > rdlocks;
  set< SimpleLock* > xlocks;
  set< SimpleLock*, SimpleLock::ptr_lt > locks;

  // projected updates
  map< inodeno_t, inode_t > projected_inode;


  MDRequest() : request(0), ref(0) {}
  MDRequest(metareqid_t ri, Message *req=0) : reqid(ri), request(req), ref(0) {}
  
  // requeest
  MClientRequest *client_request() {
    return (MClientRequest*)request;
  }

  // pin items in cache
  void pin(CInode *in) {
    if (inode_pins.count(in) == 0) {
      in->get(CInode::PIN_REQUEST);
      inode_pins.insert(in);
    }      
  }
  void pin(CDir *dir) {
    if (dir_pins.count(dir) == 0) {
      dir->get(CDir::PIN_REQUEST);
      dir_pins.insert(dir);
    }      
  }
  void pin(CDentry *dn) {
    if (dentry_pins.count(dn) == 0) {
      dn->get(CDentry::PIN_REQUEST);
      dentry_pins.insert(dn);
    }      
  }

  // auth pins
  bool is_auth_pinned(CInode *in) { return inode_auth_pins.count(in); }
  bool is_auth_pinned(CDir *dir) { return dir_auth_pins.count(dir); }
  void auth_pin(CInode *in) {
    if (!is_auth_pinned(in)) {
      in->auth_pin();
      inode_auth_pins.insert(in);
    }
  }
  void auth_pin(CDir *dir) {
    if (!is_auth_pinned(dir)) {
      dir->auth_pin();
      dir_auth_pins.insert(dir);
    }
  }
  void drop_auth_pins() {
    for (set<CInode*>::iterator it = inode_auth_pins.begin();
	 it != inode_auth_pins.end();
	 it++) 
      (*it)->auth_unpin();
    inode_auth_pins.clear();
    for (set<CDir*>::iterator it = dir_auth_pins.begin();
	 it != dir_auth_pins.end();
	 it++) 
      (*it)->auth_unpin();
    dir_auth_pins.clear();
  }
};

inline ostream& operator<<(ostream& out, MDRequest &mdr)
{
  out << "request(" << mdr.reqid;
  //if (mdr.request) out << " " << *mdr.request;
  out << ")";
  return out;
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
  CInode                       *stray;       // my stray dir

  // root
  list<Context*> waiting_for_root;
  map<inodeno_t,list<Context*> > waiting_for_stray;

public:
  int get_num_inodes() { return inode_map.size(); }
  int get_num_dentries() { return lru.lru_get_size(); }


  // -- subtrees --
protected:
  map<CDir*,set<CDir*> > subtrees;   // nested bounds on subtrees.
  
  // adjust subtree auth specification
  //  dir->dir_auth
  //  imports/exports/nested_exports
  //  join/split subtrees as appropriate
public:
  bool is_subtrees() { return !subtrees.empty(); }
  void adjust_subtree_auth(CDir *root, pair<int,int> auth);
  void adjust_subtree_auth(CDir *root, int a, int b=CDIR_AUTH_UNKNOWN) {
    adjust_subtree_auth(root, pair<int,int>(a,b)); 
  }
  void adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, pair<int,int> auth);
  void adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, int a) {
    adjust_bounded_subtree_auth(dir, bounds, pair<int,int>(a, CDIR_AUTH_UNKNOWN));
  }
  void adjust_bounded_subtree_auth(CDir *dir, list<dirfrag_t>& bounds, pair<int,int> auth);
  void adjust_bounded_subtree_auth(CDir *dir, list<dirfrag_t>& bounds, int a) {
    adjust_bounded_subtree_auth(dir, bounds, pair<int,int>(a, CDIR_AUTH_UNKNOWN));
  }
  void adjust_export_state(CDir *dir);
  void try_subtree_merge(CDir *root);
  void try_subtree_merge_at(CDir *root);
  CDir *get_subtree_root(CDir *dir);
  void remove_subtree(CDir *dir);
  void get_subtree_bounds(CDir *root, set<CDir*>& bounds);
  void get_wouldbe_subtree_bounds(CDir *root, set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const list<dirfrag_t>& bounds);

  void adjust_subtree_after_rename(CInode *diri, CDir *olddir);

  void get_auth_subtrees(set<CDir*>& s);
  void get_fullauth_subtrees(set<CDir*>& s);

  int num_subtrees();
  int num_subtrees_fullauth();
  int num_subtrees_fullnonauth();

  
protected:
  // delayed cache expire
  map<CDir*, map<int, MCacheExpire*> > delayed_expire; // import|export dir -> expire msg

  // -- discover --
  hash_map<inodeno_t, set<int> > dir_discovers;  // dirino -> mds set i'm trying to discover.


  // -- requests --
public:

  
protected:
  hash_map<metareqid_t, MDRequest*> slave_requests; 
  
public:
  MDRequest* request_start(metareqid_t rid);
  MDRequest* request_start(MClientRequest *req);
  void request_pin_ref(MDRequest *r, CInode *ref, vector<CDentry*>& trace);
  void request_finish(MDRequest *mdr);
  void request_forward(MDRequest *mdr, int mds, int port=0);
  void dispatch_request(MDRequest *mdr);
  void request_drop_locks(MDRequest *mdr);
  void request_cleanup(MDRequest *r);



  // inode purging
  map<inodeno_t, map<off_t, inode_t> >         purging;
  map<inodeno_t, map<off_t, list<Context*> > > waiting_for_purge;
  
  // shutdown crap
  int shutdown_commits;
  bool did_shutdown_log_cap;
  friend class C_MDC_ShutdownCommit;

  // -- recovery --
protected:
  // from EImportStart w/o EImportFinish during journal replay
  map<dirfrag_t, list<dirfrag_t> >            my_ambiguous_imports;  
  // from MMDSImportMaps
  map<int, map<dirfrag_t, list<dirfrag_t> > > other_ambiguous_imports;  

  set<int> recovery_set;
  set<int> wants_import_map;   // nodes i need to send my import map to
  set<int> got_import_map;     // nodes i got import_maps from
  set<int> rejoin_ack_gather;  // nodes i need a rejoin ack from
  
  void handle_import_map(MMDSImportMap *m);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_ack(MMDSCacheRejoinAck *m);
  void disambiguate_imports();
  void cache_rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin);
  void send_cache_rejoin_acks();
  void recalc_auth_bits();

public:
  void set_recovery_set(set<int>& s);
  void handle_mds_failure(int who);
  void handle_mds_recovery(int who);
  void send_import_map(int who);
  void send_import_map_now(int who);
  void send_import_map_later(int who);
  void send_pending_import_maps();  // maybe.
  void send_cache_rejoins();
  void log_import_map(Context *onsync=0);


  // ambiguous imports
  void add_ambiguous_import(dirfrag_t base, list<dirfrag_t>& bounds);
  void add_ambiguous_import(CDir *base, const set<CDir*>& bounds);
  void cancel_ambiguous_import(dirfrag_t dirino);
  void finish_ambiguous_import(dirfrag_t dirino);



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
  void set_root(CInode *r);
  CInode *get_stray() { return stray; }

  // cache
  void set_cache_size(size_t max) { lru.lru_set_max(max); }
  size_t get_cache_size() { return lru.lru_get_size(); }
  bool trim(int max = -1);   // trim cache
  void trim_dirfrag(CDir *dir, CDir *con,
		    map<int, MCacheExpire*>& expiremap);
  void trim_inode(CDentry *dn, CInode *in, CDir *con,
		  map<int,class MCacheExpire*>& expiremap);
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
      return inode_map[ino];
    return NULL;
  }
  CDir* get_dir(inodeno_t dirino) {  // deprecated
    return get_dirfrag(dirfrag_t(dirino, frag_t()));
  }    
  CDir* get_dirfrag(dirfrag_t df) {
    if (!have_inode(df.ino)) return NULL;
    return inode_map[df.ino]->get_dirfrag(df.frag);
  }

  int hash_dentry(inodeno_t ino, const string& s) {
    return 0; // fixme
  }
  

 public:
  CInode *create_inode();
  void add_inode(CInode *in);

  void remove_inode(CInode *in);
 protected:
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
  void purge_inode(inode_t *inode, off_t newsize);
  void purge_inode_finish(inodeno_t ino, off_t newsize);
  void purge_inode_finish_2(inodeno_t ino, off_t newsize);
  bool is_purging(inodeno_t ino, off_t newsize) {
    return purging.count(ino) && purging[ino].count(newsize);
  }
  void wait_for_purge(inodeno_t ino, off_t newsize, Context *c) {
    waiting_for_purge[ino][newsize].push_back(c);
  }

  void add_recovered_purge(const inode_t& inode, off_t newsize);
  void remove_recovered_purge(inodeno_t ino, off_t newsize);
  void start_recovered_purges();


 public:
  CDir *get_auth_container(CDir *in);
  CDir *get_export_container(CDir *dir);
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);

 public:
  CInode *create_root_inode();
  void open_root(Context *c);
  CInode *create_stray_inode(int whose=-1);
  void open_local_stray();
  void open_foreign_stray(int who, Context *c);
  int path_traverse(MDRequest *mdr,
		    CInode *base,
		    filepath& path, vector<CDentry*>& trace, bool follow_trailing_sym,
                    Message *req, Context *ondelay,
                    int onfail, 
                    bool is_client_req = false,
		    bool null_okay = false);
  void open_remote_dir(CInode *diri, frag_t fg, Context *fin);
  CInode *get_dentry_inode(CDentry *dn, MDRequest *mdr);
  void open_remote_ino(inodeno_t ino, MDRequest *mdr, Context *fin);
  void open_remote_ino_2(inodeno_t ino, MDRequest *mdr,
                         vector<Anchor>& anchortrace,
                         Context *onfinish);

  void make_trace(vector<CDentry*>& trace, CInode *in);
  
  // -- anchors --
public:
  void anchor_create(CInode *in, Context *onfinish);
  void anchor_destroy(CInode *in, Context *onfinish);
protected:
  void _anchor_create_prepared(CInode *in, version_t atid);
  void _anchor_create_logged(CInode *in, version_t atid, version_t pdv);
  void _anchor_destroy_prepared(CInode *in, version_t atid);
  void _anchor_destroy_logged(CInode *in, version_t atid, version_t pdv);

  friend class C_MDC_AnchorCreatePrepared;
  friend class C_MDC_AnchorCreateLogged;
  friend class C_MDC_AnchorDestroyPrepared;
  friend class C_MDC_AnchorDestroyLogged;

  // -- stray --
public:
  void eval_stray(CDentry *dn);
protected:
  void _purge_stray(CDentry *dn);
  void _purge_stray_logged(CDentry *dn, version_t pdv);
  friend class C_MDC_PurgeStray;
  void reintegrate_stray(CDentry *dn, CDentry *rlink);
  void migrate_stray(CDentry *dn, int dest);


  // == messages ==
 public:
  void dispatch(Message *m);

 protected:
  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);

  CDir* add_replica_dir(CInode *diri, 
			frag_t fg, CDirDiscover& dis, 
			int from,
			list<Context*>& finished);
  CDir* forge_replica_dir(CInode *diri, frag_t fg, int from);
    

  // -- hard links --
  void handle_inode_link(class MInodeLink *m);

  // -- namespace --
  void handle_dentry_unlink(MDentryUnlink *m);


  // -- updates --
  //int send_inode_updates(CInode *in);
  //void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, bool bcast=false);
  void handle_dir_update(MDirUpdate *m);

  // -- cache expiration --
  void handle_cache_expire(MCacheExpire *m);
  void process_delayed_expire(CDir *dir);
  void discard_delayed_expire(CDir *dir);


  // == crap fns ==
 public:
  void show_cache();
  void dump_cache();
  void show_subtrees(int dbl=10);

  CInode *hack_pick_random_inode() {
    assert(!inode_map.empty());
    int n = rand() % inode_map.size();
    hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
    while (n--) p++;
    return p->second;
  }

};

class C_MDS_RetryRequest : public Context {
  MDCache *cache;
  MDRequest *mdr;
 public:
  C_MDS_RetryRequest(MDCache *c, MDRequest *r) : cache(c), mdr(r) {}
  virtual void finish(int r) {
    cache->dispatch_request(mdr);
  }
};

#endif
