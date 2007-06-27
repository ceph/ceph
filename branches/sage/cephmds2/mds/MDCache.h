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
#include "events/EMetaBlob.h"

class MDS;
class Migrator;
class Renamer;

class Logger;

class Message;

class MMDSImportMap;
class MMDSResolveAck;
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
class MMDSSlaveRequest;

// MDCache

//typedef const char* pchar;


struct PVList {
  map<MDSCacheObject*,version_t> ls;

  version_t add(MDSCacheObject* o, version_t v) {
    return ls[o] = v;
  }
};

/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
struct MDRequest {
  metareqid_t reqid;

  // -- i am a client (master) request
  MClientRequest *client_request; // client request (if any)
  set<int> slaves;            // mds nodes that have slave requests to me (implies client_request)
  set<int> waiting_on_slave;  // peers i'm waiting for slavereq replies from. 

  vector<CDentry*> trace;  // original path traversal.
  CInode *ref;             // reference inode.  if there is only one, and its path is pinned.

  // -- i am a slave request
  MMDSSlaveRequest *slave_request; // slave request (if one is pending; implies slave == true)
  int slave_to_mds;                // this is a slave request if >= 0.

  // -- my pins and locks --
  // cache pins (so things don't expire)
  set< MDSCacheObject* > pins;

  // auth pins
  set< MDSCacheObject* > auth_pins;
  
  // held locks
  set< SimpleLock* > rdlocks;  // always local.
  set< SimpleLock* > wrlocks;  // always local.
  set< SimpleLock* > xlocks;   // local or remote.
  set< SimpleLock*, SimpleLock::ptr_lt > locks;  // full ordering

  // if this flag is set, do not attempt to acquire further locks.
  //  (useful for wrlock, which may be a moving auth target)
  bool done_locking; 
  bool committing;
  bool aborted;

  // for rename/link/unlink
  utime_t now;
  set<int> witnessed;       // nodes who have journaled a RenamePrepare
  map<MDSCacheObject*,version_t> pvmap;

  // for rename
  set<int> extra_witnesses; // replica list from srcdn auth (rename)
  version_t src_reanchor_atid;  // src->dst
  version_t dst_reanchor_atid;  // dst->stray
  bufferlist inode_import;
  version_t inode_import_v;
  CDentry *srcdn; // srcdn, if auth, on slave
  
  // called when slave commits
  Context *slave_commit;


  // ---------------------------------------------------
  MDRequest() : 
    client_request(0), ref(0), 
    slave_request(0), slave_to_mds(-1), 
    done_locking(false), committing(false), aborted(false),
    src_reanchor_atid(0), dst_reanchor_atid(0), inode_import_v(0),
    slave_commit(0) { }
  MDRequest(metareqid_t ri, MClientRequest *req) : 
    reqid(ri), client_request(req), ref(0), 
    slave_request(0), slave_to_mds(-1), 
    done_locking(false), committing(false), aborted(false),
    src_reanchor_atid(0), dst_reanchor_atid(0), inode_import_v(0),
    slave_commit(0) { }
  MDRequest(metareqid_t ri, int by) : 
    reqid(ri), client_request(0), ref(0),
    slave_request(0), slave_to_mds(by), 
    done_locking(false), committing(false), aborted(false),
    src_reanchor_atid(0), dst_reanchor_atid(0), inode_import_v(0),
    slave_commit(0) { }
  
  bool is_master() { return slave_to_mds < 0; }
  bool is_slave() { return slave_to_mds >= 0; }

  bool slave_did_prepare() { return slave_commit; }
  
  // pin items in cache
  void pin(MDSCacheObject *o) {
    if (pins.count(o) == 0) {
      o->get(MDSCacheObject::PIN_REQUEST);
      pins.insert(o);
    }      
  }

  // auth pins
  bool is_auth_pinned(MDSCacheObject *object) { 
    return auth_pins.count(object); 
  }
  void auth_pin(MDSCacheObject *object) {
    if (!is_auth_pinned(object)) {
      object->auth_pin();
      auth_pins.insert(object);
    }
  }
  void drop_local_auth_pins() {
    set<MDSCacheObject*>::iterator it = auth_pins.begin();
    while (it != auth_pins.end()) {
      if ((*it)->is_auth()) {
	(*it)->auth_unpin();
	auth_pins.erase(it++);
      } else {
	it++;
      }
    }
    auth_pins.clear();
  }
};

inline ostream& operator<<(ostream& out, MDRequest &mdr)
{
  out << "request(" << mdr.reqid;
  //if (mdr.request) out << " " << *mdr.request;
  if (mdr.is_slave()) out << " slave_to mds" << mdr.slave_to_mds;
  if (mdr.client_request) out << " cr=" << mdr.client_request;
  if (mdr.slave_request) out << " sr=" << mdr.slave_request;
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
  hash_map<metareqid_t, MDRequest*> active_requests; 
  
public:
  MDRequest* request_start(MClientRequest *req);
  MDRequest* request_start_slave(metareqid_t rid, int by);
  bool have_request(metareqid_t rid) {
    return active_requests.count(rid);
  }
  MDRequest* request_get(metareqid_t rid);
  void request_pin_ref(MDRequest *r, CInode *ref, vector<CDentry*>& trace);
  void request_finish(MDRequest *mdr);
  void request_forward(MDRequest *mdr, int mds, int port=0);
  void dispatch_request(MDRequest *mdr);
  void request_forget_foreign_locks(MDRequest *mdr);
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
  set<int> recovery_set;

public:
  void set_recovery_set(set<int>& s);
  void handle_mds_failure(int who);
  void handle_mds_recovery(int who);

protected:
  // [resolve]
  // from EImportStart w/o EImportFinish during journal replay
  map<dirfrag_t, list<dirfrag_t> >            my_ambiguous_imports;  
  // from MMDSImportMaps
  map<int, map<dirfrag_t, list<dirfrag_t> > > other_ambiguous_imports;  

  map<int, map<metareqid_t, EMetaBlob> > uncommitted_slave_updates;
  friend class ESlaveUpdate;

  set<int> wants_import_map;   // nodes i need to send my import map to
  set<int> got_import_map;     // nodes i got import_maps from
  set<int> need_resolve_ack;   // nodes i need a resolve_ack from
  
  void handle_import_map(MMDSImportMap *m);
  void handle_resolve_ack(MMDSResolveAck *m);
  void maybe_resolve_finish();
  void disambiguate_imports();
  void recalc_auth_bits();
public:
  // ambiguous imports
  void add_ambiguous_import(dirfrag_t base, list<dirfrag_t>& bounds);
  void add_ambiguous_import(CDir *base, const set<CDir*>& bounds);
  void cancel_ambiguous_import(dirfrag_t dirino);
  void finish_ambiguous_import(dirfrag_t dirino);
  void send_import_map(int who);
  void send_import_map_now(int who);
  void send_import_map_later(int who);
  void send_pending_import_maps();  // maybe.
  void log_import_map(Context *onsync=0);

protected:
  // [rejoin]
  set<int> rejoin_gather;      // nodes from whom i need a rejoin
  set<int> rejoin_ack_gather;  // nodes from whom i need a rejoin ack
  set<int> want_rejoin_ack;    // nodes to whom i need to send a rejoin ack

  void cache_rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_ack(MMDSCacheRejoin *m);
  void handle_cache_rejoin_missing(MMDSCacheRejoin *m);
  void handle_cache_rejoin_full(MMDSCacheRejoin *m);
  void send_cache_rejoin_acks();
public:
  void send_cache_rejoins();




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

  // trimming
  bool trim(int max = -1);   // trim cache
  void trim_dentry(CDentry *dn, map<int, MCacheExpire*>& expiremap);
  void trim_dirfrag(CDir *dir, CDir *con,
		    map<int, MCacheExpire*>& expiremap);
  void trim_inode(CDentry *dn, CInode *in, CDir *con,
		  map<int,class MCacheExpire*>& expiremap);
  void send_expire_messages(map<int, MCacheExpire*>& expiremap);
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

  MDSCacheObject *get_object(MDSCacheObjectInfo &info);

  

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
  void dentry_remove_replica(CDentry *dn, int rep);

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
  CDentry *get_or_create_stray_dentry(CInode *in);

  Context *_get_waiter(MDRequest *mdr, Message *req);
  int path_traverse(MDRequest *mdr, Message *req, 
		    CInode *base, filepath& path, 
		    vector<CDentry*>& trace, bool follow_trailing_sym,
                    int onfail);

  void open_remote_dir(CInode *diri, frag_t fg, Context *fin);
  CInode *get_dentry_inode(CDentry *dn, MDRequest *mdr);
  void open_remote_ino(inodeno_t ino, MDRequest *mdr, Context *fin);
  void open_remote_ino_2(inodeno_t ino, MDRequest *mdr,
                         vector<Anchor>& anchortrace,
                         Context *onfinish);

  void make_trace(vector<CDentry*>& trace, CInode *in);
  
  // -- anchors --
public:
  void anchor_create(MDRequest *mdr, CInode *in, Context *onfinish);
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

  CDentry *add_replica_dentry(CDir *dir, CDentryDiscover &dis, list<Context*>& finished);
  CInode *add_replica_inode(CInodeDiscover& dis, CDentry *dn);

public:
  CDentry *add_replica_stray(bufferlist &bl, CInode *strayin, int from);
protected:

    

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
