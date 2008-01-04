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
class Session;

class MMDSResolve;
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

class MMDSFragmentNotify;

class ESubtreeMap;


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
  Session *session;

  // -- i am a client (master) request
  MClientRequest *client_request; // client request (if any)

  vector<CDentry*> trace;  // original path traversal.
  CInode *ref;             // reference inode.  if there is only one, and its path is pinned.

  // -- i am a slave request
  MMDSSlaveRequest *slave_request; // slave request (if one is pending; implies slave == true)
  int slave_to_mds;                // this is a slave request if >= 0.

  // -- misc --
  LogSegment *ls;  // the log segment i'm committing to
  utime_t now;

  // -- my pins and locks --
  // cache pins (so things don't expire)
  set< MDSCacheObject* > pins;
  set<CInode*> stickydirs;

  // auth pins
  set< MDSCacheObject* > remote_auth_pins;
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

  // break rarely-used fields into a separately allocated structure 
  // to save memory for most ops
  struct More {
    set<int> slaves;           // mds nodes that have slave requests to me (implies client_request)
    set<int> waiting_on_slave; // peers i'm waiting for slavereq replies from. 

    // for rename/link/unlink
    set<int> witnessed;       // nodes who have journaled a RenamePrepare
    map<MDSCacheObject*,version_t> pvmap;
    
    // for rename
    set<int> extra_witnesses; // replica list from srcdn auth (rename)
    version_t src_reanchor_atid;  // src->dst
    version_t dst_reanchor_atid;  // dst->stray
    bufferlist inode_import;
    version_t inode_import_v;
    CInode* destdn_was_remote_inode;
    bool was_link_merge;

    map<int,entity_inst_t> imported_client_map;
    map<CInode*, map<int,Capability::Export> > cap_imports;
    
    // called when slave commits or aborts
    Context *slave_commit;

    More() : 
      src_reanchor_atid(0), dst_reanchor_atid(0), inode_import_v(0),
      destdn_was_remote_inode(0), was_link_merge(false),
      slave_commit(0) { }
  } *_more;


  // ---------------------------------------------------
  MDRequest() : 
    session(0), client_request(0), ref(0), 
    slave_request(0), slave_to_mds(-1), 
    ls(0),
    done_locking(false), committing(false), aborted(false),
    _more(0) {}
  MDRequest(metareqid_t ri, MClientRequest *req) : 
    reqid(ri), session(0), client_request(req), ref(0), 
    slave_request(0), slave_to_mds(-1), 
    ls(0),
    done_locking(false), committing(false), aborted(false),
    _more(0) {}
  MDRequest(metareqid_t ri, int by) : 
    reqid(ri), session(0), client_request(0), ref(0),
    slave_request(0), slave_to_mds(by), 
    ls(0),
    done_locking(false), committing(false), aborted(false),
    _more(0) {}
  ~MDRequest() {
    delete _more;
  }
  
  bool is_master() { return slave_to_mds < 0; }
  bool is_slave() { return slave_to_mds >= 0; }

  More* more() { 
    if (!_more) _more = new More();
    return _more;
  }

  bool slave_did_prepare() { return more()->slave_commit; }
  

  // pin items in cache
  void pin(MDSCacheObject *o) {
    if (pins.count(o) == 0) {
      o->get(MDSCacheObject::PIN_REQUEST);
      pins.insert(o);
    }      
  }
  void set_stickydirs(CInode *in) {
    if (stickydirs.count(in) == 0) {
      in->get_stickydirs();
      stickydirs.insert(in);
    }
  }

  // auth pins
  bool is_auth_pinned(MDSCacheObject *object) { 
    return auth_pins.count(object) || remote_auth_pins.count(object); 
  }
  void auth_pin(MDSCacheObject *object) {
    if (!is_auth_pinned(object)) {
      object->auth_pin();
      auth_pins.insert(object);
    }
  }
  void auth_unpin(MDSCacheObject *object) {
    assert(is_auth_pinned(object));
    object->auth_unpin();
    auth_pins.erase(object);
  }
  void drop_local_auth_pins() {
    for (set<MDSCacheObject*>::iterator it = auth_pins.begin();
	 it != auth_pins.end();
	 it++) {
      assert((*it)->is_auth());
      (*it)->auth_unpin();
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

struct MDSlaveUpdate {
  EMetaBlob commit;
  EMetaBlob rollback;
  xlist<MDSlaveUpdate*>::item xlistitem;
  Context *waiter;
  MDSlaveUpdate() : xlistitem(this), waiter(0) {}
  MDSlaveUpdate(EMetaBlob c, EMetaBlob r, xlist<MDSlaveUpdate*> &list) :
    commit(c), rollback(r),
    xlistitem(this),
    waiter(0) {
    list.push_back(&xlistitem);
  }
  ~MDSlaveUpdate() {
    if (waiter) waiter->finish(0);
    delete waiter;
  }
};


class MDCache {
 public:
  // my master
  MDS *mds;

  // -- my cache --
  LRU lru;   // dentry lru for expiring items from cache
 protected:
  hash_map<inodeno_t,CInode*>   inode_map;  // map of inodes by ino
  CInode *root;                             // root inode
  CInode *stray;                            // my stray dir

  set<CInode*> base_inodes;  // inodes < MDS_INO_BASE (root, stray, etc.)

  // -- discover --
  // waiters
  map<int, hash_map<inodeno_t, list<Context*> > > waiting_for_base_ino;

  // in process discovers, by mds.
  //  this is just enough info to kick any waiters in the event of a failure.
  //  FIXME: use pointers here instead of identifiers?
  map<int, hash_map<inodeno_t,int> > discover_dir;
  map<int, hash_map<dirfrag_t,int> > discover_dir_sub;

  void discover_base_ino(inodeno_t want_ino, Context *onfinish, int from=-1);
  void discover_dir_frag(CInode *base, frag_t approx_fg, Context *onfinish,
			 int from=-1);
  void discover_path(CInode *base, filepath want_path, Context *onfinish,
		     bool want_xlocked=false, int from=-1);
  void discover_path(CDir *base, filepath want_path, Context *onfinish,
		     bool want_xlocked=false);
  void discover_ino(CDir *base, inodeno_t want_ino, Context *onfinish,
		    bool want_xlocked=false);

  void kick_discovers(int who);  // after a failure.


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
  void list_subtrees(list<CDir*>& ls);
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
  void map_dirfrag_set(list<dirfrag_t>& dfs, set<CDir*>& result);
  void try_subtree_merge(CDir *root);
  void try_subtree_merge_at(CDir *root);
  void subtree_merge_writebehind_finish(CInode *in, LogSegment *ls);
  void eval_subtree_root(CDir *dir);
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
  map<CDir*, map<int, MCacheExpire*> > delayed_expire; // subtree root -> expire msg


  // -- requests --
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
  void request_cleanup(MDRequest *r);


  // inode purging
  map<CInode*, map<off_t, off_t> > purging;  // inode -> newsize -> oldsize
  map<CInode*, map<off_t, LogSegment*> > purging_ls;
  map<CInode*, map<off_t, list<Context*> > > waiting_for_purge;
  
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
  // from MMDSResolves
  map<int, map<dirfrag_t, list<dirfrag_t> > > other_ambiguous_imports;  

  map<int, map<metareqid_t, MDSlaveUpdate*> > uncommitted_slave_updates;  // for replay.
  map<metareqid_t, bool>     ambiguous_slave_updates;         // for log trimming.
  map<metareqid_t, Context*> waiting_for_slave_update_commit;
  friend class ESlaveUpdate;

  set<int> wants_resolve;   // nodes i need to send my resolve to
  set<int> got_resolve;     // nodes i got resolves from
  set<int> need_resolve_ack;   // nodes i need a resolve_ack from
  
  void handle_resolve(MMDSResolve *m);
  void handle_resolve_ack(MMDSResolveAck *m);
  void maybe_resolve_finish();
  void disambiguate_imports();
  void recalc_auth_bits();
public:
  // ambiguous imports
  void add_ambiguous_import(dirfrag_t base, list<dirfrag_t>& bounds);
  void add_ambiguous_import(CDir *base, const set<CDir*>& bounds);
  bool have_ambiguous_import(dirfrag_t base) {
    return my_ambiguous_imports.count(base);
  }
  void cancel_ambiguous_import(dirfrag_t dirino);
  void finish_ambiguous_import(dirfrag_t dirino);
  void send_resolve(int who);
  void send_resolve_now(int who);
  void send_resolve_later(int who);
  void maybe_send_pending_resolves();
  
  ESubtreeMap *create_subtree_map();


protected:
  // [rejoin]
  set<int> rejoin_gather;      // nodes from whom i need a rejoin
  set<int> rejoin_sent;        // nodes i sent a rejoin to
  set<int> rejoin_ack_gather;  // nodes from whom i need a rejoin ack

  map<inodeno_t,map<int,inode_caps_reconnect_t> > cap_exports; // ino -> client -> capex
  map<inodeno_t,string> cap_export_paths;

  map<inodeno_t,map<int, map<int,inode_caps_reconnect_t> > > cap_imports;  // ino -> client -> frommds -> capex
  map<inodeno_t,string> cap_import_paths;
  
  set<CInode*> rejoin_undef_inodes;

  void rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_weak(MMDSCacheRejoin *m);
  CInode* rejoin_invent_inode(inodeno_t ino);
  void handle_cache_rejoin_strong(MMDSCacheRejoin *m);
  void rejoin_scour_survivor_replicas(int from, MMDSCacheRejoin *ack);
  void handle_cache_rejoin_ack(MMDSCacheRejoin *m);
  void handle_cache_rejoin_purge(MMDSCacheRejoin *m);
  void handle_cache_rejoin_missing(MMDSCacheRejoin *m);
  void handle_cache_rejoin_full(MMDSCacheRejoin *m);
  void rejoin_send_acks();
  void rejoin_trim_undef_inodes();
public:
  void rejoin_gather_finish();
  void rejoin_send_rejoins();
  void rejoin_export_caps(inodeno_t ino, string& path, int client, inode_caps_reconnect_t& icr) {
    cap_exports[ino][client] = icr;
    cap_export_paths[ino] = path;
  }
  void rejoin_recovered_caps(inodeno_t ino, string& path, int client, inode_caps_reconnect_t& icr, 
			     int frommds=-1) {
    cap_imports[ino][client][frommds] = icr;
    cap_import_paths[ino] = path;
  }
  void rejoin_import_cap(CInode *in, int client, inode_caps_reconnect_t& icr, int frommds);


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
  bool shutdown_export_strays();
  bool shutdown_export_caps();
  bool shutdown();                    // clear cache (ie at shutodwn)

  bool did_shutdown_log_cap;

  // inode_map
  bool have_inode( inodeno_t ino ) { return inode_map.count(ino) ? true:false; }
  CInode* get_inode( inodeno_t ino ) {
    if (have_inode(ino))
      return inode_map[ino];
    return NULL;
  }
  CDir* get_dirfrag(dirfrag_t df) {
    if (!have_inode(df.ino)) return NULL;
    return inode_map[df.ino]->get_dirfrag(df.frag);
  }
  /*
  void get_dirfrags_under(dirfrag_t df, list<CDir*>& ls) {
    if (have_inode(df.ino))
      inode_map[df.ino]->get_dirfrags_under(df.frag, ls);
  }
  */

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
  void purge_inode(CInode *in, off_t newsize, off_t oldsize, LogSegment *ls);
  void _do_purge_inode(CInode *in, off_t newsize, off_t oldsize);
  void purge_inode_finish(CInode *in, off_t newsize, off_t oldsize);
  void purge_inode_finish_2(CInode *in, off_t newsize, off_t oldsize);
  bool is_purging(CInode *in, off_t newsize, off_t oldsize) {
    return purging.count(in) && purging[in].count(newsize);
  }
  void wait_for_purge(CInode *in, off_t newsize, Context *c) {
    waiting_for_purge[in][newsize].push_back(c);
  }

  void add_recovered_purge(CInode *in, off_t newsize, off_t oldsize, LogSegment *ls);
  void remove_recovered_purge(CInode *in, off_t newsize, off_t oldsize);
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
  int path_traverse(MDRequest *mdr, Message *req, filepath& path, 
		    vector<CDentry*>& trace, bool follow_trailing_sym,
                    int onfail);
  bool path_is_mine(filepath& path);
  bool path_is_mine(string& p) {
    filepath path(p);
    return path_is_mine(path);
  }
  CDir *path_traverse_to_dir(filepath& path);
  
  void open_remote_dirfrag(CInode *diri, frag_t fg, Context *fin);
  CInode *get_dentry_inode(CDentry *dn, MDRequest *mdr);
  void open_remote_ino(inodeno_t ino, MDRequest *mdr, Context *fin);
  void open_remote_ino_2(inodeno_t ino, MDRequest *mdr,
                         vector<Anchor>& anchortrace,
                         Context *onfinish);

  C_Gather *parallel_fetch(map<inodeno_t,string>& pathmap);

  void make_trace(vector<CDentry*>& trace, CInode *in);
  
  // -- anchors --
public:
  void anchor_create(MDRequest *mdr, CInode *in, Context *onfinish);
  void anchor_destroy(CInode *in, Context *onfinish);
protected:
  void _anchor_create_prepared(CInode *in, version_t atid);
  void _anchor_create_logged(CInode *in, version_t atid, LogSegment *ls);
  void _anchor_destroy_prepared(CInode *in, version_t atid);
  void _anchor_destroy_logged(CInode *in, version_t atid, LogSegment *ls);

  friend class C_MDC_AnchorCreatePrepared;
  friend class C_MDC_AnchorCreateLogged;
  friend class C_MDC_AnchorDestroyPrepared;
  friend class C_MDC_AnchorDestroyLogged;

  // -- stray --
public:
  void eval_stray(CDentry *dn);
  void eval_remote(CDentry *dn);
protected:
  void _purge_stray(CDentry *dn);
  void _purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls);
  friend class C_MDC_PurgeStray;
  void reintegrate_stray(CDentry *dn, CDentry *rlink);
  void migrate_stray(CDentry *dn, int src, int dest);


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
public: // for Server::handle_slave_rename_prep
  CInode *add_replica_inode(CInodeDiscover& dis, CDentry *dn, list<Context*>& finished);

public:
  CDentry *add_replica_stray(bufferlist &bl, CInode *strayin, int from);
protected:

    

  // -- namespace --
  void handle_dentry_unlink(MDentryUnlink *m);


  // -- fragmenting --
private:
  void adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
			    list<CDir*>& frags, list<Context*>& waiters);
  friend class EFragment;

public:
  void split_dir(CDir *dir, int byn);

private:
  void fragment_freeze(CInode *diri, list<CDir*>& startfrags, frag_t basefrag, int bits);
  void fragment_mark_and_complete(CInode *diri, list<CDir*>& startfrags, frag_t basefrag, int bits);
  void fragment_go(CInode *diri, list<CDir*>& startfrags, frag_t basefrag, int bits);
  void fragment_stored(CInode *diri, frag_t basefrag, int bits, list<CDir*>& resultfrags);
  void fragment_logged(CInode *diri, frag_t basefrag, int bits, list<CDir*>& resultfrags, vector<version_t>& pvs, LogSegment *ls);
  friend class C_MDC_FragmentGo;
  friend class C_MDC_FragmentMarking;
  friend class C_MDC_FragmentStored;
  friend class C_MDC_FragmentLogged;

  void handle_fragment_notify(MMDSFragmentNotify *m);


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
