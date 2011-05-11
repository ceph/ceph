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



#ifndef CEPH_MDCACHE_H
#define CEPH_MDCACHE_H

#include "include/types.h"
#include "include/filepath.h"

#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "include/Context.h"
#include "events/EMetaBlob.h"

#include "messages/MClientRequest.h"
#include "messages/MMDSSlaveRequest.h"

class ProfLogger;

class MDS;
class Session;
class Migrator;

class Message;
class Session;

class MMDSResolve;
class MMDSResolveAck;
class MMDSCacheRejoin;
class MMDSCacheRejoinAck;
class MJoin;
class MJoinAck;
class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryLink;
class MDentryUnlink;
class MLock;
class MMDSFindIno;
class MMDSFindInoReply;

class Message;
class MClientRequest;
class MMDSSlaveRequest;
class MClientSnap;

class MMDSFragmentNotify;

class ESubtreeMap;


// MDCache

struct Mutation {
  metareqid_t reqid;
  LogSegment *ls;  // the log segment i'm committing to
  utime_t now;

  // flag mutation as slave
  int slave_to_mds;                // this is a slave request if >= 0.

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

  // for applying projected inode changes
  list<CInode*> projected_inodes;
  list<CDir*> projected_fnodes;
  list<ScatterLock*> updated_locks;

  list<CInode*> dirty_cow_inodes;
  list<pair<CDentry*,version_t> > dirty_cow_dentries;

  Mutation() : 
    ls(0),
    slave_to_mds(-1),
    done_locking(false), committing(false), aborted(false) { }
  Mutation(metareqid_t ri, int slave_to=-1) : 
    reqid(ri),
    ls(0),
    slave_to_mds(slave_to), 
    done_locking(false), committing(false), aborted(false) { }
  virtual ~Mutation() {
    assert(pins.empty());
    assert(auth_pins.empty());
    assert(xlocks.empty());
    assert(rdlocks.empty());
    assert(wrlocks.empty());
  }

  bool is_master() { return slave_to_mds < 0; }
  bool is_slave() { return slave_to_mds >= 0; }

  client_t get_client() {
    if (reqid.name.is_client())
      return client_t(reqid.name.num());
    return -1;
  }

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
  void drop_pins() {
    for (set<MDSCacheObject*>::iterator it = pins.begin();
	 it != pins.end();
	 it++) 
      (*it)->put(MDSCacheObject::PIN_REQUEST);
    pins.clear();
  }

  // auth pins
  bool is_auth_pinned(MDSCacheObject *object) { 
    return auth_pins.count(object) || remote_auth_pins.count(object); 
  }
  void auth_pin(MDSCacheObject *object) {
    if (!is_auth_pinned(object)) {
      object->auth_pin(this);
      auth_pins.insert(object);
    }
  }
  void auth_unpin(MDSCacheObject *object) {
    assert(auth_pins.count(object));
    object->auth_unpin(this);
    auth_pins.erase(object);
  }
  void drop_local_auth_pins() {
    for (set<MDSCacheObject*>::iterator it = auth_pins.begin();
	 it != auth_pins.end();
	 it++) {
      assert((*it)->is_auth());
      (*it)->auth_unpin(this);
    }
    auth_pins.clear();
  }

  void add_projected_inode(CInode *in) {
    projected_inodes.push_back(in);
  }
  void pop_and_dirty_projected_inodes() {
    while (!projected_inodes.empty()) {
      CInode *in = projected_inodes.front();
      projected_inodes.pop_front();
      in->pop_and_dirty_projected_inode(ls);
    }
  }

  void add_projected_fnode(CDir *dir) {
    projected_fnodes.push_back(dir);
  }
  void pop_and_dirty_projected_fnodes() {
    while (!projected_fnodes.empty()) {
      CDir *dir = projected_fnodes.front();
      projected_fnodes.pop_front();
      dir->pop_and_dirty_projected_fnode(ls);
    }
  }
  
  void add_updated_lock(ScatterLock *lock) {
    updated_locks.push_back(lock);
  }

  void add_cow_inode(CInode *in) {
    pin(in);
    dirty_cow_inodes.push_back(in);
  }
  void add_cow_dentry(CDentry *dn) {
    pin(dn);
    dirty_cow_dentries.push_back(pair<CDentry*,version_t>(dn, dn->get_projected_version()));
  }

  void apply() {
    pop_and_dirty_projected_inodes();
    pop_and_dirty_projected_fnodes();

    for (list<CInode*>::iterator p = dirty_cow_inodes.begin();
	 p != dirty_cow_inodes.end();
	 p++) 
      (*p)->_mark_dirty(ls);
    for (list<pair<CDentry*,version_t> >::iterator p = dirty_cow_dentries.begin();
	 p != dirty_cow_dentries.end();
	 p++)
      p->first->mark_dirty(p->second, ls);

    for (list<ScatterLock*>::iterator p = updated_locks.begin();
	 p != updated_locks.end();
	 p++)
      (*p)->mark_dirty();
  }

  void cleanup() {
    drop_local_auth_pins();
    drop_pins();
  }

  virtual void print(ostream &out) {
    out << "mutation(" << this << ")";
  }
};

inline ostream& operator<<(ostream& out, Mutation &mut)
{
  mut.print(out);
  return out;
}


/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
struct MDRequest : public Mutation {
  int ref;
  Session *session;
  elist<MDRequest*>::item item_session_request;  // if not on list, op is aborted.

  // -- i am a client (master) request
  MClientRequest *client_request; // client request (if any)

  // store up to two sets of dn vectors, inode pointers, for request path1 and path2.
  vector<CDentry*> dn[2];
  CDentry *straydn;
  CInode *in[2];
  snapid_t snapid;

  CInode *tracei;
  CDentry *tracedn;

  inodeno_t alloc_ino, used_prealloc_ino;  
  interval_set<inodeno_t> prealloc_inos;

  int snap_caps;
  bool did_early_reply;

  // inos we did a embedded cap release on, and may need to eval if we haven't since reissued
  map<vinodeno_t, ceph_seq_t> cap_releases;  

  // -- i am a slave request
  MMDSSlaveRequest *slave_request; // slave request (if one is pending; implies slave == true)

  // -- i am an internal op
  int internal_op;

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

    map<client_t,entity_inst_t> imported_client_map;
    map<client_t,uint64_t> sseq_map;
    map<CInode*, map<client_t,Capability::Export> > cap_imports;
    
    // for snaps
    version_t stid;
    bufferlist snapidbl;

    // called when slave commits or aborts
    Context *slave_commit;
    bufferlist rollback_bl;

    More() : 
      src_reanchor_atid(0), dst_reanchor_atid(0), inode_import_v(0),
      destdn_was_remote_inode(0), was_link_merge(false),
      stid(0),
      slave_commit(0) { }
  } *_more;


  // ---------------------------------------------------
  MDRequest() : 
    ref(1),
    session(0), item_session_request(this),
    client_request(0), straydn(NULL), snapid(CEPH_NOSNAP), tracei(0), tracedn(0),
    alloc_ino(0), used_prealloc_ino(0), snap_caps(0), did_early_reply(false),
    slave_request(0),
    internal_op(-1),
    _more(0) {
    in[0] = in[1] = 0; 
  }
  MDRequest(metareqid_t ri, MClientRequest *req) : 
    Mutation(ri),
    ref(1),
    session(0), item_session_request(this),
    client_request(req), straydn(NULL), snapid(CEPH_NOSNAP), tracei(0), tracedn(0),
    alloc_ino(0), used_prealloc_ino(0), snap_caps(0), did_early_reply(false),
    slave_request(0),
    internal_op(-1),
    _more(0) {
    in[0] = in[1] = 0; 
  }
  MDRequest(metareqid_t ri, int by) : 
    Mutation(ri, by),
    ref(1),
    session(0), item_session_request(this),
    client_request(0), straydn(NULL), snapid(CEPH_NOSNAP), tracei(0), tracedn(0),
    alloc_ino(0), used_prealloc_ino(0), snap_caps(0), did_early_reply(false),
    slave_request(0),
    internal_op(-1),
    _more(0) {
    in[0] = in[1] = 0; 
  }
  ~MDRequest() {
    if (client_request)
      client_request->put();
    if (slave_request)
      slave_request->put();
    delete _more;
  }

  MDRequest *get() {
    ++ref;
    return this;
  }
  void put() {
    if (--ref == 0)
      delete this;
  }
  
  More* more() { 
    if (!_more) _more = new More();
    return _more;
  }

  bool are_slaves() {
    return _more && !_more->slaves.empty();
  }

  bool slave_did_prepare() { return more()->slave_commit; }

  bool did_ino_allocation() {
    return alloc_ino || used_prealloc_ino || prealloc_inos.size();
  }      

  void print(ostream &out) {
    out << "request(" << reqid;
    //if (request) out << " " << *request;
    if (is_slave()) out << " slave_to mds" << slave_to_mds;
    if (client_request) out << " cr=" << client_request;
    if (slave_request) out << " sr=" << slave_request;
    out << ")";
  }
};


struct MDSlaveUpdate {
  int origop;
  bufferlist rollback;
  elist<MDSlaveUpdate*>::item item;
  Context *waiter;
  MDSlaveUpdate(int oo, bufferlist &rbl, elist<MDSlaveUpdate*> &list) :
    origop(oo),
    item(this),
    waiter(0) {
    rollback.claim(rbl);
    list.push_back(&item);
  }
  ~MDSlaveUpdate() {
    item.remove_myself();
    if (waiter)
      waiter->finish(0);
    delete waiter;
  }
};


// flags for predirty_journal_parents()
static const int PREDIRTY_PRIMARY = 1; // primary dn, adjust nested accounting
static const int PREDIRTY_DIR = 2;     // update parent dir mtime/size
static const int PREDIRTY_SHALLOW = 4; // only go to immediate parent (for easier rollback)



class MDCache {
 public:
  // my master
  MDS *mds;

  // -- my cache --
  LRU lru;   // dentry lru for expiring items from cache
 protected:
  hash_map<vinodeno_t,CInode*> inode_map;  // map of inodes by ino
  CInode *root;                            // root inode
  CInode *myin;                            // .ceph/mds%d dir

  CInode *strays[NUM_STRAY];         // my stray dir
  int stray_index;

  CInode *get_stray() {
    return strays[stray_index];
  }

  set<CInode*> base_inodes;

public:
  void advance_stray() {
    stray_index = (stray_index+1)%NUM_STRAY;
  }

  DecayRate decayrate;

  int num_inodes_with_caps;
  int num_caps;

  unsigned max_dir_commit_size;

  ceph_file_layout default_file_layout;
  ceph_file_layout default_log_layout;

  // -- client leases --
public:
  static const int client_lease_pools = 3;
  float client_lease_durations[client_lease_pools];
protected:
  xlist<ClientLease*> client_leases[client_lease_pools];
public:
  void touch_client_lease(ClientLease *r, int pool, utime_t ttl) {
    client_leases[pool].push_back(&r->item_lease);
    r->ttl = ttl;
  }

  // -- client caps --
  uint64_t              last_cap_id;
  


  // -- discover --
  struct discover_info_t {
    tid_t tid;
    int mds;
    inodeno_t ino;
    frag_t frag;
    snapid_t snap;
    filepath want_path;
    inodeno_t want_ino;
    bool want_base_dir;
    bool want_xlocked;

    discover_info_t() : tid(0), mds(-1), snap(CEPH_NOSNAP), want_base_dir(false), want_xlocked(false) {}
  };

  map<tid_t, discover_info_t> discovers;
  tid_t discover_last_tid;

  void _send_discover(discover_info_t& dis);
  discover_info_t& _create_discover(int mds) {
    tid_t t = ++discover_last_tid;
    discover_info_t& d = discovers[t];
    d.tid = t;
    d.mds = mds;
    return d;
  }

  // waiters
  map<int, map<inodeno_t, list<Context*> > > waiting_for_base_ino;

  void discover_base_ino(inodeno_t want_ino, Context *onfinish, int from=-1);
  void discover_dir_frag(CInode *base, frag_t approx_fg, Context *onfinish,
			 int from=-1);
  void discover_path(CInode *base, snapid_t snap, filepath want_path, Context *onfinish,
		     bool want_xlocked=false, int from=-1);
  void discover_path(CDir *base, snapid_t snap, filepath want_path, Context *onfinish,
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
  void adjust_subtree_auth(CDir *root, pair<int,int> auth, bool do_eval=true);
  void adjust_subtree_auth(CDir *root, int a, int b=CDIR_AUTH_UNKNOWN, bool do_eval=true) {
    adjust_subtree_auth(root, pair<int,int>(a,b), do_eval); 
  }
  void adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, pair<int,int> auth);
  void adjust_bounded_subtree_auth(CDir *dir, set<CDir*>& bounds, int a) {
    adjust_bounded_subtree_auth(dir, bounds, pair<int,int>(a, CDIR_AUTH_UNKNOWN));
  }
  void adjust_bounded_subtree_auth(CDir *dir, vector<dirfrag_t>& bounds, pair<int,int> auth);
  void adjust_bounded_subtree_auth(CDir *dir, vector<dirfrag_t>& bounds, int a) {
    adjust_bounded_subtree_auth(dir, bounds, pair<int,int>(a, CDIR_AUTH_UNKNOWN));
  }
  void map_dirfrag_set(list<dirfrag_t>& dfs, set<CDir*>& result);
  void try_subtree_merge(CDir *root);
  void try_subtree_merge_at(CDir *root, bool do_eval=true);
  void subtree_merge_writebehind_finish(CInode *in, Mutation *mut);
  void eval_subtree_root(CInode *diri);
  CDir *get_subtree_root(CDir *dir);
  CDir *get_projected_subtree_root(CDir *dir);
  bool is_leaf_subtree(CDir *dir) {
    assert(subtrees.count(dir));
    return subtrees[dir].empty();
  }
  void remove_subtree(CDir *dir);
  void get_subtree_bounds(CDir *root, set<CDir*>& bounds);
  void get_wouldbe_subtree_bounds(CDir *root, set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const set<CDir*>& bounds);
  void verify_subtree_bounds(CDir *root, const list<dirfrag_t>& bounds);

  void adjust_subtree_after_rename(CInode *diri, CDir *olddir,
                                   bool imported = false);

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
  int get_num_active_requests() { return active_requests.size(); }

  MDRequest* request_start(MClientRequest *req);
  MDRequest* request_start_slave(metareqid_t rid, int by);
  MDRequest* request_start_internal(int op);
  bool have_request(metareqid_t rid) {
    return active_requests.count(rid);
  }
  MDRequest* request_get(metareqid_t rid);
  void request_pin_ref(MDRequest *r, CInode *ref, vector<CDentry*>& trace);
  void request_finish(MDRequest *mdr);
  void request_forward(MDRequest *mdr, int mds, int port=0);
  void dispatch_request(MDRequest *mdr);
  void request_drop_foreign_locks(MDRequest *mdr);
  void request_drop_non_rdlocks(MDRequest *r);
  void request_drop_locks(MDRequest *r);
  void request_cleanup(MDRequest *r);
  
  void request_kill(MDRequest *r);  // called when session closes

  // journal/snap helpers
  CInode *pick_inode_snap(CInode *in, snapid_t follows);
  CInode *cow_inode(CInode *in, snapid_t last);
  void journal_cow_dentry(Mutation *mut, EMetaBlob *metablob, CDentry *dn, snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0, CDentry::linkage_t *dnl=0);
  void journal_cow_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0);
  inode_t *journal_dirty_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP);

  void project_rstat_inode_to_frag(CInode *cur, CDir *parent, snapid_t first, int linkunlink);
  void _project_rstat_inode_to_frag(inode_t& inode, snapid_t ofirst, snapid_t last,
				    CDir *parent, int linkunlink=0);
  void project_rstat_frag_to_inode(nest_info_t& rstat, nest_info_t& accounted_rstat,
				   snapid_t ofirst, snapid_t last, 
				   CInode *pin, bool cow_head);
  void predirty_journal_parents(Mutation *mut, EMetaBlob *blob,
				CInode *in, CDir *parent,
				int flags, int linkunlink=0,
				snapid_t follows=CEPH_NOSNAP);

  // slaves
  void add_uncommitted_master(metareqid_t reqid, LogSegment *ls, set<int> &slaves) {
    uncommitted_masters[reqid].ls = ls;
    uncommitted_masters[reqid].slaves = slaves;
  }
  void wait_for_uncommitted_master(metareqid_t reqid, Context *c) {
    uncommitted_masters[reqid].waiters.push_back(c);
  }
  void log_master_commit(metareqid_t reqid);
  void _logged_master_commit(metareqid_t reqid, LogSegment *ls, list<Context*> &waiters);
  void committed_master_slave(metareqid_t r, int from);

  void _logged_slave_commit(int from, metareqid_t reqid);

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
  map<dirfrag_t, vector<dirfrag_t> >            my_ambiguous_imports;  
  // from MMDSResolves
  map<int, map<dirfrag_t, vector<dirfrag_t> > > other_ambiguous_imports;  

  map<int, map<metareqid_t, MDSlaveUpdate*> > uncommitted_slave_updates;  // slave: for replay.

  // track master requests whose slaves haven't acknowledged commit
  struct umaster {
    set<int> slaves;
    LogSegment *ls;
    list<Context*> waiters;
  };
  map<metareqid_t, umaster>                 uncommitted_masters;         // master: req -> slave set

  //map<metareqid_t, bool>     ambiguous_slave_updates;         // for log trimming.
  //map<metareqid_t, Context*> waiting_for_slave_update_commit;
  friend class ESlaveUpdate;
  friend class ECommitted;

  set<int> wants_resolve;   // nodes i need to send my resolve to
  set<int> got_resolve;     // nodes i got resolves from
  set<int> need_resolve_ack;   // nodes i need a resolve_ack from
  set<metareqid_t> need_resolve_rollback;  // rollbacks i'm writing to the journal
  
  void handle_resolve(MMDSResolve *m);
  void handle_resolve_ack(MMDSResolveAck *m);
  void maybe_resolve_finish();
  void disambiguate_imports();
  void recalc_auth_bits();
  void trim_unlinked_inodes();
public:
  void remove_inode_recursive(CInode *in);

  void add_rollback(metareqid_t reqid) {
    need_resolve_rollback.insert(reqid);
  }
  void finish_rollback(metareqid_t reqid) {
    need_resolve_rollback.erase(reqid);
    if (need_resolve_rollback.empty())
      maybe_resolve_finish();
  }

  // ambiguous imports
  void add_ambiguous_import(dirfrag_t base, const vector<dirfrag_t>& bounds);
  void add_ambiguous_import(CDir *base, const set<CDir*>& bounds);
  bool have_ambiguous_import(dirfrag_t base) {
    return my_ambiguous_imports.count(base);
  }
  void get_ambiguous_import_bounds(dirfrag_t base, vector<dirfrag_t>& bounds) {
    assert(my_ambiguous_imports.count(base));
    bounds = my_ambiguous_imports[base];
  }
  void cancel_ambiguous_import(CDir *);
  void finish_ambiguous_import(dirfrag_t dirino);
  void resolve_start();
  void send_resolves();
  void send_resolve_now(int who);
  void send_resolve_later(int who);
  void maybe_send_pending_resolves();
  
  ESubtreeMap *create_subtree_map();


  void clean_open_file_lists();

protected:
  // [rejoin]
  set<int> rejoin_gather;      // nodes from whom i need a rejoin
  set<int> rejoin_sent;        // nodes i sent a rejoin to
  set<int> rejoin_ack_gather;  // nodes from whom i need a rejoin ack

  map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> > cap_exports; // ino -> client -> capex
  map<inodeno_t,filepath> cap_export_paths;

  map<inodeno_t,map<client_t,map<int,ceph_mds_cap_reconnect> > > cap_imports;  // ino -> client -> frommds -> capex
  map<inodeno_t,filepath> cap_import_paths;
  set<inodeno_t> cap_imports_missing;
  
  set<CInode*> rejoin_undef_inodes;
  set<CInode*> rejoin_potential_updated_scatterlocks;
  set<CDir*>   rejoin_undef_dirfrags;

  vector<CInode*> rejoin_recover_q, rejoin_check_q;
  list<Context*> rejoin_waiters;

  void rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_weak(MMDSCacheRejoin *m);
  CInode* rejoin_invent_inode(inodeno_t ino, snapid_t last);
  CDir* rejoin_invent_dirfrag(dirfrag_t df);
  void handle_cache_rejoin_strong(MMDSCacheRejoin *m);
  void rejoin_scour_survivor_replicas(int from, MMDSCacheRejoin *ack, set<vinodeno_t>& acked_inodes);
  void handle_cache_rejoin_ack(MMDSCacheRejoin *m);
  void handle_cache_rejoin_purge(MMDSCacheRejoin *m);
  void handle_cache_rejoin_missing(MMDSCacheRejoin *m);
  void handle_cache_rejoin_full(MMDSCacheRejoin *m);
  void rejoin_send_acks();
  void rejoin_trim_undef_inodes();
public:
  void rejoin_gather_finish();
  void rejoin_send_rejoins();
  void rejoin_export_caps(inodeno_t ino, client_t client, cap_reconnect_t& icr) {
    cap_exports[ino][client] = icr.capinfo;
    cap_export_paths[ino] = filepath(icr.path, (uint64_t)icr.capinfo.pathbase);
  }
  void rejoin_recovered_caps(inodeno_t ino, client_t client, cap_reconnect_t& icr, 
			     int frommds=-1) {
    cap_imports[ino][client][frommds] = icr.capinfo;
    cap_import_paths[ino] = filepath(icr.path, (uint64_t)icr.capinfo.pathbase);
  }
  ceph_mds_cap_reconnect *get_replay_cap_reconnect(inodeno_t ino, client_t client) {
    if (cap_imports.count(ino) &&
	cap_imports[ino].count(client) &&
	cap_imports[ino][client].count(-1)) {
      return &cap_imports[ino][client][-1];
    }
    return NULL;
  }
  void remove_replay_cap_reconnect(inodeno_t ino, client_t client) {
    assert(cap_imports[ino].size() == 1);
    assert(cap_imports[ino][client].size() == 1);
    cap_imports.erase(ino);
  }

  // [reconnect/rejoin caps]
  map<CInode*,map<client_t, inodeno_t> >  reconnected_caps;   // inode -> client -> realmino
  map<inodeno_t,map<client_t, snapid_t> > reconnected_snaprealms;  // realmino -> client -> realmseq

  void add_reconnected_cap(CInode *in, client_t client, inodeno_t realm) {
    reconnected_caps[in][client] = realm;
  }
  void add_reconnected_snaprealm(client_t client, inodeno_t ino, snapid_t seq) {
    reconnected_snaprealms[ino][client] = seq;
  }
  void process_imported_caps();
  void choose_lock_states_and_reconnect_caps();
  void prepare_realm_split(SnapRealm *realm, client_t client, inodeno_t ino,
			   map<client_t,MClientSnap*>& splits);
  void do_realm_invalidate_and_update_notify(CInode *in, int snapop, bool nosend=false);
  void send_snaps(map<client_t,MClientSnap*>& splits);
  void rejoin_import_cap(CInode *in, client_t client, ceph_mds_cap_reconnect& icr, int frommds);
  void finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq);
  void try_reconnect_cap(CInode *in, Session *session);

  // cap imports.  delayed snap parent opens.
  //  realm inode -> client -> cap inodes needing to split to this realm
  map<CInode*,map<client_t, set<inodeno_t> > > missing_snap_parents; 
  map<client_t,set<CInode*> > delayed_imported_caps;

  void do_cap_import(Session *session, CInode *in, Capability *cap);
  void do_delayed_cap_imports();
  void check_realm_past_parents(SnapRealm *realm);
  void open_snap_parents();

  void open_undef_dirfrags();
  void opened_undef_dirfrag(CDir *dir) {
    rejoin_undef_dirfrags.erase(dir);
  }

  void reissue_all_caps();
  

  friend class Locker;
  friend class Migrator;
  friend class MDBalancer;


  // file size recovery
  set<CInode*> file_recover_queue;
  set<CInode*> file_recovering;

  void queue_file_recover(CInode *in);
  void unqueue_file_recover(CInode *in);
  void _queued_file_recover_cow(CInode *in, Mutation *mut);
  void _queue_file_recover(CInode *in);
  void identify_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q);
  void start_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q);

  void do_file_recover();
  void _recovered(CInode *in, int r, uint64_t size, utime_t mtime);

  void purge_prealloc_ino(inodeno_t ino, Context *fin);



 public:

  // subsystems
  Migrator *migrator;

 public:
  MDCache(MDS *m);
  ~MDCache();
  
  // debug
  void log_stat();

  // root inode
  CInode *get_root() { return root; }
  CInode *get_myin() { return myin; }

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
  bool trim_non_auth_subtree(CDir *directory);
  void try_trim_non_auth_subtree(CDir *dir);

  void trim_client_leases();
  void check_memory_usage();

  // shutdown
  void shutdown_start();
  void shutdown_check();
  bool shutdown_pass();
  bool shutdown_export_strays();
  bool shutdown_export_caps();
  bool shutdown();                    // clear cache (ie at shutodwn)

  bool did_shutdown_log_cap;

  // inode_map
  bool have_inode(vinodeno_t vino) {
    return inode_map.count(vino) ? true:false;
  }
  bool have_inode(inodeno_t ino, snapid_t snap=CEPH_NOSNAP) {
    return have_inode(vinodeno_t(ino, snap));
  }
  CInode* get_inode(vinodeno_t vino) {
    if (have_inode(vino))
      return inode_map[vino];
    return NULL;
  }
  CInode* get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }

  CDir* get_dirfrag(dirfrag_t df) {
    if (!have_inode(df.ino)) return NULL;
    return get_inode(df.ino)->get_dirfrag(df.frag);
  }
  CDir* get_force_dirfrag(dirfrag_t df) {
    CInode *diri = get_inode(df.ino);
    if (!diri)
      return NULL;
    CDir *dir = force_dir_fragment(diri, df.frag);
    if (!dir)
      dir = diri->get_dirfrag(df.frag);
    return dir;
  }

  MDSCacheObject *get_object(MDSCacheObjectInfo &info);

  

 public:
  void add_inode(CInode *in);

  void remove_inode(CInode *in);
 protected:
  void touch_inode(CInode *in) {
    if (in->get_parent_dn())
      touch_dentry(in->get_projected_parent_dn());
  }
public:
  void touch_dentry(CDentry *dn) {
    // touch ancestors
    if (dn->get_dir()->get_inode()->get_projected_parent_dn())
      touch_dentry(dn->get_dir()->get_inode()->get_projected_parent_dn());
    
    // touch me
    if (dn->is_auth())
      lru.lru_touch(dn);
    else
      lru.lru_midtouch(dn);
  }
  void touch_dentry_bottom(CDentry *dn) {
    lru.lru_bottouch(dn);
  }
protected:

  void inode_remove_replica(CInode *in, int rep);
  void dentry_remove_replica(CDentry *dn, int rep);

  void rename_file(CDentry *srcdn, CDentry *destdn);

 public:
  // truncate
  void truncate_inode(CInode *in, LogSegment *ls);
  void _truncate_inode(CInode *in, LogSegment *ls);
  void truncate_inode_finish(CInode *in, LogSegment *ls);
  void truncate_inode_logged(CInode *in, Mutation *mut);

  void add_recovered_truncate(CInode *in, LogSegment *ls);
  void remove_recovered_truncate(CInode *in);
  void start_recovered_truncates();


 public:
  CDir *get_auth_container(CDir *in);
  CDir *get_export_container(CDir *dir);
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);


private:
  bool opening_root, open;
  list<Context*> waiting_for_open;

public:
  void init_layouts();
  CInode *create_system_inode(inodeno_t ino, int mode);
  CInode *create_root_inode();

  void create_empty_hierarchy(C_Gather *gather);
  void create_mydir_hierarchy(C_Gather *gather);

  bool is_open() { return open; }
  void wait_for_open(Context *c) {
    waiting_for_open.push_back(c);
  }

  void open_root_inode(Context *c);
  void open_root();
  void open_mydir_inode(Context *c);
  void populate_mydir();

  void _create_system_file(CDir *dir, const char *name, CInode *in, Context *fin);
  void _create_system_file_finish(Mutation *mut, CDentry *dn, version_t dpv, Context *fin);

  void open_foreign_mdsdir(inodeno_t ino, Context *c);
  CDentry *get_or_create_stray_dentry(CInode *in);

  Context *_get_waiter(MDRequest *mdr, Message *req, Context *fin);
  int path_traverse(MDRequest *mdr, Message *req, Context *c, const filepath& path,
		    vector<CDentry*> *pdnvec, CInode **pin, int onfail);
  bool path_is_mine(filepath& path);
  bool path_is_mine(string& p) {
    filepath path(p, 1);
    return path_is_mine(path);
  }

  CInode *cache_traverse(const filepath& path);

  void open_remote_dirfrag(CInode *diri, frag_t fg, Context *fin);
  CInode *get_dentry_inode(CDentry *dn, MDRequest *mdr, bool projected=false);
  void open_remote_ino(inodeno_t ino, Context *fin, inodeno_t hadino=0, version_t hadv=0);
  void open_remote_ino_2(inodeno_t ino,
                         vector<Anchor>& anchortrace,
			 inodeno_t hadino, version_t hadv,
                         Context *onfinish);
  void open_remote_dentry(CDentry *dn, bool projected, Context *fin);
  void _open_remote_dentry_finish(int r, CDentry *dn, bool projected, Context *fin);

  C_Gather *parallel_fetch(map<inodeno_t,filepath>& pathmap, set<inodeno_t>& missing);
  bool parallel_fetch_traverse_dir(inodeno_t ino, filepath& path, 
				   set<CDir*>& fetch_queue, set<inodeno_t>& missing, C_Gather *gather);


  void make_trace(vector<CDentry*>& trace, CInode *in);
  
  // -- find_ino_peer --
  struct find_ino_peer_info_t {
    inodeno_t ino;
    tid_t tid;
    Context *fin;
    int hint;
    int checking;
    set<int> checked;

    find_ino_peer_info_t() : tid(0), fin(NULL), hint(-1), checking(-1) {}
  };

  map<tid_t, find_ino_peer_info_t> find_ino_peer;
  tid_t find_ino_peer_last_tid;

  void find_ino_peers(inodeno_t ino, Context *c, int hint=-1);
  void _do_find_ino_peer(find_ino_peer_info_t& fip);
  void handle_find_ino(MMDSFindIno *m);
  void handle_find_ino_reply(MMDSFindInoReply *m);
  void kick_find_ino_peers(int who);

  // -- find_ino_dir --
  struct find_ino_dir_info_t {
    inodeno_t ino;
    Context *fin;
  };

  void find_ino_dir(inodeno_t ino, Context *c);
  void _find_ino_dir(inodeno_t ino, Context *c, bufferlist& bl, int r);

  // -- anchors --
public:
  void anchor_create_prep_locks(MDRequest *mdr, CInode *in, set<SimpleLock*>& rdlocks,
				set<SimpleLock*>& xlocks);
  void anchor_create(MDRequest *mdr, CInode *in, Context *onfinish);
  void anchor_destroy(CInode *in, Context *onfinish);
protected:
  void _anchor_prepared(CInode *in, version_t atid, bool add);
  void _anchor_logged(CInode *in, version_t atid, Mutation *mut);
  friend class C_MDC_AnchorPrepared;
  friend class C_MDC_AnchorLogged;

  // -- snaprealms --
public:
  void snaprealm_create(MDRequest *mdr, CInode *in);
  void _snaprealm_create_finish(MDRequest *mdr, Mutation *mut, CInode *in);

  // -- stray --
public:
  void scan_stray_dir();
  void eval_stray(CDentry *dn);
  void eval_remote(CDentry *dn);

  void maybe_eval_stray(CInode *in) {
    if (in->inode.nlink > 0 || in->is_base())
      return;
    CDentry *dn = in->get_projected_parent_dn();
    if (dn->get_projected_linkage()->is_primary() &&
	dn->get_dir()->get_inode()->is_stray() &&
	!dn->is_replicated())
      eval_stray(dn);
  }
protected:
  void purge_stray(CDentry *dn);
  void _purge_stray_purged(CDentry *dn, int r=0);
  void _purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls);
  void _purge_stray_logged_truncate(CDentry *dn, LogSegment *ls);
  friend class C_MDC_PurgeStrayLogged;
  friend class C_MDC_PurgeStrayLoggedTruncate;
  friend class C_MDC_PurgeStrayPurged;
  void reintegrate_stray(CDentry *dn, CDentry *rlink);
  void migrate_stray(CDentry *dn, int dest);


  // == messages ==
 public:
  void dispatch(Message *m);

 protected:
  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);
  friend class C_MDC_Join;

public:
  void replicate_dir(CDir *dir, int to, bufferlist& bl) {
    dirfrag_t df = dir->dirfrag();
    ::encode(df, bl);
    dir->encode_replica(to, bl);
  }
  void replicate_dentry(CDentry *dn, int to, bufferlist& bl) {
    ::encode(dn->name, bl);
    ::encode(dn->last, bl);
    dn->encode_replica(to, bl);
  }
  void replicate_inode(CInode *in, int to, bufferlist& bl) {
    ::encode(in->inode.ino, bl);  // bleh, minor assymetry here
    ::encode(in->last, bl);
    in->encode_replica(to, bl);
  }
  
  CDir* add_replica_dir(bufferlist::iterator& p, CInode *diri, int from, list<Context*>& finished);
  CDir* forge_replica_dir(CInode *diri, frag_t fg, int from);
  CDentry *add_replica_dentry(bufferlist::iterator& p, CDir *dir, list<Context*>& finished);
  CInode *add_replica_inode(bufferlist::iterator& p, CDentry *dn, list<Context*>& finished);

  void replicate_stray(CDentry *straydn, int who, bufferlist& bl);
  CDentry *add_replica_stray(bufferlist &bl, int from);

  // -- namespace --
public:
  void send_dentry_link(CDentry *dn);
  void send_dentry_unlink(CDentry *dn, CDentry *straydn);
protected:
  void handle_dentry_link(MDentryLink *m);
  void handle_dentry_unlink(MDentryUnlink *m);


  // -- fragmenting --
public:
  set< pair<dirfrag_t,int> > uncommitted_fragments;  // prepared but uncommitted refragmentations

private:
  void adjust_dir_fragments(CInode *diri, frag_t basefrag, int bits,
			    list<CDir*>& frags, list<Context*>& waiters, bool replay);
  void adjust_dir_fragments(CInode *diri,
			    list<CDir*>& srcfrags,
			    frag_t basefrag, int bits,
			    list<CDir*>& resultfrags, 
			    list<Context*>& waiters,
			    bool replay);
  CDir *force_dir_fragment(CInode *diri, frag_t fg);
  void get_force_dirfrag_bound_set(vector<dirfrag_t>& dfs, set<CDir*>& bounds);


  friend class EFragment;

  bool can_fragment_lock(CInode *diri);
  bool can_fragment(CInode *diri, list<CDir*>& dirs);

public:
  void split_dir(CDir *dir, int byn);
  void merge_dir(CInode *diri, frag_t fg);

private:
  void fragment_freeze_dirs(list<CDir*>& dirs, C_Gather *gather);
  void fragment_mark_and_complete(list<CDir*>& dirs);
  void fragment_frozen(list<CDir*>& dirs, frag_t basefrag, int bits);
  void fragment_unmark_unfreeze_dirs(list<CDir*>& dirs);
  void fragment_logged_and_stored(Mutation *mut, list<CDir*>& resultfrags, frag_t basefrag, int bits);
public:
  void rollback_uncommitted_fragments();
private:

  friend class C_MDC_FragmentFrozen;
  friend class C_MDC_FragmentMarking;
  friend class C_MDC_FragmentLoggedAndStored;

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
  void dump_cache(const char *fn=0);
  void show_subtrees(int dbl=10);

  CInode *hack_pick_random_inode() {
    assert(!inode_map.empty());
    int n = rand() % inode_map.size();
    hash_map<vinodeno_t,CInode*>::iterator p = inode_map.begin();
    while (n--) p++;
    return p->second;
  }

};

class C_MDS_RetryRequest : public Context {
  MDCache *cache;
  MDRequest *mdr;
 public:
  C_MDS_RetryRequest(MDCache *c, MDRequest *r) : cache(c), mdr(r) {
    mdr->get();
  }
  virtual void finish(int r) {
    cache->dispatch_request(mdr);
    mdr->put();
  }
};

#endif
