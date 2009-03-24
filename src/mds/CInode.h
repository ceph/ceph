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



#ifndef __CINODE_H
#define __CINODE_H

#include "config.h"
#include "include/types.h"
#include "include/lru.h"

#include "mdstypes.h"

#include "CDentry.h"
#include "SimpleLock.h"
#include "ScatterLock.h"
#include "LocalLock.h"
#include "Capability.h"
#include "snap.h"
#include "SessionMap.h"

#include <list>
#include <vector>
#include <set>
#include <map>
#include <iostream>
using namespace std;

class Context;
class CDentry;
class CDir;
class Message;
class CInode;
class MDCache;
class LogSegment;
class SnapRealm;
class Session;
class MClientCaps;

ostream& operator<<(ostream& out, CInode& in);


// cached inode wrapper
class CInode : public MDSCacheObject {
 public:
  // -- pins --
  static const int PIN_DIRFRAG =         -1; 
  static const int PIN_CAPS =             2;  // client caps
  static const int PIN_IMPORTING =       -4;  // importing
  static const int PIN_ANCHORING =        5;
  static const int PIN_UNANCHORING =      6;
  static const int PIN_OPENINGDIR =       7;
  static const int PIN_REMOTEPARENT =     8;
  static const int PIN_BATCHOPENJOURNAL = 9;
  static const int PIN_SCATTERED =        10;
  static const int PIN_STICKYDIRS =       11;
  static const int PIN_PURGING =         -12;	
  static const int PIN_FREEZING =         13;
  static const int PIN_FROZEN =           14;
  static const int PIN_IMPORTINGCAPS =    15;
  static const int PIN_PASTSNAPPARENT =  -16;
  static const int PIN_OPENINGSNAPPARENTS = 17;
  static const int PIN_TRUNCATING =       18;
  static const int PIN_STRAY =            19;  // we pin our stray inode while active

  const char *pin_name(int p) {
    switch (p) {
    case PIN_DIRFRAG: return "dirfrag";
    case PIN_CAPS: return "caps";
    case PIN_IMPORTING: return "importing";
    case PIN_ANCHORING: return "anchoring";
    case PIN_UNANCHORING: return "unanchoring";
    case PIN_OPENINGDIR: return "openingdir";
    case PIN_REMOTEPARENT: return "remoteparent";
    case PIN_BATCHOPENJOURNAL: return "batchopenjournal";
    case PIN_SCATTERED: return "scattered";
    case PIN_STICKYDIRS: return "stickydirs";
    case PIN_PURGING: return "purging";
    case PIN_FREEZING: return "freezing";
    case PIN_FROZEN: return "frozen";
    case PIN_IMPORTINGCAPS: return "importingcaps";
    case PIN_PASTSNAPPARENT: return "pastsnapparent";
    case PIN_OPENINGSNAPPARENTS: return "openingsnapparents";
    case PIN_TRUNCATING: return "truncating";
    case PIN_STRAY: return "stray";
    default: return generic_pin_name(p);
    }
  }

  // -- state --
  static const int STATE_EXPORTING =   (1<<2);   // on nonauth bystander.
  static const int STATE_ANCHORING =   (1<<3);
  static const int STATE_UNANCHORING = (1<<4);
  static const int STATE_OPENINGDIR =  (1<<5);
  static const int STATE_REJOINUNDEF = (1<<6);   // inode contents undefined.
  static const int STATE_FREEZING =    (1<<7);
  static const int STATE_FROZEN =      (1<<8);
  static const int STATE_AMBIGUOUSAUTH = (1<<9);
  static const int STATE_EXPORTINGCAPS = (1<<10);
  static const int STATE_NEEDSRECOVER = (1<<11);
  static const int STATE_RECOVERING =   (1<<12);

  // -- waiters --
  static const __u64 WAIT_DIR         = (1<<0);
  static const __u64 WAIT_ANCHORED    = (1<<1);
  static const __u64 WAIT_UNANCHORED  = (1<<2);
  static const __u64 WAIT_FROZEN      = (1<<3);
  static const __u64 WAIT_TRUNC       = (1<<4);
  
  static const int WAIT_AUTHLOCK_OFFSET        = 5;
  static const int WAIT_LINKLOCK_OFFSET        = 5 +   SimpleLock::WAIT_BITS;
  static const int WAIT_DIRFRAGTREELOCK_OFFSET = 5 + 2*SimpleLock::WAIT_BITS;
  static const int WAIT_FILELOCK_OFFSET        = 5 + 3*SimpleLock::WAIT_BITS;
  static const int WAIT_VERSIONLOCK_OFFSET     = 5 + 4*SimpleLock::WAIT_BITS;
  static const int WAIT_XATTRLOCK_OFFSET       = 5 + 5*SimpleLock::WAIT_BITS;
  static const int WAIT_SNAPLOCK_OFFSET        = 5 + 6*SimpleLock::WAIT_BITS;
  static const int WAIT_NESTLOCK_OFFSET        = 5 + 7*SimpleLock::WAIT_BITS;

  static const __u64 WAIT_ANY_MASK	= (__u64)(-1);

  // misc
  static const int EXPORT_NONCE = 1; // nonce given to replicas created by export

  ostream& print_db_line_prefix(ostream& out);

 public:
  MDCache *mdcache;

  // inode contents proper
  inode_t          inode;        // the inode itself
  string           symlink;      // symlink dest, if symlink
  map<string, bufferptr> xattrs;
  fragtree_t       dirfragtree;  // dir frag tree, if any.  always consistent with our dirfrag map.
  SnapRealm        *snaprealm;

  SnapRealm        *containing_realm;
  snapid_t          first, last;
  map<snapid_t, old_inode_t> old_inodes;  // key = last, value.first = first
  set<snapid_t> dirty_old_rstats;

  bool is_multiversion() {
    return snaprealm ||  // other snaprealms will link to me
      inode.is_dir() ||  // links to me in other snaps
      inode.nlink > 1 || // there are remote links, possibly snapped, that will need to find me
      old_inodes.size(); // once multiversion, always multiversion.  until old_inodes gets cleaned out.
  }
  snapid_t get_oldest_snap();

  loff_t last_journaled;       // log offset for the last time i was journaled
  loff_t last_open_journaled;  // log offset for the last journaled EOpen

  //bool hack_accessed;
  //utime_t hack_load_stamp;

  // projected values (only defined while dirty)
  list<inode_t*>   projected_inode;

  // if xattr* is null, it is defined to be the same as the previous version
  list<map<string,bufferptr>*>   projected_xattrs;
  
  version_t get_projected_version() {
    if (projected_inode.empty())
      return inode.version;
    else
      return projected_inode.back()->version;
  }
  bool is_projected() {
    return !projected_inode.empty();
  }

  inode_t *get_projected_inode() { 
    if (projected_inode.empty())
      return &inode;
    else
      return projected_inode.back();
  }
  map<string,bufferptr> *get_projected_xattrs() {
    if (!projected_xattrs.empty())
      for (list<map<string,bufferptr>*>::reverse_iterator p = projected_xattrs.rbegin();
	   p != projected_xattrs.rend();
	   p++)
	if (*p)
	  return *p;
    return &xattrs;
  }

  inode_t *project_inode(map<string,bufferptr> *px=0);
  void pop_and_dirty_projected_inode(LogSegment *ls);

  inode_t *get_previous_projected_inode() {
    assert(!projected_inode.empty());
    list<inode_t*>::reverse_iterator p = projected_inode.rbegin();
    p++;
    if (p != projected_inode.rend())
      return *p;
    else
      return &inode;
  }

  map<snapid_t,old_inode_t>::iterator pick_dirty_old_inode(snapid_t last);

  old_inode_t& cow_old_inode(snapid_t follows, inode_t *pi);
  void pre_cow_old_inode();
  void purge_stale_snap_data(const set<snapid_t>& snaps);

  // -- cache infrastructure --
private:
  map<frag_t,CDir*> dirfrags; // cached dir fragments
  int stickydir_ref;

public:
  frag_t pick_dirfrag(const nstring &dn);
  bool has_dirfrags() { return !dirfrags.empty(); }
  CDir* get_dirfrag(frag_t fg) {
    if (dirfrags.count(fg)) {
      assert(g_conf.debug_mds < 2 || dirfragtree.is_leaf(fg)); // performance hack FIXME
      return dirfrags[fg];
    } else
      return 0;
  }
  void get_dirfrags_under(frag_t fg, list<CDir*>& ls);
  CDir* get_approx_dirfrag(frag_t fg);
  void get_dirfrags(list<CDir*>& ls);
  void get_nested_dirfrags(list<CDir*>& ls);
  void get_subtree_dirfrags(list<CDir*>& ls);
  CDir *get_or_open_dirfrag(MDCache *mdcache, frag_t fg);
  CDir *add_dirfrag(CDir *dir);
  void close_dirfrag(frag_t fg);
  void close_dirfrags();
  bool has_subtree_root_dirfrag();

  void get_stickydirs();
  void put_stickydirs();  

 protected:
  // parent dentries in cache
  CDentry         *parent;             // primary link
  set<CDentry*>    remote_parents;     // if hard linked

  list<CDentry*>   projected_parent;   // for in-progress rename, (un)link, etc.

  pair<int,int> inode_auth;

  // -- distributed state --
protected:
  // file capabilities
  map<int, Capability*> client_caps;         // client -> caps
  map<int, int>         mds_caps_wanted;     // [auth] mds -> caps wanted
  int                   replica_caps_wanted; // [replica] what i've requested from auth
  utime_t               replica_caps_wanted_keep_until;


  // LogSegment xlists i (may) belong to
  xlist<CInode*>::item xlist_dirty;
public:
  xlist<CInode*>::item xlist_caps;
  xlist<CInode*>::item xlist_open_file;
  xlist<CInode*>::item xlist_dirty_dirfrag_dir;
  xlist<CInode*>::item xlist_dirty_dirfrag_nest;
  xlist<CInode*>::item xlist_dirty_dirfrag_dirfragtree;
  xlist<CInode*>::item xlist_purging_inode;

private:
  // auth pin
  int auth_pins;
  int nested_auth_pins;
public:
#ifdef MDS_AUTHPIN_SET
  multiset<void*> auth_pin_set;
#endif
  int auth_pin_freeze_allowance;

private:
  int nested_anchors;   // _NOT_ including me!

 public:
  inode_load_vec_t pop;

  // friends
  friend class Server;
  friend class Locker;
  friend class Migrator;
  friend class MDCache;
  friend class CDir;
  friend class CInodeExport;

 public:
  // ---------------------------
  CInode(MDCache *c, bool auth=true, snapid_t f=2, snapid_t l=CEPH_NOSNAP) : 
    mdcache(c),
    snaprealm(0), containing_realm(0),
    first(f), last(l),
    last_journaled(0), last_open_journaled(0), 
    //hack_accessed(true),
    stickydir_ref(0),
    parent(0),
    inode_auth(CDIR_AUTH_DEFAULT),
    replica_caps_wanted(0),
    xlist_dirty(this), xlist_caps(this), xlist_open_file(this), 
    xlist_dirty_dirfrag_dir(this), 
    xlist_dirty_dirfrag_nest(this), 
    xlist_dirty_dirfrag_dirfragtree(this), 
    xlist_purging_inode(this),
    auth_pins(0), nested_auth_pins(0),
    nested_anchors(0),
    versionlock(this, CEPH_LOCK_IVERSION, WAIT_VERSIONLOCK_OFFSET),
    authlock(this, CEPH_LOCK_IAUTH, WAIT_AUTHLOCK_OFFSET),
    linklock(this, CEPH_LOCK_ILINK, WAIT_LINKLOCK_OFFSET),
    dirfragtreelock(this, CEPH_LOCK_IDFT, WAIT_DIRFRAGTREELOCK_OFFSET),
    filelock(this, CEPH_LOCK_IFILE, WAIT_FILELOCK_OFFSET),
    xattrlock(this, CEPH_LOCK_IXATTR, WAIT_XATTRLOCK_OFFSET),
    snaplock(this, CEPH_LOCK_ISNAP, WAIT_SNAPLOCK_OFFSET),
    nestlock(this, CEPH_LOCK_INEST, WAIT_NESTLOCK_OFFSET),
    loner_cap(-1)
  {
    memset(&inode, 0, sizeof(inode));
    state = 0;  
    if (auth) state_set(STATE_AUTH);
  };
  ~CInode() {
    close_dirfrags();
    close_snaprealm();
  }
  

  // -- accessors --
  bool is_file()    { return inode.is_file(); }
  bool is_symlink() { return inode.is_symlink(); }
  bool is_dir()     { return inode.is_dir(); }

  bool is_anchored() { return inode.anchored; }
  bool is_anchoring() { return state_test(STATE_ANCHORING); }
  bool is_unanchoring() { return state_test(STATE_UNANCHORING); }
  
  bool is_root() { return inode.ino == MDS_INO_ROOT; }
  bool is_stray() { return MDS_INO_IS_STRAY(inode.ino); }
  bool is_system() { return inode.ino < MDS_INO_SYSTEM_BASE; }

  // note: this overloads MDSCacheObject
  bool is_ambiguous_auth() {
    return state_test(STATE_AMBIGUOUSAUTH) ||
      MDSCacheObject::is_ambiguous_auth();
  }


  inodeno_t ino() const { return inode.ino; }
  vinodeno_t vino() const { return vinodeno_t(inode.ino, last); }
  int d_type() const { return MODE_TO_DT(inode.mode); }

  inode_t& get_inode() { return inode; }
  CDentry* get_parent_dn() { return parent; }
  CDentry* get_projected_parent_dn() { return projected_parent.size() ? projected_parent.back():parent; }
  CDir *get_parent_dir();
  CInode *get_parent_inode();
  
  bool is_lt(const MDSCacheObject *r) const {
    CInode *o = (CInode*)r;
    return ino() < o->ino() ||
      (ino() == o->ino() && last < o->last);
  }

  int64_t get_layout_size_increment() {
    return ceph_file_layout_period(inode.layout);
  }

  // -- misc -- 
  bool is_ancestor_of(CInode *other);
  void make_path_string(string& s, bool force=false, CDentry *use_parent=NULL);
  void make_path_string_projected(string& s);  
  void make_path(filepath& s);
  void make_anchor_trace(vector<class Anchor>& trace);
  void name_stray_dentry(string& dname);


  
  // -- dirtyness --
  version_t get_version() { return inode.version; }

  version_t pre_dirty();
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();


  void encode_replica(int rep, bufferlist& bl) {
    assert(is_auth());
    
    // relax locks?
    if (!is_replicated())
      replicate_relax_locks();
    
    __u32 nonce = add_replica(rep);
    ::encode(nonce, bl);
    
    _encode_base(bl);
    _encode_locks_state_for_replica(bl);
  }
  void decode_replica(bufferlist::iterator& p, bool is_new) {
    __u32 nonce;
    ::decode(nonce, p);
    replica_nonce = nonce;
    
    _decode_base(p);
    _decode_locks_state(p, is_new);  
  }


  // -- waiting --
  void add_waiter(__u64 tag, Context *c);


  // -- encode/decode helpers --
  void _encode_base(bufferlist& bl);
  void _decode_base(bufferlist::iterator& p);
  void _encode_locks_full(bufferlist& bl);
  void _decode_locks_full(bufferlist::iterator& p);
  void _encode_locks_state_for_replica(bufferlist& bl);
  void _decode_locks_state(bufferlist::iterator& p, bool is_new);
  void _decode_locks_rejoin(bufferlist::iterator& p, list<Context*>& waiters);


  // -- import/export --
  void encode_export(bufferlist& bl);
  void finish_export(utime_t now);
  void abort_export() {
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(bufferlist::iterator& p, LogSegment *ls);
  

  // for giving to clients
  bool encode_inodestat(bufferlist& bl, Session *session, SnapRealm *realm,
			snapid_t snapid=CEPH_NOSNAP, bool is_replay=false);
  void encode_cap_message(MClientCaps *m, Capability *cap);


  // -- locks --
public:
  LocalLock  versionlock;
  SimpleLock authlock;
  SimpleLock linklock;
  ScatterLock dirfragtreelock;
  ScatterLock filelock;
  SimpleLock xattrlock;
  SimpleLock snaplock;
  ScatterLock nestlock;

  SimpleLock* get_lock(int type) {
    switch (type) {
    case CEPH_LOCK_IFILE: return &filelock;
    case CEPH_LOCK_IAUTH: return &authlock;
    case CEPH_LOCK_ILINK: return &linklock;
    case CEPH_LOCK_IDFT: return &dirfragtreelock;
    case CEPH_LOCK_IXATTR: return &xattrlock;
    case CEPH_LOCK_ISNAP: return &snaplock;
    case CEPH_LOCK_INEST: return &nestlock;
    }
    return 0;
  }

  void set_object_info(MDSCacheObjectInfo &info);
  void encode_lock_state(int type, bufferlist& bl);
  void decode_lock_state(int type, bufferlist& bl);

  void clear_dirty_scattered(int type);
  void finish_scatter_gather_update(int type);


  // -- snap --
  void open_snaprealm(bool no_split=false);
  void close_snaprealm(bool no_join=false);
  SnapRealm *find_snaprealm();
  void encode_snap_blob(bufferlist &bl);
  void decode_snap_blob(bufferlist &bl);
  void encode_snap(bufferlist& bl) {
    bufferlist snapbl;
    encode_snap_blob(snapbl);
    ::encode(snapbl, bl);
  }    
  void decode_snap(bufferlist::iterator& p) {
    bufferlist snapbl;
    ::decode(snapbl, p);
    decode_snap_blob(snapbl);
  }

  // -- caps -- (new)
  // client caps
  int loner_cap;

  bool try_choose_loner() {
    if (loner_cap >= 0)
      return true;

    if (!mds_caps_wanted.empty())
      return false;

    int n = 0;
    int loner = -1;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) 
      if (!it->second->is_stale() &&
	  ((it->second->wanted() & CEPH_CAP_ANY_WR) || inode.is_dir())) {
	if (n)
	  return false;
	n++;
	loner = it->first;
      }
    if (n == 1) {
      loner_cap = loner;
      return true;
    }
    return false;
  }
  
  bool try_drop_loner() {
    if (loner_cap < 0)
      return true;

    int other_allowed = get_caps_allowed_by_type(CAP_ANY);
    Capability *cap = get_client_cap(loner_cap);
    if (!cap ||
	(cap->issued() & ~other_allowed) == 0) {
      loner_cap = -1;
      return true;
    }
    return false;
  }


  int count_nonstale_caps() {
    int n = 0;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) 
      if (!it->second->is_stale())
	n++;
    return n;
  }
  int get_loner() {
    return loner_cap;
  }

  bool is_any_caps() { return !client_caps.empty(); }
  bool is_any_nonstale_caps() { return count_nonstale_caps(); }

  map<int,Capability*>& get_client_caps() { return client_caps; }
  Capability *get_client_cap(int client) {
    if (client_caps.count(client))
      return client_caps[client];
    return 0;
  }
  int get_client_cap_pending(int client) {
    Capability *c = get_client_cap(client);
    if (c) return c->pending();
    return 0;
  }
  Capability *add_client_cap(int client, Session *session,
			     xlist<Capability*> *rdcaps_list, SnapRealm *conrealm=0);
  void remove_client_cap(int client) {
    assert(client_caps.count(client) == 1);
    Capability *cap = client_caps[client];

    cap->session_caps_item.remove_myself();
    cap->rdcaps_item.remove_myself();
    containing_realm->remove_cap(client, cap);

    if (client == loner_cap)
      loner_cap = -1;

    delete cap;
    client_caps.erase(client);
    if (client_caps.empty()) {
      put(PIN_CAPS);
      xlist_caps.remove_myself();
      containing_realm = NULL;
      xlist_open_file.remove_myself();  // unpin logsegment
    }
  }
  void move_to_containing_realm(SnapRealm *realm) {
    for (map<int,Capability*>::iterator q = client_caps.begin();
	 q != client_caps.end();
	 q++) {
      containing_realm->remove_cap(q->first, q->second);
      realm->add_cap(q->first, q->second);
    }
    xlist_caps.remove_myself();
    realm->inodes_with_caps.push_back(&xlist_caps);
    containing_realm = realm;
  }

  Capability *reconnect_cap(int client, ceph_mds_cap_reconnect& icr, Session *session, xlist<Capability*> *rdcaps_list) {
    Capability *cap = get_client_cap(client);
    if (cap) {
      cap->merge(icr.wanted, icr.issued);
    } else {
      cap = add_client_cap(client, session, rdcaps_list);
      cap->set_wanted(icr.wanted);
      cap->issue(icr.issued);
    }
    cap->set_last_issue_stamp(g_clock.recent_now());
    inode.size = MAX(inode.size, icr.size);
    inode.mtime = MAX(inode.mtime, utime_t(icr.mtime));
    inode.atime = MAX(inode.atime, utime_t(icr.atime));
    return cap;
  }
  void clear_client_caps() {
    while (!client_caps.empty())
      remove_client_cap(client_caps.begin()->first);
  }
  void export_client_caps(map<int,Capability::Export>& cl) {
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) {
      cl[it->first] = it->second->make_export();
    }
  }

  // caps allowed
  int get_caps_liked() {
    if (is_dir())
      return CEPH_CAP_EXPIREABLE & ~CEPH_CAP_FILE_RD;
    else
      return CEPH_CAP_EXPIREABLE;
  }
  int get_caps_allowed_ever() {
    return 
      CEPH_CAP_PIN |
      (filelock.gcaps_allowed_ever() << filelock.get_cap_shift()) |
      (authlock.gcaps_allowed_ever() << authlock.get_cap_shift()) |
      (xattrlock.gcaps_allowed_ever() << xattrlock.get_cap_shift()) |
      (linklock.gcaps_allowed_ever() << linklock.get_cap_shift());
  }
  int get_caps_allowed_by_type(int type) {
    return 
      CEPH_CAP_PIN |
      (filelock.gcaps_allowed(type) << filelock.get_cap_shift()) |
      (authlock.gcaps_allowed(type) << authlock.get_cap_shift()) |
      (xattrlock.gcaps_allowed(type) << xattrlock.get_cap_shift()) |
      (linklock.gcaps_allowed(type) << linklock.get_cap_shift());
  }
  int get_caps_careful() {
    return 
      (filelock.gcaps_careful() << filelock.get_cap_shift()) |
      (authlock.gcaps_careful() << authlock.get_cap_shift()) |
      (xattrlock.gcaps_careful() << xattrlock.get_cap_shift()) |
      (linklock.gcaps_careful() << linklock.get_cap_shift());
  }
  int get_xlocker_mask(int client) {
    return 
      (filelock.gcaps_xlocker_mask(client) << filelock.get_cap_shift()) |
      (authlock.gcaps_xlocker_mask(client) << authlock.get_cap_shift()) |
      (xattrlock.gcaps_xlocker_mask(client) << xattrlock.get_cap_shift()) |
      (linklock.gcaps_xlocker_mask(client) << linklock.get_cap_shift());
  }
  int get_caps_allowed_for_client(int client) {
    int allowed = get_caps_allowed_by_type(client == get_loner() ? CAP_LONER : CAP_ANY);
    allowed |= get_caps_allowed_by_type(CAP_XLOCKER) & get_xlocker_mask(client);
    return allowed;
  }

  // caps issued, wanted
  int get_caps_issued(int *ploner = 0, int *pother = 0, int *pxlocker = 0,
		      int shift = 0, int mask = 0xffff) {
    int c = 0;
    int loner = 0, other = 0, xlocker = 0;
    if (!is_auth())
      loner_cap = -1;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) {
      int i = it->second->issued();
      c |= i;
      if (it->first == loner_cap)
	loner |= i;
      else
	other |= i;
      xlocker |= get_xlocker_mask(it->first) & i;
    }
    if (ploner) *ploner = (loner >> shift) & mask;
    if (pother) *pother = (other >> shift) & mask;
    if (pxlocker) *pxlocker = (xlocker >> shift) & mask;
    return c;
  }
  int get_caps_wanted(int *ploner = 0, int *pother = 0, int shift = 0, int mask = 0xffff) {
    int w = 0;
    int loner = 0, other = 0;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) {
      if (!it->second->is_stale()) {
	int t = it->second->wanted();
	w |= t;
	if (it->first == loner_cap)
	  loner |= t;
	else
	  other |= t;	
      }
      //cout << " get_caps_wanted client " << it->first << " " << cap_string(it->second.wanted()) << endl;
    }
    if (is_auth())
      for (map<int,int>::iterator it = mds_caps_wanted.begin();
           it != mds_caps_wanted.end();
           it++) {
        w |= it->second;
	other |= it->second;
        //cout << " get_caps_wanted mds " << it->first << " " << cap_string(it->second) << endl;
      }
    if (ploner) *ploner = (loner >> shift) & mask;
    if (pother) *pother = (other >> shift) & mask;
    return w;
  }

  bool issued_caps_need_gather(SimpleLock *lock) {
    int loner_issued, other_issued, xlocker_issued;
    get_caps_issued(&loner_issued, &other_issued, &xlocker_issued,
		    lock->get_cap_shift(), lock->get_cap_mask());
    if ((loner_issued & ~lock->gcaps_allowed(CAP_LONER)) ||
	(other_issued & ~lock->gcaps_allowed(CAP_ANY)) ||
	(xlocker_issued & ~lock->gcaps_allowed(CAP_XLOCKER)))
      return true;
    return false;
  }

  void replicate_relax_locks() {
    //dout(10) << " relaxing locks on " << *this << dendl;
    assert(is_auth());
    assert(!is_replicated());

    authlock.replicate_relax();
    linklock.replicate_relax();
    dirfragtreelock.replicate_relax();
    filelock.replicate_relax();
    xattrlock.replicate_relax();
    snaplock.replicate_relax();
    nestlock.replicate_relax();
  }


  // -- authority --
  pair<int,int> authority();


  // -- auth pins --
  int is_auth_pinned() { return auth_pins; }
  int get_num_auth_pins() { return auth_pins; }
  void adjust_nested_auth_pins(int a);
  bool can_auth_pin();
  void auth_pin(void *by);
  void auth_unpin(void *by);

  void adjust_nested_anchors(int by);
  int get_nested_anchors() { return nested_anchors; }

  // -- freeze --
  bool is_freezing_inode() { return state_test(STATE_FREEZING); }
  bool is_frozen_inode() { return state_test(STATE_FROZEN); }
  bool is_frozen();
  bool is_frozen_dir();
  bool is_freezing();

  bool freeze_inode(int auth_pin_allowance=0);
  void unfreeze_inode(list<Context*>& finished);


  // -- reference counting --
  void bad_put(int by) {
    generic_dout(0) << " bad put " << *this << " by " << by << " " << pin_name(by) << " was " << ref
#ifdef MDS_REF_SET
		    << " (" << ref_set << ")"
#endif
		    << dendl;
#ifdef MDS_REF_SET
    assert(ref_set.count(by) == 1);
#endif
    assert(ref > 0);
  }
  void bad_get(int by) {
    generic_dout(0) << " bad get " << *this << " by " << by << " " << pin_name(by) << " was " << ref
#ifdef MDS_REF_SET
		    << " (" << ref_set << ")"
#endif
		    << dendl;
#ifdef MDS_REF_SET
    assert(ref_set.count(by) == 0);
#endif
  }
  void first_get();
  void last_put();


  // -- hierarchy stuff --
public:
  void set_primary_parent(CDentry *p) {
    assert(parent == 0);
    parent = p;
  }
  void remove_primary_parent(CDentry *dn) {
    assert(dn == parent);
    parent = 0;
  }
  void add_remote_parent(CDentry *p);
  void remove_remote_parent(CDentry *p);
  int num_remote_parents() {
    return remote_parents.size(); 
  }

  void push_projected_parent(CDentry *dn) {
    projected_parent.push_back(dn);
  }
  void pop_projected_parent() {
    assert(projected_parent.size());
    parent = projected_parent.front();
    projected_parent.pop_front();
  }

  void print(ostream& out);

};

#endif
