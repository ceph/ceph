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
#include "FileLock.h"
#include "ScatterLock.h"
#include "LocalLock.h"
#include "Capability.h"


#include <cassert>
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
class CInodeDiscover;
class MDCache;
class LogSegment;

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

  // -- waiters --
  //static const int WAIT_SLAVEAGREE  = (1<<0);
  static const int WAIT_DIR         = (1<<1);
  static const int WAIT_ANCHORED    = (1<<2);
  static const int WAIT_UNANCHORED  = (1<<3);
  static const int WAIT_CAPS        = (1<<4);
  static const int WAIT_FROZEN      = (1<<5);
  
  static const int WAIT_AUTHLOCK_OFFSET = 5;
  static const int WAIT_LINKLOCK_OFFSET = 5 + SimpleLock::WAIT_BITS;
  static const int WAIT_DIRFRAGTREELOCK_OFFSET = 5 + 2*SimpleLock::WAIT_BITS;
  static const int WAIT_FILELOCK_OFFSET = 5 + 3*SimpleLock::WAIT_BITS;
  static const int WAIT_DIRLOCK_OFFSET = 5 + 4*SimpleLock::WAIT_BITS;
  static const int WAIT_VERSIONLOCK_OFFSET = 5 + 5*SimpleLock::WAIT_BITS;

  static const int WAIT_ANY           = 0xffffffff;

  // misc
  static const int EXPORT_NONCE = 1; // nonce given to replicas created by export

  ostream& print_db_line_prefix(ostream& out);

 public:
  MDCache *mdcache;

  // inode contents proper
  inode_t          inode;        // the inode itself
  string           symlink;      // symlink dest, if symlink
  fragtree_t       dirfragtree;  // dir frag tree, if any.  always consistent with our dirfrag map.
  map<frag_t,int>  dirfrag_size; // size of each dirfrag

  off_t last_journaled;       // log offset for the last time i was journaled
  off_t last_open_journaled;  // log offset for the last journaled EOpen

  //bool hack_accessed;
  //utime_t hack_load_stamp;

  // projected values (only defined while dirty)
  list<inode_t*>    projected_inode;
  list<fragtree_t> projected_dirfragtree;

  version_t get_projected_version() {
    if (projected_inode.empty())
      return inode.version;
    else
      return projected_inode.back()->version;
  }

  inode_t *project_inode();
  void pop_and_dirty_projected_inode(LogSegment *ls);

  // -- cache infrastructure --
private:
  map<frag_t,CDir*> dirfrags; // cached dir fragments
  int stickydir_ref;

public:
  frag_t pick_dirfrag(const string &dn);
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

  pair<int,int> inode_auth;

  // -- distributed state --
protected:
  // file capabilities
  map<int, Capability*>  client_caps;         // client -> caps
  map<int, int>         mds_caps_wanted;     // [auth] mds -> caps wanted
  int                   replica_caps_wanted; // [replica] what i've requested from auth
  utime_t               replica_caps_wanted_keep_until;


  // LogSegment xlists i (may) belong to
  xlist<CInode*>::item xlist_dirty;
public:
  xlist<CInode*>::item xlist_open_file;
  xlist<CInode*>::item xlist_dirty_inode_mtime;
  xlist<CInode*>::item xlist_purging_inode;

private:
  // auth pin
  int auth_pins;
  int nested_auth_pins;
public:
  int auth_pin_freeze_allowance;

 public:
  inode_load_vec_t pop;

  // friends
  friend class Server;
  friend class Locker;
  friend class Migrator;
  friend class MDCache;
  friend class CDir;
  friend class CInodeExport;
  friend class CInodeDiscover;

 public:
  // ---------------------------
  CInode(MDCache *c, bool auth=true) : 
    mdcache(c),
    last_journaled(0), last_open_journaled(0), 
    //hack_accessed(true),
    stickydir_ref(0),
    parent(0), inode_auth(CDIR_AUTH_DEFAULT),
    replica_caps_wanted(0),
    xlist_dirty(this), xlist_open_file(this), 
    xlist_dirty_inode_mtime(this), xlist_purging_inode(this),
    auth_pins(0), nested_auth_pins(0),
    versionlock(this, LOCK_OTYPE_IVERSION, WAIT_VERSIONLOCK_OFFSET),
    authlock(this, LOCK_OTYPE_IAUTH, WAIT_AUTHLOCK_OFFSET),
    linklock(this, LOCK_OTYPE_ILINK, WAIT_LINKLOCK_OFFSET),
    dirfragtreelock(this, LOCK_OTYPE_IDIRFRAGTREE, WAIT_DIRFRAGTREELOCK_OFFSET),
    filelock(this, LOCK_OTYPE_IFILE, WAIT_FILELOCK_OFFSET),
    dirlock(this, LOCK_OTYPE_IDIR, WAIT_DIRLOCK_OFFSET)
  {
    state = 0;  
    if (auth) state_set(STATE_AUTH);
  };
  ~CInode() {
    close_dirfrags();
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
  bool is_base() { return inode.ino < MDS_INO_BASE; }

  // note: this overloads MDSCacheObject
  bool is_ambiguous_auth() {
    return state_test(STATE_AMBIGUOUSAUTH) ||
      MDSCacheObject::is_ambiguous_auth();
  }


  inodeno_t ino() const { return inode.ino; }
  inode_t& get_inode() { return inode; }
  CDentry* get_parent_dn() { return parent; }
  CDir *get_parent_dir();
  CInode *get_parent_inode();
  
  bool is_lt(const MDSCacheObject *r) const {
    return ino() < ((CInode*)r)->ino();
  }

  // -- misc -- 
  void make_path_string(string& s);
  void make_path(filepath& s);
  void make_anchor_trace(vector<class Anchor>& trace);
  void name_stray_dentry(string& dname);


  
  // -- dirtyness --
  version_t get_version() { return inode.version; }

  version_t pre_dirty();
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();


  CInodeDiscover* replicate_to(int rep);


  // -- waiting --
  void add_waiter(int tag, Context *c);


  // -- import/export --
  void encode_export(bufferlist& bl);
  void finish_export(utime_t now);
  void abort_export() {
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(bufferlist::iterator& p, LogSegment *ls);
  

  // -- locks --
public:
  LocalLock  versionlock;
  SimpleLock authlock;
  SimpleLock linklock;
  ScatterLock dirfragtreelock;
  FileLock   filelock;
  ScatterLock dirlock;


  SimpleLock* get_lock(int type) {
    switch (type) {
    case LOCK_OTYPE_IFILE: return &filelock;
    case LOCK_OTYPE_IAUTH: return &authlock;
    case LOCK_OTYPE_ILINK: return &linklock;
    case LOCK_OTYPE_IDIRFRAGTREE: return &dirfragtreelock;
    case LOCK_OTYPE_IDIR: return &dirlock;
    default: assert(0); return 0;
    }
  }
  void set_object_info(MDSCacheObjectInfo &info);
  void encode_lock_state(int type, bufferlist& bl);
  void decode_lock_state(int type, bufferlist& bl);

  void clear_dirty_scattered(int type);

  // -- caps -- (new)
  // client caps
  bool is_any_caps() { return !client_caps.empty(); }
  map<int,Capability*>& get_client_caps() { return client_caps; }
  Capability *get_client_cap(int client) {
    if (client_caps.count(client))
      return client_caps[client];
    return 0;
  }
  Capability *add_client_cap(int client, CInode *in, xlist<Capability*>& cls) {
    if (client_caps.empty())
      get(PIN_CAPS);
    assert(client_caps.count(client) == 0);
    Capability *cap = client_caps[client] = new Capability;
    cap->set_inode(in);
    cap->add_to_cap_list(cls);
    return cap;
  }
  void remove_client_cap(int client) {
    assert(client_caps.count(client) == 1);
    delete client_caps[client];
    client_caps.erase(client);
    if (client_caps.empty())
      put(PIN_CAPS);
  }
  void reconnect_cap(int client, inode_caps_reconnect_t& icr, xlist<Capability*>& cls) {
    Capability *cap = get_client_cap(client);
    if (cap) {
      cap->merge(icr.wanted, icr.issued);
    } else {
      cap = add_client_cap(client, this, cls);
      cap->set_wanted(icr.wanted);
      cap->issue(icr.issued);
    }
    inode.size = MAX(inode.size, icr.size);
    inode.mtime = MAX(inode.mtime, icr.mtime);
    inode.atime = MAX(inode.atime, icr.atime);
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

  // caps issued, wanted
  int get_caps_issued() {
    int c = 0;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) 
      c |= it->second->issued();
    return c;
  }
  int get_caps_wanted() {
    int w = 0;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) {
      if (!it->second->is_stale())
	w |= it->second->wanted();
      //cout << " get_caps_wanted client " << it->first << " " << cap_string(it->second.wanted()) << endl;
    }
    if (is_auth())
      for (map<int,int>::iterator it = mds_caps_wanted.begin();
           it != mds_caps_wanted.end();
           it++) {
        w |= it->second;
        //cout << " get_caps_wanted mds " << it->first << " " << cap_string(it->second) << endl;
      }
    return w;
  }
  bool is_loner_cap() {
    if (!mds_caps_wanted.empty())
      return false;

    int n = 0;
    for (map<int,Capability*>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) 
      if (!it->second->is_stale()) {
	if (n) return false;
	n++;
      }
    return (n == 1);
  }

  void replicate_relax_locks() {
    //dout(10) << " relaxing locks on " << *this << dendl;
    assert(is_auth());
    assert(!is_replicated());

    authlock.replicate_relax();
    linklock.replicate_relax();
    dirfragtreelock.replicate_relax();

    if ((get_caps_issued() & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) == 0) 
      filelock.replicate_relax();

    dirlock.replicate_relax();
  }


  // -- authority --
  pair<int,int> authority();


  // -- auth pins --
  int is_auth_pinned() { 
    return auth_pins;
  }
  void adjust_nested_auth_pins(int a);
  bool can_auth_pin();
  void auth_pin();
  void auth_unpin();


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
    generic_dout(7) << " bad put " << *this << " by " << by << " " << pin_name(by) << " was " << ref << " (" << ref_set << ")" << dendl;
#ifdef MDS_REF_SET
    assert(ref_set.count(by) == 1);
#endif
    assert(ref > 0);
  }
  void bad_get(int by) {
    generic_dout(7) << " bad get " << *this << " by " << by << " " << pin_name(by) << " was " << ref << " (" << ref_set << ")" << dendl;
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


  /*
  // for giving to clients
  void get_dist_spec(set<int>& ls, int auth, timepair_t& now) {
    if (( is_dir() && popularity[MDS_POP_CURDOM].get(now) > g_conf.mds_bal_replicate_threshold) ||
        (!is_dir() && popularity[MDS_POP_JUSTME].get(now) > g_conf.mds_bal_replicate_threshold)) {
      //if (!cached_by.empty() && inode.ino > 1) dout(1) << "distributed spec for " << *this << dendl;
      ls = cached_by;
    }
  }
  */

  void print(ostream& out);

};




// -- encoded state

// discover

class CInodeDiscover {
  
  inode_t    inode;
  string     symlink;
  fragtree_t dirfragtree;

  int        replica_nonce;
  
  int        authlock_state;
  int        linklock_state;
  int        dirfragtreelock_state;
  int        filelock_state;
  int        dirlock_state;

 public:
  CInodeDiscover() {}
  CInodeDiscover(CInode *in, int nonce) {
    inode = in->inode;
    symlink = in->symlink;
    dirfragtree = in->dirfragtree;

    replica_nonce = nonce;

    authlock_state = in->authlock.get_replica_state();
    linklock_state = in->linklock.get_replica_state();
    dirfragtreelock_state = in->dirfragtreelock.get_replica_state();
    filelock_state = in->filelock.get_replica_state();
    dirlock_state = in->dirlock.get_replica_state();
  }

  inodeno_t get_ino() { return inode.ino; }
  int get_replica_nonce() { return replica_nonce; }

  void update_inode(CInode *in) {
    in->inode = inode;
    in->symlink = symlink;
    in->dirfragtree = dirfragtree;
    in->replica_nonce = replica_nonce;
  }
  void init_inode_locks(CInode *in) {
    in->authlock.set_state(authlock_state);
    in->linklock.set_state(linklock_state);
    in->dirfragtreelock.set_state(dirfragtreelock_state);
    in->filelock.set_state(filelock_state);
    in->dirlock.set_state(dirlock_state);
  }
  
  void _encode(bufferlist& bl) {
    ::_encode(inode, bl);
    ::_encode(symlink, bl);
    dirfragtree._encode(bl);
    ::_encode(replica_nonce, bl);
    ::_encode(authlock_state, bl);
    ::_encode(linklock_state, bl);
    ::_encode(dirfragtreelock_state, bl);
    ::_encode(filelock_state, bl);
    ::_encode(dirlock_state, bl);
  }

  void _decode(bufferlist& bl, int& off) {
    ::_decode(inode, bl, off);
    ::_decode(symlink, bl, off);
    dirfragtree._decode(bl, off);
    ::_decode(replica_nonce, bl, off);
    ::_decode(authlock_state, bl, off);
    ::_decode(linklock_state, bl, off);
    ::_decode(dirfragtreelock_state, bl, off);
    ::_decode(filelock_state, bl, off);
    ::_decode(dirlock_state, bl, off);
  }  

};



#endif
