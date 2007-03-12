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



#ifndef __CINODE_H
#define __CINODE_H

#include "config.h"
#include "include/types.h"
#include "include/lru.h"

#include "mdstypes.h"

#include "CDentry.h"
#include "Lock.h"
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


ostream& operator<<(ostream& out, CInode& in);


// cached inode wrapper
class CInode : public MDSCacheObject {
 public:
  // -- pins --
  static const int PIN_CACHED =     1;
  static const int PIN_DIR =        2;
  static const int PIN_DIRTY =      4;  // must flush
  static const int PIN_PROXY =      5;  // can't expire yet
  static const int PIN_WAITER =     6;  // waiter
  static const int PIN_CAPS =       7;  // local fh's
  static const int PIN_AUTHPIN =    8;
  static const int PIN_IMPORTING =  -9;  // importing
  static const int PIN_REQUEST =   -10;  // request is logging, finishing
  static const int PIN_RENAMESRC = 11;  // pinned on dest for foreign rename
  static const int PIN_ANCHORING = 12;
  static const int PIN_OPENINGDIR = 13;
  static const int PIN_REMOTEPARENT = 14;
  static const int PIN_DENTRYLOCK = 15;

  const char *pin_name(int p) {
    switch (p) {
    case PIN_CACHED: return "cached";
    case PIN_DIR: return "dir";
    case PIN_DIRTY: return "dirty";
    case PIN_PROXY: return "proxy";
    case PIN_WAITER: return "waiter";
    case PIN_CAPS: return "caps";
    case PIN_AUTHPIN: return "authpin";
    case PIN_IMPORTING: return "importing";
    case PIN_REQUEST: return "request";
    case PIN_RENAMESRC: return "renamesrc";
    case PIN_ANCHORING: return "anchoring";
    case PIN_OPENINGDIR: return "openingdir";
    case PIN_REMOTEPARENT: return "remoteparent";
    case PIN_DENTRYLOCK: return "dentrylock";
    default: assert(0);
    }
  }

  // -- state --
  static const int STATE_AUTH =       (1<<0);
  static const int STATE_ROOT =       (1<<1);
  static const int STATE_DIRTY =      (1<<2);
  static const int STATE_UNSAFE =     (1<<3);   // not logged yet
  static const int STATE_DANGLING =   (1<<4);   // delete me when i expire; i have no dentry
  static const int STATE_UNLINKING =  (1<<5);
  static const int STATE_PROXY =      (1<<6);   // can't expire yet
  static const int STATE_EXPORTING =  (1<<7);   // on nonauth bystander.
  static const int STATE_ANCHORING =  (1<<8);
  static const int STATE_OPENINGDIR = (1<<9);
  //static const int STATE_RENAMING =   (1<<8);  // moving me
  //static const int STATE_RENAMINGTO = (1<<9);  // rename target (will be unlinked)

  // -- waiters --
  static const int WAIT_AUTHPINNABLE  = (1<<10);
    // waiters: write_hard_start, read_file_start, write_file_start  (mdcache)
    //          handle_client_chmod, handle_client_touch             (mds)
    // trigger: (see CDIR_WAIT_UNFREEZE)
  static const int WAIT_DIR          = (1<<13);
    // waiters: traverse_path
    // triggers: handle_disocver_reply
  static const int WAIT_LINK        = (1<<14);  // as in remotely nlink++
  static const int WAIT_ANCHORED    = (1<<15);
  static const int WAIT_UNLINK      = (1<<16);  // as in remotely nlink--
  static const int WAIT_HARDR       = (1<<17);  // 131072
  static const int WAIT_HARDW       = (1<<18);  // 262...
  static const int WAIT_HARDB       = (1<<19);
  static const int WAIT_HARDRWB     = (WAIT_HARDR|WAIT_HARDW|WAIT_HARDB);
  static const int WAIT_HARDSTABLE  = (1<<20);
  static const int WAIT_HARDNORD    = (1<<21);
  static const int WAIT_FILER       = (1<<22);
  static const int WAIT_FILEW       = (1<<23);
  static const int WAIT_FILEB       = (1<<24);
  static const int WAIT_FILERWB     = (WAIT_FILER|WAIT_FILEW|WAIT_FILEB);
  static const int WAIT_FILESTABLE  = (1<<25);
  static const int WAIT_FILENORD    = (1<<26);
  static const int WAIT_FILENOWR    = (1<<27);
  static const int WAIT_RENAMEACK       =(1<<28);
  static const int WAIT_RENAMENOTIFYACK =(1<<29);
  static const int WAIT_CAPS            =(1<<30);
  static const int WAIT_ANY           = 0xffffffff;

  // misc
  static const int EXPORT_NONCE = 1; // nonce given to replicas created by export



 public:
  MDCache *mdcache;

  // inode contents proper
  inode_t          inode;        // the inode itself
  string           symlink;      // symlink dest, if symlink
  fragtree_t       dirfragtree;  // dir frag tree, if any

  frag_t pick_dirfrag(const string &dn);

  // -- cache infrastructure --
  // old way, deprecate me!
  CDir            *dir;       // directory, if we have it opened.
  // new way
  map<frag_t,CDir*> dirfrags; // cached dir fragments

  CDir* get_dirfrag(frag_t fg) {
    if (1) // old
      return dir;
    else { // new
      if (dirfrags.count(fg)) 
	return dirfrags[fg];
      else
	return 0;
    }
  }
  void get_dirfrags(list<CDir*>& ls);
  void get_nested_dirfrags(list<CDir*>& ls);
  void get_subtree_dirfrags(list<CDir*>& ls);
  CDir *get_or_open_dirfrag(MDCache *mdcache, frag_t fg);
  CDir *add_dirfrag(CDir *dir);
  void close_dirfrag(frag_t fg);

 protected:
  // parent dentries in cache
  CDentry         *parent;             // primary link
  set<CDentry*>    remote_parents;     // if hard linked

  // -- other --
  // distributed caching (old)
  pair<int,int> dangling_auth;    // explicit auth, when dangling.

  // waiters
  multimap<int, Context*>  waiting;


  // -- distributed state --
public:
  // inode metadata locks
  CLock        hardlock;
  CLock        filelock;
protected:
  // file capabilities
  map<int, Capability>  client_caps;         // client -> caps
  map<int, int>         mds_caps_wanted;     // [auth] mds -> caps wanted
  int                   replica_caps_wanted; // [replica] what i've requested from auth
  utime_t               replica_caps_wanted_keep_until;


 private:
  // auth pin
  int auth_pins;
  int nested_auth_pins;

 public:
  meta_load_t popularity[MDS_NPOP];

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
  CInode(MDCache *c, bool auth=true);
  ~CInode();
  

  // -- accessors --
  bool is_file()    { return ((inode.mode & INODE_TYPE_MASK) == INODE_MODE_FILE)    ? true:false; }
  bool is_symlink() { return ((inode.mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK) ? true:false; }
  bool is_dir()     { return ((inode.mode & INODE_TYPE_MASK) == INODE_MODE_DIR)     ? true:false; }

  bool is_anchored() { return inode.anchored; }

  bool is_root() { return state & STATE_ROOT; }
  bool is_proxy() { return state & STATE_PROXY; }

  bool is_auth() { return state & STATE_AUTH; }
  void set_auth(bool auth);

  inodeno_t ino() { return inode.ino; }
  inode_t& get_inode() { return inode; }
  CDentry* get_parent_dn() { return parent; }
  CDir *get_parent_dir();
  CInode *get_parent_inode();
  
  CDir *get_or_open_dir(MDCache *mdcache);  // deprecated
  //CDir *set_dir(CDir *newdir);              // deprecated
  //void close_dir();                         // deprecated

  
  bool dir_is_auth();   // FIXME deprecate me
 


  // -- misc -- 
  void make_path(string& s);
  void make_anchor_trace(vector<class Anchor*>& trace);



  // -- state --
  bool is_unsafe() { return state & STATE_UNSAFE; }
  bool is_dangling() { return state & STATE_DANGLING; }
  bool is_unlinking() { return state & STATE_UNLINKING; }

  void mark_unsafe() { state |= STATE_UNSAFE; }
  void mark_safe() { state &= ~STATE_UNSAFE; }

  // -- state encoding --
  //void encode_basic_state(bufferlist& r);
  //void decode_basic_state(bufferlist& r, int& off);


  void encode_file_state(bufferlist& r);
  void decode_file_state(bufferlist& r, int& off);

  void encode_hard_state(bufferlist& r);
  void decode_hard_state(bufferlist& r, int& off);

  
  // -- dirtyness --
  version_t get_version() { return inode.version; }

  bool is_dirty() { return state & STATE_DIRTY; }
  bool is_clean() { return !is_dirty(); }
  
  version_t pre_dirty();
  void _mark_dirty();
  void mark_dirty(version_t projected_dirv);
  void mark_clean();




  CInodeDiscover* replicate_to(int rep);


  // -- waiting --
  bool waiting_for(int tag);
  void add_waiter(int tag, Context *c);
  void take_waiting(int tag, list<Context*>& ls);
  void finish_waiting(int mask, int result = 0);


  bool is_hardlock_write_wanted() {
    return waiting_for(WAIT_HARDW);
  }
  bool is_filelock_write_wanted() {
    return waiting_for(WAIT_FILEW);
  }

  // -- caps -- (new)
  // client caps
  map<int,Capability>& get_client_caps() { return client_caps; }
  void add_client_cap(int client, Capability& cap) {
    if (client_caps.empty())
      get(PIN_CAPS);
    assert(client_caps.count(client) == 0);
    client_caps[client] = cap;
  }
  void remove_client_cap(int client) {
    assert(client_caps.count(client) == 1);
    client_caps.erase(client);
    if (client_caps.empty())
      put(PIN_CAPS);
  }
  Capability* get_client_cap(int client) {
    if (client_caps.count(client))
      return &client_caps[client];
    return 0;
  }
  /*
  void set_client_caps(map<int,Capability>& cl) {
    if (client_caps.empty() && !cl.empty())
      get(PIN_CAPS);
    client_caps.clear();
    client_caps = cl;
  }
  */
  void take_client_caps(map<int,Capability>& cl) {
    if (!client_caps.empty())
      put(PIN_CAPS);
    cl = client_caps;
    client_caps.clear();
  }
  void merge_client_caps(map<int,Capability>& cl, set<int>& new_client_caps) {
    if (client_caps.empty() && !cl.empty())
      get(PIN_CAPS);
    for (map<int,Capability>::iterator it = cl.begin();
         it != cl.end();
         it++) {
      new_client_caps.insert(it->first);
      if (client_caps.count(it->first)) {
        // merge
        client_caps[it->first].merge(it->second);
      } else {
        // new
        client_caps[it->first] = it->second;
      }
    }      
  }

  // caps issued, wanted
  int get_caps_issued() {
    int c = 0;
    for (map<int,Capability>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) 
      c |= it->second.issued();
    return c;
  }
  int get_caps_wanted() {
    int w = 0;
    for (map<int,Capability>::iterator it = client_caps.begin();
         it != client_caps.end();
         it++) {
      w |= it->second.wanted();
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


  void replicate_relax_locks() {
    assert(is_auth());
    assert(!is_replicated());
    dout(10) << " relaxing locks on " << *this << endl;

    if (hardlock.get_state() == LOCK_LOCK &&
        !hardlock.is_used()) {
      dout(10) << " hard now sync " << *this << endl;
      hardlock.set_state(LOCK_SYNC);
    }
    if (filelock.get_state() == LOCK_LOCK) {
      if (!filelock.is_used() &&
          (get_caps_issued() & CAP_FILE_WR) == 0) {
        filelock.set_state(LOCK_SYNC);
        dout(10) << " file now sync " << *this << endl;
      } else {
        dout(10) << " can't relax filelock on " << *this << endl;
      }
    }
  }


  // -- authority --
  pair<int,int> authority();
  bool auth_is_ambiguous();


  // -- auth pins --
  int is_auth_pinned() { 
    return auth_pins;
  }
  void adjust_nested_auth_pins(int a);
  bool can_auth_pin();
  void auth_pin();
  void auth_unpin();


  // -- freeze --
  bool is_frozen();
  bool is_frozen_dir();
  bool is_freezing();


  // -- reference counting --
  
  /* these can be pinned any # of times, and are
     linked to an active_request, so they're automatically cleaned
     up when a request is finished.  pin at will! */
  void request_pin_get() {
    get(PIN_REQUEST);
  }
  void request_pin_put() {
    put(PIN_REQUEST);
  }

  void bad_put(int by) {
    dout(7) << " bad put " << *this << " by " << by << " " << pin_name(by) << " was " << ref << " (" << ref_set << ")" << endl;
    assert(ref_set.count(by) == 1);
    assert(ref > 0);
  }
  void bad_get(int by) {
    dout(7) << " bad get " << *this << " by " << by << " " << pin_name(by) << " was " << ref << " (" << ref_set << ")" << endl;
    assert(ref_set.count(by) == 0);
  }
  void first_get();
  void last_put();


  // -- hierarchy stuff --
private:
  //void get_parent();
  //void put_parent();

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
      //if (!cached_by.empty() && inode.ino > 1) dout(1) << "distributed spec for " << *this << endl;
      ls = cached_by;
    }
  }
  */

  // dbg
  void dump(int d = 0);
};




// -- encoded state

// discover

class CInodeDiscover {
  
  inode_t    inode;
  string     symlink;
  fragtree_t dirfragtree;

  int        replica_nonce;
  
  int        hardlock_state;
  int        filelock_state;

 public:
  CInodeDiscover() {}
  CInodeDiscover(CInode *in, int nonce) {
    inode = in->inode;
    symlink = in->symlink;
    dirfragtree = in->dirfragtree;

    replica_nonce = nonce;

    hardlock_state = in->hardlock.get_replica_state();
    filelock_state = in->filelock.get_replica_state();
  }

  inodeno_t get_ino() { return inode.ino; }
  int get_replica_nonce() { return replica_nonce; }

  void update_inode(CInode *in) {
    in->inode = inode;
    in->symlink = symlink;
    in->dirfragtree = dirfragtree;

    in->replica_nonce = replica_nonce;
    in->hardlock.set_state(hardlock_state);
    in->filelock.set_state(filelock_state);
  }
  
  void _encode(bufferlist& bl) {
    bl.append((char*)&inode, sizeof(inode));
    ::_encode(symlink, bl);
    dirfragtree._encode(bl);
    bl.append((char*)&replica_nonce, sizeof(replica_nonce));
    bl.append((char*)&hardlock_state, sizeof(hardlock_state));
    bl.append((char*)&filelock_state, sizeof(filelock_state));
  }

  void _decode(bufferlist& bl, int& off) {
    bl.copy(off,sizeof(inode), (char*)&inode);
    off += sizeof(inode);
    ::_decode(symlink, bl, off);
    dirfragtree._decode(bl, off);
    bl.copy(off, sizeof(replica_nonce), (char*)&replica_nonce);
    off += sizeof(replica_nonce);
    bl.copy(off, sizeof(hardlock_state), (char*)&hardlock_state);
    off += sizeof(hardlock_state);
    bl.copy(off, sizeof(filelock_state), (char*)&filelock_state);
    off += sizeof(filelock_state);
  }  

};


// export

class CInodeExport {

  struct {
    inode_t        inode;

    meta_load_t    popularity_justme;
    meta_load_t    popularity_curdom;
    bool           is_dirty;       // dirty inode?
    
    int            num_caps;
  } st;

  string         symlink;
  fragtree_t     dirfragtree;

  map<int,int>     replicas;
  map<int,Capability>  cap_map;

  CLock         hardlock,filelock;
  //int           remaining_issued;

public:
  CInodeExport() {}
  CInodeExport(CInode *in) {
    st.inode = in->inode;
    symlink = in->symlink;
    dirfragtree = in->dirfragtree;

    st.is_dirty = in->is_dirty();
    replicas = in->replicas;

    hardlock = in->hardlock;
    filelock = in->filelock;
    
    st.popularity_justme.take( in->popularity[MDS_POP_JUSTME] );
    st.popularity_curdom.take( in->popularity[MDS_POP_CURDOM] );
    in->popularity[MDS_POP_ANYDOM] -= st.popularity_curdom;
    in->popularity[MDS_POP_NESTED] -= st.popularity_curdom;
    
    // steal WRITER caps from inode
    in->take_client_caps(cap_map);
    //remaining_issued = in->get_caps_issued();
  }
  ~CInodeExport() {
  }
  
  inodeno_t get_ino() { return st.inode.ino; }

  void update_inode(CInode *in, set<int>& new_client_caps) {
    in->inode = st.inode;
    in->symlink = symlink;
    in->dirfragtree = dirfragtree;

    in->popularity[MDS_POP_JUSTME] += st.popularity_justme;
    in->popularity[MDS_POP_CURDOM] += st.popularity_curdom;
    in->popularity[MDS_POP_ANYDOM] += st.popularity_curdom;
    in->popularity[MDS_POP_NESTED] += st.popularity_curdom;

    if (st.is_dirty) 
      in->_mark_dirty();

    in->replicas = replicas;
    if (!replicas.empty()) 
      in->get(CInode::PIN_CACHED);

    in->hardlock = hardlock;
    in->filelock = filelock;

    // caps
    in->merge_client_caps(cap_map, new_client_caps);
  }

  void _encode(bufferlist& bl) {
    st.num_caps = cap_map.size();
    bl.append((char*)&st, sizeof(st));
    ::_encode(symlink, bl);
    dirfragtree._encode(bl);

    // cached_by + nonce
    ::_encode(replicas, bl);

    hardlock.encode_state(bl);
    filelock.encode_state(bl);

    // caps
    for (map<int,Capability>::iterator it = cap_map.begin();
         it != cap_map.end();
         it++) {
      bl.append((char*)&it->first, sizeof(it->first));
      it->second._encode(bl);
    }
  }

  int _decode(bufferlist& bl, int off = 0) {
    bl.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
    ::_decode(symlink, bl, off);
    dirfragtree._decode(bl, off);
    
    ::_decode(replicas, bl, off);

    hardlock.decode_state(bl, off);
    filelock.decode_state(bl, off);

    // caps
    for (int i=0; i<st.num_caps; i++) {
      int c;
      bl.copy(off, sizeof(c), (char*)&c);
      off += sizeof(c);
      cap_map[c]._decode(bl, off);
    }

    return off;
  }
};




#endif
