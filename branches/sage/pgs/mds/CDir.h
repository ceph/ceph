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



#ifndef __CDIR_H
#define __CDIR_H

#include "include/types.h"
#include "include/buffer.h"
#include "mdstypes.h"
#include "config.h"
#include "common/DecayCounter.h"

#include <iostream>
#include <cassert>

#include <list>
#include <set>
#include <map>
#include <string>
using namespace std;

#include <ext/hash_map>
using __gnu_cxx::hash_map;


#include "CInode.h"

class CDentry;
class MDCache;
class MDCluster;
class Context;


// directory authority types
//  >= 0 is the auth mds
#define CDIR_AUTH_PARENT   -1   // default
#define CDIR_AUTH_UNKNOWN  -2


#define CDIR_NONCE_EXPORT   1


// state bits
#define CDIR_STATE_AUTH          (1<<0)   // auth for this dir (hashing doesn't count)
#define CDIR_STATE_PROXY         (1<<1)   // proxy auth

#define CDIR_STATE_COMPLETE      (1<<2)   // the complete contents are in cache
#define CDIR_STATE_DIRTY         (1<<3)   // has been modified since last commit

#define CDIR_STATE_FROZENTREE     (1<<4)   // root of tree (bounded by exports)
#define CDIR_STATE_FREEZINGTREE   (1<<5)   // in process of freezing 
#define CDIR_STATE_FROZENTREELEAF (1<<6)   // outer bound of frozen region (on import)
#define CDIR_STATE_FROZENDIR      (1<<7)
#define CDIR_STATE_FREEZINGDIR    (1<<8)

#define CDIR_STATE_COMMITTING     (1<<9)   // mid-commit
#define CDIR_STATE_FETCHING       (1<<10)   // currenting fetching

#define CDIR_STATE_DELETED        (1<<11)

#define CDIR_STATE_IMPORT           (1<<12)   // flag set if this is an import.
#define CDIR_STATE_EXPORT           (1<<13)
#define CDIR_STATE_IMPORTINGEXPORT  (1<<14)

#define CDIR_STATE_HASHED           (1<<15)   // if hashed
#define CDIR_STATE_HASHING          (1<<16)
#define CDIR_STATE_UNHASHING        (1<<17)





// these state bits are preserved by an import/export
// ...except if the directory is hashed, in which case none of them are!
#define CDIR_MASK_STATE_EXPORTED    (CDIR_STATE_COMPLETE\
                                    |CDIR_STATE_DIRTY)  
#define CDIR_MASK_STATE_IMPORT_KEPT (CDIR_STATE_IMPORT\
                                    |CDIR_STATE_EXPORT\
                                    |CDIR_STATE_IMPORTINGEXPORT\
                                    |CDIR_STATE_FROZENTREE\
                                    |CDIR_STATE_PROXY)

#define CDIR_MASK_STATE_EXPORT_KEPT (CDIR_STATE_HASHED\
                                    |CDIR_STATE_FROZENTREE\
                                    |CDIR_STATE_FROZENDIR\
                                    |CDIR_STATE_EXPORT\
                                    |CDIR_STATE_PROXY)

// common states
#define CDIR_STATE_CLEAN   0
#define CDIR_STATE_INITIAL 0  

// directory replication
#define CDIR_REP_ALL       1
#define CDIR_REP_NONE      0
#define CDIR_REP_LIST      2






// wait reasons
#define CDIR_WAIT_DENTRY         1  // wait for item to be in cache
     // waiters: path_traverse
     // trigger: handle_discover, fetch_dir_2
#define CDIR_WAIT_COMPLETE       2  // wait for complete dir contents
     // waiters: fetch_dir, commit_dir
     // trigger: fetch_dir_2
#define CDIR_WAIT_FREEZEABLE     4  // hard_pins removed
     // waiters: freeze, freeze_finish
     // trigger: auth_unpin, adjust_nested_auth_pins
#define CDIR_WAIT_UNFREEZE       8  // unfreeze
     // waiters: path_traverse, handle_discover, handle_inode_update,
     //           export_dir_frozen                                   (mdcache)
     //          handle_client_readdir                                (mds)
     // trigger: unfreeze
#define CDIR_WAIT_AUTHPINNABLE  CDIR_WAIT_UNFREEZE
    // waiters: commit_dir                                           (mdstore)
    // trigger: (see CDIR_WAIT_UNFREEZE)
#define CDIR_WAIT_COMMITTED     32  // did commit (who uses this?**)
    // waiters: commit_dir (if already committing)
    // trigger: commit_dir_2
#define CDIR_WAIT_IMPORTED      64  // import finish
    // waiters: import_dir_block
    // triggers: handle_export_dir_finish

#define CDIR_WAIT_EXPORTWARNING 8192    // on bystander.
    // watiers: handle_export_dir_notify
    // triggers: handle_export_dir_warning
#define CDIR_WAIT_EXPORTPREPACK 16384
    // waiter   export_dir
    // trigger  handel_export_dir_prep_ack

#define CDIR_WAIT_HASHED        (1<<17)  // hash finish
#define CDIR_WAIT_THISHASHEDREADDIR (1<<18)  // current readdir lock
#define CDIR_WAIT_NEXTHASHEDREADDIR (1<<19)  // after current readdir lock finishes

#define CDIR_WAIT_DNREAD        (1<<20)
#define CDIR_WAIT_DNLOCK        (1<<21)
#define CDIR_WAIT_DNUNPINNED    (1<<22)
#define CDIR_WAIT_DNPINNABLE    (CDIR_WAIT_DNREAD|CDIR_WAIT_DNUNPINNED)

#define CDIR_WAIT_DNREQXLOCK    (1<<23)

#define CDIR_WAIT_ANY   (0xffffffff)

#define CDIR_WAIT_ATFREEZEROOT  (CDIR_WAIT_AUTHPINNABLE|\
                                 CDIR_WAIT_UNFREEZE)      // hmm, same same


ostream& operator<<(ostream& out, class CDir& dir);


// CDir
typedef map<string, CDentry*> CDir_map_t;


//extern int cdir_pins[CDIR_NUM_PINS];


class CDir : public MDSCacheObject {
 public:
  // -- pins --
  static const int PIN_CHILD =    0;
  static const int PIN_OPENED =   1;  // open by another node
  static const int PIN_WAITER =   2;  // waiter(s)
  static const int PIN_IMPORT =   3;
  static const int PIN_EXPORT =   4;
  //static const int PIN_FREEZE =   5;
  static const int PIN_FREEZELEAF = 6;
  static const int PIN_PROXY =    7;  // auth just changed.
  static const int PIN_AUTHPIN =  8;
  static const int PIN_IMPORTING = 9;
  static const int PIN_IMPORTINGEXPORT = 10;
  static const int PIN_HASHED =   11;
  static const int PIN_HASHING =  12;
  static const int PIN_DIRTY =    13;
  static const int PIN_REQUEST =  14;
  static const char *pin_name(int p) {
    switch (p) {
    case PIN_CHILD: return "child";
    case PIN_OPENED: return "opened";
    case PIN_WAITER: return "waiter";
    case PIN_IMPORT: return "import";
    case PIN_EXPORT: return "export";
      //case PIN_FREEZE: return "freeze";
    case PIN_FREEZELEAF: return "freezeleaf";
    case PIN_PROXY: return "proxy";
    case PIN_AUTHPIN: return "authpin";
    case PIN_IMPORTING: return "importing";
    case PIN_IMPORTINGEXPORT: return "importingexport";
    case PIN_HASHED: return "hashed";
    case PIN_HASHING: return "hashing";
    case PIN_DIRTY: return "dirty";
    case PIN_REQUEST: return "request";
    default: assert(0);
    }
  }


 public:
  // context
  MDCache  *cache;

  // my inode
  CInode          *inode;

 protected:
  // contents
  CDir_map_t       items;              // non-null AND null
  CDir_map_t       null_items;        // null and foreign
  size_t           nitems;             // non-null
  size_t           nnull;              // null

  // state
  version_t       version;
  version_t       committing_version;
  version_t       last_committed_version;   // slight lie; we bump this on import.
  version_t       projected_version; 

  // authority, replicas
  int              dir_auth;       

  // lock nesting, freeze
  int        auth_pins;
  int        nested_auth_pins;
  int        request_pins;

  // hashed dirs
  set<int>   hashed_subset;  // HASHING: subset of mds's that are hashed
 public:
  // for class MDS
  map<int, pair< list<class InodeStat*>, list<string> > > hashed_readdir;
 protected:



  // waiters
  multimap<int, Context*> waiting;  // tag -> context
  hash_map< string, multimap<int, Context*> >
                          waiting_on_dentry;

  // cache control  (defined for authority; hints for replicas)
  int              dir_rep;
  set<int>         dir_rep_by;      // if dir_rep == CDIR_REP_LIST

  // popularity
  meta_load_t popularity[MDS_NPOP];

  // friends
  friend class Migrator;
  friend class CInode;
  friend class MDCache;
  friend class MDiscover;
  friend class MDBalancer;

  friend class CDirDiscover;
  friend class CDirExport;

 public:
  CDir(CInode *in, MDCache *mdcache, bool auth);



  // -- accessors --
  inodeno_t ino()        { return inode->ino(); }
  CInode *get_inode()    { return inode; }
  CDir *get_parent_dir() { return inode->get_parent_dir(); }

  CDir_map_t::iterator begin() { return items.begin(); }
  CDir_map_t::iterator end() { return items.end(); }
  size_t get_size() { 
    return nitems; 
  }
  size_t get_nitems() { return nitems; }
  size_t get_nnull() { return nnull; }

  /*
  float get_popularity() {
    return popularity[0].get();
  }
  */
  

  // -- dentries and inodes --
 public:
  CDentry* lookup(const string& n) {
    map<string,CDentry*>::iterator iter = items.find(n);
    if (iter == items.end()) 
      return 0;
    else
      return iter->second;
  }

  CDentry* add_dentry( const string& dname, CInode *in=0, bool auth=true );
  CDentry* add_dentry( const string& dname, inodeno_t ino, bool auth=true );
  void remove_dentry( CDentry *dn );         // delete dentry
  void link_inode( CDentry *dn, inodeno_t ino );
  void link_inode( CDentry *dn, CInode *in );
  void unlink_inode( CDentry *dn );
 private:
  void link_inode_work( CDentry *dn, CInode *in );
  void unlink_inode_work( CDentry *dn );

  void remove_null_dentries();  // on empty, clean dir

  // -- authority --
 public:
  int authority();
  int dentry_authority(const string& d);
  int get_dir_auth() { return dir_auth; }
  void set_dir_auth(int d);

 

  // for giving to clients
  void get_dist_spec(set<int>& ls, int auth) {
    if (( popularity[MDS_POP_CURDOM].pop[META_POP_IRD].get() > g_conf.mds_bal_replicate_threshold)) {
      //if (!cached_by.empty() && inode.ino > 1) dout(1) << "distributed spec for " << *this << endl;
      for (map<int,int>::iterator p = replicas_begin();
	   p != replicas_end(); 
	   ++p)
	ls.insert(p->first);
      if (!ls.empty()) 
	ls.insert(auth);
    }
  }


  // -- state --
  bool is_complete() { return state & CDIR_STATE_COMPLETE; }
  bool is_dirty() { return state_test(CDIR_STATE_DIRTY); }

  bool is_auth() { return state & CDIR_STATE_AUTH; }
  bool is_proxy() { return state & CDIR_STATE_PROXY; }
  bool is_import() { return state & CDIR_STATE_IMPORT; }
  bool is_export() { return state & CDIR_STATE_EXPORT; }

  bool is_hashed() { return state & CDIR_STATE_HASHED; }
  bool is_hashing() { return state & CDIR_STATE_HASHING; }
  bool is_unhashing() { return state & CDIR_STATE_UNHASHING; }

  bool is_rep() { 
    if (dir_rep == CDIR_REP_NONE) return false;
    return true;
  }
 


  // -- dirtyness --
  version_t get_version() { return version; }
  void set_version(version_t v) { projected_version = version = v; }
  version_t get_projected_version() { return projected_version; }
  
  version_t get_committing_version() { return committing_version; }
  version_t get_last_committed_version() { return last_committed_version; }
  // as in, we're committing the current version.
  void set_committing_version() { committing_version = version; }
  void set_last_committed_version(version_t v) { last_committed_version = v; }

  version_t pre_dirty();
  void _mark_dirty();
  void mark_dirty(version_t pv);
  void mark_clean();
  void mark_complete() { state_set(CDIR_STATE_COMPLETE); }
  bool is_clean() { return !state_test(CDIR_STATE_DIRTY); }




  // -- reference counting --
  void first_get();
  void last_put();

  void request_pin_get() {
    if (request_pins == 0) get(PIN_REQUEST);
    request_pins++;
  }
  void request_pin_put() {
    request_pins--;
    if (request_pins == 0) put(PIN_REQUEST);
  }

    
  // -- waiters --
  bool waiting_for(int tag);
  bool waiting_for(int tag, const string& dn);
  void add_waiter(int tag, Context *c);
  void add_waiter(int tag,
                  const string& dentry,
                  Context *c);
  void take_waiting(int mask, list<Context*>& ls);  // includes dentry waiters
  void take_waiting(int mask, 
                    const string& dentry, 
                    list<Context*>& ls,
                    int num=0);  
  void finish_waiting(int mask, int result = 0);    // ditto
  void finish_waiting(int mask, const string& dn, int result = 0);    // ditto


  // -- auth pins --
  bool can_auth_pin() { return !(is_frozen() || is_freezing()); }
  int is_auth_pinned() { return auth_pins; }
  void auth_pin();
  void auth_unpin();
  void adjust_nested_auth_pins(int inc);
  void on_freezeable();

  // -- freezing --
  void freeze_tree(Context *c);
  void freeze_tree_finish(Context *c);
  void unfreeze_tree();

  void freeze_dir(Context *c);
  void freeze_dir_finish(Context *c);
  void unfreeze_dir();

  bool is_freezing() { return is_freezing_tree() || is_freezing_dir(); }
  bool is_freezing_tree();
  bool is_freezing_tree_root() { return state & CDIR_STATE_FREEZINGTREE; }
  bool is_freezing_dir() { return state & CDIR_STATE_FREEZINGDIR; }

  bool is_frozen() { return is_frozen_dir() || is_frozen_tree(); }
  bool is_frozen_tree();
  bool is_frozen_tree_root() { return state & CDIR_STATE_FROZENTREE; }
  bool is_frozen_tree_leaf() { return state & CDIR_STATE_FROZENTREELEAF; }
  bool is_frozen_dir() { return state & CDIR_STATE_FROZENDIR; }
  
  bool is_freezeable() {
    if (auth_pins == 0 && nested_auth_pins == 0) return true;
    return false;
  }
  bool is_freezeable_dir() {
    if (auth_pins == 0) return true;
    return false;
  }



  // debuggin bs
  void dump(int d = 0);
};



// -- encoded state --

// discover

class CDirDiscover {
  inodeno_t ino;
  int       nonce;
  int       dir_auth;
  int       dir_rep;
  set<int>  rep_by;

 public:
  CDirDiscover() {}
  CDirDiscover(CDir *dir, int nonce) {
    ino = dir->ino();
    this->nonce = nonce;
    dir_auth = dir->dir_auth;
    dir_rep = dir->dir_rep;
    rep_by = dir->dir_rep_by;
  }

  void update_dir(CDir *dir) {
    assert(dir->ino() == ino);
    assert(!dir->is_auth());

    dir->replica_nonce = nonce;
    dir->dir_auth = dir_auth;
    dir->dir_rep = dir_rep;
    dir->dir_rep_by = rep_by;
  }

  inodeno_t get_ino() { return ino; }

  
  void _encode(bufferlist& bl) {
    bl.append((char*)&ino, sizeof(ino));
    bl.append((char*)&nonce, sizeof(nonce));
    bl.append((char*)&dir_auth, sizeof(dir_auth));
    bl.append((char*)&dir_rep, sizeof(dir_rep));
    ::_encode(rep_by, bl);
  }

  void _decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    bl.copy(off, sizeof(nonce), (char*)&nonce);
    off += sizeof(nonce);
    bl.copy(off, sizeof(dir_auth), (char*)&dir_auth);
    off += sizeof(dir_auth);
    bl.copy(off, sizeof(dir_rep), (char*)&dir_rep);
    off += sizeof(dir_rep);
    ::_decode(rep_by, bl, off);
  }

};


// export

class CDirExport {
  struct {
    inodeno_t   ino;
    long        nitems; // actual real entries
    long        nden;   // num dentries (including null ones)
    version_t   version;
    unsigned    state;
    meta_load_t popularity_justme;
    meta_load_t popularity_curdom;
    int         dir_rep;
  } st;
  map<int,int> replicas;
  set<int>     rep_by;

 public:
  CDirExport() {}
  CDirExport(CDir *dir) {
    memset(&st, 0, sizeof(st));

    assert(dir->get_version() == dir->get_projected_version());

    st.ino = dir->ino();
    st.nitems = dir->nitems;
    st.nden = dir->items.size();
    st.version = dir->version;
    st.state = dir->state;
    st.dir_rep = dir->dir_rep;

    st.popularity_justme.take( dir->popularity[MDS_POP_JUSTME] );
    st.popularity_curdom.take( dir->popularity[MDS_POP_CURDOM] );
    dir->popularity[MDS_POP_ANYDOM] -= st.popularity_curdom;
    dir->popularity[MDS_POP_NESTED] -= st.popularity_curdom;

    rep_by = dir->dir_rep_by;
    replicas = dir->replicas;
  }

  inodeno_t get_ino() { return st.ino; }
  __uint64_t get_nden() { return st.nden; }

  void update_dir(CDir *dir) {
    assert(dir->ino() == st.ino);

    //dir->nitems = st.nitems;

    // set last_committed_version at old version
    dir->committing_version = dir->last_committed_version = st.version;
    dir->projected_version = dir->version = st.version;    // this is bumped, below, if dirty

    // twiddle state
    if (dir->state & CDIR_STATE_HASHED) 
      dir->state_set( CDIR_STATE_AUTH );         // just inherit auth flag when hashed
    else
      dir->state = (dir->state & CDIR_MASK_STATE_IMPORT_KEPT) |   // remember import flag, etc.
        (st.state & CDIR_MASK_STATE_EXPORTED);
    dir->dir_rep = st.dir_rep;

    dir->popularity[MDS_POP_JUSTME] += st.popularity_justme;
    dir->popularity[MDS_POP_CURDOM] += st.popularity_curdom;
    dir->popularity[MDS_POP_ANYDOM] += st.popularity_curdom;
    dir->popularity[MDS_POP_NESTED] += st.popularity_curdom;

    dir->replica_nonce = 0;  // no longer defined

    if (!dir->replicas.empty())
      dout(0) << "replicas not empty non import, " << *dir << ", " << dir->replicas << endl;

    dir->dir_rep_by = rep_by;
    dir->replicas = replicas;
    dout(12) << "replicas in export is " << replicas << ", dir now " << dir->replicas << endl;
    if (!replicas.empty())
      dir->get(CDir::PIN_OPENED);
    if (dir->is_dirty()) {
      dir->get(CDir::PIN_DIRTY);  

      // bump dir version + 1 if dirty
      dir->projected_version = dir->version = st.version + 1;
    }
  }


  void _encode(bufferlist& bl) {
    bl.append((char*)&st, sizeof(st));
    ::_encode(replicas, bl);
    ::_encode(rep_by, bl);
  }

  int _decode(bufferlist& bl, int off = 0) {
    bl.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
    ::_decode(replicas, bl, off);
    ::_decode(rep_by, bl, off);
    return off;
  }

};



#endif
