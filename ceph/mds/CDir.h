
#ifndef __CDIR_H
#define __CDIR_H

#include "include/types.h"

#include "include/DecayCounter.h"

#include <iostream>
#include <cassert>

#include <ext/rope>
#include <list>
#include <set>
#include <map>
#include <ext/hash_map>
#include <string>
using namespace std;

class CInode;
class CDentry;
class MDS;
class MDCluster;
class Context;

// state bits
#define CDIR_STATE_COMPLETE      1   // the complete contents are in cache
//#define CDIR_STATE_COMPLETE_LOCK 2   // complete contents are in cache, and locked that way!  (not yet implemented)
#define CDIR_STATE_DIRTY         4   // has been modified since last commit

#define CDIR_STATE_FROZEN        8   // root of a freeze
#define CDIR_STATE_FREEZING     16   // in process of freezing

#define CDIR_STATE_COMMITTING   32   // mid-commit
#define CDIR_STATE_FETCHING     64   // currenting fetching

// these state bits are preserved by an import/export
#define CDIR_MASK_STATE_EXPORTED  (CDIR_STATE_COMPLETE\
                                   |CDIR_STATE_DIRTY)
#define CDIR_MASK_STATE_EXPORT_KEPT 0

// common states
#define CDIR_STATE_CLEAN   0
#define CDIR_STATE_INITIAL 0  

// directory replication
#define CDIR_REP_ALL       1
#define CDIR_REP_NONE      0
#define CDIR_REP_LIST      2

// CDir
typedef map<string, CDentry*> CDir_map_t;

class CDir {
 protected:
  CInode          *inode;

  CDir_map_t       items;              // use map; ordered list
  __uint64_t       nitems;
  size_t           namesize;
  unsigned         state;
  __uint64_t       version;

  // waiters
  list<Context*>   waiting_on_all;  // eg readdir
  hash_map< string, list<Context*> > waiting_on_dentry;

  // cache  (defined for authority; hints for replicas)
  int              dir_rep;
  set<int>         dir_rep_by;      // if dir_rep == CDIR_REP_LIST

  bool             auth;            // true if i'm the auth

  // lock nesting, freeze
  int        hard_pinned;
  int        nested_hard_pinned;
  list<Context*>  waiting_to_freeze;      // wannabe freezer, NOT waiting for *this to thaw

  DecayCounter popularity;

  friend class CInode;
  friend class MDCache;
  friend class MDBalancer;
  friend class MDiscover;

 public:
  CDir(CInode *in, bool auth) {
	inode = in;
	this->auth = auth;
	
	nitems = 0;
	namesize = 0;
	state = CDIR_STATE_INITIAL;
	version = 0;

	hard_pinned = 0;
	nested_hard_pinned = 0;

	dir_rep = CDIR_REP_NONE;
  }


  size_t get_size() { 
#if DEBUG_LEVEL>10
	if (nitems != items.size()) {
	  for (CDir_map_t::iterator it = items.begin();
		   it != items.end();
		   it++)
		cout << "item " << (*it).first << endl;
	  cout << "nitems " << nitems << endl;
	  assert(nitems == items.size());
	}
#endif
	return nitems; 
  }

  // state
  unsigned get_state() { return state; }
  void reset_state(unsigned s) { state = s; }
  void state_clear(unsigned mask) {	state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }
  unsigned state_test(unsigned mask) { return state & mask; }

  bool is_complete() { return state & CDIR_STATE_COMPLETE; }
  bool is_freeze_root() { return state & CDIR_STATE_FROZEN; }
  
  bool is_auth() { return auth; }

  // dirtyness
  // invariant: if clean, my version >= all inode versions
  __uint64_t get_version() {
	return version;
  }
  //void touch_version() { version++; }
  void float_version(__uint64_t ge) {
	if (version < ge)
	  version = ge;
  }
  void mark_dirty() {
	if (!state_test(CDIR_STATE_DIRTY)) {
	  version++;
	  state_set(CDIR_STATE_DIRTY);
	} 
	else if (state_test(CDIR_STATE_COMMITTING)) {
	  version++;  // dirtier than committing version!
	}
  }
  void mark_clean() {
	state_clear(CDIR_STATE_DIRTY);
  }
  bool is_clean() {
	return !state_test(CDIR_STATE_DIRTY);
  }
  
  void hit();

  crope encode_basic_state();
  int decode_basic_state(crope r, int off=0);


  // waiters  
  void add_waiter(Context *c);
  void add_waiter(string& dentry,
				  Context *c);
  void take_waiting(list<Context*>& ls);  // all waiting
  void take_waiting(string& dentry,
					list<Context*>& ls);  

  bool is_frozen();
  bool is_freezing();
  void freeze(Context *c);
  void freeze_finish();
  void unfreeze();
  void add_freeze_waiter(Context *c);

  int is_hard_pinned() { return hard_pinned; }
  int adjust_nested_hard_pinned(int a);
  bool can_hard_pin() { return !is_frozen(); }
  void add_hard_pin_waiter(Context *c);
  void hard_pin();
  void hard_unpin();




  CInode *get_inode() { return inode; }

  // distributed cache
  int dentry_authority(string& d, MDCluster *mdc);


  // for storing..
  size_t serial_size() {
	return nitems * (10+sizeof(inode_t)) + namesize;
  }
  
  CDir_map_t::iterator begin() {
	return items.begin();
  }
  CDir_map_t::iterator end() {
	return items.end();
  }
  
  // manipulation
  void add_child(CDentry *d);
  void remove_child(CDentry *d);
  CDentry* lookup(string& n);

  // debuggin
  void dump(int d = 0);
  void dump_to_disk(MDS *m);
};


#endif
