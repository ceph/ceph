
#ifndef __CDIR_H
#define __CDIR_H

#include "include/types.h"

#include "include/DecayCounter.h"

#include <map>
#include <ext/hash_map>
#include <string>

#include <iostream>
#include <cassert>

#include <list>
#include <set>
using namespace std;

class CInode;
class CDentry;
class MDS;
class MDCluster;
class Context;

// state bits
#define CDIR_MASK_COMPLETE      1   // the complete contents are in cache
#define CDIR_MASK_COMPLETE_LOCK 2   // complete contents are in cache, and locked that way!  (not yet implemented)
#define CDIR_MASK_DIRTY         4   // has been modified since last commit
#define CDIR_MASK_MID_COMMIT    8   // mid-commit

#define CDIR_MASK_FROZEN       16   // root of a freeze
#define CDIR_MASK_FREEZING     32   // in process of freezing

// common states
#define CDIR_STATE_CLEAN   0
#define CDIR_STATE_INITIAL 0   // ?

// distributions

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
  CDir(CInode *in) {
	inode = in;

	nitems = 0;
	namesize = 0;
	state = CDIR_STATE_INITIAL;
	version = 0;

	hard_pinned = 0;
	nested_hard_pinned = 0;

	dir_rep = CDIR_REP_NONE;
  }


  size_t get_size() { 
	assert(nitems == items.size());
	return nitems; 
  }

  // state
  unsigned get_state() { return state; }
  void reset_state(unsigned s) { state = s; }
  void state_clear(unsigned mask) {	state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }

  bool is_complete() { return state & CDIR_MASK_COMPLETE; }
  bool is_freeze_root() { return state & CDIR_MASK_FROZEN; }
  
  
  void hit();



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



  // version
  __uint64_t get_version() {
	return version;
  }
  void touch_version() {
	version++;
  }

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
  CDentry* lookup(string n);

  // debuggin
  void dump(int d = 0);
  void dump_to_disk(MDS *m);
};


#endif
