
#ifndef __CDIR_H
#define __CDIR_H

#include "include/types.h"
#include <map>
#include <ext/hash_map>
#include <string>

#include <iostream>

#include <list>
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

#define CDIR_MASK_FROZEN       16   // all ops suspended (for import/export)?

// common states
#define CDIR_STATE_CLEAN   0
#define CDIR_STATE_INITIAL 0   // ?

// distributions
#define CDIR_DIST_PARENT   -1   // default
#define CDIR_DIST_HASH     -2

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

  // cache
  int              dir_dist;
  int              dir_rep;
  vector<int>      dir_rep_vec;  // if dir_rep == CDIR_REP_LIST
  bool             is_import, is_export;

  friend class MDS;
  friend class MDBalancer;
  friend class CInode;

 public:
  CDir(CInode *in) {
	inode = in;

	nitems = 0;
	namesize = 0;
	state = CDIR_STATE_INITIAL;
	version = 0;

	dir_dist = CDIR_DIST_PARENT;
	dir_rep = CDIR_REP_NONE;
  }


  // state
  unsigned get_state() { return state; }
  void reset_state(unsigned s) { state = s; }
  void state_clear(unsigned mask) {	state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }

  bool is_complete() { return state & CDIR_MASK_COMPLETE; }
  bool is_frozen() { return state & CDIR_MASK_FROZEN; }


  // waiters  
  void add_waiter(Context *c);
  void add_waiter(string& dentry,
				  Context *c);
  void take_waiting(list<Context*>& ls);  // all waiting
  void take_waiting(string& dentry,
					list<Context*>& ls);  

  void freeze();
  void unfreeze();

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
  int dir_authority(MDCluster *mdc);


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
