
#ifndef __CDIR_H
#define __CDIR_H

#include "types.h"
#include <map>
#include <ext/hash_map>
#include <string>

#include <iostream>


using namespace std;

class CInode;
class CDentry;
class MDS;


// state bits
#define CDIR_MASK_COMPLETE      1   // the complete contents are in cache
#define CDIR_MASK_COMPLETE_LOCK 2   // complete contents are in cache, and locked that way!  (not yet implemented)
#define CDIR_MASK_DIRTY         4   // has been modified since last fetch/commit
#define CDIR_MASK_MID_COMMIT    8   // mid-commit

// common states
#define CDIR_STATE_CLEAN   0
#define CDIR_STATE_INITIAL 0   // ?

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

 public:
  CDir(CInode *in) {
	inode = in;
	nitems = 0;
	state = CDIR_STATE_INITIAL;
	version = 0;
  }

  // state
  unsigned get_state() {
	return state;
  }
  void set_state(unsigned s) {
	state = s;
  }
  void state_clear(unsigned mask) {
	state &= ~mask;
  }
  void state_set(unsigned mask) {
	state |= mask;
  }

  // version
  __uint64_t get_version() {
	return version;
  }
  void touch_version() {
	version++;
  }

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
