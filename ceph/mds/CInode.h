
#ifndef __CINODE_H
#define __CINODE_H

#include "include/types.h"
#include "include/lru.h"
#include "include/DecayCounter.h"
#include <sys/stat.h>

#include "CDentry.h"

#include <cassert>
#include <list>
#include <vector>
#include <set>
#include <iostream>
using namespace std;


#define CINODE_SYNC_START     1  // starting sync
#define CINODE_SYNC_LOCK      2  // am synced
#define CINODE_SYNC_FINISH    4  // finishing

#define CINODE_MASK_SYNC      (CINODE_SYNC_START|CINODE_SYNC_LOCK|CINODE_SYNC_FINISH)

#define CINODE_MASK_IMPORT    16
#define CINODE_MASK_EXPORT    32


#define CINODE_PIN_CHILD     10000
#define CINODE_PIN_CACHED    10001
#define CINODE_PIN_IMPORT    10002
#define CINODE_PIN_EXPORT    10003
#define CINODE_PIN_FREEZE    10004
#define CINODE_PIN_IMPORTING 10005
#define CINODE_PIN_WWAIT     10010
#define CINODE_PIN_RWAIT     10011
#define CINODE_PIN_DIRWAIT   10012
#define CINODE_PIN_DIRWAITDN 10013
#define CINODE_PIN_IHARDPIN   20000
#define CINODE_PIN_DHARDPIN   30000

#define CDIR_AUTH_PARENT   -1   // default
#define CDIR_AUTH_HASH     -2

class Context;
class CDentry;
class CDir;
class MDS;
class MDCluster;
class Message;



class CInode;
ostream& operator<<(ostream& out, CInode& in);
ostream& operator<<(ostream& out, set<int>& iset);


// cached inode wrapper
class CInode : LRUObject {
 public:
  inode_t          inode;     // the inode itself

  CDir            *dir;       // directory entries, if we're a directory
  int              dir_auth;  // authority for child dir

 protected:
  int              ref;            // reference count (???????)
  set<int>         ref_set;
  __uint32_t       version;


  // parent dentries in cache
  int              nparents;  
  CDentry         *parent;     // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

  // dcache lru
  CInode *lru_next, *lru_prev;

  // used by MDStore
  bool             mid_fetch;

  // distributed caching
  set<int>         cached_by;  // mds's that cache me.  not well defined on replicas.
  //unsigned         state;
  //set<int>         sync_waiting_for_ack;

 private:
  // waiters
  list<Context*>   waiting_for_write;
  list<Context*>   waiting_for_read;

  // lock nesting
  int hard_pinned;
  int nested_hard_pinned;

 public:
  DecayCounter popularity;
  

  friend class MDCache;
  friend class CDir;
  friend class MDStore;
  friend class MDS;
  friend class MDiscover;

 public:
  CInode();
  ~CInode();

  
  CInode *get_parent_inode();
  CInode *get_realm_root();   // import, hash, or root
  
  // fun
  bool is_dir() { return inode.isdir; }
  void make_path(string& s);
  bool is_root() { return (bool)(!parent); }
  
  void hit();

  inodeno_t ino() { return inode.ino; }

  // state
  /*
  unsigned get_state() { return state; }
  void reset_state(unsigned s) { state = s; }
  void state_clear(unsigned mask) {	state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }

  bool is_export() { return state & CINODE_MASK_EXPORT; }
  bool is_import() { return state & CINODE_MASK_IMPORT; }
  */

  // sync
  /*
	int get_sync() { return state & CINODE_MASK_SYNC; }
  int sync_set(int m) { 
	state = (state & ~CINODE_MASK_SYNC) | m;
  }
  */

  __uint32_t get_version() { return version; }
  
  // dist cache
  int authority(MDCluster *mdc);
  int dir_authority(MDCluster *mdc);


  int is_hard_pinned() { 
	return hard_pinned;
  }
  int adjust_nested_hard_pinned(int a);
  bool can_hard_pin();
  void hard_pin();
  void hard_unpin();
  void add_hard_pin_waiter(Context *c);


  void add_write_waiter(Context *c);
  void take_write_waiting(list<Context*>& ls);
  void add_read_waiter(Context *c);
  void take_read_waiting(list<Context*>& ls);

  // --- reference counting
  void put(int by) {
	assert(ref > 0);
	if (ref_set.count(by) != 1) {
	  cout << "bad put " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 1);
	}
	ref--;
	ref_set.erase(by);
	if (ref == 0)
	  lru_unpin();
	cout << "put " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }
  void get(int by) {
	if (ref == 0)
	  lru_pin();
	assert(ref_set.count(by) == 0);
	ref++;
	ref_set.insert(by);
	cout << "get " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }

  // --- hierarchy stuff
  void add_parent(CDentry *p);

  mdloc_t get_mdloc() {
	return inode.ino;       // use inode #
  }

  void get_dist_spec(set<int>& ls, int auth) {
	ls = cached_by;
	ls.insert(ls.begin(), auth);
  }


  // dbg
  void dump(int d = 0);
  void dump_to_disk(MDS *m);
};



#endif
