
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
#include <ext/rope>
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

#define CINODE_PIN_WWAIT     10010
#define CINODE_PIN_RWAIT     10011
#define CINODE_PIN_DIRWAIT   10012
#define CINODE_PIN_DIRWAITDN 10013

#define CINODE_PIN_IHARDPIN   20000
#define CINODE_PIN_DHARDPIN   30000
#define CINODE_PIN_DIRTY     50000

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
  int              ref;            // reference count
  set<int>         ref_set;
  __uint64_t       version;

  // parent dentries in cache
  int              nparents;  
  CDentry         *parent;     // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

  // dcache lru
  CInode *lru_next, *lru_prev;

  // distributed caching
  bool             auth;       // safety check; true if this is authoritative.
  set<int>         cached_by;  // mds's that cache me.  
  /* NOTE: on replicas, this doubles as replicated_by, but the
	 cached_by_* access methods below should NOT be used in those
	 cases, as the semantics are different! */

  // 

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

 public:
  CInode();
  ~CInode();
  
  CInode *get_parent_inode();
  CInode *get_realm_root();   // import, hash, or root
  
  // fun
  bool is_dir() { return inode.isdir; }
  bool is_root() { return (bool)(!parent); }
  bool is_auth() { return auth; }
  inodeno_t ino() { return inode.ino; }

  void make_path(string& s);

  void hit();


  // dirtyness
  __uint64_t get_version() { return version; }
  void float_version(__uint64_t ge) {
	if (version < ge)
	  version = ge;
  }
  //void touch_version();  // mark dirty instead.
  void mark_dirty();
  void mark_clean() {
	if (ref_set.count(CINODE_PIN_DIRTY)) 
	  put(CINODE_PIN_DIRTY);
  }	
  bool is_dirty() {
	return ref_set.count(CINODE_PIN_DIRTY);
  }
  bool is_clean() {
	return !ref_set.count(CINODE_PIN_DIRTY);
  }


  // state
  crope encode_basic_state();
  int decode_basic_state(crope r, int off=0);

  // cached_by  -- to be used ONLY when we're authoritative!
  bool is_cached_by_anyone() {
	return !cached_by.empty();
  }
  bool is_cached_by(int mds) {
	return cached_by.count(mds);
  }
  void cached_by_add(int mds) {
	if (is_cached_by(mds)) return;
	if (cached_by.empty()) 
	  get(CINODE_PIN_CACHED);
	cached_by.insert(mds);
  }
  void cached_by_remove(int mds) {
	if (!is_cached_by(mds)) return;
	cached_by.erase(mds);
	if (cached_by.empty())
	  put(CINODE_PIN_CACHED);	  
  }
  void cached_by_clear() {
	if (cached_by.size())
	  put(CINODE_PIN_CACHED);
	cached_by.clear();
  }
  set<int>::iterator cached_by_begin() {
	return cached_by.begin();
  }
  set<int>::iterator cached_by_end() {
	return cached_by.end();
  }
  set<int>& get_cached_by() {
	return cached_by;
  }

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
	if (ref == 0 || ref_set.count(by) != 1) {
	  cout << " bad put " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 1);
	  assert(ref > 0);
	}
	ref--;
	ref_set.erase(by);
	if (ref == 0)
	  lru_unpin();
	cout << " put " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }
  void get(int by) {
	if (ref == 0)
	  lru_pin();
	if (ref_set.count(by)) {
	  cout << " bad get " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 0);
	}
	ref++;
	ref_set.insert(by);
	cout << " get " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }
  bool is_pinned_by(int by) {
	return ref_set.count(by);
  }

  // --- hierarchy stuff
  void add_parent(CDentry *p);
  void remove_parent(CDentry *p);

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
