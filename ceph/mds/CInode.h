
#ifndef __CINODE_H
#define __CINODE_H

#include "include/config.h"
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


// crap
#define CINODE_SYNC_START     1  // starting sync
#define CINODE_SYNC_LOCK      2  // am synced
#define CINODE_SYNC_FINISH    4  // finishing

#define CINODE_MASK_SYNC      (CINODE_SYNC_START|CINODE_SYNC_LOCK|CINODE_SYNC_FINISH)

#define CINODE_MASK_IMPORT    16
#define CINODE_MASK_EXPORT    32

// pins for state, keeping an item in cache
#define CINODE_PIN_CHILD     10000
#define CINODE_PIN_CACHED    10001
#define CINODE_PIN_IMPORT    10002
#define CINODE_PIN_EXPORT    10003
#define CINODE_PIN_FREEZE    10004

#define CINODE_PIN_OPENRD    10005
#define CINODE_PIN_OPENWR    10006

#define CINODE_PIN_WWAIT     10010
#define CINODE_PIN_RWAIT     10011
#define CINODE_PIN_DIRWAIT   10012
#define CINODE_PIN_DIRWAITDN 10013

#define CINODE_PIN_IHARDPIN  20000
#define CINODE_PIN_DHARDPIN  30000
#define CINODE_PIN_DIRTY     50000

#define CINODE_PIN_LOCKING   70000

// directory authority types
//  >= is the auth mds
#define CDIR_AUTH_PARENT   -1   // default
#define CDIR_AUTH_HASH     -2

// sync => coherent soft metadata (size, mtime, etc.)
// lock => coherent hard metadata (owner, mode, etc. affecting namespace)
#define CINODE_DIST_PRESYNC         1   // mtime, size, etc.
#define CINODE_DIST_SYNCBYME        2
#define CINODE_DIST_SYNCBYTHEM      4

#define CINODE_DIST_PRELOCK         8   // file mode, owner, etc.
#define CINODE_DIST_LOCKBYME       16
#define CINODE_DIST_LOCKBYTHEM     32
#define CINODE_DIST_SOFTASYNC      64  // replica can soft write w/o sync

/*
soft:
 auth, normal:    readable,       sync->writeable
 repl, normal:    readable,       fw write to auth
 auth, softtoken: sync->readable, writeable
 repl, softtoken: sync->readable, writeable

hard:
 
*/

class Context;
class CDentry;
class CDir;
class MDS;
class MDCluster;
class Message;
class CInode;

ostream& operator<<(ostream& out, CInode& in);


// cached inode wrapper
class CInode : LRUObject {
 public:
  inode_t          inode;     // the inode itself

  CDir            *dir;       // directory entries, if we're a directory
  int              dir_auth;  // authority for child dir

 protected:
  int              ref;       // reference count
  set<int>         ref_set;
  __uint64_t       version;
  __uint64_t       parent_dir_version;  // dir version when last touched.

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
  set<int>         soft_tokens;  // replicas who can to soft update the inode
  /* ..and thus may have a newer mtime, size, etc.! .. w/o sync
	 for authority: set of nodes; self is assumed, but not included
	 for replica:   undefined */
  unsigned         dist_state;


  // open file state
  // sets of client ids!
  set<int>         open_read;
  set<int>         open_write;

 private:
  // waiters
  list<Context*>   waiting_for_sync;
  list<Context*>   waiting_for_lock;

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

  bool is_frozen();


  // dirtyness
  __uint64_t get_version() { return version; }
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

  __uint64_t get_parent_dir_version() { return parent_dir_version; }
  void float_parent_dir_version(__uint64_t ge) {
	if (parent_dir_version < ge)
	  parent_dir_version = ge;
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

  // locking
  bool is_sync() { return dist_state & (CINODE_DIST_SYNCBYME|
										CINODE_DIST_SYNCBYTHEM); }
  bool is_syncbyme() { return dist_state & CINODE_DIST_SYNCBYME; }
  bool is_syncbythem() { return dist_state & CINODE_DIST_SYNCBYTHEM; }
  bool is_presync() { return dist_state & CINODE_DIST_PRESYNC; }
  bool is_softasync() { return dist_state & CINODE_DIST_SOFTASYNC; }

  bool is_lock() { return dist_state & CINODE_DIST_LOCKED; }
  bool is_prelock() { return dist_state & CINODE_DIST_PRELOCK; }

  // open
  bool is_open() {
	if (open_read.empty() &&
		open_write.empty()) return false;
	return true;
  }
  void open_read_add(int c) {
	if (open_read.empty())
	  get(CINODE_PIN_OPENRD);
	open_read.insert(c);
  }
  void open_read_remove(int c) {
	if (open_read.count(c) == 1 &&
		open_read.size() == 1) 
	  put(CINODE_PIN_OPENRD);
	open_read.erase(c);
  }
  void open_write_add(int c) {
	if (open_write.empty())
	  get(CINODE_PIN_OPENWR);
	open_write.insert(c);
  }
  void open_write_remove(int c) {
	if (open_write.count(c) == 1 &&
		open_write.size() == 1) 
	  put(CINODE_PIN_OPENWR);
	open_write.erase(c);
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

  // locking
  int is_hard_pinned() { 
	return hard_pinned;
  }
  int adjust_nested_hard_pinned(int a);
  bool can_hard_pin();
  void hard_pin();
  void hard_unpin();
  void add_hard_pin_waiter(Context *c);


  void add_sync_waiter(Context *c);
  void take_sync_waiting(list<Context*>& ls);
  void add_lock_waiter(Context *c);
  void take_lock_waiting(list<Context*>& ls);

  // --- reference counting
  void put(int by) {
	if (ref == 0 || ref_set.count(by) != 1) {
	  if (DEBUG_LEVEL > 7)
		cout << " bad put " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 1);
	  assert(ref > 0);
	}
	ref--;
	ref_set.erase(by);
	if (ref == 0)
	  lru_unpin();
	if (DEBUG_LEVEL > 7)
	  cout << " put " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }
  void get(int by) {
	if (ref == 0)
	  lru_pin();
	if (ref_set.count(by)) {
	  if (DEBUG_LEVEL > 7)
		cout << " bad get " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 0);
	}
	ref++;
	ref_set.insert(by);
	if (DEBUG_LEVEL > 7)
	  cout << " get " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }
  bool is_pinned_by(int by) {
	return ref_set.count(by);
  }

  // --- hierarchy stuff
  void add_parent(CDentry *p);
  void remove_parent(CDentry *p);


  // for giving to clients
  void get_dist_spec(set<int>& ls, int auth) {
	ls = cached_by;
	ls.insert(ls.begin(), auth);
  }


  // dbg
  void dump(int d = 0);
  void dump_to_disk(MDS *m);
};



#endif
