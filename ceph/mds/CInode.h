
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
#include <map>
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

#define CINODE_PIN_OPENRD    10020  
#define CINODE_PIN_OPENWR    10021

#define CINODE_PIN_LOCKWAIT  10010   // waiter
#define CINODE_PIN_SYNCWAIT  10011   // "
#define CINODE_PIN_DIRWAIT   10012   // "
#define CINODE_PIN_DIRWAITDN 10013   // "

#define CINODE_PIN_IAUTHPIN  20000
#define CINODE_PIN_DAUTHPIN  30000
#define CINODE_PIN_DIRTY     50000     // must flush

//#define CINODE_PIN_SYNCBYME     70000
//#define CINODE_PIN_SYNCBYTHEM   70001
#define CINODE_PIN_PRESYNC      70002   // waiter
#define CINODE_PIN_WAITONUNSYNC 70003   // waiter

#define CINODE_PIN_PRELOCK      70004
#define CINODE_PIN_WAITONUNLOCK 70005

// directory authority types
//  >= is the auth mds
#define CDIR_AUTH_PARENT   -1   // default
#define CDIR_AUTH_HASH     -2

// sync => coherent soft metadata (size, mtime, etc.)
// lock => coherent hard metadata (owner, mode, etc. affecting namespace)
#define CINODE_DIST_PRESYNC         1   // mtime, size, etc.
#define CINODE_DIST_SYNCBYME        2
#define CINODE_DIST_SYNCBYTHEM      4
#define CINODE_DIST_WAITONUNSYNC    8

#define CINODE_DIST_SOFTASYNC      16  // replica can soft write w/o sync

#define CINODE_DIST_PRELOCK        64   // file mode, owner, etc.
#define CINODE_DIST_LOCKBYME      128 // i am auth
#define CINODE_DIST_LOCKBYAUTH    256 // i am not auth
#define CINODE_DIST_WAITONUNLOCK  512


// wait reasons
#define CINODE_WAIT_SYNC           128
#define CINODE_WAIT_UNSYNC         256
#define CINODE_WAIT_LOCK           512
#define CINODE_WAIT_UNLOCK        1024
#define CINODE_WAIT_AUTHPINNABLE  CDIR_WAIT_UNFREEZE
#define CINODE_WAIT_ANY           0xffff

class Context;
class CDentry;
class CDir;
class MDS;
class MDCluster;
class Message;
class CInode;

class MInodeSyncStart;

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
  set<int>         sync_waiting_for_ack;
  set<int>         lock_waiting_for_ack;
  int              lock_active_count;  // count for in progress or waiting locks

  // waiters
  multimap<int,Context*>  waiting;


  // open file state
  // sets of client ids!
  multiset<int>         open_read;
  multiset<int>         open_write;

  multiset<int>         client_wait_for_sync;
  MInodeSyncStart      *pending_sync_request;

 private:
  // waiters

  // lock nesting
  int auth_pins;
  int nested_auth_pins;

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
  inode_t& get_inode() { return inode; }

  void make_path(string& s);

  void hit();

  bool is_frozen();
  bool is_freezing();

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

  // waiting
  void add_waiter(int tag, Context *c);
  void take_waiting(int tag, list<Context*>& ls);

  // locking
  bool is_sync() { return dist_state & (CINODE_DIST_SYNCBYME|
										CINODE_DIST_SYNCBYTHEM); }
  bool is_syncbyme() { return dist_state & CINODE_DIST_SYNCBYME; }
  bool is_syncbythem() { return dist_state & CINODE_DIST_SYNCBYTHEM; }
  bool is_presync() { return dist_state & CINODE_DIST_PRESYNC; }
  bool is_softasync() { return dist_state & CINODE_DIST_SOFTASYNC; }
  bool is_waitonunsync() { return dist_state & CINODE_DIST_WAITONUNSYNC; }

  bool is_lockbyme() { return dist_state & CINODE_DIST_LOCKBYME; }
  bool is_lockbyauth() { return dist_state & CINODE_DIST_LOCKBYAUTH; }
  bool is_prelock() { return dist_state & CINODE_DIST_PRELOCK; }
  bool is_waitonunlock() { return dist_state & CINODE_DIST_WAITONUNLOCK; }

  // open
  bool is_open() {
	if (open_read.empty() &&
		open_write.empty()) return false;
	return true;
  }
  bool is_open_write() {
	if (open_write.empty()) return false;
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
	open_read.erase(open_read.find(c));
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
	open_write.erase(open_write.find(c));
  }
  bool open_remove(int c) {
	if (open_read.count(c))
	  open_read_remove(c);
	else if (open_write.count(c))
	  open_write_remove(c);
	else return false;
	return true;
  }
  multiset<int>& get_open_write() {
	return open_write;
  }
  multiset<int>& get_open_read() {
	return open_read;
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
  int is_auth_pinned() { 
	return auth_pins;
  }
  int adjust_nested_auth_pins(int a);
  bool can_auth_pin();
  void auth_pin();
  void auth_unpin();


  // --- reference counting
  void put(int by) {
	if (ref == 0 || ref_set.count(by) != 1) {
	  dout(7) << " bad put " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 1);
	  assert(ref > 0);
	}
	ref--;
	ref_set.erase(by);
	if (ref == 0)
	  lru_unpin();
	dout(7) << " put " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
  }
  void get(int by) {
	if (ref == 0)
	  lru_pin();
	if (ref_set.count(by)) {
	  dout(7) << " bad get " << *this << " by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 0);
	}
	ref++;
	ref_set.insert(by);
	dout(7) << " get " << *this << " by " << by << " now " << ref << " (" << ref_set << ")" << endl;
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
