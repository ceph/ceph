
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
/*
#define CINODE_SYNC_START     1  // starting sync
#define CINODE_SYNC_LOCK      2  // am synced
#define CINODE_SYNC_FINISH    4  // finishing

#define CINODE_MASK_SYNC      (CINODE_SYNC_START|CINODE_SYNC_LOCK|CINODE_SYNC_FINISH)

#define CINODE_MASK_IMPORT    16
#define CINODE_MASK_EXPORT    32
*/

// pins for keeping an item in cache (and debugging)
#define CINODE_PIN_DIR       0
#define CINODE_PIN_CACHED    1
#define CINODE_PIN_DIRTY     2   // must flush
#define CINODE_PIN_PROXY     3   // can't expire yet
#define CINODE_PIN_WAITER    4   // waiter

#define CINODE_PIN_OPENRD    5
#define CINODE_PIN_OPENWR    6
#define CINODE_PIN_UNLINKING 7

#define CINODE_PIN_AUTHPIN   8

#define CINODE_NUM_PINS       9
static char *cinode_pin_names[CINODE_NUM_PINS] = {
  "dir",
  "cached",
  "dirty",
  "proxy",
  "waiter",
  "openrd",
  "openwr",
  "unlinking",
  "authpin"
};



//#define CINODE_PIN_SYNCBYME     70000
//#define CINODE_PIN_SYNCBYAUTH   70001
#define CINODE_PIN_PRESYNC      70002   // waiter
#define CINODE_PIN_WAITONUNSYNC 70003   // waiter

#define CINODE_PIN_PRELOCK      70004
#define CINODE_PIN_WAITONUNLOCK 70005


// sync => coherent soft metadata (size, mtime, etc.)
// lock => coherent hard metadata (owner, mode, etc. affecting namespace)
#define CINODE_DIST_PRESYNC         1   // mtime, size, etc.
#define CINODE_DIST_SYNCBYME        2
#define CINODE_DIST_SYNCBYAUTH      4
#define CINODE_DIST_WAITONUNSYNC    8

#define CINODE_DIST_SOFTASYNC      16  // replica can soft write w/o sync

#define CINODE_DIST_PRELOCK        64   // file mode, owner, etc.
#define CINODE_DIST_LOCKBYME      128 // i am auth
#define CINODE_DIST_LOCKBYAUTH    256 // i am not auth
#define CINODE_DIST_WAITONUNLOCK  512


// wait reasons
#define CINODE_WAIT_SYNC           128
    // waiters: read_soft_start, write_soft_start
    // trigger: handle_inode_sync_ack
#define CINODE_WAIT_UNSYNC         256
    // waiters: read_soft_start, write_soft_start
    // trigger: handle_inode_sync_release
#define CINODE_WAIT_LOCK           512
    // waiters: write_hard_start
    // trigger: handle_inode_lock_ack
    // SPECIALNESS: lock_active_count indicates waiter, active lock count.
#define CINODE_WAIT_UNLOCK        1024
    // waiters: read_hard_try
    // trigger: handle_inode_lock_release
#define CINODE_WAIT_AUTHPINNABLE  CDIR_WAIT_UNFREEZE
    // waiters: write_hard_start, read_soft_start, write_soft_start  (mdcache)
    //          handle_client_chmod, handle_client_touch             (mds)
    // trigger: (see CDIR_WAIT_UNFREEZE)
#define CINODE_WAIT_GETREPLICA    2048  // update/replicate individual inode
    // waiters: import_dentry_inode
    // trigger: handle_inode_replicate_ack
#define CINODE_WAIT_UNLINK        4096
    // waiters: inode_unlink 
    // triggers: inode_unlink_finish

#define CINODE_WAIT_ANY           0xffff


// state
#define CINODE_STATE_ROOT        1
#define CINODE_STATE_DIRTY       2
#define CINODE_STATE_UNSAFE      4   // not logged yet
#define CINODE_STATE_DANGLING    8   // delete me when i expire; i have no dentry
#define CINODE_STATE_UNLINKING  16
#define CINODE_STATE_PROXY      32   // can't expire yet
#define CINODE_STATE_EXPORTING  64   // on nonauth bystander.

// misc
#define CINODE_EXPORT_NONCE      1 // nonce given to replicas created by export
#define CINODE_HASHREPLICA_NONCE 1 // hashed inodes that are duped ???FIXME???

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

  CDir            *dir;       // directory, if we have it opened.

 protected:
  int              ref;       // reference count
  set<int>         ref_set;
  __uint64_t       version;
  __uint64_t       parent_dir_version;  // dir version when last touched.

  unsigned         state;

  // parent dentries in cache
  int              nparents;  
  CDentry         *parent;     // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

  // dcache lru
  CInode *lru_next, *lru_prev;

  // -- distributed caching
  bool             auth;       // safety check; true if this is authoritative.
  set<int>         cached_by;  // mds's that cache me.  
  /* NOTE: on replicas, this doubles as replicated_by, but the
	 cached_by_* access methods below should NOT be used in those
	 cases, as the semantics are different! */
  /* NOTE: if replica is_cacheproxy(), cached_by is still defined! */
  map<int,int>     cached_by_nonce;  // nonce issued to each replica
  int              replica_nonce;    // defined on replica
  set<int>         soft_tokens;  // replicas who can soft update the inode
  /* ..and thus may have a newer mtime, size, etc.! .. w/o sync
	 for authority: set of nodes; self is assumed, but not included
	 for replica:   undefined */
  unsigned         dist_state;
  set<int>         sync_waiting_for_ack;
  set<int>         lock_waiting_for_ack;
  int              lock_active_count;  // count for in progress or waiting locks
  bool             sync_replicawantback;  // avoids sticky sync

  set<int>         unlink_waiting_for_ack;

  int              dangling_auth;         // explicit auth when dangling.

  // waiters
  multimap<int,Context*>  waiting;


  // open file state
  // sets of client ids!
  multiset<int>         open_read;
  multiset<int>         open_write;

  MInodeSyncStart      *pending_sync_request;

 private:
  // waiters

  // lock nesting
  int auth_pins;
  int nested_auth_pins;

  DecayCounter popularity[MDS_NPOP];
  
  friend class MDCache;
  friend class CDir;

 public:
  CInode();
  ~CInode();
  

  // -- accessors --
  bool is_dir() { return inode.isdir; }
  bool is_root() { return state & CINODE_STATE_ROOT; }
  bool is_proxy() { return state & CINODE_STATE_PROXY; }

  bool is_auth() { return auth; }
  void set_auth(bool auth);
  bool is_replica() { return !auth; }
  int get_replica_nonce() { assert(!is_auth()); return replica_nonce; }

  inodeno_t ino() { return inode.ino; }
  inode_t& get_inode() { return inode; }
  CDir *get_parent_dir();
  CInode *get_parent_inode();
  CInode *get_realm_root();   // import, hash, or root
  
  CDir *get_or_open_dir();
  
  bool dir_is_hashed() { 
	if (inode.isdir == INODE_DIR_HASHED) return true;
	return false; 
  }
  bool dir_is_auth() {
	if (dir)
	  return dir->is_auth();
	else
	  return is_auth();
  }

  float get_popularity() {
	return popularity[0].get();
  }


  // -- misc -- 
  void make_path(string& s);
  void hit(int type);                // popularity


  // -- state --
  unsigned get_state() { return state; }
  void state_clear(unsigned mask) {	state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }
  unsigned state_test(unsigned mask) { return state & mask; }

  bool is_unsafe() { return state & CINODE_STATE_UNSAFE; }
  bool is_dangling() { return state & CINODE_STATE_DANGLING; }
  bool is_unlinking() { return state & CINODE_STATE_UNLINKING; }

  void mark_unsafe() { state |= CINODE_STATE_UNSAFE; }
  void mark_safe() { state &= ~CINODE_STATE_UNSAFE; }

  // -- state encoding --
  crope encode_basic_state();
  int decode_basic_state(crope r, int off=0);

  crope encode_export_state();


  
  // -- dirtyness --
  __uint64_t get_version() { return version; }
  __uint64_t get_parent_dir_version() { return parent_dir_version; }
  void float_parent_dir_version(__uint64_t ge) {
	if (parent_dir_version < ge)
	  parent_dir_version = ge;
  }
  
  bool is_dirty() { return state & CINODE_STATE_DIRTY; }
  bool is_clean() { return !is_dirty(); }
  
  void mark_dirty();
  void mark_clean() {
	dout(10) << " mark_clean " << *this << endl;
	if (state & CINODE_STATE_DIRTY) {
	  state &= ~CINODE_STATE_DIRTY;
	  put(CINODE_PIN_DIRTY);
	}
  }	



  // -- cached_by -- to be used ONLY when we're authoritative or cacheproxy
  bool is_cached_by_anyone() { return !cached_by.empty(); }
  bool is_cached_by(int mds) { return cached_by.count(mds); }
  int num_cached_by() { return cached_by.size(); }
  // cached_by_add returns a nonce
  int cached_by_add(int mds) {
	if (is_cached_by(mds)) {    // already had it?
      // new nonce (+1)
      map<int,int>::iterator it = cached_by_nonce.find(mds);
      cached_by_nonce.insert(pair<int,int>(mds,it->second + 1));
      return it->second + 1;
    }
	if (cached_by.empty()) 
	  get(CINODE_PIN_CACHED);
	cached_by.insert(mds);
    cached_by_nonce.insert(pair<int,int>(mds,1));   // first! serial of 1.
    return 1;   // default nonce
  }
  void cached_by_add(int mds, int nonce) {
	if (cached_by.empty()) 
	  get(CINODE_PIN_CACHED);
    cached_by.insert(mds);
    cached_by_nonce.insert(pair<int,int>(mds,nonce));
  }
  int get_cached_by_nonce(int mds) {
    map<int,int>::iterator it = cached_by_nonce.find(mds);
    return it->second;
  }
  void cached_by_remove(int mds) {
	if (!is_cached_by(mds)) return;
	cached_by.erase(mds);
	cached_by_nonce.erase(mds);
	if (cached_by.empty())
	  put(CINODE_PIN_CACHED);	  
  }
  void cached_by_clear() {
	if (cached_by.size())
	  put(CINODE_PIN_CACHED);
	cached_by.clear();
    cached_by_nonce.clear();
  }
  set<int>::iterator cached_by_begin() { return cached_by.begin(); }
  set<int>::iterator cached_by_end() { return cached_by.end(); }
  set<int>& get_cached_by() { return cached_by; }


  // -- waiting --
  void add_waiter(int tag, Context *c);
  void take_waiting(int tag, list<Context*>& ls);
  void finish_waiting(int mask, int result = 0);


  // -- sync, lock --
  bool is_sync() { return dist_state & (CINODE_DIST_SYNCBYME|
										CINODE_DIST_SYNCBYAUTH); }
  bool is_syncbyme() { return dist_state & CINODE_DIST_SYNCBYME; }
  bool is_syncbyauth() { return dist_state & CINODE_DIST_SYNCBYAUTH; }
  bool is_presync() { return dist_state & CINODE_DIST_PRESYNC; }
  bool is_softasync() { return dist_state & CINODE_DIST_SOFTASYNC; }
  bool is_waitonunsync() { return dist_state & CINODE_DIST_WAITONUNSYNC; }

  bool is_lockbyme() { return dist_state & CINODE_DIST_LOCKBYME; }
  bool is_lockbyauth() { return dist_state & CINODE_DIST_LOCKBYAUTH; }
  bool is_prelock() { return dist_state & CINODE_DIST_PRELOCK; }
  bool is_waitonunlock() { return dist_state & CINODE_DIST_WAITONUNLOCK; }

  // -- open files --
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


  // -- authority --
  int authority();
  //int dir_authority(MDCluster *mdc); 


  // -- auth pins --
  int is_auth_pinned() { 
	return auth_pins;
  }
  int adjust_nested_auth_pins(int a);
  bool can_auth_pin();
  void auth_pin();
  void auth_unpin();


  // -- freeze --
  bool is_frozen();
  bool is_freezing();


  // -- reference counting --
  bool is_pinned() { return ref > 0; }
  set<int>& get_ref_set() { return ref_set; }
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

  // -- hierarchy stuff --
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





// -- encoded state

// discover

class CInodeDiscover {
  
  inode_t    inode;
  int        replica_nonce;
  bool       is_syncbyauth;
  bool       is_softasync;
  bool       is_lockbyauth;

  CInodeDiscover() {}
  CInodeDiscover(CInode *in, int nonce) {
	inode = in->inode;
	replica_nonce = nonce;
	is_syncbyauth = in->is_syncbyauth() || in->is_presync();
	is_softasync = in->is_softasync();
	is_lockbyauth = in->is_lockbyauth() || in->is_prelock();
  }
  
  crope _rope() {
	crope r;
	r.append((char*)&inode, sizeof(inode));
	r.append((char*)&replica_nonce, sizeof(replica_nonce));
	r.append((char*)&is_syncbyauth, sizeof(bool));
	r.append((char*)&is_softasync, sizeof(bool));
	r.append((char*)&is_lockbyauth, sizeof(bool));
	return r;
  }

  int _unrope(crope s, int off = 0) {
	s.copy(off,sizeof(inode_t), (char*)&inode);
	off += sizeof(inode_t);
	s.copy(off, sizeof(int), (char*)&replica_nonce);
	off += sizeof(int);
	s.copy(off, sizeof(bool), (char*)&is_syncbyauth);
	off += sizeof(bool);
	s.copy(off, sizeof(bool), (char*)&is_softasync);
	off += sizeof(bool);
	s.copy(off, sizeof(bool), (char*)&is_lockbyauth);
	off += sizeof(bool);
	return off;
  }  

};


// export

typedef struct {
  inode_t        inode;
  __uint64_t     version;
  DecayCounter   popularity;
  bool           dirty;       // dirty inode?
  bool           is_softasync;

  int            ncached_by;  // int pairs follow
} CInodeExport_st;

class CInodeExport {

  CInodeExport_st st;
  set<int>      cached_by;
  map<int,int>  cached_by_nonce;

  CInodeExport() {}
  CInodeExport(CInode *in) {
	st.inode = in->inode;
	st.version = in->get_version();
	st.popularity = in->get_popularity();
	st.dirty = in->is_dirty();
	st.is_softasync = in->is_softasync();
	cached_by = in->get_cached_by();
	cached_by_nonce = in->get_cached_by_nonce(); 
  }

  crope _rope() {
	crope r;
    st.ncached_by = cached_by.size();
    r.append((char*)&st, sizeof(st));
    
    // cached_by + nonce
    for (map<int,int>::iterator it = cached_by_nonce.begin();
         it != cached_by_nonce.end();
         it++) {
      int m = it->first;
      r.append((char*)&m, sizeof(int));
      int n = it->second;
      r.append((char*)&n, sizeof(int));
    }
    
	return r;
  }

  int _unrope(crope s, int off = 0) {
    s.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);

    for (int i=0; i<st.ncached_by; i++) {
      int m,n;
      s.copy(off, sizeof(int), (char*)&m);
      off += sizeof(int);
      s.copy(off, sizeof(int), (char*)&n);
      off += sizeof(int);
      cached_by.insert(m);
      cached_by_nonce.insert(pair<int,int>(m,n));
    }
    return off;
  }
};




#endif
