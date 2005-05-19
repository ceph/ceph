
#ifndef __CINODE_H
#define __CINODE_H

#include "include/config.h"
#include "include/types.h"
#include "include/lru.h"
#include "common/DecayCounter.h"
#include <sys/stat.h>

#include "CDentry.h"
#include "CFile.h"
#include "Lock.h"

#include <cassert>
#include <list>
#include <vector>
#include <set>
#include <map>
#include <iostream>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;


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

#define CINODE_PIN_OPEN      5  // local fh's
#define CINODE_PIN_OPENTOK   6  // fh tokens to replicas

#define CINODE_PIN_DNDIRTY   7  // dentry is dirty

#define CINODE_PIN_AUTHPIN   8
#define CINODE_PIN_IMPORTING  9   // multipurpose, for importing
#define CINODE_PIN_REQUEST   10  // request is logging, finishing
#define CINODE_PIN_RENAMESRC 11  // pinned on dest for foreign rename
#define CINODE_PIN_ANCHORING 12

#define CINODE_PIN_DENTRYLOCK   14

#define CINODE_NUM_PINS       15

static char *cinode_pin_names[CINODE_NUM_PINS] = {
  "dir",
  "cached",
  "dirty",
  "proxy",
  "waiter",
  "open",
  "opentok",
  "dndirty",
  "authpin",
  "importing",
  "request",
  "renamesrc",
  "archoring",
  "--",
  "dentrylock"
};






// wait reasons
#define CINODE_WAIT_AUTHPINNABLE  CDIR_WAIT_UNFREEZE
    // waiters: write_hard_start, read_soft_start, write_soft_start  (mdcache)
    //          handle_client_chmod, handle_client_touch             (mds)
    // trigger: (see CDIR_WAIT_UNFREEZE)
#define CINODE_WAIT_GETREPLICA    (1<<11)  // update/replicate individual inode
    // waiters: import_dentry_inode
    // trigger: handle_inode_replicate_ack
#define CINODE_WAIT_UNLINK        (1<<12)
    // waiters: inode_unlink 
    // triggers: inode_unlink_finish

#define CINODE_WAIT_DIR           (1<<13)
    // waiters: traverse_path
    // triggers: handle_disocver_reply

#define CINODE_WAIT_LINK         (1<<15)  // as in remotely nlink++
#define CINODE_WAIT_ANCHORED     (1<<16)

#define CINODE_WAIT_HARDR        (1<<17)  // 131072
#define CINODE_WAIT_HARDW        (1<<18)  // 262...
#define CINODE_WAIT_HARDB        (1<<19)
#define CINODE_WAIT_HARDRWB      (CINODE_WAIT_HARDR|CINODE_WAIT_HARDW|CINODE_WAIT_HARDB)
#define CINODE_WAIT_HARDSTABLE   (1<<20)
#define CINODE_WAIT_HARDNORD     (1<<21)
#define CINODE_WAIT_SOFTR        (1<<22)  
#define CINODE_WAIT_SOFTW        (1<<23)
#define CINODE_WAIT_SOFTB        (1<<24)
#define CINODE_WAIT_SOFTRWB      (CINODE_WAIT_SOFTR|CINODE_WAIT_SOFTW|CINODE_WAIT_SOFTB)
#define CINODE_WAIT_SOFTSTABLE   (1<<25)
#define CINODE_WAIT_SOFTNORD     (1<<26)
#define CINODE_WAIT_SOFTNOWR     (1<<27)

#define CINODE_WAIT_RENAMEACK       (1<<28)
#define CINODE_WAIT_RENAMENOTIFYACK (1<<29)

#define CINODE_WAIT_CAPS            (1<<30)




#define CINODE_WAIT_ANY           0xffffffff


// state
#define CINODE_STATE_AUTH        (1<<0)
#define CINODE_STATE_ROOT        (1<<1)

#define CINODE_STATE_DIRTY       (1<<2)
#define CINODE_STATE_UNSAFE      (1<<3)   // not logged yet
#define CINODE_STATE_DANGLING    (1<<4)   // delete me when i expire; i have no dentry
#define CINODE_STATE_UNLINKING   (1<<5)
#define CINODE_STATE_PROXY       (1<<6)   // can't expire yet
#define CINODE_STATE_EXPORTING   (1<<7)   // on nonauth bystander.

#define CINODE_STATE_ANCHORING   (1<<8)

//#define CINODE_STATE_RENAMING    (1<<8)  // moving me
//#define CINODE_STATE_RENAMINGTO  (1<<9)  // rename target (will be unlinked)


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

//class MInodeSyncStart;

ostream& operator<<(ostream& out, CInode& in);


// cached inode wrapper
class CInode : LRUObject {
 public:
  inode_t          inode;     // the inode itself

  CDir            *dir;       // directory, if we have it opened.
  string           symlink;   // symlink dest, if symlink

  // inode metadata locks
  CLock        hardlock;
  CLock        softlock;

 protected:
  int              ref;       // reference count
  set<int>         ref_set;
  __uint64_t       version;
  __uint64_t       parent_dir_version;  // dir version when last touched.

  unsigned         state;

  // parent dentries in cache
  CDentry         *parent;             // primary link
  set<CDentry*>    remote_parents;     // if hard linked

  // -- distributed caching
  set<int>         cached_by;        // [auth] mds's that cache me.  
  /* NOTE: on replicas, this doubles as replicated_by, but the
	 cached_by_* access methods below should NOT be used in those
	 cases, as the semantics are different! */
  map<int,int>     cached_by_nonce;  // [auth] nonce issued to each replica
  int              replica_nonce;    // [replica] defined on replica

  // this is sort of a mess.
  set<int>         rename_waiting_for_ack;

  int              dangling_auth;    // explicit auth, when dangling.

  int              num_request_pins;

  // waiters
  multimap<int,Context*>  waiting;

  // open file state (me)
  map<fileh_t, CFile*>  fh_map;                   // locally opened files
  int                   nrdonly, nrdwr, nwronly;  // file mode counts

 private:
  // lock nesting
  int auth_pins;
  int nested_auth_pins;

  DecayCounter popularity[MDS_NPOP];

  // friends
  friend class MDCache;
  friend class CDir;
  friend class CInodeExport;
  friend class CInodeDiscover;

 public:
  // ---------------------------
  CInode(bool auth=true);
  ~CInode();
  

  // -- accessors --
  bool is_file()    { return ((inode.mode & INODE_TYPE_MASK) == INODE_MODE_FILE)    ? true:false; }
  bool is_symlink() { return ((inode.mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK) ? true:false; }
  bool is_dir()     { return ((inode.mode & INODE_TYPE_MASK) == INODE_MODE_DIR)     ? true:false; }

  bool is_anchored() { return inode.anchored; }

  bool is_root() { return state & CINODE_STATE_ROOT; }
  bool is_proxy() { return state & CINODE_STATE_PROXY; }

  bool is_auth() { return state & CINODE_STATE_AUTH; }
  void set_auth(bool auth);
  bool is_replica() { return !is_auth(); }
  int get_replica_nonce() { assert(!is_auth()); return replica_nonce; }

  inodeno_t ino() { return inode.ino; }
  inode_t& get_inode() { return inode; }
  CDentry* get_parent_dn() { return parent; }
  CDir *get_parent_dir();
  CInode *get_parent_inode();
  CInode *get_realm_root();   // import, hash, or root
  
  CDir *get_or_open_dir(MDS *mds);
  CDir *set_dir(CDir *newdir);
  
  bool dir_is_hashed() { 
	if (inode.hash_seed) return true;
	return false; 
  }
  bool dir_is_auth();

  float get_popularity() {
	return popularity[0].get();
  }


  // -- misc -- 
  void make_path(string& s);
  void make_anchor_trace(vector<class Anchor*>& trace);
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

  void encode_soft_state(crope& r);
  void decode_soft_state(crope& r, int& off);
  void decode_merge_soft_state(crope& r, int& off);

  void encode_hard_state(crope& r);
  void decode_hard_state(crope& r, int& off);

  void replicate_relax_locks() {
	assert(is_auth());
	assert(!is_cached_by_anyone());
	dout(10) << " relaxing locks on " << *this << endl;

	if (hardlock.get_state() == LOCK_LOCK &&
		!hardlock.is_used()) {
	  dout(10) << " hard now sync " << *this << endl;
	  hardlock.set_state(LOCK_SYNC);
	}
	if (softlock.get_state() == LOCK_LOCK &&
		!softlock.is_used()) {
	  if (softlock.get_mode() == LOCK_MODE_SYNC) {
		softlock.set_state(LOCK_SYNC);
		dout(10) << " soft now sync " << *this << endl;
	  } else {
		softlock.set_state(LOCK_ASYNC);
		dout(10) << " soft now async " << *this << endl;
	  }
	}
  }
  
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
	int nonce = 1;
	if (is_cached_by(mds)) {    // already had it?
	  nonce = get_cached_by_nonce(mds) + 1;   // new nonce (+1)
	  dout(10) << *this << " issuing new nonce " << nonce << " to mds" << mds << endl;
	  cached_by_nonce.erase(mds);
    } else {
	  if (cached_by.empty()) 
		get(CINODE_PIN_CACHED);
	  cached_by.insert(mds);
	}
    cached_by_nonce.insert(pair<int,int>(mds,nonce));   // first! serial of 1.
    return nonce;   // default nonce
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
	//if (!is_cached_by(mds)) return;
	assert(is_cached_by(mds));

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
  bool waiting_for(int tag);
  void add_waiter(int tag, Context *c);
  void take_waiting(int tag, list<Context*>& ls);
  void finish_waiting(int mask, int result = 0);



  // -- open files --
  bool is_open_write() { return nwronly; }
  bool is_open_read() { return nrdonly; }
  bool is_open() { return is_open_write() || is_open_read(); }

  int get_num_fh() { return fh_map.size(); }
  CFile* get_fh(int fh) {
	if (fh_map.count(fh)) return fh_map[fh];
	return 0;
  }
  void add_fh(CFile *f) {
	if (f->mode == CFILE_MODE_R) nrdonly++;
	if (f->mode == CFILE_MODE_W) {
	  nwronly++;
	  softlock.get_write();
	}
	if (f->mode == CFILE_MODE_RW) nrdwr++;
	
	if (fh_map.empty()) get(CINODE_PIN_OPEN);
	fh_map[f->fh] = f;
  }
  void remove_fh(CFile *f) {
	if (f->mode == CFILE_MODE_R) nrdonly--;
	if (f->mode == CFILE_MODE_W) {
	  nwronly--;
	  softlock.put_write();
	}
	if (f->mode == CFILE_MODE_RW) nrdwr--;

	fh_map.erase(f->fh);
	if (fh_map.empty()) put(CINODE_PIN_OPEN);
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
  
  /* these can be pinned any # of times, and are
	 linked to an active_request, so they're automatically cleaned
	 up when a request is finished.  pin at will! */
  void request_pin_get() {
	if (num_request_pins == 0) get(CINODE_PIN_REQUEST);
	num_request_pins++;
  }
  void request_pin_put() {
	num_request_pins--;
	if (num_request_pins == 0) put(CINODE_PIN_REQUEST);
	assert(num_request_pins >= 0);
  }


  bool is_pinned() { return ref > 0; }
  set<int>& get_ref_set() { return ref_set; }
  void put(int by) {
	if (ref == 0 || ref_set.count(by) != 1) {
	  dout(7) << " bad put " << *this << " by " << by << " " << cinode_pin_names[by] << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 1);
	  assert(ref > 0);
	}
	ref--;
	ref_set.erase(by);
	if (ref == 0)
	  lru_unpin();
	dout(7) << " put " << *this << " by " << by << " " << cinode_pin_names[by] << " now " << ref << " (" << ref_set << ")" << endl;
  }
  void get(int by) {
	if (ref == 0)
	  lru_pin();
	if (ref_set.count(by)) {
	  dout(7) << " bad get " << *this << " by " << by << " " << cinode_pin_names[by] << " was " << ref << " (" << ref_set << ")" << endl;
	  assert(ref_set.count(by) == 0);
	}
	ref++;
	ref_set.insert(by);
	dout(7) << " get " << *this << " by " << by << " " << cinode_pin_names[by] << " now " << ref << " (" << ref_set << ")" << endl;
  }
  bool is_pinned_by(int by) {
	return ref_set.count(by);
  }

  // -- hierarchy stuff --
  void set_primary_parent(CDentry *p) {
	parent = p;
  }
  void remove_primary_parent(CDentry *dn) {
	assert(dn == parent);
	parent = 0;
  }
  void add_remote_parent(CDentry *p) {
	remote_parents.insert(p);
  }
  void remove_remote_parent(CDentry *p) {
	remote_parents.erase(p);
  }
  int num_remote_parents() {
	return remote_parents.size(); 
  }


  // for giving to clients
  void get_dist_spec(set<int>& ls, int auth) {
	ls = cached_by;
	ls.insert(auth);
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
  
  int        hardlock_state;
  int        softlock_state;

 public:
  CInodeDiscover() {}
  CInodeDiscover(CInode *in, int nonce) {
	inode = in->inode;
	replica_nonce = nonce;
	hardlock_state = in->hardlock.get_replica_state();
	softlock_state = in->softlock.get_replica_state();
  }

  inodeno_t get_ino() { return inode.ino; }
  int get_replica_nonce() { return replica_nonce; }

  int update_inode(CInode *in) {
	in->inode = inode;

	in->replica_nonce = replica_nonce;
	in->hardlock.set_state(hardlock_state);
	in->softlock.set_state(softlock_state);
  }
  
  void _rope(crope& r) {
	r.append((char*)&inode, sizeof(inode));
	r.append((char*)&replica_nonce, sizeof(replica_nonce));
	r.append((char*)&hardlock_state, sizeof(hardlock_state));
	r.append((char*)&softlock_state, sizeof(softlock_state));
  }

  int _unrope(crope s, int off = 0) {
	s.copy(off,sizeof(inode_t), (char*)&inode);
	off += sizeof(inode_t);
	s.copy(off, sizeof(int), (char*)&replica_nonce);
	off += sizeof(int);
	s.copy(off, sizeof(hardlock_state), (char*)&hardlock_state);
	off += sizeof(hardlock_state);
	s.copy(off, sizeof(softlock_state), (char*)&softlock_state);
	off += sizeof(softlock_state);
	return off;
  }  

};


// export

typedef struct {
  inode_t        inode;
  __uint64_t     version;
  DecayCounter   popularity;
  bool           is_dirty;       // dirty inode?

  int            ncached_by;  // int pairs follow
  int            num_fh;
} CInodeExport_st;


class CInodeExport {

  CInodeExport_st st;
  set<int>      cached_by;
  map<int,int>  cached_by_nonce;
  list<CFile*>  fh_list;

  CLock         hardlock,softlock;

public:
  CInodeExport() {}
  CInodeExport(CInode *in) {
	st.inode = in->inode;
	st.version = in->get_version();
	st.popularity = in->get_popularity();
	st.is_dirty = in->is_dirty();
	cached_by = in->cached_by;
	cached_by_nonce = in->cached_by_nonce; 
	hardlock = in->hardlock;
	softlock = in->softlock;
	
	// suck up fh's from inode
	for (map<fileh_t, CFile*>::iterator it = in->fh_map.begin();
		 it != in->fh_map.end();
		 it++) {
	  fh_list.push_back(it->second);
	}
	for (list<CFile*>::iterator it = fh_list.begin();
		 it != fh_list.end();
		 it++) 
	  in->remove_fh(*it);
  }
  ~CInodeExport() {
	for (list<CFile*>::iterator it = fh_list.begin();
		 it != fh_list.end();
		 it++) delete *it;
  }
  
  inodeno_t get_ino() { return st.inode.ino; }

  int update_inode(CInode *in) {
	in->inode = st.inode;

	in->version = st.version;
	in->popularity[0] = st.popularity;

	if (st.is_dirty)
	  in->mark_dirty();

	in->cached_by.clear();
	in->cached_by = cached_by;
	in->cached_by_nonce = cached_by_nonce;
	if (!cached_by.empty()) 
	  in->get(CINODE_PIN_CACHED);

	in->hardlock = hardlock;
	in->softlock = softlock;

	// fh's
	for (list<CFile*>::iterator it = fh_list.begin();
		 it != fh_list.end();
		 it++) {
	  in->add_fh(*it);
	}
	fh_list.clear();

  }

  void _rope(crope& r) {
    st.ncached_by = cached_by.size();
	st.num_fh = fh_list.size();
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

	hardlock.encode_state(r);
	softlock.encode_state(r);

	// fh
	for (list<CFile*>::iterator it = fh_list.begin();
		 it != fh_list.end();
		 it++) {
	  r.append((char*)*it, sizeof(CFile));
	  CFile *f = *it;
	  //cout << "f in client " << f->client << " fh " << f->fh << endl;
	}
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

	hardlock.decode_state(s, off);
	softlock.decode_state(s, off);

	// fh
	for (int i=0; i<st.num_fh; i++) {
	  CFile *f = new CFile;
	  s.copy(off, sizeof(CFile), (char*)f);
	  //cout << "f out client " << f->client << " fh " << f->fh << endl;
	  off += sizeof(CFile);
	  fh_list.push_back(f);
	}

    return off;
  }
};




#endif
