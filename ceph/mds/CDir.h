
#ifndef __CDIR_H
#define __CDIR_H

#include "include/types.h"
#include "include/bufferlist.h"
#include "include/config.h"
#include "common/DecayCounter.h"

#include <iostream>
#include <cassert>

#include <list>
#include <set>
#include <map>
#include <string>
using namespace std;

#include <ext/rope>
#include <ext/hash_map>
using namespace __gnu_cxx;

class CInode;
class CDentry;
class MDS;
class MDCluster;
class Context;


// directory authority types
//  >= 0 is the auth mds
#define CDIR_AUTH_PARENT   -1   // default


#define CDIR_NONCE_EXPORT   1


// state bits
#define CDIR_STATE_AUTH          (1<<0)   // auth for this dir (hashing doesn't count)
#define CDIR_STATE_PROXY         (1<<1)   // proxy auth

#define CDIR_STATE_COMPLETE      (1<<2)   // the complete contents are in cache
#define CDIR_STATE_DIRTY         (1<<3)   // has been modified since last commit

#define CDIR_STATE_FROZENTREE    (1<<4)   // root of tree (bounded by exports)
#define CDIR_STATE_FREEZINGTREE  (1<<5)   // in process of freezing 
#define CDIR_STATE_FROZENDIR     (1<<6)
#define CDIR_STATE_FREEZINGDIR   (1<<7)

#define CDIR_STATE_COMMITTING    (1<<8)   // mid-commit
#define CDIR_STATE_FETCHING      (1<<9)   // currenting fetching

#define CDIR_STATE_IMPORT        (1<<10)   // flag set if this is an import.
#define CDIR_STATE_EXPORT        (1<<11)

#define CDIR_STATE_HASHED        (1<<12)   // if hashed.  only hashed+auth on auth node.
#define CDIR_STATE_HASHING       (1<<13)
#define CDIR_STATE_UNHASHING     (1<<14)

#define CDIR_STATE_SYNCBYME         (1<<15)
#define CDIR_STATE_PRESYNC          (1<<16) 
#define CDIR_STATE_SYNCBYAUTH       (1<<17) 
#define CDIR_STATE_WAITONUNSYNC     (1<<18)

#define CDIR_STATE_AUTHMOVING       (1<<19)  // dir replica bystander
#define CDIR_STATE_IMPORTINGEXPORT  (1<<20)

#define CDIR_STATE_DELETED          (1<<21)


// these state bits are preserved by an import/export
// ...except if the directory is hashed, in which case none of them are!
#define CDIR_MASK_STATE_EXPORTED    (CDIR_STATE_COMPLETE\
                                    |CDIR_STATE_DIRTY)  
#define CDIR_MASK_STATE_IMPORT_KEPT (CDIR_STATE_IMPORT\
                                    |CDIR_STATE_EXPORT\
                                    |CDIR_STATE_IMPORTINGEXPORT)
#define CDIR_MASK_STATE_EXPORT_KEPT (CDIR_STATE_HASHED\
                                    |CDIR_STATE_FROZENTREE\
                                    |CDIR_STATE_FROZENDIR\
                                    |CDIR_STATE_EXPORT)

// common states
#define CDIR_STATE_CLEAN   0
#define CDIR_STATE_INITIAL 0  

// directory replication
#define CDIR_REP_ALL       1
#define CDIR_REP_NONE      0
#define CDIR_REP_LIST      2



// pins

#define CDIR_PIN_CHILD     0
#define CDIR_PIN_OPENED    1  // open by another node
#define CDIR_PIN_HASHED    2  // hashed
#define CDIR_PIN_WAITER    3  // waiter(s)

#define CDIR_PIN_IMPORT    4
#define CDIR_PIN_EXPORT    5
#define CDIR_PIN_FREEZE    6
#define CDIR_PIN_PROXY     7  // auth just changed.

#define CDIR_PIN_AUTHPIN   8

#define CDIR_PIN_IMPORTING 9
#define CDIR_PIN_IMPORTINGEXPORT 10

#define CDIR_PIN_DIRTY     11

#define CDIR_PIN_REQUEST   12

#define CDIR_NUM_PINS      13
static char* cdir_pin_names[CDIR_NUM_PINS] = {
  "child",
  "opened",
  "hashed",
  "waiter",
  "import",
  "export",
  "freeze",
  "proxy",
  "authpin",
  "imping",
  "impgex",
  "reqpins",
  "dirty"
};



// wait reasons
#define CDIR_WAIT_DENTRY         1  // wait for item to be in cache
     // waiters: path_traverse
     // trigger: handle_discover, fetch_dir_2
#define CDIR_WAIT_COMPLETE       2  // wait for complete dir contents
     // waiters: fetch_dir, commit_dir
     // trigger: fetch_dir_2
#define CDIR_WAIT_FREEZEABLE     4  // hard_pins removed
     // waiters: freeze, freeze_finish
     // trigger: auth_unpin, adjust_nested_auth_pins
#define CDIR_WAIT_UNFREEZE       8  // unfreeze
     // waiters: path_traverse, handle_discover, handle_inode_update,
     //           export_dir_frozen                                   (mdcache)
     //          handle_client_readdir                                (mds)
     // trigger: unfreeze
#define CDIR_WAIT_AUTHPINNABLE  CDIR_WAIT_UNFREEZE
    // waiters: commit_dir                                           (mdstore)
    // trigger: (see CDIR_WAIT_UNFREEZE)
#define CDIR_WAIT_COMMITTED     32  // did commit (who uses this?**)
    // waiters: commit_dir (if already committing)
    // trigger: commit_dir_2
#define CDIR_WAIT_IMPORTED      64  // import finish
    // waiters: import_dir_block
    // triggers: handle_export_dir_finish

#define CDIR_WAIT_EXPORTWARNING 8192    // on bystander.
    // watiers: handle_export_dir_notify
    // triggers: handle_export_dir_warning
#define CDIR_WAIT_EXPORTPREPACK 16384
    // waiter   export_dir
    // trigger  handel_export_dir_prep_ack

#define CDIR_WAIT_DNREAD        (1<<20)
#define CDIR_WAIT_DNLOCK        (1<<21)
#define CDIR_WAIT_DNUNPINNED    (1<<22)
#define CDIR_WAIT_DNPINNABLE    (CDIR_WAIT_DNREAD|CDIR_WAIT_DNUNPINNED)

#define CDIR_WAIT_DNREQXLOCK    (1<<23)

#define CDIR_WAIT_ANY   (0xffffffff)

#define CDIR_WAIT_ATFREEZEROOT  (CDIR_WAIT_AUTHPINNABLE|\
                                 CDIR_WAIT_UNFREEZE)      // hmm, same same


ostream& operator<<(ostream& out, class CDir& dir);


// CDir
typedef map<string, CDentry*> CDir_map_t;


extern map<int, int> cdir_pins;  // counts


class CDir {
 public:
  CInode          *inode;

 protected:
  // contents
  CDir_map_t       items;              // non-null AND null
  CDir_map_t       null_items;        // null and foreign
  size_t           nitems;             // non-null
  size_t           nnull;              // null
  //size_t           nauthitems;
  //size_t           namesize;

  // state
  unsigned         state;
  __uint64_t       version;
  __uint64_t       committing_version;
  __uint64_t       last_committed_version;

  // authority, replicas
  set<int>         open_by;        // nodes that have me open
  map<int,int>     open_by_nonce;
  int              replica_nonce;
  int              dir_auth;       

  // reference countin/pins
  int              ref;       // reference count
  set<int>         ref_set;

  // lock nesting, freeze
  int        auth_pins;
  int        nested_auth_pins;
  int        request_pins;

  // context
  MDS              *mds;


  // waiters
  multimap<int, Context*> waiting;  // tag -> context
  hash_map< string, multimap<int, Context*> >
	                      waiting_on_dentry;

  // cache control  (defined for authority; hints for replicas)
  int              dir_rep;
  set<int>         dir_rep_by;      // if dir_rep == CDIR_REP_LIST


  // sync (for hashed dirs)
  set<int>   sync_waiting_for_ack;

  DecayCounter popularity[MDS_NPOP];

  friend class CInode;
  friend class MDCache;
  friend class MDiscover;
  friend class MDBalancer;

  friend class CDirDiscover;
  friend class CDirExport;

 public:
  CDir(CInode *in, MDS *mds, bool auth);



  // -- accessors --
  CInode *get_inode() { return inode; }
  CDir *get_parent_dir();
  inodeno_t ino();

  CDir_map_t::iterator begin() { return items.begin(); }
  CDir_map_t::iterator end() { return items.end(); }
  size_t get_size() { 
	
	//if ( is_auth() && !is_hashed()) assert(nauthitems == nitems);
	//if (!is_auth() && !is_hashed()) assert(nauthitems == 0);
	
	return nitems; 
  }
  size_t get_nitems() { return nitems; }
  size_t get_nnull() { return nnull; }
  /*
  size_t get_auth_size() { 
	assert(nauthitems <= nitems);
	return nauthitems; 
  }
  */

  /*
  float get_popularity() {
	return popularity[0].get();
  }
  */
  

  // -- manipulation --
  CDentry* lookup(const string& n);

  // dentries and inodes
 public:
  CDentry* add_dentry( const string& dname, CInode *in=0 );
  CDentry* add_dentry( const string& dname, inodeno_t ino );
  void remove_dentry( CDentry *dn );         // delete dentry
  void link_inode( CDentry *dn, inodeno_t ino );
  void link_inode( CDentry *dn, CInode *in );
  void unlink_inode( CDentry *dn );
 private:
  void link_inode_work( CDentry *dn, CInode *in );
  void unlink_inode_work( CDentry *dn );

  void remove_null_dentries();  // on empty, clean dir

  // -- authority --
 public:
  int authority();
  int dentry_authority(const string& d);
  int get_dir_auth() { return dir_auth; }

  bool is_open_by_anyone() { return !open_by.empty(); }
  bool is_open_by(int mds) { return open_by.count(mds); }
  int get_open_by_nonce(int mds) {
	map<int,int>::iterator it = open_by_nonce.find(mds);
	return it->second;
  }
  set<int>::iterator open_by_begin() { return open_by.begin(); }
  set<int>::iterator open_by_end() { return open_by.end(); }
  set<int>& get_open_by() { return open_by; }

  int get_replica_nonce() { assert(!is_auth()); return replica_nonce; }
  
  int open_by_add(int mds) {
	int nonce = 1;
	
	if (is_open_by(mds)) {    // already had it?
      nonce = get_open_by_nonce(mds) + 1; // new nonce (+1)
	  dout(10) << *this << " issuing new nonce " << nonce << " to mds" << mds << endl;
	  open_by_nonce.erase(mds);
    } else {
	  if (open_by.empty()) 
		get(CDIR_PIN_OPENED);
	  open_by.insert(mds);
	}
	open_by_nonce.insert(pair<int,int>(mds,nonce));   // first! serial of 1.
    return nonce;   // default nonce
  }
  void open_by_remove(int mds) {
	//if (!is_open_by(mds)) return;
	assert(is_open_by(mds));

	open_by.erase(mds);
	open_by_nonce.erase(mds);
	if (open_by.empty())
	  put(CDIR_PIN_OPENED);	  
  }
  void open_by_clear() {
	if (open_by.size())
	  put(CDIR_PIN_OPENED);
	open_by.clear();
    open_by_nonce.clear();
  }

  


  // -- state --
  unsigned get_state() { return state; }
  void reset_state(unsigned s) { 
	state = s; 
	dout(10) << " cdir:" << *this << " state reset" << endl;
  }
  void state_clear(unsigned mask) {	
	state &= ~mask; 
	dout(10) << " cdir:" << *this << " state -" << mask << " = " << state << endl;
  }
  void state_set(unsigned mask) { 
	state |= mask; 
	dout(10) << " cdir:" << *this << " state +" << mask << " = " << state << endl;
  }
  unsigned state_test(unsigned mask) { return state & mask; }

  bool is_complete() { return state & CDIR_STATE_COMPLETE; }
  bool is_dirty() { return state_test(CDIR_STATE_DIRTY); }

  bool is_auth() { return state & CDIR_STATE_AUTH; }
  bool is_proxy() { return state & CDIR_STATE_PROXY; }
  bool is_import() { return state & CDIR_STATE_IMPORT; }
  bool is_export() { return state & CDIR_STATE_EXPORT; }

  bool is_hashed() { return state & CDIR_STATE_HASHED; }
  bool is_hashing() { return state & CDIR_STATE_HASHING; }
  bool is_unhashing() { return state & CDIR_STATE_UNHASHING; }

  bool is_rep() { 
	if (dir_rep == CDIR_REP_NONE) return false;
	return true;
  }
  int get_rep_count(MDCluster *mdc);
  
  void update_auth(int whoami);


  // -- dirtyness --
  __uint64_t get_version() { return version; }
  void float_version(__uint64_t ge) {
	if (version < ge)
	  version = ge;
  }
  __uint64_t get_committing_version() { return committing_version; }
  __uint64_t get_last_committed_version() { return last_committed_version; }
  // as in, we're committing the current version.
  void set_committing_version() { committing_version = version; }
  void set_last_committed_version(__uint64_t v) { last_committed_version = v; }
  void mark_dirty();
  void mark_clean();
  void mark_complete() { state_set(CDIR_STATE_COMPLETE); }
  bool is_clean() { return !state_test(CDIR_STATE_DIRTY); }


  // -- encoded state --
  crope encode_basic_state();
  int decode_basic_state(crope r, int off=0);



  // -- reference counting --
  void put(int by);
  void get(int by);
  bool is_pinned_by(int by) {
	return ref_set.count(by);
  }
  bool is_pinned() { return ref > 0; }
  int get_ref() { return ref; }
  set<int>& get_ref_set() { return ref_set; }
  void request_pin_get() {
	if (request_pins == 0) get(CDIR_PIN_REQUEST);
	request_pins++;
  }
  void request_pin_put() {
	request_pins--;
	if (request_pins == 0) put(CDIR_PIN_REQUEST);
  }

  
  // -- sync --
  bool is_sync() { return is_syncbyme() || is_syncbyauth(); }
  bool is_syncbyme() { return state & CDIR_STATE_SYNCBYME; }
  bool is_syncbyauth() { return state & CDIR_STATE_SYNCBYAUTH; }
  bool is_presync() { return state & CDIR_STATE_PRESYNC; }
  bool is_waitonnsync() { return state & CDIR_STATE_WAITONUNSYNC; }

  
  // -- waiters --
  bool waiting_for(int tag);
  bool waiting_for(int tag, const string& dn);
  void add_waiter(int tag, Context *c);
  void add_waiter(int tag,
				  const string& dentry,
				  Context *c);
  void take_waiting(int mask, list<Context*>& ls);  // includes dentry waiters
  void take_waiting(int mask, 
					const string& dentry, 
					list<Context*>& ls,
					int num=0);  
  void finish_waiting(int mask, int result = 0);    // ditto
  void finish_waiting(int mask, const string& dn, int result = 0);    // ditto


  // -- auth pins --
  bool can_auth_pin() { return !(is_frozen() || is_freezing()); }
  int is_auth_pinned() { return auth_pins; }
  void auth_pin();
  void auth_unpin();
  void adjust_nested_auth_pins(int inc);
  void on_freezeable();

  // -- freezing --
  void freeze_tree(Context *c);
  void freeze_tree_finish(Context *c);
  void unfreeze_tree();

  void freeze_dir(Context *c);
  void freeze_dir_finish(Context *c);
  void unfreeze_dir();

  bool is_freezing() { return is_freezing_tree() || is_freezing_dir(); }
  bool is_freezing_tree();
  bool is_freezing_tree_root() { return state & CDIR_STATE_FREEZINGTREE; }
  bool is_freezing_dir() { return state & CDIR_STATE_FREEZINGDIR; }

  bool is_frozen() { return is_frozen_dir() || is_frozen_tree(); }
  bool is_frozen_tree();
  bool is_frozen_tree_root() { return state & CDIR_STATE_FROZENTREE; }
  bool is_frozen_dir() { return state & CDIR_STATE_FROZENDIR; }
  
  bool is_freezeable() {
	if (auth_pins == 0 && nested_auth_pins == 0) return true;
	return false;
  }
  bool is_freezeable_dir() {
	if (auth_pins == 0) return true;
	return false;
  }



  // debuggin bs
  void dump(int d = 0);
  void dump_to_disk(MDS *m);
};



// -- encoded state --

// discover

class CDirDiscover {
  inodeno_t ino;
  int       nonce;
  int       dir_auth;
  int       dir_rep;
  set<int>  rep_by;

 public:
  CDirDiscover() {}
  CDirDiscover(CDir *dir, int nonce) {
    ino = dir->ino();
    this->nonce = nonce;
    dir_auth = dir->dir_auth;
	dir_rep = dir->dir_rep;
    rep_by = dir->dir_rep_by;
  }

  void update_dir(CDir *dir) {
	assert(dir->ino() == ino);
	assert(!dir->is_auth());

	dir->replica_nonce = nonce;
	dir->dir_auth = dir_auth;
	dir->dir_rep = dir_rep;
	dir->dir_rep_by = rep_by;
  }

  inodeno_t get_ino() { return ino; }

  
  void _rope(crope& r) {
    r.append((char*)&ino, sizeof(ino));
    r.append((char*)&nonce, sizeof(nonce));
    r.append((char*)&dir_auth, sizeof(dir_auth));
    r.append((char*)&dir_rep, sizeof(dir_rep));

    int nrep_by = rep_by.size();
    r.append((char*)&nrep_by, sizeof(nrep_by));
    
    // rep_by
    for (set<int>::iterator it = rep_by.begin();
         it != rep_by.end();
         it++) {
      int m = *it;
      r.append((char*)&m, sizeof(int));
    }
  }

  int _unrope(crope s, int off = 0) {
    s.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    s.copy(off, sizeof(nonce), (char*)&nonce);
    off += sizeof(nonce);
    s.copy(off, sizeof(dir_auth), (char*)&dir_auth);
    off += sizeof(dir_auth);
    s.copy(off, sizeof(dir_rep), (char*)&dir_rep);
    off += sizeof(dir_rep);

    int nrep_by;
    s.copy(off, sizeof(int), (char*)&nrep_by);
    off += sizeof(int);
    
    // open_by
    for (int i=0; i<nrep_by; i++) {
      int m;
      s.copy(off, sizeof(int), (char*)&m);
      off += sizeof(int);
      rep_by.insert(m);
    }

    return off;
  }

};


// export

typedef struct {
  inodeno_t      ino;
  __uint64_t     nitems; // actual real entries
  __uint64_t     nden;   // num dentries (including null ones)
  __uint64_t     version;
  unsigned       state;
  DecayCounter   popularity_justme;
  DecayCounter   popularity_curdom;
  int            dir_auth;
  int            dir_rep;
  int            nopen_by;
  int            nrep_by;
  // ints follow
} CDirExport_st;

class CDirExport {
  CDirExport_st st;
  set<int>     open_by;
  map<int,int> open_by_nonce;
  set<int>     rep_by;

 public:
  CDirExport() {}
  CDirExport(CDir *dir) {
    st.ino = dir->ino();
    st.nitems = dir->nitems;
	st.nden = dir->items.size();
    st.version = dir->version;
    st.state = dir->state;
    st.dir_auth = dir->dir_auth;
    st.dir_rep = dir->dir_rep;

    st.popularity_justme.take( dir->popularity[MDS_POP_JUSTME] );
    st.popularity_curdom.take( dir->popularity[MDS_POP_CURDOM] );
	dir->popularity[MDS_POP_ANYDOM].adjust_down(st.popularity_curdom);

    rep_by = dir->dir_rep_by;
	open_by = dir->open_by;
    open_by_nonce = dir->open_by_nonce;
  }

  inodeno_t get_ino() { return st.ino; }
  __uint64_t get_nden() { return st.nden; }

  void update_dir(CDir *dir, timepair_t& now) {
	assert(dir->ino() == st.ino);

	//dir->nitems = st.nitems;
	dir->version = st.version;
	dir->state = (dir->state & CDIR_MASK_STATE_IMPORT_KEPT) |   // remember import flag, etc.
	  (st.state & CDIR_MASK_STATE_EXPORTED);
	dir->dir_auth = st.dir_auth;
	dir->dir_rep = st.dir_rep;

	double newcurdom = st.popularity_curdom.get(now) - dir->popularity[MDS_POP_CURDOM].get(now);
	dir->popularity[MDS_POP_JUSTME].take( st.popularity_justme );
	dir->popularity[MDS_POP_CURDOM].take( st.popularity_curdom );
	dir->popularity[MDS_POP_ANYDOM].adjust(now, newcurdom);

	dir->dir_rep_by = rep_by;
	dir->open_by = open_by;
	dout(12) << "open_by in export is " << open_by << ", dir now " << dir->open_by << endl;
	dir->open_by_nonce = open_by_nonce;
	if (!open_by.empty())
	  dir->get(CDIR_PIN_OPENED);
	if (dir->is_dirty())
	  dir->get(CDIR_PIN_DIRTY);
  }


  void _encode(bufferlist& bl) {
    st.nrep_by = rep_by.size();
    st.nopen_by = open_by_nonce.size();
    bl.append((char*)&st, sizeof(st));
    
    // open_by
    for (map<int,int>::iterator it = open_by_nonce.begin();
         it != open_by_nonce.end();
         it++) {
      int m = it->first;
      bl.append((char*)&m, sizeof(int));
      int n = it->second;
      bl.append((char*)&n, sizeof(int));
    }

    // rep_by
    for (set<int>::iterator it = rep_by.begin();
         it != rep_by.end();
         it++) {
      int m = *it;
      bl.append((char*)&m, sizeof(int));
    }
  }

  int _decode(bufferlist& bl, int off = 0) {
    bl.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);

    // open_by
    for (int i=0; i<st.nopen_by; i++) {
      int m,n;
      bl.copy(off, sizeof(int), (char*)&m);
      off += sizeof(int);
      bl.copy(off, sizeof(int), (char*)&n);
      off += sizeof(int);
	  open_by.insert(m);
      open_by_nonce.insert(pair<int,int>(m,n));
    }
    
    // rep_by
    for (int i=0; i<st.nrep_by; i++) {
      int m;
      bl.copy(off, sizeof(int), (char*)&m);
      off += sizeof(int);
      rep_by.insert(m);
    }

    return off;
  }

};



#endif
