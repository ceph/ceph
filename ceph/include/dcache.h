
#ifndef __DCACHE_H
#define __DCACHE_H

#include <sys/types.h>
#include <string>
#include <vector>
#include <map>

#include <ext/hash_map>


#include "inode.h"
#include "lru.h"
#include "DecayCounter.h"

using namespace std;

class CDentry;
class CDir;

// cached inode wrapper
class CInode : LRUObject {
 public:
  inode_t  inode;     // the inode itself
 protected:
  CDir    *dir;       // directory entries, if we're a directory

  int             ref;            // reference count (???????)

  // parent dentries in cache
  int             nparents;  
  CDentry        *parent;            // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

  // dcache lru
  CInode *lru_next, *lru_prev;

  // import/export
  bool is_import, is_export;

  // accounting
  DecayCounter popularity;
  

  friend class DentryCache;
  friend class CDir;

 public:
  CInode() : LRUObject() {
	ref = 0;

	parent = NULL;
	nparents = 0;

	is_import = is_export = false;

	dir = NULL;

	lru_next = lru_prev = NULL;

  }
  ~CInode();

	

  // --- reference counting
  void put() {
	if (ref == 0) 
	  throw 1;
	ref--;
	if (ref == 0)
	  lru_unpin();
  }
  void get() {
	if (ref == 0)
	  lru_pin();
	ref++;
  }
  
  // --- hierarchy stuff
  void add_parent(CDentry *p);

  void dump(int d = 0);
};




// dentry
class CDentry {
 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  friend class DentryCache;

 public:
  // cons
  CDentry() {
	inode = NULL;
	dir = NULL;
  }
  CDentry(string& n, CInode *in) {
	name = n;
	inode = in;
  }
  

  // copy cons
  CDentry(const CDentry& m);
  const CDentry& operator= (const CDentry& right);

  // comparisons
  bool operator== (const CDentry& right) const;
  bool operator!= (const CDentry& right) const;
  bool operator< (const CDentry& right) const;
  bool operator> (const CDentry& right) const;
  bool operator>= (const CDentry& right) const;
  bool operator<= (const CDentry& right) const;

  
  // -- hierarchy
  void remove();

  friend class CDir;
};


// CDir

class CDir {
 protected:
  CInode          *inode;

  map<string, CDentry*> items;              // use map; ordered list
  __uint64_t       nitems;
  bool             complete;

 public:
  CDir(CInode *in) {
	inode = in;
	nitems = 0;
	complete = true;
  }
  
  void add_child(CDentry *d);
  void remove_child(CDentry *d);
  CDentry* lookup(string n);

  void dump(int d = 0);

};




// DCache

class DentryCache {
 protected:
  CInode                       *root;        // root inode
  LRU                          *lru;         // lru for expiring items
  hash_map<inodeno_t, CInode*> inode_map;   // map of inodes by ino             

 public:
  DentryCache() {
	root = NULL;
	lru = new LRU(25);
  }
  DentryCache(CInode *r) {
	root = r;
	lru = new LRU(25);
  }
  ~DentryCache() {
	if (lru) { delete lru; lru = NULL; }
  }
  
  // accessors
  CInode *get_root() {
	return root;
  }

  // fn
  bool trim(__int32_t max = -1);   // trim cache
  bool clear() {                  // clear cache
	return trim(0);  
  }

  bool remove_inode(CInode *ino);
  bool add_inode(CInode *ino);

  // crap fns
  CInode* get_file(string& fn);
  void add_file(string& fn, CInode* in);

  void dump() {
	if (root) root->dump();
  }
};


#endif
