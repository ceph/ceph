
#ifndef __MDCACHE_H
#define __MDCACHE_H

#include <sys/types.h>
#include <string>
#include <vector>
#include <map>

#include <ext/hash_map>

#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"


// DCache

typedef hash_map<inodeno_t, CInode*> inode_map_t;

class DentryCache {
 protected:
  CInode                       *root;        // root inode
  LRU                          *lru;         // lru for expiring items
  inode_map_t                   inode_map;   // map of inodes by ino             

 public:
  DentryCache() {
	root = NULL;
	lru = new LRU();
  }
  ~DentryCache() {
	if (lru) { delete lru; lru = NULL; }
  }
  
  // accessors
  CInode *get_root() {
	return root;
  }
  void set_root(CInode *r) {
	root = r;
  }

  // fn
  size_t set_cache_size(size_t max) {
	lru->set_max(max);
  }
  bool trim(__int32_t max = -1);   // trim cache
  bool clear() {                  // clear cache
	return trim(0);  
  }

  // have_inode?
  bool have_inode( inodeno_t ino ) {
	inode_map_t::iterator it = inode_map.find(ino);
	if (it == inode_map.end()) return false;
	return true;
  }

  // return inode* or null
  CInode* get_inode( inodeno_t ino ) {
	if (have_inode(ino))
	  return inode_map[ ino ];
	return NULL;
  }

  // adding/removing
  bool remove_inode(CInode *ino);
  bool add_inode(CInode *ino);

  int link_inode( CInode *parent, string& dname, CInode *inode );



  // crap fns
  CInode* get_file(string& fn);
  void add_file(string& fn, CInode* in);

  void dump() {
	if (root) root->dump();
  }

  void dump_to_disk() {
	if (root) root->dump_to_disk();
  }
};


#endif
