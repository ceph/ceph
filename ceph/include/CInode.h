
#ifndef __CINODE_H
#define __CINODE_H

#include "types.h"
#include "lru.h"
#include "DecayCounter.h"
#include <sys/stat.h>

#include <list>
#include <vector>
using namespace std;


class CDentry;
class CDir;
class MDS;
class Context;

// cached inode wrapper
class CInode : LRUObject {
 public:
  inode_t          inode;     // the inode itself
  CDir            *dir;       // directory entries, if we're a directory

 protected:
  int              ref;            // reference count (???????)

  // used by MDStore
  bool             mid_fetch;
  list<Context*>   waiting_for_fetch;

  // parent dentries in cache
  int              nparents;  
  CDentry         *parent;            // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

  // dcache lru
  CInode *lru_next, *lru_prev;


  // import/export
  bool is_import, is_export;

  // accounting
  DecayCounter popularity;
  

  friend class DentryCache;
  friend class CDir;
  friend class MDStore;

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

	
  // fun
  bool is_dir() {
	return inode.isdir;
  }


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


  mdloc_t get_mdloc() {
	return inode.ino;       // use inode #
  }

  bit_vector get_dist_spec(MDS *mds);


  // dbg
  void dump(int d = 0);
  void dump_to_disk(MDS *m);
};



#endif
