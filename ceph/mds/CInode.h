
#ifndef __CINODE_H
#define __CINODE_H

#include "../include/types.h"
#include "../include/lru.h"
#include "../include/DecayCounter.h"
#include <sys/stat.h>

#include <list>
#include <vector>
#include <set>
using namespace std;


class Context;
class CDentry;
class CDir;
class MDS;
class MDCluster;

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
  CDentry         *parent;     // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

  // dcache lru
  CInode *lru_next, *lru_prev;

  // distributed caching
  int              dir_dist;
  bit_vector       dir_rep;
  set<int>         cached_by;  // mds's that cache me
  bool             is_import, is_export;
  hash_map< string, list<Context*> >
                   waiting_for_discover;
  

  // accounting
  DecayCounter popularity;
  

  friend class DentryCache;
  friend class CDir;
  friend class MDStore;
  friend class MDS;

 public:
  CInode();
  ~CInode();

	
  // fun
  bool is_dir() {
	return inode.isdir;
  }


  // dist cache
  int authority(MDCluster *mdc);


  void wait_on_discover(string& dentry,
						Context *c) {
	waiting_for_discover[ dentry ].push_back(c);
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
