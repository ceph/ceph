
#ifndef __CDIR_H
#define __CDIR_H

#include <sys/types.h>
#include <map>
#include <ext/hash_map>
#include <string>

#include <iostream>

#include "inode.h"

using namespace std;

class CInode;
class CDentry;

// CDir
typedef map<string, CDentry*> CDir_map_t;

class CDir {
 protected:
  CInode          *inode;

  CDir_map_t       items;              // use map; ordered list
  __uint64_t       nitems;
  size_t           namesize;
  bool             complete;

 public:
  CDir(CInode *in) {
	inode = in;
	nitems = 0;
	complete = false;
  }

  size_t serial_size() {
	return nitems * (10+sizeof(inode_t)) + namesize;
  }
  
  CDir_map_t::iterator begin() {
	return items.begin();
  }
  CDir_map_t::iterator end() {
	return items.end();
  }
  
  void add_child(CDentry *d);
  void remove_child(CDentry *d);
  CDentry* lookup(string n);

  void dump(int d = 0);
  void dump_to_disk();
};


#endif
