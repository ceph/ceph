
#ifndef __DCACHE_H
#define __DCACHE_H

#include <sys/types.h>
#include <string>
#include <vector>
#include <map>

using namespace std;

class CDentry;
class CDir;

// cached inode wrapper
class CInode {
 protected:
  inode_t  inode;     // the inode itself
 public:
  CDir    *dentries;  // directory entries, if we're a directory
 protected:

  int             ref;            // reference count (???????)

  int             nparents;  
  CDentry        *parent;            // if 1 parent (usually)
  vector<CDentry*> parents;    // if > 1

 public:
  CInode() {
	ref = 0;

	parent = NULL;
	nparents = 0;

	dentries = NULL;
  }

  // --- reference counting
  void put() {
	if (ref == 0) 
	  throw 1;
	ref--;
  }
  void get() {
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



class CDir {
 protected:
  CInode          *inode;

  map<string, CDentry*> items;
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




#endif
