
#ifndef __CDENTRY_H
#define __CDENTRY_H

#include <string>
using namespace std;

class CInode;
class CDir;

#define CDENTRY_STATE_FROZEN  1

// dentry
class CDentry {
 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;
  int            state;

  friend class MDCache;
  friend class MDS;
  friend class CInode;

 public:
  // cons
  CDentry() {
	inode = NULL;
	dir = NULL;
	state = 0;
  }
  CDentry(string& n, CInode *in) {
	name = n;
	inode = in;
	state = 0;
  }

  CInode *get_inode() {
	return inode;
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

  // -- locking
  //bool is_frozen() { return is_frozen_dentry() || dir->is_frozen_dir(); }
  //bool is_frozen_dentry() { return state & CDENTRY_STATE_FROZENDENTRY; }
  
  
  // -- hierarchy
  void remove();

  friend class CDir;
};

#endif
