
#ifndef __CDENTRY_H
#define __CDENTRY_H

#include <string>
using namespace std;

class CInode;
class CDir;

// dentry
class CDentry {
 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  friend class DentryCache;
  friend class MDS;

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

  
  // -- hierarchy
  void remove();

  friend class CDir;
};

#endif
