
#ifndef __CDENTRY_H
#define __CDENTRY_H

#include <string>
#include <set>
using namespace std;

class CInode;
class CDir;

#define DN_LOCK_SYNC     0
#define DN_LOCK_PRELOCK  1
#define DN_LOCK_LOCK     2


// dentry
class CDentry {
 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  int            lockstate;
  set<int>       gather_set;

  friend class MDCache;
  friend class MDS;
  friend class CInode;

 public:
  // cons
  CDentry() {
	inode = NULL;
	dir = NULL;
	lockstate = DN_LOCK_SYNC;
  }
  CDentry(string& n, CInode *in) {
	name = n;
	inode = in;
	lockstate = DN_LOCK_SYNC;
  }

  CInode *get_inode() { return inode; }
  CDir *get_dir() { return dir; }
  string& get_name() { return name; }

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
  bool can_read()  { return lockstate == DN_LOCK_SYNC; }
  bool is_locked() { return lockstate == DN_LOCK_LOCK; }
  
  // -- hierarchy
  void remove();

  friend class CDir;
};

#endif
