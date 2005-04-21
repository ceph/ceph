
#ifndef __CDENTRY_H
#define __CDENTRY_H

#include <assert.h>
#include <string>
#include <set>
using namespace std;

class CInode;
class CDir;

#define DN_LOCK_SYNC      0
#define DN_LOCK_PREXLOCK  1
#define DN_LOCK_XLOCK     2
#define DN_LOCK_UNPINNING 3  // waiting for pins to go away

class Message;

// dentry
class CDentry {
 protected:
  string          name;
  CInode         *inode;
  CDir           *dir;

  int            lockstate;
  Message        *xlockedby;
  set<int>       gather_set;
  int            npins;

  friend class MDCache;
  friend class MDS;
  friend class CInode;

 public:
  // cons
  CDentry() {
	inode = NULL;
	dir = NULL;
	lockstate = DN_LOCK_SYNC;
	npins = 0;
  }
  CDentry(const string& n, CInode *in) {
	name = n;
	inode = in;
	lockstate = DN_LOCK_SYNC;
	npins = 0;
  }

  CInode *get_inode() { return inode; }
  CDir *get_dir() { return dir; }
  const string& get_name() { return name; }
  int get_lockstate() { return lockstate; }
  set<int>& get_gather_set() { return gather_set; }

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
  bool can_read()  { return (lockstate == DN_LOCK_SYNC) || (lockstate == DN_LOCK_UNPINNING);  }
  bool is_xlocked() { return lockstate == DN_LOCK_XLOCK; }
  bool is_xlockedbyother(Message *m) { return (lockstate == DN_LOCK_XLOCK) && m != xlockedby; }
  bool is_xlockedbyme(Message *m) { return (lockstate == DN_LOCK_XLOCK) && m == xlockedby; }
  bool is_prexlockbyother(Message *m) {
	return (lockstate == DN_LOCK_PREXLOCK) && m != xlockedby;
  }
  
  // pins
  void pin() { npins++; }
  void unpin() { npins--; assert(npins >= 0); }
  bool is_pinnable() { return lockstate == DN_LOCK_SYNC; }
  bool is_pinned() { return npins>0; }
  int num_pins() { return npins; }

  // -- hierarchy
  void remove();

  friend class CDir;
};

ostream& operator<<(ostream& out, CDentry& dn);


#endif
