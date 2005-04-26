
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

  // state
  bool             dirty;
  __uint64_t       parent_dir_version;  // dir version when last touched.

  // locking
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
	dirty = 0;
	parent_dir_version = 0;
  }
  CDentry(const string& n, CInode *in) {
	name = n;
	inode = in;
	lockstate = DN_LOCK_SYNC;
	npins = 0;
	dirty = 0;
	parent_dir_version = 0;
  }

  CInode *get_inode() { return inode; }
  CDir *get_dir() { return dir; }
  const string& get_name() { return name; }
  int get_lockstate() { return lockstate; }
  set<int>& get_gather_set() { return gather_set; }
  bool is_null() { return (inode == 0) ? true:false; }

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

  // misc
  void make_path(string& p);

  // -- state
  __uint64_t get_parent_dir_version() { return parent_dir_version; }
  void float_parent_dir_version(__uint64_t ge) {
	if (parent_dir_version < ge)
	  parent_dir_version = ge;
  }
  
  bool is_dirty() { return dirty; }
  bool is_clean() { return !dirty; }

  void mark_dirty();
  void mark_clean();


  // -- locking
  bool is_sync() { return lockstate == DN_LOCK_SYNC; }
  bool can_read()  { return (lockstate == DN_LOCK_SYNC) || (lockstate == DN_LOCK_UNPINNING);  }
  bool can_read(Message *m) { return is_xlockedbyme(m) || can_read(); }
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

  friend class CDir;
};

ostream& operator<<(ostream& out, CDentry& dn);


#endif
