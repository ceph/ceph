#ifndef CEPH_CLIENT_FH_H
#define CEPH_CLIENT_FH_H

#include "common/Readahead.h"
#include "include/types.h"
#include "InodeRef.h"
#include "UserPerm.h"
#include "mds/flock.h"

class Cond;
class Inode;

// file handle for any open file state

struct Fh {
  InodeRef  inode;
  int	    _ref;
  loff_t    pos;
  int       mds;        // have to talk to mds we opened with (for now)
  int       mode;       // the mode i opened the file with

  int flags;
  bool pos_locked;           // pos is currently in use
  list<Cond*> pos_waiters;   // waiters for pos

  UserPerm actor_perms; // perms I opened the file with

  Readahead readahead;

  // file lock
  std::unique_ptr<ceph_lock_state_t> fcntl_locks;
  std::unique_ptr<ceph_lock_state_t> flock_locks;

  // IO error encountered by any writeback on this Inode while
  // this Fh existed (i.e. an fsync on another Fh will still show
  // up as an async_err here because it could have been the same
  // bytes we wrote via this Fh).
  int async_err = {0};

  int take_async_err()
  {
      int e = async_err;
      async_err = 0;
      return e;
  }

  Fh() = delete;
  Fh(InodeRef in, int flags, int cmode, const UserPerm &perms);
  ~Fh();

  void get() { ++_ref; }
  int put() { return --_ref; }
};


#endif
