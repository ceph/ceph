#ifndef CEPH_CLIENT_DENTRY_H
#define CEPH_CLIENT_DENTRY_H

#include "include/lru.h"
#include "include/xlist.h"

#include "mds/mdstypes.h"
#include "InodeRef.h"

class Dir;
struct Inode;

class Dentry : public LRUObject {
public:
  ~Dentry() {
    assert(ref == 0);
  }

  /*
   * ref==1 -> cached, unused
   * ref >1 -> pinned in lru
   */
  void get() {
    assert(ref > 0);
    if (++ref == 2)
      lru_pin();
    //cout << "dentry.get on " << this << " " << name << " now " << ref << std::endl;
  }
  void put() {
    assert(ref > 0);
    if (--ref == 1)
      lru_unpin();
    //cout << "dentry.put on " << this << " " << name << " now " << ref << std::endl;
    if (ref == 0)
      delete this;
  }

  void dump(Formatter *f) const;

  string   name;                      // sort of lame
  Dir	   *dir = nullptr;
  InodeRef inode;
  int	   ref = 1; // 1 if there's a dir beneath me.
  int64_t offset = 0;
  mds_rank_t lease_mds = -1;
  utime_t lease_ttl;
  uint64_t lease_gen = 0;
  ceph_seq_t lease_seq = 0;
  int cap_shared_gen = 0;
};

#endif
