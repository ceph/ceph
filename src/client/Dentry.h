#ifndef CEPH_CLIENT_DENTRY_H
#define CEPH_CLIENT_DENTRY_H

#include "include/lru.h"
#include "include/xlist.h"

#include "mds/mdstypes.h"
#include "Inode.h"
#include "InodeRef.h"

class Dentry : public LRUObject {
public:
  explicit Dentry(const std::string &name) : name(name), inode_xlist_link(this) {}
  ~Dentry() {
    assert(ref == 0);
    inode_xlist_link.remove_myself();
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
  void link(InodeRef in) {
    inode = in;
    inode->dentries.push_back(&inode_xlist_link);
    if (inode->is_dir()) {
      if (inode->dir)
        get(); // dir -> dn pin
      if (inode->ll_ref)
        get(); // ll_ref -> dn pin
    }
  }
  void unlink(void) {
    if (inode->is_dir()) {
      if (inode->dir)
        put(); // dir -> dn pin
      if (inode->ll_ref)
        put(); // ll_ref -> dn pin
    }
    assert(inode_xlist_link.get_list() == &inode->dentries);
    inode_xlist_link.remove_myself();
    inode.reset();
  }

  void dump(Formatter *f) const;
  friend std::ostream &operator<<(std::ostream &oss, const Dentry &Dentry);

  string   name;                      // sort of lame
  class Dir	   *dir = nullptr;
  InodeRef inode;
  int	   ref = 1; // 1 if there's a dir beneath me.
  int64_t offset = 0;
  mds_rank_t lease_mds = -1;
  utime_t lease_ttl;
  uint64_t lease_gen = 0;
  ceph_seq_t lease_seq = 0;
  int cap_shared_gen = 0;

private:
  xlist<Dentry *>::item inode_xlist_link;
};

#endif
