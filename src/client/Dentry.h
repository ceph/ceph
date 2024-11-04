#ifndef CEPH_CLIENT_DENTRY_H
#define CEPH_CLIENT_DENTRY_H

#include "include/lru.h"
#include "include/xlist.h"

#include "mds/mdstypes.h"
#include "Inode.h"
#include "InodeRef.h"
#include "Dir.h"

class Dentry : public LRUObject {
public:
  explicit Dentry(Dir *_dir, const std::string &_name) :
    dir(_dir), name(_name), inode_xlist_link(this)
  {
    auto r = dir->dentries.insert(make_pair(name, this));
    ceph_assert(r.second);
    dir->num_null_dentries++;
  }
  ~Dentry() {
    ceph_assert(ref == 0);
    ceph_assert(dir == nullptr);
  }

  /*
   * ref==1 -> cached, unused
   * ref >1 -> pinned in lru
   */
  void get() {
    ceph_assert(ref > 0);
    if (++ref == 2)
      lru_pin();
    //cout << "dentry.get on " << this << " " << name << " now " << ref << std::endl;
  }
  void put() {
    ceph_assert(ref > 0);
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
    dir->num_null_dentries--;
  }
  void unlink(void) {
    if (inode->is_dir()) {
      if (inode->dir)
        put(); // dir -> dn pin
      if (inode->ll_ref)
        put(); // ll_ref -> dn pin
    }
    ceph_assert(inode_xlist_link.get_list() == &inode->dentries);
    inode_xlist_link.remove_myself();
    inode.reset();
    dir->num_null_dentries++;
  }
  void mark_primary() {
    if (inode && inode->dentries.front() != this)
      inode->dentries.push_front(&inode_xlist_link);
  }
  void detach(void) {
    ceph_assert(!inode);
    auto p = dir->dentries.find(name);
    ceph_assert(p != dir->dentries.end());
    dir->dentries.erase(p);
    dir->num_null_dentries--;
    dir = nullptr;
  }

  bool make_path_string(std::string& s)
  {
    bool ret = false;

    if (dir) {
      ret = dir->parent_inode->make_path_string(s);
    } else {
      // Couldn't link all the way to our mount point
      return false;
    }
    s += "/";
    s.append(name.data(), name.length());

    return ret;
  }

  void dump(Formatter *f) const;
  friend std::ostream &operator<<(std::ostream &oss, const Dentry &Dentry);

  Dir	   *dir;
  const std::string name;
  InodeRef inode;
  int	   ref = 1; // 1 if there's a dir beneath me.
  int64_t offset = 0;
  mds_rank_t lease_mds = -1;
  utime_t lease_ttl;
  uint64_t lease_gen = 0;
  ceph_seq_t lease_seq = 0;
  int cap_shared_gen = 0;
  std::string alternate_name;
  bool is_renaming = false;

private:
  xlist<Dentry *>::item inode_xlist_link;
};

#endif
