#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

struct Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  ceph::unordered_map<string, Dentry*> dentries;
  xlist<Dentry*> dentry_list;
  uint64_t release_count;

  Dir(Inode* in) : release_count(0) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

#endif
