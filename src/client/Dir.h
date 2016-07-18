#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

struct Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  ceph::unordered_map<string, Dentry*> dentries;
  vector<Dentry*> readdir_cache;

  explicit Dir(Inode* in) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

#endif
