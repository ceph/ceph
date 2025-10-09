#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

#include <string>
#include <unordered_map>
#include <vector>

class Dentry;
struct Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  std::unordered_map<std::string, Dentry*> dentries;
  unsigned num_null_dentries = 0;

  std::vector<Dentry*> readdir_cache;

  explicit Dir(Inode* in) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

#endif
