#ifndef CEPH_CLIENT_DIR_H
#define CEPH_CLIENT_DIR_H

class Inode;

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  ceph::unordered_map<string, Dentry*> dentries;
  map<string, Dentry*> dentry_map;
  uint64_t release_count;
  uint64_t max_offset;

  Dir(Inode* in) : release_count(0), max_offset(2) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

#endif
