
#ifndef __INODE_H
#define __INODE_H

#include <sys/types.h>

#include <ext/hash_map>

// raw inode

namespace __gnu_cxx {
template<> struct hash<unsigned long long> {
  size_t operator()(unsigned long long __x) const { return __x; }
};
}

typedef __uint64_t inodeno_t;   // ino

typedef __uint64_t mdloc_t;     // dir locator?

struct inode_t {
  inodeno_t ino;

  __uint64_t size;
  __uint32_t mode;
  uid_t uid;
  gid_t gid;
  time_t atime, mtime, ctime;
};

#endif
