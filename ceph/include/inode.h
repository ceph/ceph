
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

typedef __uint64_t inodeno_t;

struct inode_t {
  inodeno_t ino;

  __uint64_t size;
  __uint32_t mode;
};

#endif
