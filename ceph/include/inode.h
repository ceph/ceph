
#ifndef __INODE_H
#define __INODE_H

#include <sys/types.h>

// raw inode
struct inode_t {
  __uint64_t ino;

  __uint64_t size;
  __uint32_t mode;
};

#endif
