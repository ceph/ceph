
#ifndef __MDS_H
#define __MDS_H

#include <sys/types.h>

// raw inode
struct inode_t {
  __uint64_t ino;

  __uint64_t size;
  __uint32_t mode;
};




// 
class CInode;

class CMDS {
 protected:
  int          nodeid;
  int          num_nodes;

  // cache
  CInode      *root;
  
  
 public:
  CMDS() {

	root = NULL;
  }
};





#endif
