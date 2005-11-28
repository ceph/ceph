#ifndef __EBOFS_ALLOCATOR_H
#define __EBOFS_ALLOCATOR_H

#include "types.h"

class Ebofs;

class Allocator {
 protected:
  Ebofs *fs;

  static int pick_bucket(block_t num) {
	int b = 0;
	while (num > 1) {
	  b++;
	  num = num >> 1;
	}
	if (b >= EBOFS_NUM_FREE_BUCKETS)
	  b = EBOFS_NUM_FREE_BUCKETS-1;
	return b;
  }

  int find(Extent& ex, int bucket, block_t num, block_t near);

 public:
  Allocator(Ebofs *f) : fs(f) {}
  
  int allocate(Extent& ex, block_t num, block_t near=0);
  int release(Extent& ex);
};

#endif
