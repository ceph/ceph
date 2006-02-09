#ifndef __EBOFS_ALLOCATOR_H
#define __EBOFS_ALLOCATOR_H

#include "types.h"

#include "include/interval_set.h"

class Ebofs;

class Allocator {
 protected:
  Ebofs *fs;

  interval_set<block_t> limbo;

  static int pick_bucket(block_t num) {
	int b = 0;
	while (num > 1) {
	  b++;
	  num = num >> EBOFS_FREE_BUCKET_BITS;
	}
	if (b >= EBOFS_NUM_FREE_BUCKETS)
	  b = EBOFS_NUM_FREE_BUCKETS-1;
	return b;
  }

  int find(Extent& ex, int bucket, block_t num, block_t near);

  void dump_freelist();

  int _release_loner(Extent& ex);  // release loner extent
  int _release_merge(Extent& ex);  // release any extent (searches for adjacent)

 public:
  Allocator(Ebofs *f) : fs(f) {}
  
  int allocate(Extent& ex, block_t num, block_t near=0);
  int release(Extent& ex);

  int commit_limbo();  // limbo -> fs->limbo_tab
  int release_limbo(); // fs->limbo_tab -> free_tabs

};

#endif
