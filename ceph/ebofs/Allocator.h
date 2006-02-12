// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

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
