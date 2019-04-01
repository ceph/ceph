// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include "StupidAllocator.h"
#include "BitmapAllocator.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore

Allocator *Allocator::create(CephContext* cct, string type,
                             int64_t size, int64_t block_size)
{
  if (type == "stupid") {
    return new StupidAllocator(cct);
  } else if (type == "bitmap") {
    return new BitmapAllocator(cct, size, block_size);
  }
  lderr(cct) << "Allocator::" << __func__ << " unknown alloc type "
	     << type << dendl;
  return nullptr;
}

void Allocator::release(const PExtentVector& release_vec)
{
  interval_set<uint64_t> release_set;
  for (auto e : release_vec) {
    release_set.insert(e.offset, e.length);
  }
  release(release_set);
}
