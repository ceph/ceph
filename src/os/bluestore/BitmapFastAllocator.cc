// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BitmapFastAllocator.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "fbmap_alloc 0x" << this << " "

BitmapFastAllocator::BitmapFastAllocator(CephContext* _cct,
					 int64_t capacity,
					 int64_t alloc_unit) :
    cct(_cct)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << capacity << "/"
		 << alloc_unit << std::dec << dendl;
  _init(capacity, alloc_unit, false);
}

int64_t BitmapFastAllocator::allocate(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, PExtentVector *extents)
{
  uint64_t allocated = 0;

  _allocate_l2(want_size, alloc_unit, max_alloc_size, hint,
    &allocated, extents);
  if (!allocated) {
    return -ENOSPC;
  }
  for (auto e : *extents) {
    ldout(cct, 10) << __func__
                   << " 0x" << std::hex << e.offset << "~" << e.length
		   << "/" << alloc_unit << "," << max_alloc_size << "," << hint
		   << std::dec << dendl;
  }
  return int64_t(allocated);
}

void BitmapFastAllocator::release(
  const interval_set<uint64_t>& release_set)
{
  for (auto r : release_set) {
    ldout(cct, 10) << __func__ << " 0x" << std::hex << r.first << "~" << r.second
		  << std::dec << dendl;
  }
  _free_l2(release_set);
}


void BitmapFastAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		  << std::dec << dendl;

  auto mas = get_min_alloc_size();
  uint64_t offs = round_up_to(offset, mas);
  uint64_t l = p2align(offset + length - offs, mas);

  _mark_free(offs, l);
}
void BitmapFastAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  auto mas = get_min_alloc_size();
  uint64_t offs = round_up_to(offset, mas);
  uint64_t l = p2align(offset + length - offs, mas);
  _mark_allocated(offs, l);
}

void BitmapFastAllocator::shutdown()
{
  ldout(cct, 1) << __func__ << dendl;
  _shutdown();
}
