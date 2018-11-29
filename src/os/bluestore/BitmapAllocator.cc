// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BitmapAllocator.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "fbmap_alloc " << this << " "

#ifdef WITH_LTTNG_LOGGING
#include "include/tracing/bitmapallocator_impl.h"
#else
#define trace(...)
#define trace_error(...)
#endif

BitmapAllocator::BitmapAllocator(CephContext* _cct,
					 int64_t capacity,
					 int64_t alloc_unit) :
    cct(_cct)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << capacity << "/"
		 << alloc_unit << std::dec << dendl;
  _init(capacity, alloc_unit, false);
}

int64_t BitmapAllocator::allocate(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, PExtentVector *extents)
{
  uint64_t allocated = 0;
  size_t old_size = extents->size();
  ldout(cct, 10) << __func__ << std::hex << " 0x" << want_size
		 << "/" << alloc_unit << "," << max_alloc_size << "," << hint
		 << std::dec << dendl;
  trace_allocate_info(10, bitmapallocator,
    uint64_t, want_size, want_size,
    uint64_t, alloc_unit, alloc_unit,
    uint64_t, max_alloc_size, max_alloc_size,
    int64_t, hint, hint,
    "0x{0:x}/{1:x},{2:x},{3:x}");
    
  _allocate_l2(want_size, alloc_unit, max_alloc_size, hint,
    &allocated, extents);
  if (!allocated) {
    return -ENOSPC;
  }
  for (auto i = old_size; i < extents->size(); ++i) {
    auto& e = (*extents)[i];
    ldout(cct, 10) << __func__
                   << " extent: 0x" << std::hex << e.offset << "~" << e.length
		   << "/" << alloc_unit << "," << max_alloc_size << "," << hint
		   << std::dec << dendl;
    trace_allocate_extents(10, bitmapallocator,
      uint64_t, offset, e.offset,
      uint64_t, length, e.length,
      uint64_t, alloc_unit, alloc_unit,
      uint64_t, max_alloc_size, max_alloc_size,
      int, hint, hint,
      "0x{0:x}/{1:x},{2:x},{3:x},{4:x}");
  }
  return int64_t(allocated);
}

void BitmapAllocator::release(
  const interval_set<uint64_t>& release_set)
{
  for (auto r : release_set) {
//    ldout(cct, 10) << __func__ << " 0x" << std::hex << r.first << "~" << r.second
//		  << std::dec << dendl;
    trace_release_info(10, bitmapallocator,
      uint64_t, first, r.first,
      uint64_t, second, r.second,
      "0x{0:x}~{1:x}");
  }
  _free_l2(release_set);
//  ldout(cct, 10) << __func__ << " done" << dendl;
  trace_release_done(10, bitmapallocator, "done");
}


void BitmapAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		  << std::dec << dendl;

  auto mas = get_min_alloc_size();
  uint64_t offs = round_up_to(offset, mas);
  uint64_t l = p2align(offset + length - offs, mas);

  _mark_free(offs, l);
  ldout(cct, 10) << __func__ << " done" << dendl;
}
void BitmapAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << " 0x" << std::hex << offset << "~" << length
		 << std::dec << dendl;
  auto mas = get_min_alloc_size();
  uint64_t offs = round_up_to(offset, mas);
  uint64_t l = p2align(offset + length - offs, mas);
  _mark_allocated(offs, l);
  ldout(cct, 10) << __func__ << " done" << dendl;
}

void BitmapAllocator::shutdown()
{
  ldout(cct, 1) << __func__ << dendl;
  _shutdown();
}

void BitmapAllocator::dump()
{
  // bin -> interval count
  std::map<size_t, size_t> bins_overall;
  collect_stats(bins_overall);
  auto it = bins_overall.begin();
  while (it != bins_overall.end()) {
    ldout(cct, 0) << __func__
	            << " bin " << it->first
		    << "(< " << byte_u_t((1 << (it->first + 1)) * get_min_alloc_size()) << ")"
		    << " : " << it->second << " extents"
		    << dendl;
    ++it;
  }
}
