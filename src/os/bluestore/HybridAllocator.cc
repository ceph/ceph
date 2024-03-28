// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "HybridAllocator.h"

#include <bit>
#include <limits>

#include "common/config_proxy.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << "HybridAllocator "


int64_t HybridAllocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint,
  PExtentVector* extents)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " 0x" << want
                 << "/" << unit
                 << "," << max_alloc_size
                 << "," << hint
                 << std::dec << dendl;
  ceph_assert(std::has_single_bit(unit));
  ceph_assert(want % unit == 0);

  if (max_alloc_size == 0) {
    max_alloc_size = want;
  }
  if (constexpr auto cap = std::numeric_limits<decltype(bluestore_pextent_t::length)>::max();
      max_alloc_size >= cap) {
    max_alloc_size = p2align(uint64_t(cap), (uint64_t)get_block_size());
  }

  int64_t res;

  typedef
    std::function<int64_t(uint64_t, uint64_t, uint64_t, int64_t, PExtentVector*)>
    alloc_fn;
  alloc_fn priA = [&](uint64_t _want,
                      uint64_t _unit,
                      uint64_t _max_alloc_size,
                      int64_t  _hint,
                      PExtentVector* _extents) {
    return _allocate(_want, _unit, _max_alloc_size, _hint, _extents);
  };
  alloc_fn secA = [&](uint64_t _want,
                      uint64_t _unit,
                      uint64_t _max_alloc_size,
                      int64_t  _hint,
                      PExtentVector* _extents) {
    return bmap_alloc ?
      bmap_alloc->allocate(_want, _unit, _max_alloc_size, _hint, _extents) :
      0;
  };

  std::lock_guard l(lock);
  // try bitmap first to avoid unneeded contiguous extents split if
  // desired amount is less than shortes range in AVL
  if (bmap_alloc && bmap_alloc->get_free() &&
    want < _lowest_size_available()) {
    std::swap(priA, secA);
  }

  {
    auto orig_size = extents->size();
    res = priA(want, unit, max_alloc_size, hint, extents);
    if (res < 0) {
      // allocator shouldn't return new extents on error
      ceph_assert(orig_size == extents->size());
      res = 0;
    }
  }
  if ((uint64_t)res < want) {
    auto orig_size = extents->size();
    auto res2 = secA(want - res, unit, max_alloc_size, hint, extents);
    if (res2 > 0) {
      res += res2;
    } else {
      ceph_assert(orig_size == extents->size());
    }
  }
  return res ? res : -ENOSPC;
}

void HybridAllocator::release(const interval_set<uint64_t>& release_set) {
  std::lock_guard l(lock);
  // this will attempt to put free ranges into AvlAllocator first and
  // fallback to bitmap one via _try_insert_range call
  _release(release_set);
}

uint64_t HybridAllocator::get_free()
{
  std::lock_guard l(lock);
  return (bmap_alloc ? bmap_alloc->get_free() : 0) + _get_free();
}

double HybridAllocator::get_fragmentation()
{
  std::lock_guard l(lock);
  auto f = AvlAllocator::_get_fragmentation();
  auto bmap_free = bmap_alloc ? bmap_alloc->get_free() : 0;
  if (bmap_free) {
    auto _free = _get_free() + bmap_free;
    auto bf = bmap_alloc->get_fragmentation();

    f = f * _get_free() / _free + bf * bmap_free / _free;
  }
  return f;
}

void HybridAllocator::dump()
{
  std::lock_guard l(lock);
  AvlAllocator::_dump();
  if (bmap_alloc) {
    bmap_alloc->dump();
  }
  ldout(cct, 0) << __func__
    << " avl_free: " << _get_free()
    << " bmap_free: " << (bmap_alloc ? bmap_alloc->get_free() : 0)
    << dendl;
}

void HybridAllocator::foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify)
{
  std::lock_guard l(lock);
  AvlAllocator::_foreach(notify);
  if (bmap_alloc) {
    bmap_alloc->foreach(notify);
  }
}

void HybridAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  if (!length)
    return;
  std::lock_guard l(lock);
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  _try_remove_from_tree(offset, length,
    [&](uint64_t o, uint64_t l, bool found) {
      if (!found) {
        if (bmap_alloc) {
          bmap_alloc->init_rm_free(o, l);
        } else {
          lderr(cct) << "init_rm_free lambda " << std::hex
            << "Uexpected extent: "
            << " 0x" << o << "~" << l
            << std::dec << dendl;
          ceph_assert(false);
        }
      }
    });
}

void HybridAllocator::shutdown()
{
  std::lock_guard l(lock);
  _shutdown();
  if (bmap_alloc) {
    bmap_alloc->shutdown();
    delete bmap_alloc;
    bmap_alloc = nullptr;
  }
}

void HybridAllocator::_spillover_range(uint64_t start, uint64_t end)
{
  auto size = end - start;
  dout(20) << __func__
    << std::hex << " "
    << start << "~" << size
    << std::dec
    << dendl;
  ceph_assert(size);
  if (!bmap_alloc) {
    dout(1) << __func__
      << " constructing fallback allocator"
      << dendl;
    bmap_alloc = new BitmapAllocator(cct,
      get_capacity(),
      get_block_size(),
      get_name() + ".fallback");
  }
  bmap_alloc->init_add_free(start, size);
}

void HybridAllocator::_add_to_tree(uint64_t start, uint64_t size)
{
  if (bmap_alloc) {
    uint64_t head = bmap_alloc->claim_free_to_left(start);
    uint64_t tail = bmap_alloc->claim_free_to_right(start + size);
    ceph_assert(head <= start);
    start -= head;
    size += head + tail;
  }
  AvlAllocator::_add_to_tree(start, size);
}
