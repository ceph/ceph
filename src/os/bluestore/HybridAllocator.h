// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>

#include "AvlAllocator.h"
#include "Btree2Allocator.h"
#include "BitmapAllocator.h"

template <typename PrimaryAllocator>
class HybridAllocatorBase : public PrimaryAllocator {
  std::unique_ptr<BitmapAllocator> bmap_alloc;
public:
  HybridAllocatorBase(CephContext* cct, int64_t device_size, int64_t _block_size,
                      uint64_t max_mem,
	              std::string_view name) :
      PrimaryAllocator(cct, device_size, _block_size, max_mem, name) {
  }
  ~HybridAllocatorBase() = default;
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents) override;
  using PrimaryAllocator::release;
  uint64_t get_free() override {
    std::lock_guard l(PrimaryAllocator::get_lock());
    return (bmap_alloc ? bmap_alloc->get_free() : 0) +
      PrimaryAllocator::_get_free();
  }

  double get_fragmentation() override {
    std::lock_guard l(PrimaryAllocator::get_lock());
    auto f = PrimaryAllocator::_get_fragmentation();
    auto bmap_free = bmap_alloc ? bmap_alloc->get_free() : 0;
    if (bmap_free) {
      auto _free = PrimaryAllocator::_get_free() + bmap_free;
      auto bf = bmap_alloc->get_fragmentation();

      f = f * PrimaryAllocator::_get_free() / _free + bf * bmap_free / _free;
    }
    return f;
  }

  void dump() override;

  void foreach(
      std::function<void(uint64_t, uint64_t)> notify) override {
    std::lock_guard l(PrimaryAllocator::get_lock());
    PrimaryAllocator::_foreach(notify);
    if (bmap_alloc) {
      bmap_alloc->foreach(notify);
    }
  }
  void init_rm_free(uint64_t offset, uint64_t length) override;
  void shutdown() override {
    std::lock_guard l(PrimaryAllocator::get_lock());
    PrimaryAllocator::_shutdown();
    if (bmap_alloc) {
      bmap_alloc.reset();
    }
  }

protected:
  // intended primarily for UT
  BitmapAllocator* get_bmap() {
    return bmap_alloc.get();
  }
  const BitmapAllocator* get_bmap() const {
    return bmap_alloc.get();
  }
private:
  void _spillover_range(uint64_t start, uint64_t end) override;
  uint64_t _spillover_allocate(uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector* extents) override;

  // Allocates up to 'want' bytes from primary or secondary allocator.
  // Returns:
  // 0 (and unmDodified extents) if error occurred or nothing
  //     has been allocated. 'extents' vector remains unmodified
  // amount of allocated bytes (<= want) if something has been allocated,
  //  'extents' vector gets new extents, existing ones are preserved.
  uint64_t _allocate_or_rollback(bool primary,
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector* extents);

  uint64_t _get_spilled_over() const override {
    return bmap_alloc ? bmap_alloc->get_free() : 0;
  }

  // called when extent to be released/marked free
  void _add_to_tree(uint64_t start, uint64_t size) override {
    if (bmap_alloc) {
      uint64_t head = bmap_alloc->claim_free_to_left(start);
      uint64_t tail = bmap_alloc->claim_free_to_right(start + size);
      ceph_assert(head <= start);
      start -= head;
      size += head + tail;
    }
    PrimaryAllocator::_add_to_tree(start, size);
  }
};

class HybridAvlAllocator : public HybridAllocatorBase<AvlAllocator> {
public:
  HybridAvlAllocator(CephContext* cct, int64_t device_size, int64_t _block_size,
    uint64_t max_mem,
    std::string_view name) :
    HybridAllocatorBase<AvlAllocator>(cct,
      device_size, _block_size, max_mem, name) {
  }
  const char* get_type() const override;
};


class HybridBtree2Allocator : public HybridAllocatorBase<Btree2Allocator>
{
public:
  HybridBtree2Allocator(CephContext* cct,
    int64_t device_size,
    int64_t _block_size,
    uint64_t max_mem,
    double weight_factor,
    std::string_view name) :
      HybridAllocatorBase<Btree2Allocator>(cct,
					  device_size,
					  _block_size,
					  max_mem,
					  name) {
    set_weight_factor(weight_factor);
  }
  const char* get_type() const override;

  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector* extents) override;
  void release(const release_set_t& release_set) override;
};
