// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>

#include "AvlAllocator.h"
#include "BitmapAllocator.h"

class HybridAllocator : public AvlAllocator {
  BitmapAllocator* bmap_alloc = nullptr;
public:
  HybridAllocator(CephContext* cct, int64_t device_size, int64_t _block_size,
                  uint64_t max_mem,
	          const std::string& name) :
      AvlAllocator(cct, device_size, _block_size, max_mem, name) {
  }
  const char* get_type() const override
  {
    return "hybrid";
  }
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents) override;
  void release(const interval_set<uint64_t>& release_set) override;
  uint64_t get_free() override;
  double get_fragmentation() override;

  void dump() override;
  void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;
  void shutdown() override;

protected:
  // intended primarily for UT
  BitmapAllocator* get_bmap() {
    return bmap_alloc;
  }
  const BitmapAllocator* get_bmap() const {
    return bmap_alloc;
  }
private:

  void _spillover_range(uint64_t start, uint64_t end) override;

  // called when extent to be released/marked free
  void _add_to_tree(uint64_t start, uint64_t size) override;
};
