// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <array>
#include <cassert>
#include <new>
#include <vector>

#include <spdk/env.h>

#include "include/buffer_raw.h"
#include "include/intarith.h"   // CEPH_PAGE_SIZE

namespace crimson::os::seastore {

namespace detail {

// Per-shard recycling pool of DMA-capable hugepage buffers, bucketed by
// power-of-two capacity.
//
// The SPDK write/read paths allocate a DMA buffer for every extent, journal
// record and coalescing bounce. Going through the DPDK heap allocator each time
// (spdk_dma_malloc -> malloc_heap_alloc -> find_suitable_element) is a free-list
// walk taken under the heap's lock; profiling the SPDK reactor showed it
// dominating the OSD core. Recycling freed buffers keeps the hot path off the
// DPDK allocator entirely: alloc/free become a vector push/pop on a thread-local
// free list. Buffers are not zeroed (matching the kernel create_page_aligned
// path, which does not zero either; callers that need zeroing do it explicitly).
class dma_pool {
public:
  static constexpr unsigned MIN_SHIFT = 12;            // 4 KiB == CEPH_PAGE_SIZE
  static constexpr unsigned MAX_SHIFT = 22;            // pool up to 4 MiB
  static constexpr unsigned NUM_BUCKETS = MAX_SHIFT - MIN_SHIFT + 1;
  static constexpr size_t   MAX_FREE_BYTES_PER_BUCKET = 32u << 20;  // 32 MiB

  // Returns {ptr, capacity}: capacity >= len, page-aligned, DMA-safe.
  std::pair<char*, unsigned> alloc(unsigned len) {
    const unsigned shift = bucket_shift(len);
    if (shift > MAX_SHIFT) {
      return {raw_alloc(len), len};   // too large to pool
    }
    const unsigned cap = 1u << shift;
    auto& fl = m_free[shift - MIN_SHIFT];
    if (!fl.empty()) {
      char* p = fl.back();
      fl.pop_back();
      return {p, cap};
    }
    return {raw_alloc(cap), cap};
  }

  void free(char* p, unsigned cap) {
    if (!p) {
      return;
    }
    const unsigned shift = bucket_shift(cap);
    if (shift > MAX_SHIFT || (1u << shift) != cap) {
      spdk_dma_free(p);               // non-pooled (oversized / non-power-of-two)
      return;
    }
    auto& fl = m_free[shift - MIN_SHIFT];
    if (((size_t)fl.size() + 1) * cap > MAX_FREE_BYTES_PER_BUCKET) {
      spdk_dma_free(p);               // bucket full; release back to DPDK
      return;
    }
    fl.push_back(p);
  }

private:
  static char* raw_alloc(unsigned len) {
    void* p = spdk_dma_malloc(len, CEPH_PAGE_SIZE, nullptr);
    if (!p) {
      throw std::bad_alloc();
    }
    return static_cast<char*>(p);
  }
  static unsigned bucket_shift(unsigned len) {
    unsigned shift = MIN_SHIFT;
    while ((1u << shift) < len) {
      ++shift;
    }
    return shift;
  }
  std::array<std::vector<char*>, NUM_BUCKETS> m_free;
};

inline dma_pool& tls_dma_pool()
{
  static thread_local dma_pool pool;
  return pool;
}

}  // namespace detail

/**
 * raw_spdk_dma
 *
 * A buffer::raw backed by SPDK's registered hugepage pool, so the memory is
 * DMA-safe for direct submission to an NVMe queue pair with no bounce copy.
 * Backing buffers are drawn from / returned to a per-shard recycling pool
 * (detail::dma_pool) to keep the DPDK heap allocator off the I/O hot path.
 *
 * This deliberately lives in the SPDK-guarded crimson tier rather than in
 * libcommon: src/common/buffer.cc is always compiled and must not depend on
 * SPDK. buffer::raw is public, so the subclass is free to live here.
 */
class raw_spdk_dma : public ceph::buffer::raw {
public:
  explicit raw_spdk_dma(unsigned len)
    : raw(nullptr, len) {
    if (len) {
      auto& pool = detail::tls_dma_pool();
      auto [p, cap] = pool.alloc(len);
      data = p;
      m_capacity = cap;
      m_pool = &pool;
    }
  }
  ~raw_spdk_dma() override {
    // The recycling pool is per-shard (thread_local), so a buffer must be freed
    // on the shard that allocated it. Crimson is shared-nothing, so this holds;
    // the assert catches a stray cross-shard free in debug builds. (The memory
    // is process-global DMA either way -- a wrong-shard free is not a
    // use-after-free, it would only migrate pool accounting -- so we still
    // return it to the local pool rather than touch another shard's vector.)
    assert(m_capacity == 0 || m_pool == &detail::tls_dma_pool());
    detail::tls_dma_pool().free(data, m_capacity);
  }
private:
  unsigned m_capacity = 0;
  detail::dma_pool* m_pool = nullptr;
};

inline ceph::unique_leakable_ptr<ceph::buffer::raw> create_spdk_dma(unsigned len)
{
  return ceph::unique_leakable_ptr<ceph::buffer::raw>(new raw_spdk_dma(len));
}

}
