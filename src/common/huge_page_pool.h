// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <array>
#include <atomic>
#include <thread>

#include <boost/container/flat_map.hpp>

constexpr unsigned long long operator"" _M (unsigned long long n) {
  return n << 20;
}

namespace ceph {

class huge_page_pool {
public:
  static constexpr std::size_t huge_page_size { 2_M };
  // TOOD: align to cache line boundary
  // TODO: this should a vector of atomic address for the sake
  // of correctness.
  std::array<std::atomic<void*>, 64> pages;
  boost::container::flat_map<void*, std::uint8_t> page2owner;

  huge_page_pool(const std::size_t pool_size) {
    assert(pool_size == pages.size());
    for (std::size_t i = 0; i < pages.size(); i++) {
      pages[i] = ::mmap(nullptr, huge_page_size, PROT_READ | PROT_WRITE,
      		  MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE |
      		  MAP_HUGETLB, -1, 0);
      if (pages[i] == MAP_FAILED) {
        // let's fallback-allocate in a way that stil allows the kernel
        // to give us a THP (transparent huge page).
        const int r = \
          ::posix_memalign((void**)(void*)&pages[i],
      		     huge_page_size, huge_page_size);
        if (r) {
          // there is no jumbo, sorry.
          pages[i] = nullptr;
        }
      }

      if (pages[i] != nullptr) {
        page2owner[pages[i]] = i;
      }
    }
  }

  ~huge_page_pool() {
    // move this to ptr's deleter, handle free()
    for (const auto& p : pages) {
      if (p) {
        ::munmap(p, huge_page_size);
      }
    }
  }

  void* get_page() {
    // let's check our slot own slot first
    // TODO: cache the id calculation in TLS?
    const std::uint8_t tidx = \
      std::hash<std::thread::id>()(std::this_thread::get_id()) % pages.size();
    void* const tval = pages[tidx].exchange(nullptr);
    if (nullptr != tval) {
      return tval;
    }

    // oops, it's not available. Let's iterate through the pool
    // keeping in mind this can cause a cacheline ping-pong
    // between CPUs.
    for (std::size_t idx = 0; idx < pages.size(); idx++) {
      void* const val = pages[idx].exchange(nullptr);
      if (nullptr != val) {
        return val;
      }
    }

    // sorry, pool depleted.
    return nullptr;
  }

  void put_page(void* p) {
    const std::uint8_t owner_idx = page2owner.at(p);
    const void* const oldval = pages[owner_idx].exchange(p);
    assert(oldval == nullptr);
  }
};

}

extern void ceph_init_huge_page_pools(class md_config_t* cct);
extern ceph::huge_page_pool& ceph_get_huge_page_pool();
