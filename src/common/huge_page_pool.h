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

#include "common/containers.h"

constexpr unsigned long long operator"" _M (unsigned long long n) {
  return n << 20;
}

namespace ceph {

class huge_page_pool {
  std::size_t huge_page_size;

  // TOOD: align to cache line boundary
  struct page_info_entry_t {
    std::uint8_t index;
    bool is_mmaped;
  };

  boost::container::flat_map<void*, page_info_entry_t> page_info;
  ceph::containers::tiny_vector<std::atomic<void*>, 64> pages;

public:
  huge_page_pool(const std::size_t pool_size,
		 const std::size_t huge_page_size)
    : huge_page_size(huge_page_size),
      pages(pool_size, [&](const std::uint8_t idx, auto emplacer) {
        void* page;
        page = ::mmap(nullptr, huge_page_size, PROT_READ | PROT_WRITE,
      		      MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE |
      		      MAP_HUGETLB, -1, 0);
        if (page == MAP_FAILED) {
          // let's fallback-allocate in a way that stil allows the kernel
          // to give us a THP (transparent huge page).
          const int r = ::posix_memalign((void**)(void*)&page,
					 huge_page_size, huge_page_size);
	  if (r) {
	    // there is no jumbo, sorry.
            page = nullptr;
	  } else {
	    page_info[page] = { idx, false };
	  }
	} else {
          page_info[page] = { idx, true };
        }
	emplacer.emplace(page);
      }) {
   }

  ~huge_page_pool() {
    // move this to ptr's deleter, handle free()
    for (const auto& p : pages) {
      if (p) {
	if (page_info.at(p).is_mmaped) {
          ::munmap(p, huge_page_size);
	} else {
	  free(p);
	}
      }
    }
  }

  std::size_t get_size() const {
    return huge_page_size;
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
    const std::uint8_t owner_idx = page_info.at(p).index;
    const void* const oldval = pages[owner_idx].exchange(p);
    assert(oldval == nullptr);
  }
};

}

extern void ceph_init_huge_page_pools(class md_config_t* cct);
extern ceph::huge_page_pool& ceph_get_huge_page_pool();
