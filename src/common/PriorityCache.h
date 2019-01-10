// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_PRIORITY_CACHE_H
#define CEPH_PRIORITY_CACHE_H

#include <stdint.h>
#include <string>

namespace PriorityCache {
  enum Priority {
    PRI0,  // Reserved for special items
    PRI1,  // High priority cache items
    PRI2,  // Medium priority cache items
    PRI3,  // Low priority cache items
    LAST = PRI3,
  };

  int64_t get_chunk(uint64_t usage, uint64_t total_bytes);

  struct PriCache {
    virtual ~PriCache();

    /* Ask the cache to request memory for the given priority. Note that the
     * cache may ultimately be allocated less memory than it requests here.
     */
    virtual int64_t request_cache_bytes(PriorityCache::Priority pri, uint64_t total_cache) const = 0;

    // Get the number of bytes currently allocated to the given priority.
    virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const = 0;

    // Get the number of bytes currently allocated to all priorities.
    virtual int64_t get_cache_bytes() const = 0;

    // Allocate bytes for a given priority.
    virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) = 0;

    // Allocate additional bytes for a given priority.
    virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) = 0;

    /* Commit the current number of bytes allocated to the cache.  Space is
     * allocated in chunks based on the allocation size and current total size
     * of memory available for caches. */
    virtual int64_t commit_cache_size(uint64_t total_cache) = 0;

    /* Get the current number of bytes allocated to the cache. this may be
     * larger than the value returned by get_cache_bytes as it includes extra
     * space for future growth. */
    virtual int64_t get_committed_size() const = 0;

    // Get the ratio of available memory this cache should target.
    virtual double get_cache_ratio() const = 0;

    // Set the ratio of available memory this cache should target.
    virtual void set_cache_ratio(double ratio) = 0;

    // Get the name of this cache.
    virtual std::string get_cache_name() const = 0;
  };
}

#endif
