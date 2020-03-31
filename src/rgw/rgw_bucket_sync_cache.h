// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "common/intrusive_lru.h"
#include "rgw_data_sync.h"

namespace rgw::bucket_sync {

// per bucket-shard state cached by DataSyncShardCR
struct State {
  // the source bucket shard to sync
  rgw_bucket_shard key;
  // current sync obligation being processed by DataSyncSingleEntry
  std::optional<rgw_data_sync_obligation> obligation;
  // highest timestamp applied by all sources
  ceph::real_time progress_timestamp;

  State(const rgw_bucket_shard& key) noexcept : key(key) {}
};

struct Entry;
struct EntryToKey;
class Handle;

using lru_config = ceph::common::intrusive_lru_config<
    rgw_bucket_shard, Entry, EntryToKey>;

// a recyclable cache entry
struct Entry : State, ceph::common::intrusive_lru_base<lru_config> {
  using State::State;
};

struct EntryToKey {
  using type = rgw_bucket_shard;
  const type& operator()(const Entry& e) { return e.key; }
};

// use a non-atomic reference count since these aren't shared across threads
template <typename T>
using thread_unsafe_ref_counter = boost::intrusive_ref_counter<
    T, boost::thread_unsafe_counter>;

// a state cache for entries within a single datalog shard
class Cache : public thread_unsafe_ref_counter<Cache> {
  ceph::common::intrusive_lru<lru_config> cache;
 protected:
  // protected ctor to enforce the use of factory function create()
  explicit Cache(size_t target_size) {
    cache.set_target_size(target_size);
  }
 public:
  static boost::intrusive_ptr<Cache> create(size_t target_size) {
    return new Cache(target_size);
  }

  // find or create a cache entry for the given key, and return a Handle that
  // keeps it lru-pinned until destruction
  Handle get(const rgw_bucket_shard& key);
};

// a State handle that keeps the Cache referenced
class Handle {
  boost::intrusive_ptr<Cache> cache;
  boost::intrusive_ptr<Entry> entry;
 public:
  Handle() noexcept = default;
  ~Handle() = default;
  Handle(boost::intrusive_ptr<Cache> cache,
         boost::intrusive_ptr<Entry> entry) noexcept
    : cache(std::move(cache)), entry(std::move(entry)) {}
  Handle(Handle&&) noexcept = default;
  Handle(const Handle&) noexcept = default;
  Handle& operator=(Handle&& o) noexcept {
    // move the entry first so that its cache stays referenced over destruction
    entry = std::move(o.entry);
    cache = std::move(o.cache);
    return *this;
  }
  Handle& operator=(const Handle& o) noexcept {
    // copy the entry first so that its cache stays referenced over destruction
    entry = o.entry;
    cache = o.cache;
    return *this;
  }

  explicit operator bool() const noexcept { return static_cast<bool>(entry); }
  State& operator*() const noexcept { return *entry; }
  State* operator->() const noexcept { return entry.get(); }
};

inline Handle Cache::get(const rgw_bucket_shard& key)
{
  auto result = cache.get_or_create(key);
  return {this, std::move(result.first)};
}

} // namespace rgw::bucket_sync
