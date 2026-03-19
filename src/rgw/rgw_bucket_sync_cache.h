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
#include "common/ceph_time.h"

namespace rgw::bucket_sync {

// per bucket-shard state cached by DataSyncShardCR
struct ShardState {
  using key_type = std::pair<rgw_bucket_shard, std::optional<uint64_t>>;
  // the source bucket shard to sync
  key_type key;
  // current sync obligation being processed by DataSyncSingleEntry
  std::optional<rgw_data_sync_obligation> obligation;
  // incremented with each new obligation
  uint32_t counter = 0;
  // highest timestamp applied by all sources
  ceph::real_time progress_timestamp;

  ShardState(const key_type& key) noexcept
  : key(key) {}
  ShardState(const rgw_bucket_shard& shard, std::optional<uint64_t> gen) noexcept
    : key(shard, gen) {}
};

// per bucket state cached by DataSyncShardCR
struct BucketState {
  using key_type = std::pair<std::string, std::optional<uint64_t>>;
  // the source bucket/generation to sync
  key_type key;
  // Last future generation recovery timestamp
  ceph::coarse_mono_time last_future_generation_recovery = ceph::coarse_mono_clock::zero();

  BucketState(const key_type& key) noexcept
    : key(key) {}
  BucketState(std::string bucket, std::optional<uint64_t> gen) noexcept
  : key(std::move(bucket), gen) {}
};


template<typename State>
struct Entry;
template<typename State>
struct EntryToKey;
template<typename State>
class Handle;

template<typename State>
using lru_config = ceph::common::intrusive_lru_config<
    typename State::key_type, Entry<State>, EntryToKey<State>>;

// a recyclable cache entry
template<typename State>
struct Entry : State, ceph::common::intrusive_lru_base<lru_config<State>> {
  using State::State;
};

template<typename State>
struct EntryToKey {
  using type = typename State::key_type;
  const type& operator()(const Entry<State>& e) { return e.key; }
};

// use a non-atomic reference count since these aren't shared across threads
template <typename T>
using thread_unsafe_ref_counter = boost::intrusive_ref_counter<
    T, boost::thread_unsafe_counter>;

// A state cache for entries within a single datalog shard
template<typename State>
class Cache : public thread_unsafe_ref_counter<Cache<State>> {
  ceph::common::intrusive_lru<lru_config<State>> cache;
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
  Handle<State> get(const auto& ...args);
};

// a State handle that keeps the Cache referenced
template<typename State>
class Handle {
  boost::intrusive_ptr<Cache<State>> cache;
  boost::intrusive_ptr<Entry<State>> entry;
 public:
  Handle() noexcept = default;
  ~Handle() = default;
  Handle(boost::intrusive_ptr<Cache<State>> cache,
         boost::intrusive_ptr<Entry<State>> entry) noexcept
    : cache(std::move(cache)), entry(std::move(entry)) {}
  Handle(Handle&&) = default;
  Handle(const Handle&) = default;
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

template<typename State>
inline Handle<State> Cache<State>::get(const auto& ...args)
{
  auto result = cache.get_or_create({ args... });
  return {this, std::move(result.first)};
}

using ShardHandle = Handle<ShardState>;
using ShardCache = Cache<ShardState>;

using BucketHandle = Handle<BucketState>;
using BucketCache = Cache<BucketState>;

} // namespace rgw::bucket_sync
