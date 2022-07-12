// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_TYPES_H
#define CEPH_JOURNAL_TYPES_H

namespace journal {

struct CacheRebalanceHandler {
  virtual ~CacheRebalanceHandler() {
  }

  virtual void handle_cache_rebalanced(uint64_t new_cache_bytes) = 0;
};

struct CacheManagerHandler {
  virtual ~CacheManagerHandler() {
  }

  virtual void register_cache(const std::string &cache_name,
                              uint64_t min_size, uint64_t max_size,
                              CacheRebalanceHandler* handler) = 0;
  virtual void unregister_cache(const std::string &cache_name) = 0;
};

} // namespace journal

#endif // # CEPH_JOURNAL_TYPES_H
