// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "seastar/core/metrics_registration.hh"

namespace crimson::os::seastore {

class BackgroundListener;
class TransactionManager;
class ExtentPlacementManager;

class LogicalAddressCache {
public:
  LogicalAddressCache();
  ~LogicalAddressCache();

  void move_to_top(laddr_t laddr, extent_len_t length, extent_types_t type);
  void move_to_top_if_cached(laddr_t laddr, extent_len_t length, extent_types_t type);
  void remove(laddr_t laddr, extent_len_t length, extent_types_t type);

  bool should_start_evict() const {
    return sizeof(laddr_bucket_t) * laddr_lru.size() >= buckets_memory_capacity;
  }
  std::size_t get_evict_size_per_cycle() const {
    return buckets_memory_capacity;
  }

  void set_epm(ExtentPlacementManager *epm_) {
    epm = epm_;
  }
  void set_background_callback(BackgroundListener *listener_) {
    listener = listener_;
  }
  void set_transaction_manager(TransactionManager *tm_) {
    tm = tm_;
  }

  seastar::future<> evict();

private:
  struct laddr_bucket_t : boost::intrusive_ref_counter<
      laddr_bucket_t, boost::thread_unsafe_counter> {
    laddr_bucket_t(laddr_t laddr, extent_len_t length, extent_types_t type)
      : laddr(laddr), length(length), type(type) {}

    friend auto operator<=>(const laddr_bucket_t &lhs,
			    const laddr_bucket_t &rhs) {
      return lhs.laddr <=> rhs.laddr;
    }

    struct bucket_key_t {
      using type = laddr_t;
      const type& operator()(const laddr_bucket_t& bucket) {
	return bucket.laddr;
      }
    };

    boost::intrusive::set_member_hook<> set_hook;
    boost::intrusive::list_member_hook<> list_hook;

    laddr_t laddr;
    extent_len_t length;
    extent_types_t type;

    friend std::ostream& operator<<(std::ostream& out, const laddr_bucket_t& bucket) {
      return out << "laddr_bucket_t(laddr=" << bucket.laddr
		 << ", length=" << bucket.length
		 << ", extent_type=" << bucket.type
		 << ")";
    }
  };
  using laddr_bucket_ref_t = boost::intrusive_ptr<laddr_bucket_t>;
  using laddr_list_t = boost::intrusive::list<
    laddr_bucket_t,
    boost::intrusive::member_hook<
      laddr_bucket_t,
      boost::intrusive::list_member_hook<>,
      &laddr_bucket_t::list_hook>>;
  using laddr_set_t = boost::intrusive::set<
    laddr_bucket_t,
    boost::intrusive::key_of_value<laddr_bucket_t::bucket_key_t>,
    boost::intrusive::member_hook<
      laddr_bucket_t,
      boost::intrusive::set_member_hook<>,
      &laddr_bucket_t::set_hook>>;

  laddr_bucket_t* find_bucket(laddr_t laddr);
  laddr_bucket_ref_t create_bucket(laddr_t laddr, extent_len_t len, extent_types_t type);
  void release_bucket(laddr_bucket_ref_t bucket);

  void move_to_top(laddr_bucket_t &bucket);
  void check_bucket(const laddr_bucket_t &bucket,
		    laddr_t laddr,
		    extent_len_t len,
		    extent_types_t type) {
    ceph_assert(bucket.laddr <= laddr);
    ceph_assert(bucket.laddr + bucket.length >= laddr + len);
    ceph_assert(bucket.type == type);
  }

  ExtentPlacementManager *epm;
  BackgroundListener *listener;
  TransactionManager *tm;

  laddr_list_t laddr_lru;
  laddr_set_t laddr_set;

  std::size_t buckets_memory_capacity;
  std::size_t evict_size_per_cycle;

  struct {
    std::size_t evicted_bucket;
    std::size_t evicted_size;
  } stats;
  seastar::metrics::metric_group metrics;

  void register_metrics();

  struct eviction_state_t {
    std::list<CachedExtentRef> cold_extents;
    std::size_t cold_extents_size;

    std::list<laddr_bucket_ref_t> cold_buckets;
    std::list<laddr_bucket_ref_t> completed_buckets;
    std::list<laddr_bucket_ref_t>::iterator cold_bucket_iter;

    lba_pin_list_t cold_pin_list;
    std::unordered_map<laddr_t, extent_types_t> pin_type_map;

    friend std::ostream& operator<<(std::ostream &out,
				    const eviction_state_t &state) {
      return out << "eviction_state_t(cold_extents_count="
		 << state.cold_extents.size()
		 << ", cold_extents_size=" << state.cold_extents_size
		 << ", cold_buckets_count=" << state.cold_buckets.size()
		 << ", completed_buckets=" << state.completed_buckets.size()
		 << ")";
    }

    void reset() {
      cold_extents.clear();
      cold_extents_size = 0;
      cold_buckets.clear();
      completed_buckets.clear();
      cold_bucket_iter = cold_buckets.end();
      cold_pin_list.clear();
      pin_type_map.clear();
    }
  } evict_state;
};
using LogicalAddressCacheRef = std::unique_ptr<LogicalAddressCache>;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LogicalAddressCache::eviction_state_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::LogicalAddressCache::laddr_bucket_t> : fmt::ostream_formatter {};
#endif
