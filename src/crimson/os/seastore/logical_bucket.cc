// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar/core/metrics.hh"

#include "crimson/common/coroutine.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/logical_bucket.h"
#include "crimson/os/seastore/transaction_manager.h"

#include <boost/unordered/unordered_flat_map.hpp>

namespace crimson::os::seastore {

SET_SUBSYS(seastore_cache);

class LogicalBucketCache : public LogicalBucket {
public:
  LogicalBucketCache(std::size_t memory_capacity,
                     std::size_t proceed_size_per_cycle)
    : memory_capacity(memory_capacity),
      proceed_size_per_cycle(proceed_size_per_cycle) {
    LOG_PREFIX(LogicalBucketCache);
    INFO("init memory_capacity={}, proceed_size_per_cycle={}",
	 memory_capacity, proceed_size_per_cycle);
    register_metrics();
  }

  ~LogicalBucketCache() {
    clear();
  }

  void move_to_top(
      laddr_t laddr,
      bool create_if_absent) final {
    LOG_PREFIX(LogicalBucketCache::move_to_top);
    assert(laddr != L_ADDR_NULL);
    assert(laddr == laddr.get_object_prefix());
    auto iter = index.find(laddr);
    if (iter != index.end()) {
      TRACE("find bucket: {}", iter->first);
      lru.splice(lru.end(), lru, iter->second);
    } else if (create_if_absent) {
      TRACE("create bucket: {}", laddr);
      index[laddr] = lru.emplace(lru.end(), laddr);
    } else {
      TRACE("prefix {} doesn't exist, skipping", laddr);
    }
  }

  void remove(laddr_t laddr) final {
    LOG_PREFIX(LogicalBucketCache::remove);
    TRACE("laddr: {}", laddr);
    assert(laddr != L_ADDR_NULL);
    assert(laddr == laddr.get_object_prefix());
    auto iter = index.find(laddr);
    if (iter != index.end()) {
      TRACE("remove bucket: {}", laddr);
      lru.erase(iter->second);
      index.erase(iter);
    }
  }

  bool is_cached(laddr_t laddr) final {
    assert(laddr != L_ADDR_NULL);
    assert(laddr == laddr.get_object_prefix());
    return index.contains(laddr);
  }

  void clear() final {
    index.clear();
    lru.clear();
  }

  void set_background_callback(BackgroundListener *l) final {
    listener = l;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) final {
    ecb = cb;
  }

  bool could_demote() const final {
    return !lru.empty();
  }

  bool should_demote() const {
    // lru element: laddr_t + pointer * 2
    // index element: laddr_t + lru iterator(void*)
    return (sizeof(laddr_t) * 2 + sizeof(void*) * 3) > memory_capacity;
  }

  using run_demote_iertr = base_iertr;
  using run_demote_ret = run_demote_iertr::future<>;
  run_demote_ret run_demote(Transaction &t) {
    LOG_PREFIX(LogicalBucketCache::demote);
    std::vector<laddr_t> pending_buckets;
    std::vector<laddr_t> completed_buckets;
    ceph_assert(pending_buckets_target > 0);
    ceph_assert(!lru.empty());
    for (auto &b : lru) {
      if (pending_buckets.size() == pending_buckets_target) {
	break;
      }
      pending_buckets.push_back(b);
    }

    DEBUGT("start demote {} buckets", t, pending_buckets.size());
    std::size_t demoted_size = 0;
    std::size_t evicted_size = 0;
    for (auto &bucket : pending_buckets) {
      TRACET("start demote {}", t, bucket);
      auto res = co_await ecb->demote_region(
	t,
	bucket,
	proceed_size_per_cycle - demoted_size - evicted_size);

      TRACET("demote_size: {}, evicted_size: {}, complete: {}",
	     t, res.demoted_size, res.evicted_size, res.complete);
      demoted_size += res.demoted_size;
      evicted_size += res.evicted_size;
      if (res.complete) {
	completed_buckets.push_back(bucket);
      }
      if (demoted_size + evicted_size >= proceed_size_per_cycle) {
	break;
      }
    }

    co_await ecb->submit_transaction_direct(t);

    DEBUGT("finish demoting {} buckets with {} bytes evicted and {} bytes demoted",
	   t, completed_buckets.size(), evicted_size, demoted_size);
    stat.demoted_bucket_count += completed_buckets.size();
    stat.demoted_size += demoted_size;
    stat.evicted_size += evicted_size;
    for (auto &p : completed_buckets) {
      remove(p);
    }
    auto old_count = pending_buckets_target;
    if (demoted_size != 0 && !completed_buckets.empty()) {
      auto demote_ratio = (double)demoted_size /
	  (double)completed_buckets.size();
      assert(!std::isnan(demote_ratio));
      pending_buckets_target = (proceed_size_per_cycle / demote_ratio) + 1;
    }
    DEBUGT("update init buckets count {} -> {}",
	   t, old_count, pending_buckets_target);
    co_return;
  }

  seastar::future<> demote() final {
    return repeat_eagain([this] {
      return ecb->with_transaction_intr(
        Transaction::src_t::DEMOTE,
        "demote", cache_hint_t::get_nocache(),
        [this](auto &t) {
	  return run_demote(t);
	});
    }).handle_error(crimson::ct_error::assert_all{ "impossible" });
  }

private:
  using laddr_lru_t = std::list<laddr_t>;
  laddr_lru_t lru;
  boost::unordered_flat_map<laddr_t, laddr_lru_t::iterator> index;
  int pending_buckets_target = 20;

  void register_metrics() {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
	sm::make_gauge(
	  "non_volatile_cache_buckets_count",
	  [this] { return lru.size(); },
	  sm::description("the count of laddr bucket used by non volatile cache")),
	sm::make_counter(
	  "non_volatile_cache_evicted_size",
	  [this] { return stat.evicted_size; },
	  sm::description("total bytes of extents evicted by non volatile cache")),
	sm::make_counter(
	  "non_volatile_cache_demoted_size",
	  [this] { return stat.demoted_size; },
	  sm::description("total bytes of extents demoted by non volatile cache")),
	sm::make_counter(
	  "non_volatile_cache_demoted_bucket_count",
	  [this] { return stat.demoted_bucket_count; },
	  sm::description("the count of laddr bucket demoted by non volatile cache")),
      });
  }

  struct {
    uint64_t evicted_size = 0;
    uint64_t demoted_size = 0;
    uint64_t demoted_bucket_count = 0;
  } stat;

  seastar::metrics::metric_group metrics;

  const std::size_t memory_capacity;
  const std::size_t proceed_size_per_cycle;

  ExtentCallbackInterface *ecb;
  BackgroundListener *listener;
};

LogicalBucketRef create_logical_bucket(
  std::size_t memory_capacity,
  std::size_t proceed_size_per_cycle)
{
  return std::make_unique<LogicalBucketCache>(
    memory_capacity, proceed_size_per_cycle);
}

}
