// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar/core/metrics.hh"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/cache/non_volatile_cache.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore {

SET_SUBSYS(seastore_cache);

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

  friend std::ostream& operator<<(std::ostream& out,
				  const laddr_bucket_t& bucket) {
    return out << "laddr_bucket_t(laddr=" << bucket.laddr
               << ", length=" << bucket.length
               << ", extent_type=" << bucket.type
               << ")";
  }
};
using laddr_bucket_ref_t = boost::intrusive_ptr<laddr_bucket_t>;
using laddr_bucket_list_t = boost::intrusive::list<
  laddr_bucket_t,
  boost::intrusive::member_hook<
    laddr_bucket_t,
    boost::intrusive::list_member_hook<>,
    &laddr_bucket_t::list_hook>>;
using laddr_bucket_set_t = boost::intrusive::set<
  laddr_bucket_t,
  boost::intrusive::key_of_value<laddr_bucket_t::bucket_key_t>,
  boost::intrusive::member_hook<
    laddr_bucket_t,
    boost::intrusive::set_member_hook<>,
    &laddr_bucket_t::set_hook>>;

class LogicalBucketCache : public NonVolatileCache {
public:
  LogicalBucketCache(std::size_t memory_capacity,
                     std::size_t demote_size_per_cycle)
    : memory_capacity(memory_capacity),
      demote_size_per_cycle(demote_size_per_cycle) {
    LOG_PREFIX(LogicalBucketCache);
    INFO("init memory_capacity={}, demote_size_per_cycle={}",
	 memory_capacity, demote_size_per_cycle);
    register_metrics();
  }

  ~LogicalBucketCache() {
    clear();
  }

  void move_to_top(
      laddr_t laddr,
      extent_len_t length,
      extent_types_t type) final {
    LOG_PREFIX(LogicalBucketCache::move_to_top);
    DEBUG("laddr: {}, length {}, type: {}", laddr, length, type);
    assert(laddr != L_ADDR_NULL);
    assert(length != 0);
    assert(is_logical_type(type));
    if (auto bucket = find_bucket(laddr);
        bucket) {
      DEBUG("find bucket: {}", *bucket);
      check_bucket(*bucket, laddr, length, type);
      move_to_top(*bucket);
    } else {
      auto b = create_bucket(laddr, length, type);
      DEBUG("create bucket: {}", *b);
      move_to_top(*b);
    }
  }

  bool move_to_top_if_cached(
      laddr_t laddr,
      extent_len_t length,
      extent_types_t type) final {
    LOG_PREFIX(LogicalBucketCache::move_to_top_if_cached);
    DEBUG("laddr: {}, length {}, type: {}", laddr, length, type);
    assert(laddr != L_ADDR_NULL);
    assert(length != 0);
    assert(is_logical_type(type));
    bool ret = false;
    if (auto bucket = find_bucket(laddr);
        bucket) {
      DEBUG("find bucket: {}", *bucket);
      check_bucket(*bucket, laddr, length, type);
      move_to_top(*bucket);
      ret = true;
    }
    return ret;
  }

  void move_to_top_if_not_cached(
      laddr_t laddr,
      extent_len_t length,
      extent_types_t type) final {
    LOG_PREFIX(LogicalBucketCache::move_to_top_if_not_cached);
    DEBUG("laddr: {}, length {}, type: {}", laddr, length, type);
    assert(laddr != L_ADDR_NULL);
    assert(length != 0);
    assert(is_logical_type(type));
    if (auto bucket = find_bucket(laddr);
        !bucket) {
      auto b = create_bucket(laddr, length, type);
      DEBUG("create bucket: {}", *b);
      move_to_top(*b);
    }
  }

  void remove(
      laddr_t laddr,
      extent_len_t length,
      extent_types_t type) final {
    LOG_PREFIX(LogicalBucketCache::remove);
    TRACE("laddr: {}, length: {}, type: {}", laddr, length, type);
    assert(laddr != L_ADDR_NULL);
    assert(length != 0);
    assert(is_logical_type(type));
    if (auto bucket = find_bucket(laddr);
        bucket) {
      TRACE("find bucket: {}", *bucket);
      check_bucket(*bucket, laddr, length, type);
      release_bucket(bucket);
    }
  }

  bool is_cached(
      laddr_t laddr,
      extent_len_t length,
      extent_types_t type) final {
    LOG_PREFIX(LogicalBucketCache::is_cached);
    assert(laddr != L_ADDR_NULL);
    assert(length != 0);
    assert(is_logical_type(type));
    if (auto bucket = find_bucket(laddr);
        bucket) {
      check_bucket(*bucket, laddr, length, type);
      TRACE("laddr: {}, length: {}, type: {} is cached in bucket: {}",
	    laddr, length, type, *bucket);
      return true;
    } else {
      TRACE("laddr: {}, length: {}, type: {} isn't cached", laddr, length, type);
      return false;
    }
  }

  void clear() final {
    for (auto iter = lru.begin(); iter != lru.end();) {
      release_bucket(&*(iter++));
    }
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
    auto bucket_size = sizeof(laddr_bucket_t);
    // estimate memory usage
    auto rbtree_internal_node_size = sizeof(boost::intrusive::set_member_hook<>);
    auto average = bucket_size + rbtree_internal_node_size;
    return (average * buckets_set.size()) > memory_capacity;
  }

  seastar::future<> demote() final {
    return repeat_eagain([this] {
      init_state();
      return ecb->with_transaction_intr(
        Transaction::src_t::DEMOTE,
       "demote",
       [this](auto &t) {
         return trans_intr::repeat([this, &t] {
	   LOG_PREFIX(LogicalBucketCache::demote);
	   auto &bucket = *s.cold_iter;
	   DEBUG("demote {}", *bucket);
	   assert(demote_size_per_cycle > s.demote_size);
           return ecb->demote_region(
             t,
             bucket->laddr,
             bucket->length,
             demote_size_per_cycle - s.demote_size
           ).si_then([this, FNAME](auto &&res) {
	     TRACE("proceed_size: {}, demote_size: {}",
		   res.proceed_size, res.demotion_size);
	     s.demote_size += res.demotion_size;
	     auto &bucket = *s.cold_iter;
	     if (res.completed) {
	       s.completed_buckets.push_back(bucket);
	       s.cold_iter++;
	     }
	     
	     if (s.cold_iter == s.cold_buckets.end() ||
		 s.demote_size >= demote_size_per_cycle) {
	       return seastar::stop_iteration::yes;
	     } else {
	       return seastar::stop_iteration::no;
	     }
	   });
         }).si_then([this, &t] {
           return ecb->submit_transaction_direct(t);
         }).si_then([this] {
	   LOG_PREFIX(LogicalBucketCache::demote);
	   DEBUG("demote {} bytes in the hot tier", s.demote_size);
	   stat.demoted_bucket_count += s.completed_buckets.size();
	   stat.demoted_size += s.demote_size;
           for (auto &p : s.completed_buckets) {
             release_bucket(p);
           }
	   auto old_count = s.init_buckets_count;
	   s.update_init_buckets_count(
	     demote_size_per_cycle,
	     stat.demoted_size,
	     stat.demoted_bucket_count);
	   TRACE("update init buckets count {} -> {}",
		 old_count, s.init_buckets_count);
           return ExtentCallbackInterface::demote_region_iertr::
	     make_ready_future();
         });
       });
    }).handle_error(crimson::ct_error::assert_all{ "impossible" });
  }

private:

  laddr_bucket_ref_t create_bucket(
    laddr_t laddr,
    extent_len_t length,
    extent_types_t type) {
    auto b = laddr_bucket_ref_t(new laddr_bucket_t(laddr, length, type));
    intrusive_ptr_add_ref(b.get());
    auto p = buckets_set.insert(*b);
    ceph_assert(p.second);
    stat.tracked_size += length;
    if (should_demote()) {
      // TODO: wake up listener
    }
    return b;
  }

  void release_bucket(laddr_bucket_ref_t bucket) {
    if (bucket->set_hook.is_linked()) {
      buckets_set.erase(buckets_set.s_iterator_to(*bucket));
    }
    if (bucket->list_hook.is_linked()) {
      lru.erase(lru.s_iterator_to(*bucket));
    }
    stat.tracked_size -= bucket->length;
    intrusive_ptr_release(bucket.get());
  }

  void check_bucket(
      const laddr_bucket_t &bucket,
      laddr_t laddr,
      extent_len_t length,
      extent_types_t type) {
    ceph_assert(bucket.type == type);
    ceph_assert(bucket.laddr <= laddr);
    ceph_assert(bucket.laddr + bucket.length >= laddr + length);
  }

  laddr_bucket_t *find_bucket(laddr_t laddr) {
    auto p = buckets_set.lower_bound(laddr);
    if (p != buckets_set.begin() &&
        (p == buckets_set.end() || p->laddr > laddr)) {
      --p;
      if (p->laddr + p->length <= laddr) {
        ++p;
      }
    }

    if (p == buckets_set.end() ||
        !(p->laddr <= laddr &&
          p->laddr + p->length > laddr)) {
      return nullptr;
    } else {
      return &*p;
    }
  }

  void move_to_top(laddr_bucket_t &bucket) {
    if (bucket.list_hook.is_linked()) {
      lru.erase(lru.s_iterator_to(bucket));
    }
    lru.push_back(bucket);
  }

  struct demote_state_t {
    std::list<laddr_bucket_ref_t> cold_buckets;
    std::list<laddr_bucket_ref_t> completed_buckets;
    std::list<laddr_bucket_ref_t>::iterator cold_iter;

    std::size_t demote_size;

    int init_buckets_count = 20;

    void reset() {
      cold_buckets.clear();
      completed_buckets.clear();
      cold_iter = cold_buckets.end();
      demote_size = 0;
    }

    void update_init_buckets_count(
      extent_len_t demote_size_per_cycle,
      double demote_size,
      double demoted_buckets_count) {
      if (demote_size != 0 && demoted_buckets_count != 0) {
	auto demote_ratio = (double)demote_size /
	  (double)demoted_buckets_count;
	assert(!std::isnan(demote_ratio));
	init_buckets_count = (demote_size_per_cycle / demote_ratio) + 1;
      }
    }
  };

  void init_state() {
    s.reset();
    int count = 0;
    assert(s.init_buckets_count > 0);
    for (auto &b : lru) {
      if (count >= s.init_buckets_count) {
	break;
      }
      s.cold_buckets.push_back(&b);
      count++;
    }
    s.cold_iter = s.cold_buckets.begin();
    ceph_assert(s.cold_iter != s.cold_buckets.end());
  }

  void register_metrics() {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
	sm::make_counter(
	  "non_volatile_cache_tracked_size",
	  [this] { return stat.tracked_size; },
	  sm::description("total bytes tracked by non volatile cache")),
	sm::make_gauge(
	  "non_volatile_cache_buckets_count",
	  [this] { return lru.size(); },
	  sm::description("the count of laddr bucket used by non volatile cache")),
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

  demote_state_t s;

  struct {
    uint64_t tracked_size = 0;
    uint64_t demoted_size = 0;
    uint64_t demoted_bucket_count = 0;
  } stat;

  seastar::metrics::metric_group metrics;

  const std::size_t memory_capacity;
  const std::size_t demote_size_per_cycle;

  laddr_bucket_list_t lru;
  laddr_bucket_set_t buckets_set;
  ExtentCallbackInterface *ecb;
  BackgroundListener *listener;
};

NonVolatileCacheRef create_non_volatile_cache(
  std::size_t memory_capacity,
  std::size_t demote_size_per_cycle)
{
  return std::make_unique<LogicalBucketCache>(
    memory_capacity, demote_size_per_cycle);
}

}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::laddr_bucket_t> : fmt::ostream_formatter {};
#endif
