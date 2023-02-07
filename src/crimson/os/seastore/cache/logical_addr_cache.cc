// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache/logical_addr_cache.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "seastar/core/metrics.hh"

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

LogicalAddressCache::LogicalAddressCache()
  : buckets_memory_capacity(
      crimson::common::get_conf<Option::size_t>(
        "seastore_logical_address_cache_memory_size")),
    evict_size_per_cycle(
      crimson::common::get_conf<Option::size_t>(
        "seastore_evict_size_per_cycle"))
{
  register_metrics();
}

LogicalAddressCache::~LogicalAddressCache()
{
  for (auto iter = laddr_lru.begin();
       iter != laddr_lru.end();) {
    auto p = iter++;
    laddr_lru.erase(laddr_lru.s_iterator_to(*p));
    release_bucket(&*p);
  }
}

void LogicalAddressCache::move_to_top(laddr_t laddr, extent_len_t length, extent_types_t type)
{
  if (auto bucket = find_bucket(laddr);
      bucket) {
    check_bucket(*bucket, laddr, length, type);
    move_to_top(*bucket);
  } else {
    auto b = create_bucket(laddr, length, type);
    move_to_top(*b);
  }
}

void LogicalAddressCache::move_to_top_if_cached(laddr_t laddr, extent_len_t length, extent_types_t type)
{
  if (auto bucket = find_bucket(laddr);
      bucket) {
    check_bucket(*bucket, laddr, length, type);
    move_to_top(*bucket);
  }
}

void LogicalAddressCache::remove(laddr_t laddr, extent_len_t length, extent_types_t type)
{
  auto bucket = find_bucket(laddr);
  if (bucket) {
    check_bucket(*bucket, laddr, length, type);
    release_bucket(bucket);
  }
}

seastar::future<> LogicalAddressCache::evict()
{
  LOG_PREFIX(LogicalAddressCache::evict);
  INFO("start");
  return repeat_eagain([this, FNAME] {
    evict_state.reset();
    {
      auto iter = laddr_lru.begin();
      for (int i = 0; i < 20 && iter != laddr_lru.end(); ++i) {
	evict_state.cold_buckets.emplace_back(&*iter++);
      }
      evict_state.cold_bucket_iter = evict_state.cold_buckets.begin();
      ceph_assert(evict_state.cold_bucket_iter !=
		  evict_state.cold_buckets.end());
    }
    DEBUG("{}", evict_state);
    return tm->with_transaction_intr(
        Transaction::src_t::CLEANER_MAIN,
	"evict",
	[this, FNAME](auto &t) {
      return trans_intr::repeat([this, &t, FNAME] {
	DEBUGT("scan bucket {}", t, **evict_state.cold_bucket_iter);
	return tm->get_pins(
	  t,
	  (*evict_state.cold_bucket_iter)->laddr,
	  (*evict_state.cold_bucket_iter)->length
	).si_then([this, &t, FNAME](auto pins) {
	  unsigned processed_pins_count = 0;

	  for (auto iter = pins.begin(); iter != pins.end(); iter++) {
	    auto &pin = *iter;
	    if (evict_state.cold_extents_size < evict_size_per_cycle) {
	      TRACET("consume pin {}", t, *pin);
	      processed_pins_count++;
	      auto paddr = pin->get_val();
	      if (paddr.is_absolute() &&
		  epm->is_hot_device(paddr.get_device_id())) {
		evict_state.cold_extents_size += pin->get_length();
		evict_state.pin_type_map[pin->get_key()] =
		  (*evict_state.cold_bucket_iter)->type;
		evict_state.cold_pin_list.emplace_back(std::move(*iter));
	      }
	    } else {
	      break;
	    }
	  }

	  if (processed_pins_count == pins.size()) {
	    evict_state.completed_buckets.push_back(*evict_state.cold_bucket_iter);
	  }
	  evict_state.cold_bucket_iter++;

	  if (evict_state.cold_extents_size >= evict_size_per_cycle ||
	      evict_state.cold_bucket_iter == evict_state.cold_buckets.end()) {
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	  } else {
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no);
	  }
	});
      }).si_then([this, &t, FNAME] {
	return trans_intr::parallel_for_each(evict_state.cold_pin_list,
					     [this, &t, FNAME](auto &pin) {
	  return tm->pin_to_extent_by_type(
	    t, pin->duplicate(), evict_state.pin_type_map[pin->get_key()]
	  ).si_then([this, &t, FNAME](auto extent) {
	    TRACET("evict old extent {}", t, *extent);
	    evict_state.cold_extents.emplace_back(extent);
	  });
	});
      }).si_then([this, &t, FNAME] {
	DEBUGT("finish scan {}", t, evict_state);
	auto mtime = seastar::lowres_system_clock::now();
	return trans_intr::do_for_each(evict_state.cold_extents,
				       [this, &t, mtime, FNAME](auto &extent) {
	  if (!extent->is_valid()) {
	    ERROR("{}", *extent);
	    ceph_abort();
	  }
	  return tm->rewrite_extent(t, extent, MIN_COLD_GENERATION, mtime);
	});
      }).si_then([this, &t] {
	return tm->submit_transaction_direct(t);
      }).si_then([this] {
	stats.evicted_bucket += evict_state.completed_buckets.size();
	stats.evicted_size += evict_state.cold_extents_size;
	for (auto &b : evict_state.completed_buckets) {
	  release_bucket(b);
	}
	return seastar::now();
      });
    });
  }).handle_error(crimson::ct_error::assert_all{ "impossible" });
}

LogicalAddressCache::laddr_bucket_t*
LogicalAddressCache::find_bucket(laddr_t laddr)
{
  auto p = laddr_set.lower_bound(laddr);
  if (p != laddr_set.begin() &&
      (p == laddr_set.end() || p->laddr > laddr)) {
    --p;
    if (p->laddr + p->length <= laddr) {
      ++p;
    }
  }

  if (p == laddr_set.end() ||
      !(p->laddr <= laddr &&
        p->laddr + p->length > laddr)) {
    return nullptr;
  } else {
    return &*p;
  }
}

LogicalAddressCache::laddr_bucket_ref_t
LogicalAddressCache::create_bucket(laddr_t laddr, extent_len_t len, extent_types_t type)
{
  auto b = laddr_bucket_ref_t(new laddr_bucket_t(laddr, len, type));
  intrusive_ptr_add_ref(&*b);
  auto p = laddr_set.insert(*b);
  ceph_assert(p.second);
  return b;
}

void LogicalAddressCache::release_bucket(
    LogicalAddressCache::laddr_bucket_ref_t bucket)
{
  if (bucket->list_hook.is_linked()) {
    laddr_lru.erase(laddr_lru.s_iterator_to(*bucket));
  }
  if (bucket->set_hook.is_linked()) {
    laddr_set.erase(laddr_set.s_iterator_to(*bucket));
  }
  intrusive_ptr_release(&*bucket);
}

void LogicalAddressCache::move_to_top(LogicalAddressCache::laddr_bucket_t &bucket) {
  if (bucket.list_hook.is_linked()) {
    laddr_lru.erase(laddr_lru.s_iterator_to(bucket));
  }
  laddr_lru.push_back(bucket);
}

void LogicalAddressCache::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("logical_cache", {
      sm::make_counter("buckets_count",
		       [this] { return laddr_lru.size(); },
		       sm::description("The size of all buckets used")),
      sm::make_counter("evicted_buckets_count",
		       stats.evicted_bucket,
		       sm::description("The count of evicted buckets")),
      sm::make_counter("evicted_buckets_size",
		       stats.evicted_size,
		       sm::description("The size of evicted buckets")),
  });
}

}
