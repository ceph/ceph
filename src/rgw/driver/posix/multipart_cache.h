// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <mutex>
#include <string>
#include <tuple>
#include <boost/container/flat_map.hpp>
#include <boost/intrusive/avl_set.hpp>
#include "include/function2.hpp"
#include "common/cohort_lru.h"
#include "include/scope_guard.h"
#include "rgw/rgw_cksum.h"
#include <xxhash.h>
#include <atomic>

/* MultipartCache — in-memory cache for uncompleted multipart upload state
 *
 * Each entry represents one in-progress multipart upload, holding a
 * flat_map of per-part metadata (size, etag, mtime, cksum).  The cache
 * uses cohort_lru for LRU eviction and TreeX for partitioned lookup,
 * matching the patterns established by BucketCache.
 *
 * Unlike BucketCache, this cache has NO LMDB involvement.  Eviction is
 * harmless in writethrough/writeback modes because the staging directory
 * on disk is the source of truth; a miss just rescans.  In volatile mode,
 * eviction loses metadata for parts that were never written to disk.
 *
 * INVARIANT:  a resident entry is COMPLETE.  It holds every part of its
 * upload, up to S3's maximum of 10000, and nothing caps it below that.
 * A per-entry cap cannot work here:  any bound lower than the legal part
 * maximum turns into a cliff that a valid upload can cross, and on the
 * far side the parts of one upload come from two places -- which is how
 * an upload of 513 parts came to behave differently from one of 512.
 *
 * Memory is bounded instead by parts_budget, a ceiling on the TOTAL part
 * records held across the cache.  When it is exhausted the loser is a
 * whole entry, never part of one:  add_part() drops what it has cached
 * for that upload and reports false, the caller writes the xattr, and a
 * later read refills from disk.  Filling is allowed to overshoot the
 * budget -- correctness first, and the next write brings it back.
 *
 * Three cache policies govern xattr write behavior:
 *
 *   writethrough — always write xattrs on part upload; cache is pure
 *                  acceleration.
 *   writeback    — skip xattrs; flush dirty parts to disk on eviction.
 *   volatile_    — skip xattrs; discard on eviction.
 *
 * Not templated on driver type — fill-on-miss is delegated to the
 * caller via a lambda, keeping all filesystem knowledge in the driver.
 */

#define dout_subsys ceph_subsys_rgw
namespace file::listing {

namespace bi = boost::intrusive;

enum class MultipartCachePolicy { writethrough, writeback, volatile_ };

struct MultipartPartInfo {
  uint32_t num;
  uint64_t size;
  std::string etag;
  ceph::real_time mtime;
  std::optional<rgw::cksum::Cksum> cksum;
  /* where the part's bytes are and how many, for a staging layout
   * which writes several parts into one file.  `stored` is bytes on
   * disk, which is not `size` -- that is the accounted length the
   * client sent, and the two differ whenever a filter between the op
   * layer and the writer changed the byte count.  A layout giving
   * every part its own file leaves all three at their defaults. */
  bool shared{false};
  uint64_t offset{0};
  uint64_t stored{0};
};

struct MultipartCacheKey {
  std::string bucket_name;
  std::string upload_meta;
};

template <typename YP = cohort::lru::NullYieldPolicy>
struct MultipartCache;

typedef bi::link_mode<bi::safe_link> mp_link_mode;
typedef bi::avl_set_member_hook<mp_link_mode> mp_member_hook_t;

template <typename YP = cohort::lru::NullYieldPolicy>
struct MultipartCacheEntry : public cohort::lru::Object
{
  using lock_guard = std::lock_guard<std::mutex>;
  using unique_lock = std::unique_lock<std::mutex>;

  static constexpr uint32_t FLAG_NONE      = 0x0000;
  static constexpr uint32_t FLAG_FILLED    = 0x0001;
  static constexpr uint32_t FLAG_DELETED   = 0x0002;
  static constexpr uint32_t FLAG_DIRTY     = 0x0004;

  static constexpr uint64_t seed = 3141592653;

  MultipartCache<YP>* mc;
  MultipartCacheKey key;
  uint64_t hk;
  mp_member_hook_t name_hook;

  std::mutex mtx;
  uint32_t flags;
  boost::container::flat_map<uint32_t, MultipartPartInfo> parts;
  /* how many of `parts` are currently counted in MultipartCache::
   * total_parts;  kept so every mutation can post a delta rather than
   * the cache having to recount */
  uint32_t counted{0};

public:
  MultipartCacheEntry(MultipartCache<YP>* mc, const MultipartCacheKey& key,
		      uint64_t hk)
    : mc(mc), key(key), hk(hk), flags(FLAG_NONE) {}

  inline bool deleted() const { return flags & FLAG_DELETED; }
  inline bool filled() const { return flags & FLAG_FILLED; }
  inline bool dirty() const { return flags & FLAG_DIRTY; }

  struct MultipartCacheEntryLT
  {
    bool operator()(const MultipartCacheEntry& lhs,
		    const MultipartCacheEntry& rhs) const {
      return std::tie(lhs.key.bucket_name, lhs.key.upload_meta)
	   < std::tie(rhs.key.bucket_name, rhs.key.upload_meta);
    }

    bool operator()(const MultipartCacheKey& k,
		    const MultipartCacheEntry& rhs) const {
      return std::tie(k.bucket_name, k.upload_meta)
	   < std::tie(rhs.key.bucket_name, rhs.key.upload_meta);
    }

    bool operator()(const MultipartCacheEntry& lhs,
		    const MultipartCacheKey& k) const {
      return std::tie(lhs.key.bucket_name, lhs.key.upload_meta)
	   < std::tie(k.bucket_name, k.upload_meta);
    }
  };

  struct MultipartCacheEntryEQ
  {
    bool operator()(const MultipartCacheEntry& lhs,
		    const MultipartCacheEntry& rhs) const {
      return std::tie(lhs.key.bucket_name, lhs.key.upload_meta)
	  == std::tie(rhs.key.bucket_name, rhs.key.upload_meta);
    }

    bool operator()(const MultipartCacheKey& k,
		    const MultipartCacheEntry& rhs) const {
      return std::tie(k.bucket_name, k.upload_meta)
	  == std::tie(rhs.key.bucket_name, rhs.key.upload_meta);
    }

    bool operator()(const MultipartCacheEntry& lhs,
		    const MultipartCacheKey& k) const {
      return std::tie(lhs.key.bucket_name, lhs.key.upload_meta)
	  == std::tie(k.bucket_name, k.upload_meta);
    }
  };

  typedef cohort::lru::LRU<std::mutex, YP> mp_lru;

  typedef bi::member_hook<MultipartCacheEntry, mp_member_hook_t,
			  &MultipartCacheEntry::name_hook> name_hook_t;
  typedef bi::avltree<MultipartCacheEntry,
		      bi::compare<MultipartCacheEntryLT>,
		      name_hook_t> mp_avl_t;
  typedef cohort::lru::TreeX<MultipartCacheEntry, mp_avl_t,
			     MultipartCacheEntryLT, MultipartCacheEntryEQ,
			     MultipartCacheKey, std::mutex> mp_avl_cache;

  void lru_cleanup() override {
    {
      auto lock = lock_guard{mtx};
      if (deleted()) {
	return;
      }
      flags |= FLAG_DELETED;

      if (dirty() && mc->policy == MultipartCachePolicy::writeback
	  && mc->stabilize_fn) {
	mc->stabilize_fn(key, parts);
      }
      parts.clear();
    } /* mtx released */

    if (name_hook.is_linked()) {
      mc->cache.remove(hk, this, mp_avl_cache::FLAG_LOCK);
    }
  }

  virtual ~MultipartCacheEntry() {
    if (name_hook.is_linked()) {
      mc->cache.remove(hk, this, mp_avl_cache::FLAG_LOCK);
    }
  }

  class Factory : public cohort::lru::ObjectFactory
  {
  public:
    MultipartCache<YP>* mc;
    const MultipartCacheKey& key;
    uint64_t hk;
    uint32_t flags;

    Factory() = delete;
    Factory(MultipartCache<YP>* mc, const MultipartCacheKey& key)
      : mc(mc), key(key), flags(FLAG_NONE) {
      uint64_t h = XXH64(key.bucket_name.c_str(), key.bucket_name.length(),
			  MultipartCacheEntry::seed);
      hk = XXH64(key.upload_meta.c_str(), key.upload_meta.length(), h);
    }

    void recycle(cohort::lru::Object* o) override {
      o->~Object();
      new (o) MultipartCacheEntry(mc, key, hk);
    }

    cohort::lru::Object* alloc() override {
      return new MultipartCacheEntry(mc, key, hk);
    }
  }; /* Factory */

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override {
    auto factory =
      dynamic_cast<const MultipartCacheEntry<YP>::Factory*>(newobj_fac);
    if (factory == nullptr) {
      return false;
    }

    bool need_cross_remove = false;
    {
      auto lock = lock_guard{mtx};
      if (!deleted()) {
	flags |= FLAG_DELETED;

	if (dirty() && mc->policy == MultipartCachePolicy::writeback
	    && mc->stabilize_fn) {
	  mc->stabilize_fn(key, parts);
	}
	parts.clear();
	mc->account(this);

	if (mc->cache.is_same_partition(hk, factory->hk)) {
	  mc->cache.remove(hk, this, mp_avl_cache::FLAG_NONE);
	} else {
	  need_cross_remove = true;
	}
      }
    } /* mtx released */

    if (need_cross_remove) {
      mc->cache.unlock_for(factory->hk);
      mc->cache.remove(hk, this, mp_avl_cache::FLAG_LOCK);
      mc->cache.lock_for(factory->hk);
    }
    return true;
  } /* reclaim */

}; /* MultipartCacheEntry */

using stabilize_fn_t =
  fu2::unique_function<void(const MultipartCacheKey&,
    const boost::container::flat_map<uint32_t, MultipartPartInfo>&) const>;

using fill_parts_fn_t =
  fu2::unique_function<void(
    boost::container::flat_map<uint32_t, MultipartPartInfo>&) const>;

struct MultipartListResult {
  std::vector<MultipartPartInfo> parts;
  bool truncated{false};
  uint32_t next_marker{0};
};

template <typename YP>
struct MultipartCache
{
  using Entry = MultipartCacheEntry<YP>;
  using lock_guard = std::lock_guard<std::mutex>;

  static constexpr uint32_t FLAG_CREATE = 0x0001;
  static constexpr uint32_t FLAG_LOCK   = 0x0002;

  typename Entry::mp_lru lru;
  typename Entry::mp_avl_cache cache;
  MultipartCachePolicy policy;
  stabilize_fn_t stabilize_fn;
  /* ceiling on part records across the whole cache, not per entry */
  uint64_t parts_budget;
  std::atomic<uint64_t> total_parts{0};

  MultipartCache(uint32_t max_entries, uint8_t n_lanes,
		 uint8_t n_partitions, uint64_t parts_budget,
		 MultipartCachePolicy policy,
		 stabilize_fn_t&& stabilize = nullptr)
    : lru(n_lanes, max_entries / n_lanes),
      cache(n_partitions, max_entries / n_partitions),
      policy(policy),
      stabilize_fn(std::move(stabilize)),
      parts_budget(parts_budget) {}

  ~MultipartCache() {
    cache.drain(
      [this](Entry* e) {
	lru.unref(e, cohort::lru::FLAG_NONE);
      },
      Entry::mp_avl_cache::FLAG_LOCK);
  }

  /* post the difference between what an entry holds and what it is
   * counted for.  Called with the entry locked, after any mutation. */
  void account(Entry* b) {
    const uint64_t now = b->parts.size();
    if (now >= b->counted) {
      total_parts += (now - b->counted);
    } else {
      total_parts -= (b->counted - now);
    }
    b->counted = now;
  }

  typedef std::tuple<Entry*, uint32_t> GetEntryResult;

  GetEntryResult get_entry(const MultipartCacheKey& key, uint32_t flags) {
    Entry* b{nullptr};
    typename Entry::Factory fac(this, key);
    typename Entry::mp_avl_cache::Latch lat;
    uint32_t iflags{cohort::lru::FLAG_INITIAL};
    GetEntryResult result{nullptr, 0};

  retry:
    b = cache.find_latch(fac.hk, key, lat,
			 Entry::mp_avl_cache::FLAG_LOCK);
    /* LATCHED */
    if (b) {
      b->mtx.lock();
      if (b->deleted() ||
	  !lru.ref(b, cohort::lru::FLAG_INITIAL)) {
	lat.lock->unlock();
	b->mtx.unlock();
	goto retry;
      }
      lat.lock->unlock();
      /* LOCKED */
    } else {
      if (!(flags & MultipartCache::FLAG_CREATE)) {
	lat.lock->unlock();
	return result;
      }
      b = static_cast<Entry*>(
	lru.insert(&fac, cohort::lru::Edge::MRU, iflags));
      if (b) [[likely]] {
	b->mtx.lock();

	if (!(iflags & cohort::lru::FLAG_RECYCLE)) [[likely]] {
	  cache.insert_latched(b, lat, Entry::mp_avl_cache::FLAG_UNLOCK);
	} else {
	  cache.insert(fac.hk, b, Entry::mp_avl_cache::FLAG_NONE);
	  lat.lock->unlock();
	}
	std::get<1>(result) |= MultipartCache::FLAG_CREATE;
      } else {
	lat.lock->unlock();
	goto retry;
      }
    }

    if (!(flags & MultipartCache::FLAG_LOCK)) {
      b->mtx.unlock();
    }
    std::get<0>(result) = b;
    return result;
  } /* get_entry */

  bool add_part(const MultipartCacheKey& key, MultipartPartInfo&& info) {
    auto [b, rflags] = get_entry(key, FLAG_CREATE | FLAG_LOCK);
    if (!b) {
      return false;
    }
    auto sg = make_scope_guard(
      [this, b]() {
	b->mtx.unlock();
	lru.unref(b, cohort::lru::FLAG_NONE);
      });

    /* replacing a part already held costs nothing;  only a new record
     * has to fit the budget */
    const bool is_new = !b->parts.contains(info.num);
    if (is_new && (total_parts.load() >= parts_budget)) {
      /* Cannot afford to keep this upload cached.  Drop what we have
       * rather than hold a partial map:  a resident entry is complete, so
       * that no upload is ever served from two places.  The caller writes
       * the xattr, and a later read refills from disk. */
      b->parts.clear();
      b->flags &= ~Entry::FLAG_FILLED;
      account(b);
      return false;
    }
    b->parts[info.num] = std::move(info);
    account(b);
    if (policy != MultipartCachePolicy::writethrough) {
      b->flags |= Entry::FLAG_DIRTY;
    }
    return true;
  }

  MultipartListResult list_parts(const MultipartCacheKey& key,
				 uint32_t marker, uint32_t max_parts,
				 const fill_parts_fn_t& fill_fn) {
    MultipartListResult result;
    auto [b, rflags] = get_entry(key, FLAG_CREATE | FLAG_LOCK);
    if (!b) {
      return result;
    }
    auto sg = make_scope_guard(
      [this, b]() {
	b->mtx.unlock();
	lru.unref(b, cohort::lru::FLAG_NONE);
      });

    if (!b->filled()) {
      /* allowed to overshoot the budget:  the page has to be correct, and
       * the next add_part() brings the total back */
      fill_fn(b->parts);
      b->flags |= Entry::FLAG_FILLED;
      account(b);
    }

    auto it = b->parts.lower_bound(marker + 1);
    uint32_t count = 0;
    while (it != b->parts.end() && count < max_parts) {
      result.parts.push_back(it->second);
      result.next_marker = it->first;
      ++it;
      ++count;
    }
    result.truncated = (it != b->parts.end());
    return result;
  }

  void remove(const MultipartCacheKey& key) {
    auto [b, rflags] = get_entry(key, FLAG_LOCK);
    if (!b) {
      return;
    }
    b->parts.clear();
    account(b);
    b->flags = Entry::FLAG_NONE;
    b->mtx.unlock();
    lru.unref(b, cohort::lru::FLAG_NONE);
  }

  friend struct MultipartCacheEntry<YP>;

}; /* MultipartCache */

} /* namespace file::listing */
#undef dout_subsys
