// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <iostream>
#include <lmdb.h>
#include <memory>
#include <tuple>
#include <vector>
#include <string>
#include <string_view>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <filesystem>
#include <boost/container/flat_map.hpp>
#include <boost/intrusive/avl_set.hpp>
#include "include/function2.hpp"
#include "common/cohort_lru.h"
#include "include/scope_guard.h"
#include "lmdb-safe.hh"
#include "zpp_bits.h"
#include "notify.h"
#include <stdint.h>
#include <time.h> // struct timespec
#include <xxhash.h>

#include "rgw_common.h"
#include "rgw_sal.h"

#include "fmt/format.h"

/* BucketCache — LMDB-backed listing cache for POSIX/NSFS buckets
 *
 * Three resource lifecycles must stay in sync:
 *
 *   1. LRU entries (BucketCacheEntry, managed by cohort::lru::LRU)
 *      Bounded by lane_hiwat per lane.  Entries cycle between the
 *      active list (in-flight request holds a ref) and the q list
 *      (idle, eligible for eviction).
 *
 *   2. AVL tree nodes (TreeX, the lookup index)
 *      One per live LRU entry.  Removed when the entry is reclaimed
 *      or deleted via the hiwat discard path.
 *
 *   3. LMDB DBI handles (per-partition, bounded by max_dbs_per_partition)
 *      Each bucket's listing is stored in a named LMDB database,
 *      accessed through a DBI handle.  Handles are permanent —
 *      mdb_dbi_close() is unsafe with concurrent readers, so we
 *      never call it.  The only way to reuse a slot is:
 *        a. mdb_drop(dbi, 0) — clears data, keeps handle valid
 *        b. recycle_dbi()    — moves handle to the free pool
 *        c. get_dbi()        — reuses from the free pool on the
 *                              next bucket needing a handle
 *      A recycled handle still points to its original named database
 *      in LMDB, but data is accessed through the handle (not the
 *      name), so the name mismatch is harmless as long as all
 *      lookups go through dbi_map.
 *
 * Eviction paths (both must recycle DBIs):
 *
 *   - evict_block() → reclaim(): triggered by lru.insert() when a
 *     lane exceeds hiwat.  Runs with the lane lock DROPPED (safe for
 *     LMDB I/O).  Recycles via mdb_drop + recycle_dbi in reclaim().
 *
 *   - unref() hiwat discard: triggered when an entry returns to the
 *     q list and q.size() > lane_hiwat.  The LRU tail entry is
 *     deleted directly.  Recycles via lru_cleanup() (called before
 *     delete, outside the lane lock).
 *
 * NOTE: cohort_lru.h is also used by rgw_file.h (RGWFileHandle).
 * Changes to eviction semantics should be reviewed for impact there.
 */

#define dout_subsys ceph_subsys_rgw
namespace file::listing {

namespace bi = boost::intrusive;
namespace sf = std::filesystem;

typedef bi::link_mode<bi::safe_link> link_mode; /* XXX normal */
typedef bi::avl_set_member_hook<link_mode> member_hook_t;

template <typename D, typename B>
struct BucketCache;

template <typename D, typename B>
struct BucketCacheEntry : public cohort::lru::Object
{
  using lock_guard = std::lock_guard<std::mutex>;
  using unique_lock = std::unique_lock<std::mutex>;

  static constexpr uint32_t FLAG_NONE     = 0x0000;
  static constexpr uint32_t FLAG_FILLED   = 0x0001;
  static constexpr uint32_t FLAG_DELETED  = 0x0002;

  static constexpr uint64_t seed = 8675309;

  BucketCache<D, B>* bc;
  std::string name;
  std::shared_ptr<LMDBSafe::MDBEnv> env;
  LMDBSafe::MDBDbi dbi;
  uint64_t hk;
  member_hook_t name_hook;

  // XXX clean this up
  std::mutex mtx; // XXX Adam's preferred shared mtx?
  std::condition_variable cv;
  uint32_t flags;

public:
  BucketCacheEntry(BucketCache<D, B>* bc, const std::string& name, uint64_t hk)
    : bc(bc), name(name), env{nullptr}, hk(hk), flags(FLAG_NONE) {}

  void set_env(std::shared_ptr<LMDBSafe::MDBEnv>& _env, LMDBSafe::MDBDbi& _dbi) {
    env = _env;
    dbi = _dbi;
  }

  void lru_cleanup() override {
    bc->cleanup_count++;
    if (env) {
      try {
	auto txn = env->getRWTransaction();
	mdb_drop(*txn, dbi, 0);
	txn->commit();
	bc->lmdbs.recycle_dbi(this);
      } catch (const std::exception& e) {
	lsubdout(bc->driver->ctx(), rgw, 0)
	  << "BucketCache: hiwat dbi recycle failed for "
	  << name << ": " << e.what() << dendl;
      }
      env.reset();
    }
  }

  virtual ~BucketCacheEntry()
  {
    if (name_hook.is_linked()) {
      bc->cache.remove(hk, this, bucket_avl_cache::FLAG_LOCK);
    }
  }

  inline bool deleted() const {
    return flags & FLAG_DELETED;
  }

  class Factory : public cohort::lru::ObjectFactory
  {
  public:
    BucketCache<D, B>* bc;
    const std::string& name;
    uint64_t hk;
    uint32_t flags;

    Factory() = delete;
    Factory(BucketCache<D, B> *bc, const std::string& name)
      : bc(bc), name(name), flags(FLAG_NONE) {
      hk = XXH64(name.c_str(), name.length(), BucketCacheEntry::seed);
    }

    void recycle (cohort::lru::Object* o) override {
      /* re-use an existing object */
      o->~Object(); // call lru::Object virtual dtor
      // placement new!
      new (o) BucketCacheEntry(bc, name, hk);
    }

    cohort::lru::Object* alloc() override {
      return new BucketCacheEntry(bc, name, hk);
    }
  }; /* Factory */

  struct BucketCacheEntryLT
  {
    // for internal ordering
    bool operator()(const BucketCacheEntry& lhs, const BucketCacheEntry& rhs) const
      { return (lhs.name < rhs.name); }

    // for external search by name
    bool operator()(const std::string& k, const BucketCacheEntry& rhs) const
      { return k < rhs.name; }

    bool operator()(const BucketCacheEntry& lhs, const std::string& k) const
      { return lhs.name < k; }
  };

  struct BucketCacheEntryEQ
  {
    bool operator()(const BucketCacheEntry& lhs, const BucketCacheEntry& rhs) const
      { return (lhs.name == rhs.name); }

    bool operator()(const std::string& k, const BucketCacheEntry& rhs) const
      { return k == rhs.name; }

    bool operator()(const BucketCacheEntry& lhs, const std::string& k) const
      { return lhs.name == k; }
  };

  typedef cohort::lru::LRU<std::mutex> bucket_lru;

  typedef bi::member_hook<BucketCacheEntry, member_hook_t, &BucketCacheEntry::name_hook> name_hook_t;
  typedef bi::avltree<BucketCacheEntry, bi::compare<BucketCacheEntryLT>, name_hook_t> bucket_avl_t;
  typedef cohort::lru::TreeX<BucketCacheEntry, bucket_avl_t, BucketCacheEntryLT, BucketCacheEntryEQ, std::string,
			     std::mutex> bucket_avl_cache;

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) {
    auto factory = dynamic_cast<const BucketCacheEntry<D, B>::Factory*>(newobj_fac);
    if (factory == nullptr) {
        return false;
    }

    /* Two locks are involved: this entry's mtx and the TreeX partition
     * lock.  get_bucket() acquires them partition→mtx; reclaim must not
     * do mtx→partition or we deadlock.  So we mark the entry deleted
     * under mtx (short critical section), release mtx, then do the
     * cross-partition tree remove outside. */
    bool need_cross_remove = false;
    {
      auto lock = lock_guard{mtx};
      if (! deleted()) {
	if (! name_hook.is_linked()) {
	  abort();
	}

	flags |= FLAG_DELETED;
	bc->recycle_count++;
	lsubdout(bc->driver->ctx(), rgw, 2) << "BucketCache: reclaim evicting bucket=" << name << dendl;
	bc->un->remove_watch(name);

	/* recycle the DBI handle before dropping env — mdb_drop clears
	 * the data, then recycle_dbi moves the handle to the free pool
	 * for reuse by a future bucket */
	if (env) {
	  try {
	    auto txn = env->getRWTransaction();
	    mdb_drop(*txn, dbi, 0);
	    txn->commit();
	    bc->lmdbs.recycle_dbi(this);
	  } catch (const std::exception& e) {
	    lsubdout(bc->driver->ctx(), rgw, 0)
	      << "BucketCache: reclaim dbi recycle failed for "
	      << name << ": " << e.what() << dendl;
	  }
	}
	env.reset();

	if (bc->cache.is_same_partition(hk, factory->hk)) {
	  bc->cache.remove(hk, this, bucket_avl_cache::FLAG_NONE);
	} else {
	  need_cross_remove = true;
	}
      }
    } /* mtx released */

    if (need_cross_remove) {
      bc->cache.unlock_for(factory->hk);
      bc->cache.remove(hk, this, bucket_avl_cache::FLAG_LOCK);
      bc->cache.lock_for(factory->hk);
    }
    return true;
} /* reclaim */

}; /* BucketCacheEntry */

using fill_cache_cb_t =
  const fu2::unique_function<int(const DoutPrefixProvider* dpp,
    rgw_bucket_dir_entry&) const>;

using list_bucket_each_t =
  const fu2::unique_function<bool(const rgw_bucket_dir_entry&) const>;

template <typename D, typename B>
struct BucketCache : public Notifiable
{
  using lock_guard = std::lock_guard<std::mutex>;
  using unique_lock = std::unique_lock<std::mutex>;

  D* driver;
  std::string bucket_root;
  uint32_t max_buckets;
  std::atomic<uint64_t> recycle_count;
  std::atomic<uint64_t> cleanup_count;
  std::mutex mtx;

  /* the bucket lru cache keeps track of the buckets whose listings are
   * being cached in lmdb databases and updated from notify */
  typename BucketCacheEntry<D, B>::bucket_lru lru;
  typename BucketCacheEntry<D, B>::bucket_avl_cache cache;
  sf::path rp;

  /* the lmdb handle cache maintains a vector of lmdb environments,
   * each supports 1 rw and unlimited ro transactions;  the materialized
   * listing for each bucket is stored as a database in one of these
   * environments, selected by a hash of the bucket name; a bucket's database
   * is dropped/cleared whenever its entry is reclaimed from cache; the entire
   * complex is cleared on restart to preserve consistency */
  class Lmdbs
  {
    CephContext* cct;
    std::string database_root;
    uint8_t lmdb_count;
    MDB_dbi max_dbs_per_partition;

    struct Partition {
      std::shared_ptr<LMDBSafe::MDBEnv> env;
      boost::container::flat_map<std::string, LMDBSafe::MDBDbi> dbi_map;
      std::vector<LMDBSafe::MDBDbi> free_dbis;
      std::mutex mtx;
    };
    std::vector<Partition> parts;
    sf::path dbp;

  public:
    Lmdbs(CephContext* cct, std::string& database_root, uint8_t lmdb_count,
	  uint32_t max_buckets)
      : cct(cct), database_root(database_root), lmdb_count(lmdb_count),
        max_dbs_per_partition((max_buckets / lmdb_count) * 5 / 4 + 16),
        parts(lmdb_count), dbp(database_root) {

      sf::path safe_root_path{dbp / fmt::format("rgw_posix_lmdbs")};
      sf::create_directory(safe_root_path);

      /* purge cache completely */
      for (const auto& dir_entry : sf::directory_iterator{safe_root_path}) {
	sf::remove_all(dir_entry);
      }

      /* repopulate cache basis */
      for (int ix = 0; ix < lmdb_count; ++ix) {
	sf::path env_path{safe_root_path / fmt::format("part_{}", ix)};
	sf::create_directory(env_path);
	parts[ix].env = LMDBSafe::getMDBEnv(
	  env_path.string().c_str(), 0, 0600, max_dbs_per_partition);
      }
    }

    uint8_t partition_ix(BucketCacheEntry<D, B>* bucket) {
      return bucket->hk % lmdb_count;
    }

    inline std::shared_ptr<LMDBSafe::MDBEnv>& get_sp_env(BucketCacheEntry<D, B>* bucket)  {
      return parts[partition_ix(bucket)].env;
    }

    inline LMDBSafe::MDBEnv& get_env(BucketCacheEntry<D, B>* bucket) {
      return *(get_sp_env(bucket));
    }

    /* look up or create a dbi for the given bucket name in its
     * partition; reuses a recycled handle if available, otherwise
     * opens a new one; returns nullopt on failure (e.g. MDB_DBS_FULL) */
    std::optional<LMDBSafe::MDBDbi> get_dbi(
      BucketCacheEntry<D, B>* bucket,
      std::function<LMDBSafe::MDBDbi()> open_fn)
    {
      auto& part = parts[partition_ix(bucket)];
      std::lock_guard lk(part.mtx);

      auto it = part.dbi_map.find(bucket->name);
      if (it != part.dbi_map.end()) {
        return it->second;
      }

      if (!part.free_dbis.empty()) {
	auto dbi = part.free_dbis.back();
	part.free_dbis.pop_back();
	part.dbi_map.emplace(bucket->name, dbi);
	return dbi;
      }

      try {
        auto dbi = open_fn();
        part.dbi_map.emplace(bucket->name, dbi);
        return dbi;
      } catch (const LMDBSafe::LMDBError& e) {
	lsubdout(cct, rgw, 0) << "BucketCache: openDB failed for "
	  << bucket->name << ": " << e.what()
	  << " (dbi_map size=" << part.dbi_map.size()
	  << " free_dbis=" << part.free_dbis.size() << ")"
	  << dendl;
        return std::nullopt;
      }
    }

    /* move a dbi from the active map to the free pool for reuse;
     * the caller must have already cleared the database with
     * mdb_drop(dbi, 0) */
    void recycle_dbi(BucketCacheEntry<D, B>* bucket) {
      auto& part = parts[partition_ix(bucket)];
      std::lock_guard lk(part.mtx);
      auto it = part.dbi_map.find(bucket->name);
      if (it != part.dbi_map.end()) {
	part.free_dbis.push_back(it->second);
	part.dbi_map.erase(it);
      }
    }

    const std::string& get_root() const { return database_root; }

    size_t total_dbi_map_size() const {
      size_t total = 0;
      for (auto& p : parts) {
	std::lock_guard lk(const_cast<std::mutex&>(p.mtx));
	total += p.dbi_map.size();
      }
      return total;
    }

    size_t total_free_dbis() const {
      size_t total = 0;
      for (auto& p : parts) {
	std::lock_guard lk(const_cast<std::mutex&>(p.mtx));
	total += p.free_dbis.size();
      }
      return total;
    }
  } lmdbs;

  std::unique_ptr<Notify> un;

public:
  BucketCache(D* driver, std::string bucket_root, std::string database_root,
	      uint32_t max_buckets=100, uint8_t max_lanes=3,
	      uint8_t max_partitions=3, uint8_t lmdb_count=3,
	      bool use_inotify=false)
    : driver(driver), bucket_root(bucket_root), max_buckets(max_buckets),
      lru(max_lanes, max_buckets/max_lanes),
      cache(max_lanes, max_buckets/max_partitions),
      rp(bucket_root),
      lmdbs(driver->ctx(), database_root, lmdb_count, max_buckets),
      un(Notify::factory(this, bucket_root, use_inotify))
    {
      if (! (sf::exists(rp) && sf::is_directory(rp))) {
	std::cerr << fmt::format("{} bucket root {} invalid", __func__,
				 bucket_root) << std::endl;
	exit(1);
      }

      sf::path dp{database_root};
      if (! (sf::exists(dp) && sf::is_directory(dp))) {
	std::cerr << fmt::format("{} database root {} invalid", __func__,
				 database_root) << std::endl;
	exit(1);
      }
    }

  ~BucketCache() {
    /* Stop the inotify thread first to prevent it from accessing
     * cache entries while we're draining them. This prevents
     * heap-use-after-free errors. */
    un.reset();

    /* Clean up all cached bucket entries to prevent memory leaks.
     * Drain the AVL cache and unref each entry to trigger deletion.
     *
     * Entries should have refcount=1 (sentinel state) after normal use
     * because every public method that calls get_bucket() uses a scope_guard
     * to pair lru.unref() with the get_bucket() ref on all paths (including
     * exceptions). When we call unref here, the refcount goes to 0 and the
     * entry is deleted.
     *
     * The drain() method safely removes all entries
     * from the AVL cache partitions and calls the supplied lambda on each,
     * allowing proper cleanup without accessing private members. */
    cache.drain([this](BucketCacheEntry<D, B>* entry) {
      lru.unref(entry, cohort::lru::FLAG_NONE);
    }, cache.FLAG_LOCK);
  }

  static constexpr uint32_t FLAG_NONE     = 0x0000;
  static constexpr uint32_t FLAG_CREATE   = 0x0001;
  static constexpr uint32_t FLAG_LOCK     = 0x0002;

  typedef std::tuple<BucketCacheEntry<D, B>*, uint32_t> GetBucketResult;

  GetBucketResult get_bucket(const DoutPrefixProvider* dpp, const std::string& name, uint32_t flags)
    {
      /* this fn returns a bucket locked appropriately, having atomically
       * found or inserted the required BucketCacheEntry in_avl*/
      BucketCacheEntry<D, B>* b{nullptr};
      typename BucketCacheEntry<D, B>::Factory fac(this, name);
      typename BucketCacheEntry<D, B>::bucket_avl_cache::Latch lat;
      uint32_t iflags{cohort::lru::FLAG_INITIAL};
      GetBucketResult result{nullptr, 0};

    retry:
      b = cache.find_latch(fac.hk /* partition selector */,
			   name /* key */, lat /* serializer */, BucketCacheEntry<D, B>::bucket_avl_cache::FLAG_LOCK);
      /* LATCHED */
      if (b) {
	b->mtx.lock();
	if (b->deleted() ||
	    ! lru.ref(b, cohort::lru::FLAG_INITIAL)) {
	  // lru ref failed
	  lat.lock->unlock();
	  b->mtx.unlock();
	  goto retry;
	}
	lat.lock->unlock();
	/* LOCKED */
      } else {
	/* BucketCacheEntry not in cache */
	if (! (flags & BucketCache<D, B>::FLAG_CREATE)) {
          /* the caller does not want to instantiate a new cache
	   * entry (i.e., only wants to notify on an existing one) */
        lat.lock->unlock();
        return result;
        }
	/* we need to create it */
	b = static_cast<BucketCacheEntry<D, B>*>(
	  lru.insert(&fac, cohort::lru::Edge::MRU, iflags));
	if (b) [[likely]] {
	  ldpp_dout(dpp, 2) << "BucketCache: "
	    << (iflags & cohort::lru::FLAG_RECYCLE ? "recycled" : "allocated new")
	    << " entry, LRU total=" << lru.get_size()
	    << " (q=" << lru.get_q_size()
	    << " active=" << lru.get_active_size() << ")"
	    << ", bucket=" << name << dendl;
	  b->mtx.lock();

	  /* attach bucket to an lmdb partition and prepare it for i/o */
	  auto& env = lmdbs.get_sp_env(b);
	  auto cmp = B::lmdb_cmp();
	  auto dbi_opt = lmdbs.get_dbi(b, [&]() {
	    return cmp
	      ? env->openDB(b->name, MDB_CREATE, cmp)
	      : env->openDB(b->name, MDB_CREATE);
	  });
	  if (!dbi_opt) {
	    b->mtx.unlock();
	    lat.lock->unlock();
	    return result;
	  }
	  b->set_env(env, *dbi_opt);

	  if (! (iflags & cohort::lru::FLAG_RECYCLE)) [[likely]] {
	    /* inserts at cached insert iterator, releasing latch */
	    cache.insert_latched(b, lat, BucketCacheEntry<D, B>::bucket_avl_cache::FLAG_UNLOCK);
	  } else {
	    /* recycle invalidated the cached insert position, but the latch's
	     * partition lock still guards this partition; hold it across the
	     * insert so we don't race concurrent tree ops, then release it */
	    cache.insert(fac.hk, b, BucketCacheEntry<D, B>::bucket_avl_cache::FLAG_NONE);
	    lat.lock->unlock(); /* !LATCHED */
	  }
	  get<1>(result) |= BucketCache<D, B>::FLAG_CREATE;
	} else {
	  /* XXX lru allocate failed? seems impossible--that would mean that
	   * fallback to the allocator also failed, and maybe we should abend */
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
      } /* have BucketCacheEntry */

      if (! (flags & BucketCache<D, B>::FLAG_LOCK)) {
	b->mtx.unlock();
      }
      get<0>(result) = b;
      return result;
    } /* get_bucket */

  static inline std::string concat_key(const rgw_obj_index_key& k) {
    std::string k_str;
    k_str.reserve(k.name.size() + 1 + k.instance.size());
    k_str += k.name;
    k_str += '\0';
    k_str += k.instance;
    return k_str;
  }

  int fill(const DoutPrefixProvider* dpp, BucketCacheEntry<D, B>* bucket,
	    B* sal_bucket, uint32_t flags, optional_yield y) /* assert: LOCKED */
  {
    try {
      auto txn = bucket->env->getRWTransaction();

      /* clear stale data from a prior hiwat eviction before repopulating */
      mdb_drop(*txn, bucket->dbi, 0);

      /* instruct the bucket provider to enumerate all entries,
       * in any order */
      auto rc = sal_bucket->fill_cache(dpp, y,
	[&](const DoutPrefixProvider* dpp, rgw_bucket_dir_entry& bde) -> int {
	  auto concat_k = concat_key(bde.key);
	  std::string ser_data;
	  zpp::bits::out out(ser_data);
	  struct timespec ts{ceph::real_clock::to_timespec(bde.meta.mtime)};
          auto errc =
              out(bde.key.name, bde.key.instance, /* XXX bde.key.ns, */
                  bde.ver.pool, bde.ver.epoch, bde.exists, bde.meta.category,
                  bde.meta.size, ts.tv_sec, ts.tv_nsec, bde.meta.owner,
                  bde.meta.owner_display_name, bde.meta.accounted_size,
                  bde.meta.storage_class, bde.meta.appendable, bde.meta.etag,
                  bde.flags);
#ifndef NDEBUG
          std::cout << fmt::format("fill: bde.key.name: {}", bde.key.name)
          << std::endl;
#endif
	  if (errc.code != std::errc{0}) {
	    return -EIO;
	  }
	  txn->put(bucket->dbi, concat_k, ser_data);
#ifndef NDEBUG
	  std::cout << fmt::format("{} {}", __func__, bde.key.name) << '\n';
#endif
	  return 0;
	});

      txn->commit();
      bucket->flags |= BucketCacheEntry<D, B>::FLAG_FILLED;
      un->add_watch(bucket->name, bucket);
      return rc;
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "BucketCache: fill failed for "
	<< bucket->name << ": " << e.what() << dendl;
      return -EIO;
    }
    } /* fill */

  int list_bucket(const DoutPrefixProvider* dpp, optional_yield y, B* sal_bucket,
		  std::string marker, list_bucket_each_t each_func) {

    using namespace LMDBSafe;

    int rc __attribute__((unused)) = 0;
    GetBucketResult gbr;
    try {
      gbr = get_bucket(dpp, sal_bucket->get_name(),
		   BucketCache<D, B>::FLAG_LOCK | BucketCache<D, B>::FLAG_CREATE);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "BucketCache: get_bucket failed for "
	<< sal_bucket->get_name() << ": " << e.what() << dendl;
      return -EIO;
    }
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (!b) {
      ldpp_dout(dpp, 0) << "BucketCache: get_bucket returned null for "
	<< sal_bucket->get_name() << dendl;
      return -EIO;
    }
    auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
    unique_lock ulk{b->mtx, std::adopt_lock};
    if (! (b->flags & BucketCacheEntry<D, B>::FLAG_FILLED)) {
      /* bulk load into lmdb cache */
      ldpp_dout(dpp, 2) << "BucketCache: filling bucket=" << sal_bucket->get_name()
	<< (flags & BucketCache<D, B>::FLAG_CREATE ? " (new entry)" : " (refill)") << dendl;
      rc = fill(dpp, b, sal_bucket, FLAG_NONE, y);
    }
    /* display them */
    ulk.unlock();
    /*! LOCKED */

    try {
      auto txn = b->env->getROTransaction();
      auto cursor=txn->getCursor(b->dbi);
      MDBOutVal key, data;
      bool again{true};

      const auto proc_result = [&]() {
        zpp::bits::errc errc{};
        rgw_bucket_dir_entry bde{};
        /* XXX we may not need to recover the cache key */
        std::string_view svk __attribute__((unused)) =
          key.get<string_view>(); // {name, instance, [ns]}
        std::string_view svv = data.get<string_view>();
        std::string ser_v{svv};
        zpp::bits::in in_v(ser_v);
        struct timespec ts;
        errc = in_v(
            bde.key.name, bde.key.instance, /* bde.key.ns, */
            bde.ver.pool, bde.ver.epoch, bde.exists, bde.meta.category,
            bde.meta.size, ts.tv_sec, ts.tv_nsec, bde.meta.owner,
            bde.meta.owner_display_name, bde.meta.accounted_size,
            bde.meta.storage_class, bde.meta.appendable, bde.meta.etag,
            bde.flags);
        if (errc.code != std::errc{0}) {
	  ldpp_dout(dpp, 0) << "BucketCache: deserialization error listing "
	    << sal_bucket->get_name()
	    << " errc=" << static_cast<int>(errc.code) << dendl;
          return;
        }
        bde.meta.mtime = ceph::real_clock::from_timespec(ts);
        again = each_func(bde);
      };

      if (! marker.empty()) {
	MDBInVal k(marker);
	auto rc = cursor.lower_bound(k, key, data);
	if (rc == MDB_NOTFOUND) {
	  /* no key sorts after k/marker, so there is nothing to do */
	  return 0;
	}
	proc_result();
      } else {
	/* position at start of index */
	auto rc = cursor.get(key, data, MDB_FIRST);
	if (rc == MDB_NOTFOUND) {
	  /* no initial key */
	  return 0;
	}
	if (rc == MDB_SUCCESS) {
	  proc_result();
	}
      }
      while(cursor.get(key, data, MDB_NEXT) == MDB_SUCCESS) {
	if (!again) {
	  return 0;
	}
	proc_result();
      }
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "BucketCache: LMDB error listing "
	<< sal_bucket->get_name() << ": " << e.what() << dendl;
      return -EIO;
    }

    return 0;
  } /* list_bucket */

  int notify(const std::string& bname, void* opaque,
	     const std::vector<Notifiable::Event>& evec) override {

    using namespace LMDBSafe;

    int rc{0};
    GetBucketResult gbr = get_bucket(nullptr, bname, BucketCache<D, B>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      if ((b->name != bname) ||
	  (opaque && (b != opaque)) ||
	  (! (b->flags & BucketCacheEntry<D, B>::FLAG_FILLED))) {
	/* do nothing */
	return 0;
      }
      /* NB: b->mtx is held (not unlocked here) across the LMDB writes below
       * so that fill() and notify() remain mutually exclusive on this
       * bucket -- see b54e38b0d41 (fix notify() race with invalidation) */
      try {
        auto txn = b->env->getRWTransaction();
        for (const auto& ev : evec) {
          using EventType = Notifiable::EventType;
#ifndef NDEBUG
          std::string_view nil{""};
          std::cout << fmt::format("notify {} {}!",
                                  ev.name ? *ev.name : nil,
                                  uint32_t(ev.type))
                                  << std::endl;
#endif
          switch (ev.type)
          {
            case EventType::ADD:
            {
              rgw_bucket_dir_entry bde{};
              bde.key.name = *ev.name;
              /* XXX will need work (if not straight up magic) to have
              * side loading support instance and ns */
              auto concat_k = concat_key(bde.key);
              rc = driver->mint_listing_entry(b->name, bde);
              std::string ser_data;
              zpp::bits::out out(ser_data);
              struct timespec ts{ceph::real_clock::to_timespec(bde.meta.mtime)};
              auto errc =
                  out(bde.key.name, bde.key.instance, /* XXX bde.key.ns, */
                      bde.ver.pool, bde.ver.epoch, bde.exists, bde.meta.category,
                      bde.meta.size, ts.tv_sec, ts.tv_nsec, bde.meta.owner,
                      bde.meta.owner_display_name, bde.meta.accounted_size,
                      bde.meta.storage_class, bde.meta.appendable, bde.meta.etag,
                      bde.flags);
              if (errc.code != std::errc{0}) {
                break;
              }
              txn->put(b->dbi, concat_k, ser_data);
            }
              break;
            case EventType::REMOVE:
            {
              auto& ev_name = *ev.name;
              txn->del(b->dbi, ev_name);
            }
              break;
            [[unlikely]] case EventType::INVALIDATE:
            {
              /* yikes, cache blown */
              lsubdout(driver->ctx(), rgw, 2) << "BucketCache: notify INVALIDATE (inotify overflow)"
                << " bucket=" << b->name << dendl;
              mdb_drop(*txn, b->dbi, 0);
              txn->commit();
              b->flags &= ~BucketCacheEntry<D, B>::FLAG_FILLED;
              ulk.unlock();
              return 0; /* don't process any more events in this batch */
            }
              break;
            default:
              /* unknown event */
              break;
          }
        } /* all events */
        txn->commit();
      } catch (const std::exception& e) {
	lsubdout(driver->ctx(), rgw, 0) << "BucketCache: notify failed for "
	  << b->name << ": " << e.what() << dendl;
      }
      ulk.unlock();
    } /* b */
    return rc;
  } /* notify */

  int add_entry(const DoutPrefixProvider* dpp, std::string bname, rgw_bucket_dir_entry bde) {
    using namespace LMDBSafe;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      ulk.unlock();

      try {
        auto txn = b->env->getRWTransaction();
        auto concat_k = concat_key(bde.key);
        std::string ser_data;
        zpp::bits::out out(ser_data);
        struct timespec ts {
          ceph::real_clock::to_timespec(bde.meta.mtime)
        };
        auto errc =
            out(bde.key.name, bde.key.instance, /* XXX bde.key.ns, */
                bde.ver.pool, bde.ver.epoch, bde.exists, bde.meta.category,
                bde.meta.size, ts.tv_sec, ts.tv_nsec, bde.meta.owner,
                bde.meta.owner_display_name, bde.meta.accounted_size,
                bde.meta.storage_class, bde.meta.appendable, bde.meta.etag,
                bde.flags);
        if (errc.code != std::errc{0}) {
          return -EIO;
        }
        txn->put(b->dbi, concat_k, ser_data);

        txn->commit();
      } catch (const std::exception& e) {
	ldpp_dout(dpp, 0) << "BucketCache: add_entry failed for "
	  << bname << ": " << e.what() << dendl;
      }
    } /* b */

    return 0;
  } /* add_entry */

  int remove_entry(const DoutPrefixProvider* dpp, std::string bname, cls_rgw_obj_key key) {
    using namespace LMDBSafe;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      ulk.unlock();

      try {
        auto txn = b->env->getRWTransaction();
        auto concat_k = concat_key(key);
        txn->del(b->dbi, concat_k);
        txn->commit();
      } catch (const std::exception& e) {
	ldpp_dout(dpp, 2) << "BucketCache: remove_entry failed for "
	  << bname << ": " << e.what() << dendl;
      }
    } /* b */

    return 0;
  } /* remove_entry */

  /* invalidate a bucket's listing cache.
   *
   * recycle=false (default): clear the LMDB data but keep the DBI
   *   handle mapped to this bucket name for future reuse.
   *
   * recycle=true: clear the LMDB data and return the DBI handle to
   *   a free pool so it can be assigned to a different name.  Use
   *   for multipart upload shadows whose unique names are never
   *   reused — without recycling, each upload permanently consumes
   *   a DBI slot. */
  int invalidate_bucket(const DoutPrefixProvider* dpp, std::string bname,
			bool recycle = false) {
    using namespace LMDBSafe;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};

      ldpp_dout(dpp, 2) << "BucketCache: invalidate bucket=" << bname
	<< (recycle ? " (recycle)" : "") << dendl;

      try {
        auto txn = b->env->getRWTransaction();
        mdb_drop(*txn, b->dbi, 0);
        txn->commit();
      } catch (const LMDBSafe::LMDBError& e) {
	ldpp_dout(dpp, 0) << "BucketCache: invalidate mdb_drop failed for "
	  << bname << ": " << e.what() << dendl;
      }
      if (recycle) {
	lmdbs.recycle_dbi(b);
      }
      b->flags &= ~BucketCacheEntry<D, B>::FLAG_FILLED;

      ulk.unlock();
    } /* b */

    return 0;
  } /* invalidate_bucket */
}; /* BucketCache */

} // namespace file::listing
