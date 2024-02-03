// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include <boost/intrusive/avl_set.hpp>
#include "include/function2.hpp"
#include "common/cohort_lru.h"
#include "lmdb-safe.hh"
#include "zpp_bits.h"
#include "notify.h"
#include <stdint.h>
#include <time.h> // struct timespec
#include <xxhash.h>

#include "rgw_common.h"
#include "rgw_sal.h"

#include "fmt/format.h"

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
    : bc(bc), name(name), hk(hk), flags(FLAG_NONE) {}

  void set_env(std::shared_ptr<LMDBSafe::MDBEnv>& _env, LMDBSafe::MDBDbi& _dbi) {
    env = _env;
    dbi = _dbi;
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
    { /* anon block */
      /* in this case, we are being called from a context which holds
       * A partition lock, and this may be still in use */
      lock_guard{mtx};
      if (! deleted()) {
	flags |= FLAG_DELETED;
	bc->recycle_count++;

	//std::cout << fmt::format("reclaim {}!", name) << std::endl;
	bc->un->remove_watch(name);
#if 1
	// depends on safe_link
	if (! name_hook.is_linked()) {
	  // this should not happen!
	  abort();
	}
#endif
	bc->cache.remove(hk, this, bucket_avl_cache::FLAG_NONE);

	/* discard lmdb data associated with this bucket */
	auto txn = env->getRWTransaction();
	mdb_drop(*txn, dbi, 0);
	txn->commit();
	/* LMDB applications don't "normally" close database handles,
	 * but doing so (atomically) is supported, and we must as
	 * we continually recycle them */
	mdb_dbi_close(*env, dbi); // return db handle
      } /* ! deleted */
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
    std::string database_root;
    uint8_t lmdb_count;
    std::vector<std::shared_ptr<LMDBSafe::MDBEnv>> envs;
    sf::path dbp;

  public:
    Lmdbs(std::string& database_root, uint8_t lmdb_count)
      : database_root(database_root), lmdb_count(lmdb_count),
        dbp(database_root) {

      /* create a root for lmdb directory partitions (if it doesn't
       * exist already) */
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
	auto env = LMDBSafe::getMDBEnv(env_path.string().c_str(), 0 /* flags? */, 0600);
	envs.push_back(env);
      }
    }

    inline std::shared_ptr<LMDBSafe::MDBEnv>& get_sp_env(BucketCacheEntry<D, B>* bucket)  {
      return envs[(bucket->hk % lmdb_count)];
    }

    inline LMDBSafe::MDBEnv& get_env(BucketCacheEntry<D, B>* bucket) {
      return *(get_sp_env(bucket));
    }

    const std::string& get_root() const { return database_root; }
  } lmdbs;

  std::unique_ptr<Notify> un;

public:
  BucketCache(D* driver, std::string bucket_root, std::string database_root,
	      uint32_t max_buckets=100, uint8_t max_lanes=3,
	      uint8_t max_partitions=3, uint8_t lmdb_count=3)
    : driver(driver), bucket_root(bucket_root), max_buckets(max_buckets),
      lru(max_lanes, max_buckets/max_lanes),
      cache(max_lanes, max_buckets/max_partitions),
      rp(bucket_root),
      lmdbs(database_root, lmdb_count),
      un(Notify::factory(this, bucket_root))
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

  static constexpr uint32_t FLAG_NONE     = 0x0000;
  static constexpr uint32_t FLAG_CREATE   = 0x0001;
  static constexpr uint32_t FLAG_LOCK     = 0x0002;

  typedef std::tuple<BucketCacheEntry<D, B>*, uint32_t> GetBucketResult;

  GetBucketResult get_bucket(const std::string& name, uint32_t flags)
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
	  return result;
	}
	/* we need to create it */
	b = static_cast<BucketCacheEntry<D, B>*>(
	  lru.insert(&fac, cohort::lru::Edge::MRU, iflags));
	if (b) [[likely]] {
	  b->mtx.lock();

	  /* attach bucket to an lmdb partition and prepare it for i/o */
	  auto& env = lmdbs.get_sp_env(b);
	  auto dbi = env->openDB(b->name, MDB_CREATE);
	  b->set_env(env, dbi);

	  if (! (iflags & cohort::lru::FLAG_RECYCLE)) [[likely]] {
	    /* inserts at cached insert iterator, releasing latch */
	    cache.insert_latched(b, lat, BucketCacheEntry<D, B>::bucket_avl_cache::FLAG_UNLOCK);
	  } else {
	    /* recycle step invalidates Latch */
	    lat.lock->unlock(); /* !LATCHED */
	    cache.insert(fac.hk, b, BucketCacheEntry<D, B>::bucket_avl_cache::FLAG_NONE);
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
    k_str.reserve(k.name.size() + k.instance.size());
    k_str += k.name;
    k_str += k.instance;
    return k_str;
  }

  int fill(const DoutPrefixProvider* dpp, BucketCacheEntry<D, B>* bucket,
	    B* sal_bucket, uint32_t flags, optional_yield y) /* assert: LOCKED */
  {
      auto txn = bucket->env->getRWTransaction();

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
		bde.ver.pool, bde.ver.epoch, bde.exists,
		bde.meta.category, bde.meta.size, ts.tv_sec, ts.tv_nsec,
		bde.meta.owner, bde.meta.owner_display_name, bde.meta.accounted_size,
		bde.meta.storage_class, bde.meta.appendable, bde.meta.etag);
	  /*std::cout << fmt::format("fill: bde.key.name: {}", bde.key.name)
	    << std::endl;*/
	  if (errc.code != std::errc{0}) {
	    abort();
	    return 0; // XXX non-zero return?
	  }
	  txn->put(bucket->dbi, concat_k, ser_data);
	  //std::cout << fmt::format("{} {}", __func__, bde.key.name) << '\n';
	  return 0;
	});

      txn->commit();
      bucket->flags |= BucketCacheEntry<D, B>::FLAG_FILLED;
      un->add_watch(bucket->name, bucket);
      return rc;
    } /* fill */

  int list_bucket(const DoutPrefixProvider* dpp, optional_yield y, B* sal_bucket,
		  std::string marker, list_bucket_each_t each_func) {

    using namespace LMDBSafe;

    int rc __attribute__((unused)) = 0;
    GetBucketResult gbr =
      get_bucket(sal_bucket->get_name(),
		 BucketCache<D, B>::FLAG_LOCK | BucketCache<D, B>::FLAG_CREATE);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b /* XXX again, can this fail? */) {
      if (! (b->flags & BucketCacheEntry<D, B>::FLAG_FILLED)) {
	/* bulk load into lmdb cache */
	rc = fill(dpp, b, sal_bucket, FLAG_NONE, y);
      }
      /* display them */
      b->mtx.unlock();
      /*! LOCKED */

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
	errc =
	  in_v(bde.key.name, bde.key.instance, /* bde.key.ns, */
	       bde.ver.pool, bde.ver.epoch, bde.exists,
	       bde.meta.category, bde.meta.size, ts.tv_sec, ts.tv_nsec,
	       bde.meta.owner, bde.meta.owner_display_name, bde.meta.accounted_size,
	       bde.meta.storage_class, bde.meta.appendable, bde.meta.etag);
	if (errc.code != std::errc{0}) {
	  abort();
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
      lru.unref(b, cohort::lru::FLAG_NONE);
    } /* b */

    return 0;
  } /* list_bucket */

  int notify(const std::string& bname, void* opaque,
	     const std::vector<Notifiable::Event>& evec) override {

    using namespace LMDBSafe;

    int rc{0};
    GetBucketResult gbr = get_bucket(bname, BucketCache<D, B>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      unique_lock ulk{b->mtx, std::adopt_lock};
      if ((b->name != bname) ||
	  (b != opaque) ||
	  (! (b->flags & BucketCacheEntry<D, B>::FLAG_FILLED))) {
	/* do nothing */
	return 0;
      }
      ulk.unlock();
      auto txn = b->env->getRWTransaction();
      for (const auto& ev : evec) {
	using EventType = Notifiable::EventType;
	/*
	std::string_view nil{""};
	std::cout << fmt::format("notify {} {}!",
				 ev.name ? *ev.name : nil,
				 uint32_t(ev.type))
				 << std::endl; */
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
		bde.ver.pool, bde.ver.epoch, bde.exists,
		bde.meta.category, bde.meta.size, ts.tv_sec, ts.tv_nsec,
		bde.meta.owner, bde.meta.owner_display_name, bde.meta.accounted_size,
		bde.meta.storage_class, bde.meta.appendable, bde.meta.etag);
	  if (errc.code != std::errc{0}) {
	    abort();
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
	  ulk.lock();
	  mdb_drop(*txn, b->dbi, 0);
	  txn->commit();
	  b->flags &= ~BucketCacheEntry<D, B>::FLAG_FILLED;
	  return 0; /* don't process any more events in this batch */
	}
	  break;
	default:
	  /* unknown event */
	  break;
	}
      } /* all events */
      txn->commit();
      lru.unref(b, cohort::lru::FLAG_NONE);
    } /* b */
    return rc;
  } /* notify */
  
}; /* BucketCache */

} // namespace file::listing
