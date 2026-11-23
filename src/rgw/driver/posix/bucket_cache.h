// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <iostream>
#include <lmdb.h>
#include <map>
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
#include "include/scope_guard.h"
#include "lmdb-safe.hh"
#include "notify.h"
#include <stdint.h>
#include <time.h> // struct timespec
#include <xxhash.h>

#include "rgw_common.h"
#include "rgw_sal.h"

#include "fmt/format.h"

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

  virtual ~BucketCacheEntry()
  {
    /* XXX depends on safe_link -- but I think on balance, built-in safe_link
     * is preferable to a custom mechanism with likely the same cost */
    if (name_hook.is_linked()) {
      bc->cache.remove(hk, this, bucket_avl_cache::FLAG_NONE);
    }
    mdb_dbi_close(*env, dbi); // return db handle
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
      auto lock = lock_guard{mtx};
      if (! deleted()) {
	flags |= FLAG_DELETED;
	bc->recycle_count++;

	//std::cout << fmt::format("reclaim {}!", name) << std::endl;
	bc->un->remove_watch(name);

	// XXX depends on safe_link
	if (! name_hook.is_linked()) {
	  // this should not happen!
	  abort();
	}

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

  static std::string make_cache_key(const rgw_obj_index_key& entry)
  {
    std::string key;
    key.reserve(entry.name.size() + entry.instance.size() + 2);
    // Sort versions by name first, even when one object name is a prefix
    // of another. Escape embedded NULs so only the separator sorts first.
    for (char byte : entry.name) {
      key.push_back(byte);
      if (byte == '\0') {
        key.push_back('\1');
      }
    }
    key.append(2, '\0');
    key += entry.instance;
    return key;
  }

  // LMDB limits keys to about 511 bytes. Store keys that reach that limit
  // in groups keyed by their prefix so range scans retain S3 key order.
  using EntryGroup = std::map<std::string, rgw_bucket_dir_entry>;

  template <typename T>
  static void put_value(LMDBSafe::MDBRWTransactionImpl& txn,
                        MDB_dbi dbi, std::string_view key, const T& value)
  {
    bufferlist bl;
    using ceph::encode;
    encode(value, bl);
    txn.put(dbi, key, bl.to_str());
  }

  template <typename T>
  static void decode_value(std::string_view data, T* value)
  {
    bufferlist bl;
    bl.append(data.data(), data.size());
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(*value, iter);
  }

  static void update_entry(LMDBSafe::MDBRWTransactionImpl& txn,
                           BucketCacheEntry<D, B>* bucket,
                           const std::string& key,
                           const rgw_bucket_dir_entry* entry)
  {
    const auto max_key_size = size_t(mdb_env_get_maxkeysize(*bucket->env));
    if (key.size() < max_key_size) {
      if (entry) {
        put_value(txn, bucket->dbi, key, *entry);
      } else {
        txn.del(bucket->dbi, key);
      }
      return;
    }

    std::string_view prefix(key.data(), max_key_size);
    LMDBSafe::MDBOutVal data;
    EntryGroup group;
    if (txn.get(bucket->dbi, prefix, data) == MDB_SUCCESS) {
      decode_value(data.get<std::string_view>(), &group);
    }
    if (entry) {
      group.insert_or_assign(key, *entry);
    } else {
      group.erase(key);
    }
    if (group.empty()) {
      txn.del(bucket->dbi, prefix);
    } else {
      put_value(txn, bucket->dbi, prefix, group);
    }
  }

  int fill(const DoutPrefixProvider* dpp, BucketCacheEntry<D, B>* bucket,
           B* sal_bucket, uint32_t flags, optional_yield y) /* assert: LOCKED */
  {
    const auto max_key_size = size_t(mdb_env_get_maxkeysize(*bucket->env));
    std::map<std::string, EntryGroup> groups;
    auto txn = bucket->env->getRWTransaction();
    const auto rc = sal_bucket->fill_cache(dpp, y,
        [&](const DoutPrefixProvider*, rgw_bucket_dir_entry& entry) {
          auto key = make_cache_key(entry.key);
          if (key.size() < max_key_size) {
            put_value(*txn, bucket->dbi, key, entry);
          } else {
            auto prefix = key.substr(0, max_key_size);
            groups[std::move(prefix)].insert_or_assign(std::move(key), std::move(entry));
          }
          return 0;
        });
    if (rc < 0) {
      return rc;
    }
    for (const auto& [prefix, group] : groups) {
      put_value(*txn, bucket->dbi, prefix, group);
    }
    txn->commit();
    bucket->flags |= BucketCacheEntry<D, B>::FLAG_FILLED;
    un->add_watch(bucket->name, bucket);
    return 0;
  }

  int list_bucket(const DoutPrefixProvider* dpp, optional_yield y, B* sal_bucket,
                  std::string marker, list_bucket_each_t each_func)
  {
    using namespace LMDBSafe;

    auto [b, flags] = get_bucket(dpp, sal_bucket->get_name(),
                                 FLAG_LOCK | FLAG_CREATE);
    if (!b) {
      return 0;
    }
    auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
    unique_lock ulk{b->mtx, std::adopt_lock};
    if (!(b->flags & BucketCacheEntry<D, B>::FLAG_FILLED)) {
      const int ret = fill(dpp, b, sal_bucket, FLAG_NONE, y);
      if (ret < 0) {
        return ret;
      }
    }

    const auto max_key_size = size_t(mdb_env_get_maxkeysize(*b->env));
    auto txn = b->env->getROTransaction();
    ulk.unlock();
    auto cursor = txn->getCursor(b->dbi);
    MDBOutVal key, data;
    int ret = marker.empty()
        ? cursor.get(key, data, MDB_FIRST)
        : cursor.lower_bound(std::string_view(marker).substr(0, max_key_size),
                             key, data);
    for (; ret == MDB_SUCCESS; ret = cursor.get(key, data, MDB_NEXT)) {
      const auto value = data.get<std::string_view>();
      if (key.get<std::string_view>().size() < max_key_size) {
        rgw_bucket_dir_entry entry;
        decode_value(value, &entry);
        if (!each_func(entry)) {
          break;
        }
      } else {
        EntryGroup group;
        decode_value(value, &group);
        for (auto it = group.lower_bound(marker); it != group.end(); ++it) {
          if (!each_func(it->second)) {
            return 0;
          }
        }
      }
    }
    return 0;
  }

  int notify(const std::string& bname, void* opaque,
             const std::vector<Notifiable::Event>& evec) override
  {
    using EventType = Notifiable::EventType;

    auto [b, flags] = get_bucket(nullptr, bname, FLAG_LOCK);
    if (!b) {
      return 0;
    }
    auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
    unique_lock ulk{b->mtx, std::adopt_lock};
    if (b->name != bname || (opaque && b != opaque) ||
        !(b->flags & BucketCacheEntry<D, B>::FLAG_FILLED)) {
      return 0;
    }

    auto txn = b->env->getRWTransaction();
    for (const auto& ev : evec) {
      if (ev.type == EventType::INVALIDATE ||
          (ev.name && driver->listing_name_needs_rescan(*ev.name))) {
        // A hashed filename cannot be decoded after a remove event.
        mdb_drop(*txn, b->dbi, 0);
        txn->commit();
        b->flags &= ~BucketCacheEntry<D, B>::FLAG_FILLED;
        return 0;
      }
      if (!ev.name || ev.name->starts_with('.')) {
        continue;
      }

      if (ev.type == EventType::ADD) {
        rgw_bucket_dir_entry entry;
        entry.key.name = *ev.name;
        if (driver->mint_listing_entry(b->name, entry) == 0) {
          update_entry(*txn, b, make_cache_key(entry.key), &entry);
        }
      } else if (ev.type == EventType::REMOVE) {
        auto key = make_cache_key(driver->listing_key_from_filename(*ev.name));
        update_entry(*txn, b, key, nullptr);
      }
    }
    txn->commit();
    return 0;
  }

  int add_entry(const DoutPrefixProvider* dpp, std::string bname, rgw_bucket_dir_entry bde)
  {
    auto [b, flags] = get_bucket(dpp, bname, FLAG_LOCK);
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      if (b->flags & BucketCacheEntry<D, B>::FLAG_FILLED) {
        auto txn = b->env->getRWTransaction();
        update_entry(*txn, b, make_cache_key(bde.key), &bde);
        txn->commit();
      }
    }
    return 0;
  }

  int remove_entry(const DoutPrefixProvider* dpp, std::string bname, cls_rgw_obj_key key)
  {
    auto [b, flags] = get_bucket(dpp, bname, FLAG_LOCK);
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      if (b->flags & BucketCacheEntry<D, B>::FLAG_FILLED) {
        auto txn = b->env->getRWTransaction();
        update_entry(*txn, b, make_cache_key(key), nullptr);
        txn->commit();
      }
    }
    return 0;
  }

  int invalidate_bucket(const DoutPrefixProvider* dpp, std::string bname) {
    using namespace LMDBSafe;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};

      auto txn = b->env->getRWTransaction();
      mdb_drop(*txn, b->dbi, 0);
      txn->commit();
      b->flags &= ~BucketCacheEntry<D, B>::FLAG_FILLED;

    } /* b */

    return 0;
  } /* invalidate_bucket */
}; /* BucketCache */

} // namespace file::listing
