// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <iostream>
#include <lmdb.h>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <tuple>
#include <vector>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <filesystem>
#include <boost/container/flat_map.hpp>
#include <boost/intrusive/avl_set.hpp>
#include "include/function2.hpp"
#include "common/ceph_time.h"
#include "common/errno.h"
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

/* Revalidation:  a listing checks the tree as it walks, so that what a
 * filesystem client changed after the fill is listed (nsfs's
 * rgw_nsfs_listing_revalidate).  The cache does the walk and keeps the
 * records;  the driver supplies what touches the filesystem
 * (Revalidation, below).  A cache given none trusts its fill, as posix's
 * does.
 *
 * Every name here is an index key's name -- what the cache's keys hold
 * before the '\0' -- and a directory is named by its prefix, "" for the
 * bucket's own and otherwise ending in '/'. */

inline int64_t statx_ns(const struct statx_timestamp& t) {
  return int64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}

inline int64_t revalidation_now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    ceph::coarse_mono_clock::now().time_since_epoch()).count();
}

/* An entry's recorded stat:  the file holding a current object, as it
 * was when the entry was built, and when a listing last compared it.  A
 * value carries one whether or not anything reads it;  an unknown one
 * (ino 0) never matches, so its entry is rebuilt at its first check. */
struct EntryStat {
  uint64_t ino{0};
  uint64_t size{0};
  int64_t mtime_ns{0};
  int64_t ctime_ns{0};
  int64_t checked_ns{0};	/* revalidation_now_ns() */

  bool known() const { return ino != 0; }
  bool same_file(const EntryStat& o) const {
    return known() && (ino == o.ino) && (size == o.size) &&
      (mtime_ns == o.mtime_ns) && (ctime_ns == o.ctime_ns);
  }
  static EntryStat of(const struct statx& stx) {
    EntryStat s;
    s.ino = stx.stx_ino;
    s.size = stx.stx_size;
    s.mtime_ns = statx_ns(stx.stx_mtime);
    s.ctime_ns = statx_ns(stx.stx_ctime);
    s.checked_ns = revalidation_now_ns();
    return s;
  }
};

/* A directory's recorded inode and mtime, and its .versions/'s where it
 * has one (0 where not). */
struct DirStat {
  uint64_t ino{0};
  int64_t mtime_ns{0};
  uint64_t vino{0};
  int64_t vmtime_ns{0};
  bool operator==(const DirStat&) const = default;
};

struct DirRecord {
  DirStat st;
  /* false after RGW's own change:  the next check takes the directory's
   * stat as its record without reading it */
  bool baseline{true};
  /* set by RGW's first change since the directory was last read, and
   * cleared by the read;  once older than the provisional interval, the
   * next check reads the directory, for what landed beside RGW's change */
  std::optional<int64_t> provisional_ns;
};

template <typename D, typename B,
	  typename YP = cohort::lru::NullYieldPolicy>
struct BucketCache;

template <typename D, typename B,
	  typename YP = cohort::lru::NullYieldPolicy>
struct BucketCacheEntry : public cohort::lru::Object
{
  using lock_guard = std::lock_guard<std::mutex>;
  using unique_lock = std::unique_lock<std::mutex>;

  static constexpr uint32_t FLAG_NONE     = 0x0000;
  static constexpr uint32_t FLAG_FILLED   = 0x0001;
  static constexpr uint32_t FLAG_DELETED  = 0x0002;

  static constexpr uint64_t seed = 8675309;

  BucketCache<D, B, YP>* bc;
  std::string name;
  std::shared_ptr<LMDBSafe::MDBEnv> env;
  LMDBSafe::MDBDbi dbi;
  uint64_t hk;
  member_hook_t name_hook;

  // XXX clean this up
  std::mutex mtx; // XXX Adam's preferred shared mtx?
  std::condition_variable cv;
  uint32_t flags;

  /* Revalidation's record of each directory the fill walked, by prefix.
   * A map, not one value, because the cache flattens the tree into one
   * keyspace and a directory's mtime moves only with its own children.
   * Meaningful only while FLAG_FILLED is set:  cleared wherever that is,
   * and rebuilt by the fill that sets it.  Under mtx. */
  std::map<std::string, DirRecord> dirs;
  /* where the fill found the bucket's tree, and whether it read
   * .versions/, so that RGW's own change can stat a directory with no
   * bucket in hand.  With dirs. */
  std::string fs_root;
  bool fs_versions{false};

public:
  BucketCacheEntry(BucketCache<D, B, YP>* bc, const std::string& name, uint64_t hk)
    : bc(bc), name(name), env{nullptr}, hk(hk), flags(FLAG_NONE) {}

  void set_env(std::shared_ptr<LMDBSafe::MDBEnv>& _env, LMDBSafe::MDBDbi& _dbi) {
    env = _env;
    dbi = _dbi;
  }

  void lru_cleanup() override {
    bc->cleanup_count++;
    /* follow the same protocol as reclaim(): mark deleted and recycle
     * DBI under mtx, then remove the AVL node with FLAG_LOCK after
     * releasing mtx — never mtx → partition lock (deadlock) */
    {
      auto lock = lock_guard{mtx};
      if (deleted()) {
	return;
      }
      flags |= FLAG_DELETED;
      dirs.clear();
      if (bc->un) {
	bc->un->remove_watch(name);
      }
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
    } /* mtx released */

    if (name_hook.is_linked()) {
      bc->cache.remove(hk, this, bucket_avl_cache::FLAG_LOCK);
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
    BucketCache<D, B, YP>* bc;
    const std::string& name;
    uint64_t hk;
    uint32_t flags;

    Factory() = delete;
    Factory(BucketCache<D, B, YP> *bc, const std::string& name)
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

  typedef cohort::lru::LRU<std::mutex, YP> bucket_lru;

  typedef bi::member_hook<BucketCacheEntry, member_hook_t, &BucketCacheEntry::name_hook> name_hook_t;
  typedef bi::avltree<BucketCacheEntry, bi::compare<BucketCacheEntryLT>, name_hook_t> bucket_avl_t;
  typedef cohort::lru::TreeX<BucketCacheEntry, bucket_avl_t, BucketCacheEntryLT, BucketCacheEntryEQ, std::string,
			     std::mutex> bucket_avl_cache;

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) {
    auto factory = dynamic_cast<const BucketCacheEntry<D, B, YP>::Factory*>(newobj_fac);
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
	dirs.clear();
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

/* fill_cache return flags (non-negative rc) */
static constexpr int FILL_CACHE_FLAG_FIXUP_CURRENT = 0x0001;

/* What a fill reports beside its entries, for revalidation. */
struct FillRecorder {
  virtual ~FillRecorder() = default;
  /* a directory the fill reads, stat'd before it is read */
  virtual void directory(const std::string& prefix, const DirStat& st) = 0;
  /* the recorded stat of the entry the next callback carries */
  virtual void entry(const std::string& name, const EntryStat& st) = 0;
};

/* What revalidation needs of a driver:  the filesystem half.  Names in
 * and out are index keys' names (above);  the driver maps them to
 * paths. */
template <typename B>
struct Revalidation {
  virtual ~Revalidation() = default;

  /* whether this bucket's listings check its tree */
  virtual bool applies(B* b) const = 0;
  /* how often a listed entry is compared with its file;  nullopt never */
  virtual std::optional<ceph::timespan> entry_interval() const = 0;
  virtual ceph::timespan provisional_interval() const = 0;

  /* where a bucket's tree is, and whether its directories' .versions/
   * are recorded, for stat_dir_at() */
  virtual void locate(B* b, std::string& root, bool& versions) = 0;
  /* stat_dir(), given what locate() returned */
  virtual int stat_dir_at(const DoutPrefixProvider* dpp,
			  const std::string& root, bool versions,
			  const std::string& prefix, DirStat& st) = 0;

  /* the fill, reporting through `rec` */
  virtual int fill(const DoutPrefixProvider* dpp, optional_yield y, B* b,
		   fill_cache_cb_t& cb, FillRecorder& rec) = 0;
  /* 0, -ENOENT where the directory is gone, or -errno */
  virtual int stat_dir(const DoutPrefixProvider* dpp, optional_yield y,
		       B* b, const std::string& prefix, DirStat& st) = 0;
  /* the listable children of a directory:  a file's name, a
   * subdirectory's prefix */
  virtual int read_dir(const DoutPrefixProvider* dpp, optional_yield y,
		       B* b, const std::string& prefix,
		       std::vector<std::string>& children) = 0;
  /* entries for these children of `prefix`, a subdirectory whole, as the
   * fill builds them;  a child gone since is skipped */
  virtual int build(const DoutPrefixProvider* dpp, optional_yield y, B* b,
		    const std::string& prefix,
		    const std::vector<std::string>& children,
		    fill_cache_cb_t& cb, FillRecorder& rec) = 0;
  /* the stat of the file holding `name`'s current object:  0, or -ENOENT
   * where no file does */
  virtual int stat_entry(const DoutPrefixProvider* dpp, optional_yield y,
			 B* b, const std::string& name, EntryStat& st) = 0;
};

/* The directory holding `name`:  its prefix. */
inline std::string parent_prefix(std::string_view name) {
  const auto slash = name.rfind('/');
  return (slash == std::string_view::npos)
    ? std::string{} : std::string{name.substr(0, slash + 1)};
}

using list_bucket_each_t =
  const fu2::unique_function<bool(const rgw_bucket_dir_entry&) const>;

/* Predicate for remove_entries() and rename_entries():  true selects. */
using remove_entries_pred_t =
  const fu2::unique_function<bool(const rgw_bucket_dir_entry&) const>;

/* Applied by rename_entries() to each entry after its name is changed, for a
 * move that is not a pure rekey -- dropping the instance and FLAG_VER when
 * the destination bucket does not hold versions, say. */
using rekey_entry_cb_t =
  const fu2::unique_function<void(rgw_bucket_dir_entry&) const>;

template <typename D, typename B, typename YP>
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
  typename BucketCacheEntry<D, B, YP>::bucket_lru lru;
  typename BucketCacheEntry<D, B, YP>::bucket_avl_cache cache;
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
    /* mdb_env_get_maxkeysize(), captured from the first partition env;
     * see the comment where it is set */
    int max_key_sz{0};
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
	/* The longest key this build of LMDB will accept.  It is a
	 * compile-time constant of the library -- MDB_MAXKEYSIZE, 511 by
	 * default -- and LMDB is a system package rather than vendored,
	 * so the value is the distribution's and has to be asked for
	 * rather than assumed.  Every env answers the same, so the first
	 * one is enough.
	 *
	 * A listing is served from this cache, so this is also the real
	 * ceiling on an object name:  an entry that will not fit cannot
	 * be listed.  Callers compare against max_key_size() and reject
	 * the write, because the alternative is an object on disk that
	 * no listing shows. */
	if (max_key_sz == 0) {
	  max_key_sz = mdb_env_get_maxkeysize(*parts[ix].env);
	}
      }
    }

    int max_key_size() const { return max_key_sz; }

    uint8_t partition_ix(BucketCacheEntry<D, B, YP>* bucket) {
      return bucket->hk % lmdb_count;
    }

    inline std::shared_ptr<LMDBSafe::MDBEnv>& get_sp_env(BucketCacheEntry<D, B, YP>* bucket)  {
      return parts[partition_ix(bucket)].env;
    }

    inline LMDBSafe::MDBEnv& get_env(BucketCacheEntry<D, B, YP>* bucket) {
      return *(get_sp_env(bucket));
    }

    /* look up or create a dbi for the given bucket name in its
     * partition; reuses a recycled handle if available, otherwise
     * opens a new one; returns nullopt on failure (e.g. MDB_DBS_FULL) */
    std::optional<LMDBSafe::MDBDbi> get_dbi(
      BucketCacheEntry<D, B, YP>* bucket,
      fu2::unique_function<LMDBSafe::MDBDbi()> open_fn)
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
    void recycle_dbi(BucketCacheEntry<D, B, YP>* bucket) {
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

  /* the driver's half of revalidation;  none, the fill is trusted */
  Revalidation<B>* rv{nullptr};

public:
  /* what revalidation did, for tests and the curious */
  std::atomic<uint64_t> dir_check_count{0};
  std::atomic<uint64_t> reconcile_count{0};
  std::atomic<uint64_t> entry_check_count{0};
  std::atomic<uint64_t> rebuild_count{0};

  /* `r` outlives the cache;  set once, before the first listing */
  void set_revalidation(Revalidation<B>* r) { rv = r; }

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
    cache.drain([this](BucketCacheEntry<D, B, YP>* entry) {
      lru.unref(entry, cohort::lru::FLAG_NONE);
    }, cache.FLAG_LOCK);
  }

  static constexpr uint32_t FLAG_NONE     = 0x0000;
  static constexpr uint32_t FLAG_CREATE   = 0x0001;
  static constexpr uint32_t FLAG_LOCK     = 0x0002;

  typedef std::tuple<BucketCacheEntry<D, B, YP>*, uint32_t> GetBucketResult;

  GetBucketResult get_bucket(const DoutPrefixProvider* dpp, const std::string& name, uint32_t flags)
    {
      /* this fn returns a bucket locked appropriately, having atomically
       * found or inserted the required BucketCacheEntry in_avl*/
      BucketCacheEntry<D, B, YP>* b{nullptr};
      typename BucketCacheEntry<D, B, YP>::Factory fac(this, name);
      typename BucketCacheEntry<D, B, YP>::bucket_avl_cache::Latch lat;
      uint32_t iflags{cohort::lru::FLAG_INITIAL};
      GetBucketResult result{nullptr, 0};

    retry:
      b = cache.find_latch(fac.hk /* partition selector */,
			   name /* key */, lat /* serializer */, BucketCacheEntry<D, B, YP>::bucket_avl_cache::FLAG_LOCK);
      /* LATCHED */
      if (b) {
	b->mtx.lock();
	if (b->deleted() ||
	    ! lru.ref(b, cohort::lru::FLAG_INITIAL)) {
	  /* lru ref failed:  b is being evicted.  Its mutex is released
	   * before the latch, because the latch is what keeps the eviction
	   * from completing -- it removes b from this partition, under this
	   * lock, before recycling it, and a recycle constructs a new mutex
	   * in place.  Released after the latch, the unlock could land on
	   * that new mutex, which nobody holds, and leave it unusable. */
	  b->mtx.unlock();
	  lat.lock->unlock();
	  goto retry;
	}
	lat.lock->unlock();
	/* LOCKED */
      } else {
	/* BucketCacheEntry not in cache */
	if (! (flags & BucketCache<D, B, YP>::FLAG_CREATE)) {
          /* the caller does not want to instantiate a new cache
	   * entry (i.e., only wants to notify on an existing one) */
        lat.lock->unlock();
        return result;
        }
	/* we need to create it */
	b = static_cast<BucketCacheEntry<D, B, YP>*>(
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
	    cache.insert_latched(b, lat, BucketCacheEntry<D, B, YP>::bucket_avl_cache::FLAG_UNLOCK);
	  } else {
	    /* recycle invalidated the cached insert position, but the latch's
	     * partition lock still guards this partition; hold it across the
	     * insert so we don't race concurrent tree ops, then release it */
	    cache.insert(fac.hk, b, BucketCacheEntry<D, B, YP>::bucket_avl_cache::FLAG_NONE);
	    lat.lock->unlock(); /* !LATCHED */
	  }
	  get<1>(result) |= BucketCache<D, B, YP>::FLAG_CREATE;
	} else {
	  /* XXX lru allocate failed? seems impossible--that would mean that
	   * fallback to the allocator also failed, and maybe we should abend */
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
      } /* have BucketCacheEntry */

      if (! (flags & BucketCache<D, B, YP>::FLAG_LOCK)) {
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

  /* The fill's recorder:  directories into the entry's records, and the
   * stat of the entry the next callback carries. */
  struct Recorder final : FillRecorder {
    BucketCacheEntry<D, B, YP>* b;
    std::string pending_name;
    EntryStat pending;

    explicit Recorder(BucketCacheEntry<D, B, YP>* b) : b(b) {}
    void directory(const std::string& prefix, const DirStat& st) override {
      auto& r = b->dirs[prefix];
      r.st = st;
      r.baseline = true;
      r.provisional_ns.reset();
    }
    void entry(const std::string& name, const EntryStat& st) override {
      pending_name = name;
      pending = st;
    }
    /* the recorded stat for the entry `name`, unknown if none was
     * reported for it */
    EntryStat take(const std::string& name) {
      EntryStat st;
      if (name == pending_name) {
	st = pending;
	pending_name.clear();
      }
      return st;
    }
  };

  bool revalidating(B* sal_bucket) const {
    return rv && rv->applies(sal_bucket);
  }

  int fill(const DoutPrefixProvider* dpp, BucketCacheEntry<D, B, YP>* bucket,
	    B* sal_bucket, uint32_t flags, optional_yield y) /* assert: LOCKED */
  {
    try {
      auto txn = bucket->env->getRWTransaction();

      /* clear stale data from a prior hiwat eviction before repopulating */
      mdb_drop(*txn, bucket->dbi, 0);
      bucket->dirs.clear();
      bucket->fs_root.clear();
      if (revalidating(sal_bucket)) {
	rv->locate(sal_bucket, bucket->fs_root, bucket->fs_versions);
      }

      Recorder rec{bucket};
      auto put =
	[&](const DoutPrefixProvider* dpp, rgw_bucket_dir_entry& bde) -> int {
	  std::string ser_data;
	  if (ser_entry(bde, rec.take(bde.key.name), ser_data).code !=
	      std::errc{0}) {
	    return -EIO;
	  }
	  txn->put(bucket->dbi, concat_key(bde.key), ser_data);
	  return 0;
	};

      /* instruct the bucket provider to enumerate all entries,
       * in any order */
      auto rc = revalidating(sal_bucket)
	? rv->fill(dpp, y, sal_bucket, fill_cache_cb_t{put}, rec)
	: sal_bucket->fill_cache(dpp, y, put);

      /* fixup FLAG_CURRENT for orphaned delete markers: iterate LMDB
       * entries in key order (contiguous by object name).  For each
       * key group, if no entry has FLAG_CURRENT, find the newest
       * delete marker and add FLAG_CURRENT to it.
       *
       * Only needed for backends that don't track current version
       * via symlink (e.g. NSFS with .versions/ directory). */
      if (rc & FILL_CACHE_FLAG_FIXUP_CURRENT) {
        auto cursor = txn->getCursor(bucket->dbi);
        LMDBSafe::MDBOutVal ckey, cdata;
        if (cursor.get(ckey, cdata, MDB_FIRST) == 0) {
          std::string prev_name;
          bool group_has_current = false;
          std::string best_dm_concat_k;
          std::string best_dm_data;
          std::string best_dm_instance;

          auto flush_group = [&]() {
            if (!group_has_current && !best_dm_concat_k.empty()) {
              /* rewrite the newest DM entry with FLAG_CURRENT added */
              rgw_bucket_dir_entry bde{};
	      EntryStat st;
              if (deser_entry(best_dm_data, bde, &st).code == std::errc{0}) {
                bde.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
                std::string new_data;
                (void) ser_entry(bde, st, new_data);
                txn->put(bucket->dbi, best_dm_concat_k, new_data);
              }
            }
          };

          do {
            std::string_view svk = ckey.get<std::string_view>();
            std::string_view svv = cdata.get<std::string_view>();
            /* extract object name (before first '\0') */
            auto nul = svk.find('\0');
            std::string name(svk.substr(0, nul));
            std::string instance(nul != std::string_view::npos
                                 ? svk.substr(nul + 1) : "");

            if (name != prev_name) {
              flush_group();
              prev_name = name;
              group_has_current = false;
              best_dm_concat_k.clear();
              best_dm_data.clear();
              best_dm_instance.clear();
            }

            /* deserialize just the flags (last field) — quick check */
            rgw_bucket_dir_entry bde{};
            if (deser_entry(svv, bde).code != std::errc{0}) {
              continue;
            }

            if (bde.flags & rgw_bucket_dir_entry::FLAG_CURRENT) {
              group_has_current = true;
            } else if ((bde.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) &&
                       (bde.flags & rgw_bucket_dir_entry::FLAG_VER)) {
              /* candidate orphaned DM — keep if newest */
              if (best_dm_instance.empty() || instance > best_dm_instance) {
                best_dm_concat_k = std::string(svk);
                best_dm_data = std::string(svv);
                best_dm_instance = instance;
              }
            }
          } while (cursor.get(ckey, cdata, MDB_NEXT) == 0);
          flush_group();
        }
      } /* FILL_CACHE_FLAG_FIXUP_CURRENT */

      txn->commit();
      bucket->flags |= BucketCacheEntry<D, B, YP>::FLAG_FILLED;
      un->add_watch(bucket->name, bucket);
      return rc;
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "BucketCache: fill failed for "
	<< bucket->name << ": " << e.what() << dendl;
      bucket->dirs.clear();
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
		   BucketCache<D, B, YP>::FLAG_LOCK | BucketCache<D, B, YP>::FLAG_CREATE);
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
    if (! (b->flags & BucketCacheEntry<D, B, YP>::FLAG_FILLED)) {
      /* bulk load into lmdb cache */
      ldpp_dout(dpp, 2) << "BucketCache: filling bucket=" << sal_bucket->get_name()
	<< (flags & BucketCache<D, B, YP>::FLAG_CREATE ? " (new entry)" : " (refill)") << dendl;
      rc = fill(dpp, b, sal_bucket, FLAG_NONE, y);
    }
    /* display them */
    ulk.unlock();
    /*! LOCKED */

    if (revalidating(sal_bucket)) {
      return walk_revalidating(dpp, y, b, sal_bucket, marker, each_func);
    }

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
        errc = deser_entry(svv, bde);
        if (errc.code != std::errc{0}) {
	  ldpp_dout(dpp, 0) << "BucketCache: deserialization error listing "
	    << sal_bucket->get_name()
	    << " errc=" << static_cast<int>(errc.code) << dendl;
          return;
        }
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

  /* ---- revalidation:  the walk ---- */

  static bool namespaced(std::string_view name) {
    if (name.empty() || name.front() != '_') {
      return false;
    }
    std::string n, ns;
    rgw_obj_key::parse_index_key(std::string{name}, &n, &ns);
    return !ns.empty();
  }

  /* Whether an entry is backed by a file in its directory:  an
   * unversioned entry, or a current one that is not a delete marker.  A
   * noncurrent version and a delete marker live in .versions/, so reading
   * the directory says nothing about them. */
  static bool file_backed(const rgw_bucket_dir_entry& e) {
    if (!(e.flags & rgw_bucket_dir_entry::FLAG_VER)) {
      return true;
    }
    return (e.flags & rgw_bucket_dir_entry::FLAG_CURRENT) &&
      !(e.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER);
  }

  /* Whether a listing compares this entry with its file:  a current
   * object held by a file of its own.  Not a directory's object, whose
   * data is its sentinel's;  not a delete marker or a noncurrent version,
   * which no filesystem client writes;  not a namespaced entry. */
  static bool compares(const rgw_bucket_dir_entry& bde, std::string_view name) {
    if (name.empty() || (name.back() == '/') || namespaced(name)) {
      return false;
    }
    if (bde.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
      return false;
    }
    if ((bde.flags & rgw_bucket_dir_entry::FLAG_VER) &&
	!(bde.flags & rgw_bucket_dir_entry::FLAG_CURRENT)) {
      return false;
    }
    return true;
  }

  /* The recorded directories the walk enters after the name `after` and
   * through `upto` -- every one after `after` where `upto` is none -- and
   * those on the path to `upto` (to `after`, where none).  Each is checked
   * once a page (`checked`):  one stat, compared with its record.  Those
   * changed, gone, or provisional for longer than `prov_ns` are added to
   * `changed`, to be read. */
  void check_dirs(const DoutPrefixProvider* dpp, optional_yield y,
		  BucketCacheEntry<D, B, YP>* b, B* sal_bucket,
		  std::string_view after, std::optional<std::string_view> upto,
		  std::set<std::string>& checked, int64_t prov_ns,
		  std::vector<std::string>& changed) {
    std::vector<std::pair<std::string, DirRecord>> due;
    {
      lock_guard lk{b->mtx};
      auto consider = [&](const std::string& prefix, const DirRecord& r) {
	if (checked.insert(prefix).second) {
	  due.emplace_back(prefix, r);
	}
      };
      const std::string_view path = upto ? *upto : after;
      for (size_t pos = 0;;) {
	std::string prefix{path.substr(0, pos)};
	if (auto it = b->dirs.find(prefix); it != b->dirs.end()) {
	  consider(it->first, it->second);
	}
	const auto slash = path.find('/', pos);
	if (slash == std::string_view::npos) {
	  break;
	}
	pos = slash + 1;
      }
      for (auto it = b->dirs.upper_bound(std::string{after});
	   (it != b->dirs.end()) &&
	     (!upto || (std::string_view{it->first} <= *upto));
	   ++it) {
	consider(it->first, it->second);
      }
    }

    const int64_t now = revalidation_now_ns();
    for (auto& [prefix, r] : due) {
      ++dir_check_count;
      DirStat st;
      const int ret = rv->stat_dir(dpp, y, sal_bucket, prefix, st);
      if (ret == -ENOENT) {
	changed.push_back(prefix);
	continue;
      }
      if (ret < 0) {
	ldpp_dout(dpp, 1) << "BucketCache: cannot check " << b->name << "/"
	  << prefix << ": " << cpp_strerror(-ret) << ";  its listing is "
	  << "trusted" << dendl;
	continue;
      }
      const bool moved = r.baseline && !(st == r.st);
      const bool expired = r.provisional_ns &&
	((now - *r.provisional_ns) >= prov_ns);
      if (moved || expired) {
	changed.push_back(prefix);
	continue;
      }
      if (!r.baseline) {
	/* RGW's own change:  this stat is the record */
	lock_guard lk{b->mtx};
	if (auto it = b->dirs.find(prefix);
	    (it != b->dirs.end()) && !it->second.baseline) {
	  it->second.st = st;
	  it->second.baseline = true;
	}
      }
    }
  }

  /* Remove every entry under `prefix`, and the records of the
   * directories there. */
  template <typename TxnT>
  void remove_subtree(TxnT& txn, BucketCacheEntry<D, B, YP>* b,
		      const std::string& prefix) {
    using namespace LMDBSafe;
    auto cursor = txn->getCursor(b->dbi);
    MDBOutVal key, data;
    MDBInVal k(prefix);
    int rc = cursor.lower_bound(k, key, data);
    while (rc == 0) {
      if (!key.get<std::string_view>().starts_with(prefix)) {
	break;
      }
      cursor.del();
      rc = cursor.get(key, data, MDB_NEXT);
    }
    for (auto it = b->dirs.lower_bound(prefix);
	 (it != b->dirs.end()) && it->first.starts_with(prefix);) {
      it = b->dirs.erase(it);
    }
  }

  static constexpr int RECONCILE_REFILL = 1;

  /* Bring one directory's entries up to date with the directory:  read
   * it, build entries for the children the cache lacks, remove those of
   * children gone, and record its stat.  Its subdirectories' contents are
   * theirs to check, except that a new one is built whole.  Returns 0,
   * RECONCILE_REFILL where only a refill can bring it up to date, or
   * -errno.  LOCKED. */
  int reconcile_dir(const DoutPrefixProvider* dpp, optional_yield y,
		    BucketCacheEntry<D, B, YP>* b, B* sal_bucket,
		    const std::string& prefix) {
    using namespace LMDBSafe;
    ++reconcile_count;

    DirStat st;
    int ret = rv->stat_dir(dpp, y, sal_bucket, prefix, st);
    std::vector<std::string> disk;
    if (ret == 0) {
      /* A .versions/ changed by someone else:  its entries are versions of
       * several names, which only the fill's walk assembles. */
      auto rit = b->dirs.find(prefix);
      if ((rit != b->dirs.end()) && rit->second.baseline &&
	  ((st.vino != rit->second.st.vino) ||
	   (st.vmtime_ns != rit->second.st.vmtime_ns))) {
	return RECONCILE_REFILL;
      }
      ret = rv->read_dir(dpp, y, sal_bucket, prefix, disk);
    }
    if ((ret == -ENOENT) && prefix.empty()) {
      return RECONCILE_REFILL;
    }

    auto txn = b->env->getRWTransaction();
    if (ret == -ENOENT) {
      remove_subtree(txn, b, prefix);
      txn->commit();
      return 0;
    }
    if (ret < 0) {
      return ret;
    }
    const std::set<std::string> on_disk(disk.begin(), disk.end());

    /* what the cache holds directly under `prefix`:  names with an entry
     * a file backs, and the prefixes of subdirectories, each
     * subdirectory's range skipped with one seek */
    std::set<std::string> cached;
    {
      auto cursor = txn->getCursor(b->dbi);
      MDBOutVal key, data;
      std::string seek{prefix};
      int rc = seek.empty() ? cursor.get(key, data, MDB_FIRST)
	: cursor.lower_bound(MDBInVal(seek), key, data);
      while (rc == 0) {
	const std::string_view kv = key.get<std::string_view>();
	const std::string_view name = kv.substr(0, kv.find('\0'));
	if (!name.starts_with(prefix)) {
	  break;
	}
	const std::string_view rest = name.substr(prefix.size());
	const auto slash = rest.find('/');
	if (rest.empty() || ((slash == std::string_view::npos) &&
			     namespaced(name))) {
	  /* the directory's own object;  a namespaced entry */
	  rc = cursor.get(key, data, MDB_NEXT);
	} else if (slash == std::string_view::npos) {
	  rgw_bucket_dir_entry e{};
	  if ((deser_entry(data.get<std::string_view>(), e).code ==
	       std::errc{0}) && file_backed(e)) {
	    cached.emplace(name);
	  }
	  rc = cursor.get(key, data, MDB_NEXT);
	} else {
	  cached.emplace(name.substr(0, prefix.size() + slash + 1));
	  seek.assign(name.substr(0, prefix.size() + slash));
	  seek += char('/' + 1);
	  rc = cursor.lower_bound(MDBInVal(seek), key, data);
	}
      }
    }
    /* a subdirectory with a record and no entries is known, not new */
    for (auto it = b->dirs.upper_bound(prefix);
	 (it != b->dirs.end()) && it->first.starts_with(prefix); ++it) {
      const std::string_view rest =
	std::string_view{it->first}.substr(prefix.size());
      if (rest.find('/') == rest.size() - 1) {
	cached.insert(it->first);
      }
    }

    std::vector<std::string> added;
    for (const auto& c : on_disk) {
      if (!cached.contains(c)) {
	added.push_back(c);
      }
    }
    for (const auto& c : cached) {
      if (on_disk.contains(c)) {
	continue;
      }
      if (c.back() == '/') {
	remove_subtree(txn, b, c);
      } else {
	/* the file's entries, not its versions or delete markers */
	(void) take_entries(dpp, txn, b->dbi, c,
			    [](const rgw_bucket_dir_entry& e) {
			      return file_backed(e);
			    }, 0, nullptr);
      }
    }
    if (!added.empty()) {
      Recorder rec{b};
      fill_cache_cb_t put =
	[&](const DoutPrefixProvider* dpp, rgw_bucket_dir_entry& bde) -> int {
	  std::string ser_data;
	  if (ser_entry(bde, rec.take(bde.key.name), ser_data).code !=
	      std::errc{0}) {
	    return -EIO;
	  }
	  txn->put(b->dbi, concat_key(bde.key), ser_data);
	  return 0;
	};
      ret = rv->build(dpp, y, sal_bucket, prefix, added, put, rec);
      if (ret < 0) {
	ldpp_dout(dpp, 1) << "BucketCache: building entries in " << b->name
	  << "/" << prefix << ": " << cpp_strerror(-ret) << dendl;
      }
    }
    auto& r = b->dirs[prefix];
    r.st = st;
    r.baseline = true;
    r.provisional_ns.reset();
    txn->commit();
    ldpp_dout(dpp, 10) << "BucketCache: read " << b->name << "/" << prefix
      << ":  " << added.size() << " added, "
      << (cached.size() - (on_disk.size() - added.size())) << " gone" << dendl;
    return 0;
  }

  /* Rebuild the current entry of `name` from its file, which changed or
   * is gone.  LOCKED. */
  int rebuild_entry(const DoutPrefixProvider* dpp, optional_yield y,
		    BucketCacheEntry<D, B, YP>* b, B* sal_bucket,
		    const std::string& name) {
    using namespace LMDBSafe;
    ++rebuild_count;
    auto txn = b->env->getRWTransaction();
    (void) take_entries(dpp, txn, b->dbi, name,
			[](const rgw_bucket_dir_entry& e) {
			  return !(e.flags & rgw_bucket_dir_entry::FLAG_VER) ||
			    (e.flags & rgw_bucket_dir_entry::FLAG_CURRENT);
			}, 0, nullptr);
    Recorder rec{b};
    fill_cache_cb_t put =
      [&](const DoutPrefixProvider* dpp, rgw_bucket_dir_entry& bde) -> int {
	std::string ser_data;
	if (ser_entry(bde, rec.take(bde.key.name), ser_data).code !=
	    std::errc{0}) {
	  return -EIO;
	}
	txn->put(b->dbi, concat_key(bde.key), ser_data);
	return 0;
      };
    const int ret = rv->build(dpp, y, sal_bucket, parent_prefix(name),
			      {name}, put, rec);
    txn->commit();
    ldpp_dout(dpp, 10) << "BucketCache: rebuilt " << b->name << "/" << name
      << (ret < 0 ? ":  gone" : "") << dendl;
    return 0;
  }

  struct Touched {
    std::string key;
    std::string before;
    std::string after;
  };

  /* The checked times a page recorded, each written only where the value
   * is still the one the walk read:  an RGW write since wins. */
  void write_touched(const DoutPrefixProvider* dpp,
		     BucketCacheEntry<D, B, YP>* b,
		     std::vector<Touched>& touched) {
    using namespace LMDBSafe;
    if (touched.empty()) {
      return;
    }
    try {
      auto txn = b->env->getRWTransaction();
      for (const auto& t : touched) {
	MDBOutVal cur;
	if ((txn->get(b->dbi, MDBInVal(t.key), cur) == 0) &&
	    (cur.get<std::string_view>() == t.before)) {
	  txn->put(b->dbi, t.key, t.after);
	}
      }
      txn->commit();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 1) << "BucketCache: recording checks in " << b->name
	<< ": " << e.what() << dendl;
    }
    touched.clear();
  }

  /* The listing walk, checking the tree as it goes:  a directory as the
   * walk enters it, a current entry at most once per entry interval.  A
   * change is applied between read transactions:  the walk stops before
   * the key that found it, brings the cache up to date, and resumes after
   * the last key it returned.  Each directory and entry is checked once a
   * page, so a page ends however busy the tree. */
  int walk_revalidating(const DoutPrefixProvider* dpp, optional_yield y,
			BucketCacheEntry<D, B, YP>* b, B* sal_bucket,
			const std::string& marker,
			list_bucket_each_t& each_func) {
    using namespace LMDBSafe;
    const auto ival = rv->entry_interval();
    const int64_t ival_ns = ival
      ? std::chrono::duration_cast<std::chrono::nanoseconds>(*ival).count()
      : 0;
    const int64_t prov_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
	rv->provisional_interval()).count();

    std::set<std::string> checked;		/* directories, this page */
    std::set<std::string> entries_checked;	/* names, this page */
    std::vector<Touched> touched;
    std::string pos{marker};	/* resume here:  inclusive until a key is returned */
    bool inclusive = true;
    std::string after{marker.substr(0, marker.find('\0'))};

    for (;;) {
      std::vector<std::string> changed;
      std::optional<std::string> rebuild;
      bool done = false;
      const int64_t now = revalidation_now_ns();
      try {
	auto txn = b->env->getROTransaction();
	auto cursor = txn->getCursor(b->dbi);
	MDBOutVal key, data;
	int rc;
	if (pos.empty()) {
	  rc = cursor.get(key, data, MDB_FIRST);
	} else {
	  rc = cursor.lower_bound(MDBInVal(pos), key, data);
	  if (!inclusive && (rc == 0) &&
	      (key.get<std::string_view>() == pos)) {
	    rc = cursor.get(key, data, MDB_NEXT);
	  }
	}
	for (;;) {
	  if (rc != MDB_SUCCESS) {
	    check_dirs(dpp, y, b, sal_bucket, after, std::nullopt, checked,
		       prov_ns, changed);
	    done = changed.empty();
	    break;
	  }
	  const std::string_view kv = key.get<std::string_view>();
	  const std::string_view name = kv.substr(0, kv.find('\0'));
	  check_dirs(dpp, y, b, sal_bucket, after, name, checked, prov_ns,
		     changed);
	  if (!changed.empty()) {
	    break;
	  }
	  const std::string_view sv = data.get<std::string_view>();
	  rgw_bucket_dir_entry bde{};
	  EntryStat st;
	  if (deser_entry(sv, bde, &st).code != std::errc{0}) {
	    ldpp_dout(dpp, 0) << "BucketCache: deserialization error listing "
	      << sal_bucket->get_name() << dendl;
	    rc = cursor.get(key, data, MDB_NEXT);
	    continue;
	  }
	  if (ival && compares(bde, name) &&
	      ((now - st.checked_ns) >= ival_ns) &&
	      entries_checked.emplace(name).second) {
	    ++entry_check_count;
	    EntryStat cur;
	    const int r = rv->stat_entry(dpp, y, sal_bucket, std::string{name},
					 cur);
	    if ((r == -ENOENT) || ((r == 0) && !st.same_file(cur))) {
	      rebuild.emplace(name);
	      break;
	    }
	    if (r == 0) {
	      EntryStat nst = st;
	      nst.checked_ns = now;
	      std::string v;
	      if (ser_entry(bde, nst, v).code == std::errc{0}) {
		touched.push_back({std::string{kv}, std::string{sv},
				   std::move(v)});
	      }
	    }
	  }
	  pos.assign(kv);
	  inclusive = false;
	  after.assign(name);
	  if (!each_func(bde)) {
	    done = true;
	    break;
	  }
	  rc = cursor.get(key, data, MDB_NEXT);
	}
      } catch (const std::exception& e) {
	ldpp_dout(dpp, 0) << "BucketCache: LMDB error listing "
	  << sal_bucket->get_name() << ": " << e.what() << dendl;
	return -EIO;
      }
      write_touched(dpp, b, touched);
      if (done) {
	return 0;
      }

      unique_lock ulk{b->mtx};
      try {
	bool refill = !(b->flags & BucketCacheEntry<D, B, YP>::FLAG_FILLED);
	for (const auto& prefix : changed) {
	  if (refill) {
	    break;
	  }
	  const int r = reconcile_dir(dpp, y, b, sal_bucket, prefix);
	  if (r == RECONCILE_REFILL) {
	    refill = true;
	  } else if (r < 0) {
	    ldpp_dout(dpp, 1) << "BucketCache: reading " << b->name << "/"
	      << prefix << ": " << cpp_strerror(-r) << dendl;
	  }
	}
	if (rebuild && !refill) {
	  (void) rebuild_entry(dpp, y, b, sal_bucket, *rebuild);
	}
	if (refill) {
	  ldpp_dout(dpp, 2) << "BucketCache: refilling bucket=" << b->name
	    << " to bring it up to date" << dendl;
	  (void) fill(dpp, b, sal_bucket, FLAG_NONE, y);
	}
      } catch (const std::exception& e) {
	ldpp_dout(dpp, 0) << "BucketCache: bringing " << b->name
	  << " up to date: " << e.what() << dendl;
	return -EIO;
      }
    }
  } /* walk_revalidating */

  int notify(const std::string& bname, void* opaque,
	     const std::vector<Notifiable::Event>& evec) override {

    using namespace LMDBSafe;

    int rc{0};
    GetBucketResult gbr = get_bucket(nullptr, bname, BucketCache<D, B, YP>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      if ((b->name != bname) ||
	  (opaque && (b != opaque)) ||
	  (! (b->flags & BucketCacheEntry<D, B, YP>::FLAG_FILLED))) {
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
              /* no recorded stat:  under revalidation the entry is
               * rebuilt once, at its first check */
              std::string ser_data;
              if (ser_entry(bde, ser_data).code != std::errc{0}) {
                break;
              }
	      lsubdout(driver->ctx(), rgw, 10)
		<< "BucketCache: notify lmdb put bucket=" << b->name
		<< " key=" << bde.key.name << dendl;
              txn->put(b->dbi, concat_k, ser_data);
            }
              break;
            case EventType::REMOVE:
            {
              auto& ev_name = *ev.name;
	      lsubdout(driver->ctx(), rgw, 10)
		<< "BucketCache: notify lmdb del bucket=" << b->name
		<< " key=" << ev_name << dendl;
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
              b->flags &= ~BucketCacheEntry<D, B, YP>::FLAG_FILLED;
              b->dirs.clear();
              b->fs_root.clear();
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

  /* RGW changed `name`'s directory:  record its stat again now, so that
   * only a change landing between RGW's and this stat is absorbed, and
   * mark it provisional from now if it is not already, so that it is read
   * within the provisional interval for such a change.  A directory with
   * no record is new -- RGW made it -- so its parent changed too.  Where
   * the stat fails, the next check takes its stat as the record.  Nothing
   * to do for a bucket not filled with records.  LOCKED. */
  void note_own_change(BucketCacheEntry<D, B, YP>* b, std::string_view name) {
    if (b->dirs.empty() || !rv) {
      return;
    }
    const int64_t now = revalidation_now_ns();
    std::string prefix = parent_prefix(name);
    for (;;) {
      auto [it, inserted] = b->dirs.try_emplace(prefix);
      auto& r = it->second;
      DirStat st;
      if (!b->fs_root.empty() &&
	  (rv->stat_dir_at(nullptr, b->fs_root, b->fs_versions, prefix, st) == 0)) {
	r.st = st;
	r.baseline = true;
      } else {
	r.baseline = false;
      }
      if (!r.provisional_ns) {
	r.provisional_ns = now;
      }
      if (!inserted || prefix.empty()) {
	break;
      }
      prefix = parent_prefix(std::string_view{prefix}.substr(0, prefix.size() - 1));
    }
  }

  /* `st`, where the caller has it, is the object's recorded stat:  the
   * file as RGW left it, so that a listing's check finds it unchanged. */
  int add_entry(const DoutPrefixProvider* dpp, std::string bname,
		rgw_bucket_dir_entry bde, const EntryStat* st = nullptr) {
    using namespace LMDBSafe;

    ldpp_dout(dpp, 10) << "BucketCache: add_entry bucket=" << bname
      << " key=" << bde.key.name << dendl;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B, YP>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      note_own_change(b, bde.key.name);
      ulk.unlock();

      try {
        auto txn = b->env->getRWTransaction();
        auto concat_k = concat_key(bde.key);
        std::string ser_data;
	EntryStat rst;
	if (st) {
	  rst = *st;
	  rst.checked_ns = revalidation_now_ns();
	}
        if (ser_entry(bde, rst, ser_data).code != std::errc{0}) {
          return -EIO;
        }
	ldpp_dout(dpp, 10) << "BucketCache: lmdb put bucket=" << bname
	  << " key=" << bde.key.name << dendl;
        txn->put(b->dbi, concat_k, ser_data);

        txn->commit();
      } catch (const std::exception& e) {
	/* Reported, not swallowed.  A rejected entry means the object is
	 * on disk and absent from every listing, so a caller that
	 * ignores this leaves the cache disagreeing with the tree and
	 * tells the client the write succeeded. */
	ldpp_dout(dpp, 0) << "BucketCache: add_entry failed for "
	  << bname << " key=" << bde.key.name << ": " << e.what()
	  << dendl;
	return -EIO;
      }
    } /* b */

    return 0;
  } /* add_entry */

  /* The longest key LMDB will accept, hence the longest object name
   * this cache can hold.  Zero until the first partition env exists. */
  int max_key_size() const { return lmdbs.max_key_size(); }

  /* (de)serialisation of a cache entry, in one place:  the entry, then
   * its recorded stat.  Every writer goes through ser_entry(), so every
   * value carries a recorded stat, unknown where the writer had none. */
  static zpp::bits::errc ser_entry(const rgw_bucket_dir_entry& bde,
				   const EntryStat& st,
				   std::string& out_data) {
    zpp::bits::out out(out_data);
    struct timespec ts{ceph::real_clock::to_timespec(bde.meta.mtime)};
    return out(bde.key.name, bde.key.instance,
	       bde.ver.pool, bde.ver.epoch, bde.exists, bde.meta.category,
	       bde.meta.size, ts.tv_sec, ts.tv_nsec, bde.meta.owner,
	       bde.meta.owner_display_name, bde.meta.accounted_size,
	       bde.meta.storage_class, bde.meta.appendable, bde.meta.etag,
	       bde.flags,
	       st.ino, st.size, st.mtime_ns, st.ctime_ns, st.checked_ns);
  }

  static zpp::bits::errc ser_entry(const rgw_bucket_dir_entry& bde,
				   std::string& out_data) {
    return ser_entry(bde, EntryStat{}, out_data);
  }

  static zpp::bits::errc deser_entry(std::string_view sv,
				     rgw_bucket_dir_entry& bde,
				     EntryStat* st = nullptr) {
    std::string ser_v{sv};
    zpp::bits::in in_v(ser_v);
    struct timespec ts{};
    auto errc = in_v(bde.key.name, bde.key.instance,
		     bde.ver.pool, bde.ver.epoch, bde.exists, bde.meta.category,
		     bde.meta.size, ts.tv_sec, ts.tv_nsec, bde.meta.owner,
		     bde.meta.owner_display_name, bde.meta.accounted_size,
		     bde.meta.storage_class, bde.meta.appendable, bde.meta.etag,
		     bde.flags);
    if (errc.code == std::errc{0}) {
      bde.meta.mtime = ceph::real_clock::from_timespec(ts);
      if (st) {
	errc = in_v(st->ino, st->size, st->mtime_ns, st->ctime_ns,
		    st->checked_ns);
      }
    }
    return errc;
  }

  /* Walk the entries for one object name, deleting those pred selects.
   *
   * Keys sort as name '\0' instance (concat_key), so a name and all of its
   * versions are contiguous:  a bounded walk, not a bucket scan.
   *
   * `limit` caps how many are removed in this pass, 0 meaning no cap.  A
   * caller which must hold the removed entries in memory needs the cap:  the
   * version count of a single key is client-controlled and unbounded, so
   * buffering all of them is not safe.  `taken` may be nullptr when the
   * caller only wants them gone.
   *
   * Deletion happens at the cursor, which is safe mid-walk.  Insertion is
   * deliberately *not* done here -- putting a key mid-walk risks feeding the
   * walk its own output, or invalidating the cursor.  Callers that insert do
   * so after this returns.
   *
   * Returns the number removed.
   */
  template <typename TxnT>
  int take_entries(const DoutPrefixProvider* dpp, TxnT& txn,
		   LMDBSafe::MDBDbi& dbi, const std::string& name,
		   remove_entries_pred_t& pred, size_t limit,
		   std::vector<rgw_bucket_dir_entry>* taken) {
    using namespace LMDBSafe;

    auto cursor = txn->getCursor(dbi);
    MDBOutVal key, data;
    int removed = 0;

    std::string start{name};
    start += '\0';
    MDBInVal k(start);

    int rc = cursor.lower_bound(k, key, data);
    while (rc == 0) {
      if (limit && (size_t) removed >= limit) {
	break;
      }
      std::string_view kv = key.get<std::string_view>();
      /* out of this name's range:  what bounds the walk */
      if (kv.size() < name.size() + 1 ||
	  kv.compare(0, name.size(), name) != 0 ||
	  kv[name.size()] != '\0') {
	break;
      }

      rgw_bucket_dir_entry bde{};
      auto errc = deser_entry(data.get<std::string_view>(), bde);
      if (errc.code != std::errc{0}) {
	/* unreadable:  drop it rather than leave behind something no caller
	 * can reason about */
	ldpp_dout(dpp, 0) << "BucketCache: take_entries deserialization error"
	  << " name=" << name << " errc=" << static_cast<int>(errc.code)
	  << dendl;
	cursor.del();
	++removed;
      } else if (pred(bde)) {
	cursor.del();
	++removed;
	if (taken) {
	  taken->push_back(std::move(bde));
	}
      }
      rc = cursor.get(key, data, MDB_NEXT);
    }
    return removed;
  } /* take_entries */

  /* Remove the entries for one object name which pred selects -- its current
   * entry, its versions, or any subset -- in a single transaction and one
   * bounded walk, rather than a point delete and a transaction per version.
   *
   * Returns the number removed, or a negative errno. */
  int remove_entries(const DoutPrefixProvider* dpp, std::string bname,
		     const std::string& name, remove_entries_pred_t&& pred) {
    using namespace LMDBSafe;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B, YP>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (! b) {
      return 0;
    }
    auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
    unique_lock ulk{b->mtx, std::adopt_lock};
    note_own_change(b, name);
    ulk.unlock();

    int removed = 0;
    try {
      auto txn = b->env->getRWTransaction();
      /* nullptr:  removal needs no copy of what it removed, so there is
       * nothing here to grow with the version count */
      removed = take_entries(dpp, txn, b->dbi, name, pred,
			     0 /* no limit */, nullptr);
      txn->commit();
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 0) << "BucketCache: remove_entries failed for "
	<< bname << "/" << name << ": " << e.what() << dendl;
      return -EIO;
    }

    ldpp_dout(dpp, 10) << "BucketCache: remove_entries bucket=" << bname
      << " name=" << name << " removed=" << removed << dendl;
    return removed;
  } /* remove_entries */

  /* Move the entries for one object name to another name, and optionally
   * another bucket.
   *
   * A rename changes an object's key and nothing else, so entries are
   * *rekeyed*:  taken as they are and reinserted under the new name.  Nothing
   * is re-read from the store and nothing re-described, so etag, size, mtime,
   * owner, flags and version ids survive because they are never recomputed.
   *
   * Done in chunks.  A key's version count is client-controlled and
   * unbounded, so the entries cannot all be held in memory;  and they cannot
   * be reinserted during the walk that removes them.  Each pass therefore
   * takes at most CHUNK, inserts them, and resumes -- correct without a
   * carried cursor, because a taken entry is deleted and so the next
   * lower_bound() lands on the first one left.
   *
   * A multi-chunk move is consequently not atomic in the cache.  That is
   * acceptable here and asymmetric in the safe direction:  each chunk deletes
   * before it inserts, so an interruption can leave an entry *missing*, which
   * the next fill restores, but never duplicated, which it would not.
   *
   * Returns the number moved, or a negative errno. */
  int rename_entries(const DoutPrefixProvider* dpp,
		     std::string src_bname, const std::string& src_name,
		     std::string dst_bname, const std::string& dst_name,
		     remove_entries_pred_t&& pred, rekey_entry_cb_t&& xform) {
    using namespace LMDBSafe;

    constexpr size_t CHUNK = 1024;
    const bool same_bucket = (src_bname == dst_bname);

    if (same_bucket && src_name == dst_name) {
      return 0; /* nothing to do, and walking into our own output */
    }

    GetBucketResult sgbr = get_bucket(dpp, src_bname, BucketCache<D, B, YP>::FLAG_LOCK);
    auto [sb /* BucketCacheEntry */, sflags] = sgbr;
    if (! sb) {
      return 0;
    }
    auto sunref = make_scope_guard([this, sb]{ lru.unref(sb, cohort::lru::FLAG_NONE); });
    {
      unique_lock ulk{sb->mtx, std::adopt_lock};
      note_own_change(sb, src_name);
      if (same_bucket) {
	note_own_change(sb, dst_name);
      }
      ulk.unlock();
    }

    /* the name changes, then the caller's transform has its say */
    const auto put_rekeyed =
      [&dst_name, &xform](auto& txn, LMDBSafe::MDBDbi& dbi,
			  std::vector<rgw_bucket_dir_entry>& chunk) {
	for (auto& bde : chunk) {
	  bde.key.name = dst_name;
	  xform(bde);
	  std::string ser_data;
	  if (ser_entry(bde, ser_data).code != std::errc{0}) {
	    continue;
	  }
	  txn->put(dbi, concat_key(bde.key), ser_data);
	}
      };

    int moved = 0;
    for (;;) {
      std::vector<rgw_bucket_dir_entry> chunk;
      try {
	auto txn = sb->env->getRWTransaction();
	(void) take_entries(dpp, txn, sb->dbi, src_name, pred, CHUNK, &chunk);
	if (same_bucket) {
	  put_rekeyed(txn, sb->dbi, chunk);
	}
	txn->commit();
      } catch (const std::exception& e) {
	ldpp_dout(dpp, 0) << "BucketCache: rename_entries failed for "
	  << src_bname << "/" << src_name << ": " << e.what() << dendl;
	return -EIO;
      }

      if (! same_bucket && ! chunk.empty()) {
	GetBucketResult dgbr = get_bucket(dpp, dst_bname,
					  BucketCache<D, B, YP>::FLAG_LOCK);
	auto [db /* BucketCacheEntry */, dflags] = dgbr;
	if (db) {
	  auto dunref = make_scope_guard([this, db]{ lru.unref(db, cohort::lru::FLAG_NONE); });
	  {
	    unique_lock ulk{db->mtx, std::adopt_lock};
	    note_own_change(db, dst_name);
	    ulk.unlock();
	  }
	  try {
	    auto txn = db->env->getRWTransaction();
	    put_rekeyed(txn, db->dbi, chunk);
	    txn->commit();
	  } catch (const std::exception& e) {
	    ldpp_dout(dpp, 0) << "BucketCache: rename_entries destination "
	      << dst_bname << "/" << dst_name << " failed: " << e.what()
	      << ";  those entries are gone from the source and the next fill "
	      << "restores them" << dendl;
	    return -EIO;
	  }
	}
      }

      moved += (int) chunk.size();
      if (chunk.size() < CHUNK) {
	break;
      }
    }

    ldpp_dout(dpp, 10) << "BucketCache: rename_entries "
      << src_bname << "/" << src_name << " -> " << dst_bname << "/" << dst_name
      << " moved=" << moved << dendl;
    return moved;
  } /* rename_entries */

  int remove_entry(const DoutPrefixProvider* dpp, std::string bname, cls_rgw_obj_key key) {
    using namespace LMDBSafe;

    /* normalize null-version instance: the cache may store entries with
     * instance="" or instance="null" depending on the code path (fill_cache
     * uses "null", delete_object uses "").  Try both so removal always
     * matches regardless of which convention the caller uses. */
    if (key.instance == "null") {
      key.instance.clear();
    }
    bool try_null_instance = key.instance.empty();

    ldpp_dout(dpp, 10) << "BucketCache: remove_entry bucket=" << bname
      << " key=" << key.name << " instance=" << key.instance << dendl;

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B, YP>::FLAG_LOCK);
    auto [b /* BucketCacheEntry */, flags] = gbr;
    if (b) {
      auto unref_guard = make_scope_guard([this, b]{ lru.unref(b, cohort::lru::FLAG_NONE); });
      unique_lock ulk{b->mtx, std::adopt_lock};
      note_own_change(b, key.name);
      ulk.unlock();

      try {
        auto txn = b->env->getRWTransaction();
        auto concat_k = concat_key(key);
	ldpp_dout(dpp, 10) << "BucketCache: lmdb del bucket=" << bname
	  << " key=" << key.name << dendl;
        txn->del(b->dbi, concat_k);
        if (try_null_instance) {
          cls_rgw_obj_key null_key = key;
          null_key.instance = "null";
          auto null_concat_k = concat_key(null_key);
          txn->del(b->dbi, null_concat_k);
        }
        txn->commit();
      } catch (const std::exception& e) {
	ldpp_dout(dpp, 0) << "BucketCache: remove_entry del failed for "
	  << bname << " key=" << key.name << ": " << e.what() << dendl;
      }
    } else {
      ldpp_dout(dpp, 2) << "BucketCache: remove_entry bucket not cached: "
	<< bname << dendl;
    }

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

    GetBucketResult gbr = get_bucket(dpp, bname, BucketCache<D, B, YP>::FLAG_LOCK);
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
      b->flags &= ~BucketCacheEntry<D, B, YP>::FLAG_FILLED;
      b->dirs.clear();
      b->fs_root.clear();

      ulk.unlock();
    } /* b */

    return 0;
  } /* invalidate_bucket */
}; /* BucketCache */

} // namespace file::listing
