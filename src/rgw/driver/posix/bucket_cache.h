// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <iostream>
#include <memory>
#include <tuple>
#include <vector>
#include <string>
#include <string_view>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <boost/intrusive/avl_set.hpp>
#include "include/function2.hpp"
#include "common/cohort_lru.h"
#include "lmdb-safe/lmdb-safe.hh"
#include "zpp_bits.h"
#include "notify.h"
#include <stdint.h>
#include <xxhash.h>

#include "fmt/format.h"

namespace file::listing {

namespace bi = boost::intrusive;
namespace sf = std::filesystem; 

typedef bi::link_mode<bi::normal_link> link_mode; /* XXX normal */
typedef bi::avl_set_member_hook<link_mode> member_hook_t;

struct BucketCache;

struct Bucket : public cohort::lru::Object
{
  using lock_guard = std::lock_guard<std::mutex>;
  using unique_lock = std::unique_lock<std::mutex>;

  static constexpr uint32_t FLAG_NONE     = 0x0000;
  static constexpr uint32_t FLAG_FILLED   = 0x0001;
  static constexpr uint32_t FLAG_DELETED  = 0x0002;
  
  static constexpr uint64_t seed = 8675309;

  BucketCache* bc;
  std::string name;
  std::shared_ptr<MDBEnv> env;
  MDBDbi dbi;
  uint64_t hk;
  member_hook_t name_hook;

  // XXX clean this up
  std::mutex mtx; // XXX Adam's preferred shared mtx?
  std::condition_variable cv;
  uint32_t flags;

public:
  Bucket(BucketCache* bc, const std::string& name, uint64_t hk)
    : bc(bc), name(name), hk(hk), flags(FLAG_NONE) {}

  void set_env(std::shared_ptr<MDBEnv>& _env, MDBDbi& _dbi) {
    env = _env;
    dbi = _dbi;
  }

  inline bool deleted() const {
    return flags & FLAG_DELETED;
  }

  class Factory : public cohort::lru::ObjectFactory
  {
  public:
    BucketCache* bc;
    const std::string& name;
    uint64_t hk;
    uint32_t flags;

    Factory() = delete;
    Factory(BucketCache *bc, const std::string& name)
      : bc(bc), name(name), flags(FLAG_NONE) {
      hk = XXH64(name.c_str(), name.length(), Bucket::seed);
    }

    void recycle (cohort::lru::Object* o) override {
      /* re-use an existing object */
      o->~Object(); // call lru::Object virtual dtor
      // placement new!
      new (o) Bucket(bc, name, hk);
    }

    cohort::lru::Object* alloc() override {
      return new Bucket(bc, name, hk);
    }
  }; /* Factory */

  struct BucketLT
  {
    // for internal ordering
    bool operator()(const Bucket& lhs, const Bucket& rhs) const
      { return (lhs.name < rhs.name); }

    // for external search by name
    bool operator()(const std::string& k, const Bucket& rhs) const
      { return k < rhs.name; }

    bool operator()(const Bucket& lhs, const std::string& k) const
      { return lhs.name < k; }
  };

  struct BucketEQ
  {
    bool operator()(const Bucket& lhs, const Bucket& rhs) const
      { return (lhs.name == rhs.name); }

    bool operator()(const std::string& k, const Bucket& rhs) const
      { return k == rhs.name; }

    bool operator()(const Bucket& lhs, const std::string& k) const
      { return lhs.name == k; }
  };

  typedef cohort::lru::LRU<std::mutex> bucket_lru;

  typedef bi::member_hook<Bucket, member_hook_t, &Bucket::name_hook> name_hook_t;
  typedef bi::avltree<Bucket, bi::compare<BucketLT>, name_hook_t> bucket_avl_t;
  typedef cohort::lru::TreeX<Bucket, bucket_avl_t, BucketLT, BucketEQ, std::string,
			     std::mutex> bucket_avl_cache;

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac);

}; /* Bucket */

struct BucketCache : public Notifiable
{
  using lock_guard = std::lock_guard<std::mutex>;
  using unique_lock = std::unique_lock<std::mutex>;

  std::string bucket_root;
  uint32_t max_buckets;
  std::atomic<uint64_t> recycle_count;
  std::mutex mtx;

  /* the bucket lru cache keeps track of the buckets whose listings are
   * being cached in lmdb databases and updated from notify */
  Bucket::bucket_lru lru;
  Bucket::bucket_avl_cache cache;
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
    std::vector<std::shared_ptr<MDBEnv>> envs;
    sf::path dbp;

  public:
    Lmdbs(std::string& database_root, uint8_t lmdb_count)
      : database_root(database_root), lmdb_count(lmdb_count),
        dbp(database_root) {
      /* purge cache completely */
      for (const auto& dir_entry : sf::directory_iterator{dbp}) {
	sf::remove_all(dir_entry);
      }

      /* repopulate cache basis */
      for (int ix = 0; ix < lmdb_count; ++ix) {
	sf::path env_path{dbp / fmt::format("part_{}", ix)};
	sf::create_directory(env_path);
	auto env = getMDBEnv(env_path.string().c_str(), 0 /* flags? */, 0600);
	envs.push_back(env);
      }
    }

    inline std::shared_ptr<MDBEnv>& get_sp_env(Bucket* bucket)  {
      return envs[(bucket->hk % lmdb_count)];
    }

    inline MDBEnv& get_env(Bucket* bucket) {
      return *(get_sp_env(bucket));
    }

    const std::string& get_root() const { return database_root; }
  } lmdbs;

  std::unique_ptr<Notify> un;

public:
  BucketCache(std::string& bucket_root, std::string& database_root,
	      uint32_t max_buckets=100, uint8_t max_lanes=3,
	      uint8_t max_partitions=3, uint8_t lmdb_count=3)
    : bucket_root(bucket_root), max_buckets(max_buckets),
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

  typedef std::tuple<Bucket*, uint32_t> GetBucketResult;

  GetBucketResult get_bucket(const std::string& name, uint32_t flags)
    {
      /* this fn returns a bucket locked appropriately, having atomically
       * found or inserted the required Bucket in_avl*/
      Bucket* b{nullptr};
      Bucket::Factory fac(this, name);
      Bucket::bucket_avl_cache::Latch lat;
      uint32_t iflags{cohort::lru::FLAG_INITIAL};
      GetBucketResult result{nullptr, 0};

    retry:
      b = cache.find_latch(fac.hk /* partition selector */,
			   name /* key */, lat /* serializer */, Bucket::bucket_avl_cache::FLAG_LOCK);
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
	/* Bucket not in cache, we need to create it */
	b = static_cast<Bucket*>(
	  lru.insert(&fac, cohort::lru::Edge::MRU, iflags));
	if (b) [[likely]] {
	  b->mtx.lock();

	  /* attach bucket to an lmdb partition and prepare it for i/o */
	  auto& env = lmdbs.get_sp_env(b);
	  auto dbi = env->openDB(b->name, MDB_CREATE);
	  b->set_env(env, dbi);

	  if (! (iflags & cohort::lru::FLAG_RECYCLE)) [[likely]] {
	    /* inserts at cached insert iterator, releasing latch */
	    cache.insert_latched(b, lat, Bucket::bucket_avl_cache::FLAG_UNLOCK);
	  } else {
	    /* recycle step invalidates Latch */
	    lat.lock->unlock(); /* !LATCHED */
	    cache.insert(fac.hk, b, Bucket::bucket_avl_cache::FLAG_NONE);
	  }
	  get<1>(result) |= BucketCache::FLAG_CREATE;
	} else {
	  /* XXX lru allocate failed? seems impossible--that would mean that
	   * fallback to the allocator also failed, and maybe we should abend */
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
      } /* have Bucket */

      if (! (flags & BucketCache::FLAG_LOCK)) {
	b->mtx.unlock();
      }
      get<0>(result) = b;
      return result;
    } /* get_bucket */

  struct FakeDirEntry
  {
    std::string fname;
    std::string s1;
    int i1;
    int i2;
    FakeDirEntry()
      {}
    FakeDirEntry(std::string fname, std::string s1, int i1, int i2)
      : fname(fname), s1(s1), i1(i1), i2(i2)
      {}
  };

  void fill(Bucket* bucket, uint32_t flags) /* assert: LOCKED */
    {
      sf::path bp{rp / bucket->name};
      if (! (sf::exists(rp) && sf::is_directory(rp))) {
	std::cerr << fmt::format("{} bucket {} invalid", __func__, bucket->name)
		  << std::endl;
	exit(1);
      }
      auto txn = bucket->env->getRWTransaction();
      for (const auto& dir_entry : sf::directory_iterator{bp}) {
	auto fname = dir_entry.path().filename().string();
	// TODO: unfakening
	FakeDirEntry fde{fname, "famous", 1776, 2112};
	std::string ser_data;
	zpp::bits::out out(ser_data);
	out(fde.fname, fde.s1, fde.i1, fde.i2).or_throw();
        txn->put(bucket->dbi, fname, ser_data);
	//std::cout << fmt::format("{} {}", __func__, fname) << '\n';
      }
      txn->commit();
      bucket->flags |= Bucket::FLAG_FILLED;
      un->add_watch(bucket->name, bucket);
    } /* fill */

  void list_bucket(std::string& name, std::string& marker,
		   const fu2::unique_function<int(const std::string_view&, const FakeDirEntry&) const>& func)
    {
      GetBucketResult gbr = get_bucket(name, BucketCache::FLAG_LOCK);
      auto [b, flags] = gbr;

      if (b /* XXX again, can this fail? */) {
	if (! (b->flags & Bucket::FLAG_FILLED)) {
	  /* bulk load into lmdb cache */
	  fill(b, FLAG_NONE);
	}
	/* display them */
	b->mtx.unlock();
	/*! LOCKED */

	auto txn = b->env->getROTransaction();
	auto cursor=txn->getCursor(b->dbi);
	MDBOutVal key, data;
	uint64_t count{0};

	const auto proc_result = [&]() {
	  std::string_view svk = key.get<string_view>();
	  std::string_view svv = data.get<string_view>();
	  std::string ser_data{svv};
	  zpp::bits::in in(ser_data);
	  // TODO: unfakening
	  FakeDirEntry fde;
	  auto errc = in(fde.fname, fde.s1, fde.i1, fde.i2);
	  if (errc.code != std::errc{0}) {
	    abort();
	  }
	  (void) func(svk, fde);
	  count++;
	};

	if (! marker.empty()) {
	  MDBInVal k(marker);
	  auto rc = cursor.lower_bound(k, key, data);
	  if (rc == MDB_NOTFOUND) {
	    /* no key sorts after k/marker, so there is nothing to do */
	    return;
	  }
	  proc_result();	  
	} else {
	  /* position at start of index */
	  cursor.get(key, data, MDB_FIRST);
	  proc_result();
	}
	while(! cursor.get(key, data, MDB_NEXT)) {
	  proc_result();
	}
	lru.unref(b, cohort::lru::FLAG_NONE);
      }
    } /* list_bucket */

  int notify(const std::string& bname, void* opaque,
	     const std::vector<Notifiable::Event>& evec) override {
    GetBucketResult gbr = get_bucket(bname, BucketCache::FLAG_LOCK);
    auto [b, flags] = gbr;
    if (b) {
      unique_lock ulk{b->mtx, std::adopt_lock};
      if ((b->name != bname) ||
	  (b != opaque) ||
	  (! (b->flags & Bucket::FLAG_FILLED))) {
	/* do nothing */
	return 0;
      }
      ulk.unlock();
      auto txn = b->env->getRWTransaction();
      for (const auto& ev : evec) {
	using EventType = Notifiable::EventType;
	std::string_view nil{""};
	/*std::cout << fmt::format("notify {} {}!",
				 ev.name ? *ev.name : nil,
				 uint32_t(ev.type))
				 << std::endl; */
	switch (ev.type)
	{
	case EventType::ADD:
	{
	  auto& ev_name = *ev.name;
	  // TODO: unfakening
	  FakeDirEntry fde{std::string(ev_name), "blinkered", 1050, 3801};
	  std::string ser_data;
	  zpp::bits::out out(ser_data);
	  out(fde.fname, fde.s1, fde.i1, fde.i2).or_throw();
	  txn->put(b->dbi, ev_name, ser_data);
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
	  mdb_drop(*txn, b->dbi, 0); /* apparently, does not require
				   * commit */
	  b->flags &= ~Bucket::FLAG_FILLED;
	  return 0; /* don't process any more events in this batch */
	}
	  break;
	default:
	  /* unknown event */
	  break;
	}
      } /* all events */
      txn->commit();
    } /* b */
    return 0;
  } /* notify */
  
}; /* BucketCache */

} // namespace file::listing
