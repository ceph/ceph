// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FILE_H
#define RGW_FILE_H

#include "include/rados/rgw_file.h"

/* internal header */
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <mutex>
#include <vector>
#include <deque>
#include <algorithm>
#include <functional>
#include <boost/intrusive_ptr.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/variant.hpp>
#include <boost/utility/string_ref.hpp>
#include "xxhash.h"
#include "include/buffer.h"
#include "common/sstring.hh"
#include "common/cohort_lru.h"
#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_lib.h"
#include "rgw_ldap.h"
#include "rgw_token.h"


/* XXX
 * ASSERT_H somehow not defined after all the above (which bring
 * in common/debug.h [e.g., dout])
 */
#include "include/assert.h"


#define RGW_RWXMODE  (S_IRWXU | S_IRWXG | S_IRWXO)

#define RGW_RWMODE (RGW_RWXMODE &			\
		      ~(S_IXUSR | S_IXGRP | S_IXOTH))


namespace rgw {

  template <typename T>
  static inline void ignore(T &&) {}


  namespace bi = boost::intrusive;

  class RGWLibFS;
  class RGWFileHandle;
  class RGWWriteRequest;

  /*
   * XXX
   * The current 64-bit, non-cryptographic hash used here is intended
   * for prototyping only.
   *
   * However, the invariant being prototyped is that objects be
   * identifiable by their hash components alone.  We believe this can
   * be legitimately implemented using 128-hash values for bucket and
   * object components, together with a cluster-resident cryptographic
   * key.  Since an MD5 or SHA-1 key is 128 bits and the (fast),
   * non-cryptographic CityHash128 hash algorithm takes a 128-bit seed,
   * speculatively we could use that for the final hash computations.
   */
  struct fh_key
  {
    rgw_fh_hk fh_hk;

    static constexpr uint64_t seed = 8675309;

    fh_key() {}

    fh_key(const rgw_fh_hk& _hk)
      : fh_hk(_hk) {
      // nothing
    }

    fh_key(const uint64_t bk, const uint64_t ok) {
      fh_hk.bucket = bk;
      fh_hk.object = ok;
    }

    fh_key(const uint64_t bk, const char *_o) {
      fh_hk.bucket = bk;
      fh_hk.object = XXH64(_o, ::strlen(_o), seed);
    }
    
    fh_key(const std::string& _b, const std::string& _o) {
      fh_hk.bucket = XXH64(_b.c_str(), _o.length(), seed);
      fh_hk.object = XXH64(_o.c_str(), _o.length(), seed);
    }
  }; /* fh_key */

  inline bool operator<(const fh_key& lhs, const fh_key& rhs)
  {
    return ((lhs.fh_hk.bucket < rhs.fh_hk.bucket) ||
	    ((lhs.fh_hk.bucket == rhs.fh_hk.bucket) &&
	      (lhs.fh_hk.object < rhs.fh_hk.object)));
  }

  inline bool operator>(const fh_key& lhs, const fh_key& rhs)
  {
    return (rhs < lhs);
  }

  inline bool operator==(const fh_key& lhs, const fh_key& rhs)
  {
    return ((lhs.fh_hk.bucket == rhs.fh_hk.bucket) &&
	    (lhs.fh_hk.object == rhs.fh_hk.object));
  }

  inline bool operator!=(const fh_key& lhs, const fh_key& rhs)
  {
    return !(lhs == rhs);
  }

  inline bool operator<=(const fh_key& lhs, const fh_key& rhs)
  {
    return (lhs < rhs) || (lhs == rhs);
  }

  using boost::variant;
  using boost::container::flat_map;

  class RGWFileHandle : public cohort::lru::Object
  {
    struct rgw_file_handle fh;
    std::mutex mtx;
    RGWLibFS* fs;
    RGWFileHandle* bucket;
    RGWFileHandle* parent;
    /* const */ std::string name; /* XXX file or bucket name */
    /* const */ fh_key fhk;

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    /* median file name length (HPC) has been found to be 16,
     * w/90% of file names <= 31 (Yifan Wang, CMU) */
    using dirent_string = basic_sstring<char, uint16_t, 32>;

    using marker_cache_t = flat_map<uint64_t, dirent_string>;
    using name_cache_t = flat_map<dirent_string, uint8_t>;

    struct state {
      uint64_t dev;
      size_t size;
      uint64_t nlink;
      struct timespec ctime;
      struct timespec mtime;
      struct timespec atime;
      state() : dev(0), size(0), nlink(1), ctime{0,0}, mtime{0,0}, atime{0,0} {}
    } state;

    struct file {
      RGWWriteRequest* write_req;
      file() : write_req(nullptr) {}
      ~file();
    };

    struct directory {

      static constexpr uint32_t FLAG_NONE =     0x0000;
      static constexpr uint32_t FLAG_CACHED =   0x0001;
      static constexpr uint32_t FLAG_OVERFLOW = 0x0002;

      uint32_t flags;
      marker_cache_t marker_cache;
      name_cache_t name_cache;

      directory() : flags(FLAG_NONE) {}

      void clear_state() {
	marker_cache.clear();
	name_cache.clear();
      }

      void set_overflow() {
	clear_state();
	flags |= FLAG_OVERFLOW;
      }
    };

    boost::variant<file, directory> variant_type;

    uint16_t depth;
    uint32_t flags;

  public:
    const static std::string root_name;

    static constexpr uint16_t MAX_DEPTH = 256;

    static constexpr uint32_t FLAG_NONE =    0x0000;
    static constexpr uint32_t FLAG_OPEN =    0x0001;
    static constexpr uint32_t FLAG_ROOT =    0x0002;
    static constexpr uint32_t FLAG_CREATE =  0x0004;
    static constexpr uint32_t FLAG_CREATING =  0x0008;
    static constexpr uint32_t FLAG_DIRECTORY = 0x0010;
    static constexpr uint32_t FLAG_BUCKET = 0x0020;
    static constexpr uint32_t FLAG_LOCK =   0x0040;
    static constexpr uint32_t FLAG_DELETED = 0x0080;

    friend class RGWLibFS;

  private:
    RGWFileHandle(RGWLibFS* _fs, uint32_t fs_inst)
      : fs(_fs), bucket(nullptr), parent(nullptr), variant_type{directory()},
	depth(0), flags(FLAG_ROOT)
      {
	/* root */
	fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	variant_type = directory();
	/* stat */
	state.dev = fs_inst;
	/* pointer to self */
	fh.fh_private = this;
      }
    
    void init_rootfs(std::string& fsid, const std::string& object_name) {
      /* fh_key */
      fh.fh_hk.bucket = XXH64(fsid.c_str(), fsid.length(), fh_key::seed);
      fh.fh_hk.object = XXH64(object_name.c_str(), object_name.length(),
			      fh_key::seed);
      fhk = fh.fh_hk;
      name = object_name;
    }

  public:
    RGWFileHandle(RGWLibFS* fs, uint32_t fs_inst, RGWFileHandle* _parent,
		  const fh_key& _fhk, std::string& _name, uint32_t _flags)
      : fs(fs), bucket(nullptr), parent(_parent), name(std::move(_name)),
	fhk(_fhk), flags(_flags) {

      if (parent->is_root()) {
	fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	variant_type = directory();
	flags |= FLAG_BUCKET;
      } else {
	bucket = (parent->flags & FLAG_BUCKET) ? parent
	  : parent->bucket;
	if (flags & FLAG_DIRECTORY) {
	  fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	  variant_type = directory();
	} else {
	  fh.fh_type = RGW_FS_TYPE_FILE;
	  variant_type = file();
	}
      }

      depth = parent->depth + 1;

      /* save constant fhk */
      fh.fh_hk = fhk.fh_hk; /* XXX redundant in fh_hk */

      /* pointer to self */
      fh.fh_private = this;
    }

    const fh_key& get_key() const {
      return fhk;
    }

    directory* get_directory() {
      return get<directory>(&variant_type);
    }

    size_t get_size() const { return state.size; }

    const char* stype() {
      return is_dir() ? "DIR" : "FILE";
    }

    uint16_t get_depth() const { return depth; }

    struct rgw_file_handle* get_fh() { return &fh; }

    RGWLibFS* get_fs() { return fs; }

    RGWFileHandle* get_parent() { return parent; }

    struct timespec get_mtime() const { return state.mtime; }

    int stat(struct stat *st) {
      /* partial Unix attrs */
      memset(st, 0, sizeof(struct stat));
      st->st_dev = state.dev;
      st->st_ino = fh.fh_hk.object; // XXX

      st->st_uid = 0; // XXX
      st->st_gid = 0; // XXX

      st->st_atim = state.atime;
      st->st_mtim = state.mtime;
      st->st_ctim = state.ctime;

      switch (fh.fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
	st->st_mode = RGW_RWXMODE|S_IFDIR;
	st->st_nlink = 3;
	break;
      case RGW_FS_TYPE_FILE:
	st->st_mode = RGW_RWMODE|S_IFREG;
	st->st_nlink = 1;
	st->st_blksize = 4096;
	st->st_size = state.size;
	st->st_blocks = (state.size) / 512;
      default:
	break;
      }

      return 0;
    }

    const std::string& bucket_name() const {
      if (is_root())
	return root_name;
      if (flags & FLAG_BUCKET)
	return name;
      return bucket->object_name();
    }

    const std::string& object_name() const { return name; }

    std::string full_object_name(bool omit_bucket = false) {
      std::string path;
      std::vector<const std::string*> segments;
      int reserve = 0;
      RGWFileHandle* tfh = this;
      while (tfh && !tfh->is_root() && !(tfh->is_bucket() && omit_bucket)) {
	segments.push_back(&tfh->object_name());
	reserve += (1 + tfh->object_name().length());
	tfh = tfh->parent;
      }
      bool first = true;
      path.reserve(reserve);
      for (auto& s : boost::adaptors::reverse(segments)) {
	if (! first)
	  path += "/";
	else {
	  if (!omit_bucket && (path.front() != '/')) // pretty-print
	    path += "/";
	  first = false;
	}
	path += *s;
      }
      return path;
    }

    inline std::string relative_object_name() {
      return full_object_name(true /* omit_bucket */);
    }

    inline std::string format_child_name(const std::string& cbasename) {
      std::string child_name{relative_object_name()};
      if ((child_name.size() > 0) &&
	  (child_name.back() != '/'))
	child_name += "/";
      child_name += cbasename;
      return child_name;
    }

    inline std::string make_key_name(const char *name) {
      std::string key_name{full_object_name()};
      if (key_name.length() > 0)
	key_name += "/";
      key_name += name;
      return key_name;
    }

    fh_key make_fhk(const std::string& name) {
      if (depth <= 1)
	return fh_key(fhk.fh_hk.object, name.c_str());
      else {
	std::string key_name = make_key_name(name.c_str());
	return fh_key(fhk.fh_hk.object, key_name.c_str());
      }
    }

    void add_marker(uint64_t off, const boost::string_ref& marker,
		    uint8_t obj_type) {
      using std::get;
      directory* d = get<directory>(&variant_type);
      if (d) {
	unique_lock guard(mtx);
	// XXXX check for failure (dup key)
	d->marker_cache.insert(
	  marker_cache_t::value_type(off, marker.data()));
	/* 90% of directories hold <= 32 entries (Yifan Wang, CMU),
	 * but go big */
	if (d->name_cache.size() < 128) {
	  d->name_cache.insert(
	    name_cache_t::value_type(marker.data(), obj_type));
	} else {
	  d->set_overflow(); // too many
	}
      }
    }

    /* XXX */
    std::string find_marker(uint64_t off) { // XXX copy
      using std::get;
      directory* d = get<directory>(&variant_type);
      if (d) {
	const auto& iter = d->marker_cache.find(off);
	if (iter != d->marker_cache.end())
	  return iter->second;
      }
      return "";
    }
    
    bool is_open() const { return flags & FLAG_OPEN; }
    bool is_root() const { return flags & FLAG_ROOT; }
    bool is_bucket() const { return flags & FLAG_BUCKET; }
    bool is_object() const { return !is_bucket(); }
    bool is_file() const { return (fh.fh_type == RGW_FS_TYPE_FILE); }
    bool is_dir() const { return (fh.fh_type == RGW_FS_TYPE_DIRECTORY); }
    bool creating() const { return flags & FLAG_CREATING; }

    uint32_t open(uint32_t gsh_flags) {
      lock_guard guard(mtx);
      if (! (flags & FLAG_OPEN)) {
	flags |= FLAG_OPEN;
	return 0;
      }
      return EPERM;
    }

    int readdir(rgw_readdir_cb rcb, void *cb_arg, uint64_t *offset, bool *eof,
		uint32_t flags);
    int write(uint64_t off, size_t len, size_t *nbytes, void *buffer);
    int write_finish();
    int close();

    void open_for_create() {
      lock_guard guard(mtx);
      flags |= FLAG_CREATING;
    }

    void clear_creating() {
      lock_guard guard(mtx);
      flags &= ~FLAG_CREATING;
    }

    void set_nlink(const uint64_t n) {
      state.nlink = n;
    }

    void set_size(const size_t size) {
      state.size = size;
    }

    void set_times(time_t t) {
      state.ctime = {t, 0};
      state.mtime = {t, 0};
      state.atime = {t, 0};
    }

    void set_ctime(const struct timespec &ts) {
      state.ctime = ts;
    }

    void set_mtime(const struct timespec &ts) {
      state.mtime = ts;
    }

    void set_atime(const struct timespec &ts) {
      state.atime = ts;
    }

    virtual bool reclaim();

    typedef cohort::lru::LRU<std::mutex> FhLRU;

    struct FhLT
    {
      // for internal ordering
      bool operator()(const RGWFileHandle& lhs, const RGWFileHandle& rhs) const
	{ return (lhs.get_key() < rhs.get_key()); }

      // for external search by fh_key
      bool operator()(const fh_key& k, const RGWFileHandle& fh) const
	{ return k < fh.get_key(); }

      bool operator()(const RGWFileHandle& fh, const fh_key& k) const
	{ return fh.get_key() < k; }
    };

    struct FhEQ
    {
      bool operator()(const RGWFileHandle& lhs, const RGWFileHandle& rhs) const
	{ return (lhs.get_key() == rhs.get_key()); }

      bool operator()(const fh_key& k, const RGWFileHandle& fh) const
	{ return k == fh.get_key(); }

      bool operator()(const RGWFileHandle& fh, const fh_key& k) const
	{ return fh.get_key() == k; }
    };

    typedef bi::link_mode<bi::safe_link> link_mode; /* XXX normal */
#if defined(FHCACHE_AVL)
    typedef bi::avl_set_member_hook<link_mode> tree_hook_type;
#else
    /* RBT */
    typedef bi::set_member_hook<link_mode> tree_hook_type;
#endif
    tree_hook_type fh_hook;

    typedef bi::member_hook<
      RGWFileHandle, tree_hook_type, &RGWFileHandle::fh_hook> FhHook;

#if defined(FHCACHE_AVL)
    typedef bi::avltree<RGWFileHandle, bi::compare<FhLT>, FhHook> FHTree;
#else
    typedef bi::rbtree<RGWFileHandle, bi::compare<FhLT>, FhHook> FhTree;
#endif
    typedef cohort::lru::TreeX<RGWFileHandle, FhTree, FhLT, FhEQ, fh_key,
			       std::mutex> FHCache;

    virtual ~RGWFileHandle() {}

    class Factory : public cohort::lru::ObjectFactory
    {
    public:
      RGWLibFS* fs;
      uint32_t fs_inst;
      RGWFileHandle* parent;
      const fh_key& fhk;
      std::string& name;
      uint32_t flags;

      Factory() = delete;

      Factory(RGWLibFS* fs, uint32_t fs_inst, RGWFileHandle* parent,
	      const fh_key& fhk, std::string& name, uint32_t flags)
	: fs(fs), fs_inst(fs_inst), parent(parent), fhk(fhk), name(name),
	  flags(flags) {}

      void recycle (cohort::lru::Object* o) {
	/* re-use an existing object */
	o->~Object(); // call lru::Object virtual dtor
	// placement new!
	new (o) RGWFileHandle(fs, fs_inst, parent, fhk, name, flags);
      }

      cohort::lru::Object* alloc() {
	return new RGWFileHandle(fs, fs_inst, parent, fhk, name, flags);
      }
    }; /* Factory */

  }; /* RGWFileHandle */

  static inline RGWFileHandle* get_rgwfh(struct rgw_file_handle* fh) {
    return static_cast<RGWFileHandle*>(fh->fh_private);
  }

  typedef std::tuple<RGWFileHandle*, uint32_t> LookupFHResult;

  class RGWLibFS
  {
    CephContext* cct;
    struct rgw_fs fs;
    RGWFileHandle root_fh;

    mutable std::atomic<uint64_t> refcnt;

    RGWFileHandle::FHCache fh_cache;
    RGWFileHandle::FhLRU fh_lru;
    
    std::string uid; // should match user.user_id, iiuc

    RGWUserInfo user;
    RGWAccessKey key; // XXXX acc_key

    static atomic<uint32_t> fs_inst;
    std::string fsid;

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    struct event
    {
      enum class type : uint8_t { READDIR } ;
      type t;
      const fh_key fhk;
      struct timespec ts;
      event(type t, const fh_key& k, const struct timespec& ts)
	: t(t), fhk(k), ts(ts) {}
    };

    using event_vector = /* boost::small_vector<event, 16> */
      std::vector<event>;

    struct state {
      std::mutex mtx;
      std::atomic<uint32_t> flags;
      std::deque<event> events;
      state() : flags(0) {}
      void push_event(const event& ev) {
	lock_guard guard(mtx);
	events.push_back(ev);
      }
    } state;

    friend class RGWFileHandle;

  public:

    static constexpr uint32_t FLAG_NONE =      0x0000;
    static constexpr uint32_t FLAG_CLOSED =    0x0001;

    RGWLibFS(CephContext* _cct, const char *_uid, const char *_user_id,
	    const char* _key)
      : cct(_cct), root_fh(this, get_inst()), refcnt(1),
	fh_cache(cct->_conf->rgw_nfs_fhcache_partitions,
		 cct->_conf->rgw_nfs_fhcache_size),
	fh_lru(cct->_conf->rgw_nfs_lru_lanes,
	       cct->_conf->rgw_nfs_lru_lane_hiwat),
	uid(_uid), key(_user_id, _key) {

      /* no bucket may be named rgw_fs_inst-(.*) */
      fsid = RGWFileHandle::root_name + "rgw_fs_inst-" +
	std::to_string(++(fs_inst));

      root_fh.init_rootfs(fsid /* bucket */, RGWFileHandle::root_name);

      /* pointer to self */
      fs.fs_private = this;

      /* expose public root fh */
      fs.root_fh = root_fh.get_fh();
    }

    friend void intrusive_ptr_add_ref(const RGWLibFS* fs) {
      fs->refcnt.fetch_add(1, std::memory_order_relaxed);
    }

    friend void intrusive_ptr_release(const RGWLibFS* fs) {
      if (fs->refcnt.fetch_sub(1, std::memory_order_release) == 0) {
	std::atomic_thread_fence(std::memory_order_acquire);
	delete fs;
      }
    }

    RGWLibFS* ref() {
      intrusive_ptr_add_ref(this);
      return this;
    }

    inline void rele() {
      intrusive_ptr_release(this);
    }

    int authorize(RGWRados* store) {
      int ret = rgw_get_user_info_by_access_key(store, key.id, user);
      if (ret == 0) {
	RGWAccessKey* key0 = user.get_key0();
	if (!key0 ||
	    (key0->key != key.key))
	  return -EINVAL;
	if (user.suspended)
	  return -ERR_USER_SUSPENDED;
      } else {
	/* try external authenticators (ldap for now) */
	rgw::LDAPHelper* ldh = rgwlib.get_ldh(); /* !nullptr */
	RGWToken token{from_base64(key.id)};
	if (ldh->auth(token.id, token.key) == 0) {
	  /* try to store user if it doesn't already exist */
	  if (rgw_get_user_info_by_uid(store, token.id, user) < 0) {
	    int ret = rgw_store_user_info(store, user, NULL, NULL, 0,
					  true);
	    if (ret < 0) {
	      lsubdout(get_context(), rgw, 10)
		<< "NOTICE: failed to store new user's info: ret=" << ret
		<< dendl;
	    }
	  }
	} /* auth success */
      }
      return ret;
    } /* authorize */

    /* find or create an RGWFileHandle */
    LookupFHResult lookup_fh(RGWFileHandle* parent, const char *name,
			     const uint32_t cflags = RGWFileHandle::FLAG_NONE) {
      using std::get;

      LookupFHResult fhr { nullptr, RGWFileHandle::FLAG_NONE };

      /* mount is stale? */
      if (state.flags & FLAG_CLOSED)
	return fhr;

      RGWFileHandle::FHCache::Latch lat;
      memset(&lat, 0, sizeof(lat)); // XXXX testing

      std::string obj_name{name};
      std::string key_name{parent->make_key_name(name)};

      fh_key fhk = parent->make_fhk(key_name);

    retry:
      RGWFileHandle* fh =
	fh_cache.find_latch(fhk.fh_hk.object /* partition selector*/,
			    fhk /* key */, lat /* serializer */,
			    RGWFileHandle::FHCache::FLAG_LOCK);
      /* LATCHED */
      if (fh) {
	fh->mtx.lock(); // XXX !RAII because may-return-LOCKED
	if (fh->flags & RGWFileHandle::FLAG_DELETED) {
	  /* for now, delay briefly and retry */
	  lat.lock->unlock();
	  fh->mtx.unlock();
	  std::this_thread::sleep_for(std::chrono::milliseconds(20));
	  goto retry; /* !LATCHED */
	}
	/* need initial ref from LRU (fast path) */
	if (! fh_lru.ref(fh, cohort::lru::FLAG_INITIAL)) {
	  lat.lock->unlock();
	  fh->mtx.unlock();
	  goto retry; /* !LATCHED */
	}
	/* LATCHED, LOCKED */
	if (! (fh->flags & RGWFileHandle::FLAG_LOCK))
	  fh->mtx.unlock(); /* ! LOCKED */
      } else {
	/* make or re-use handle */
	RGWFileHandle::Factory prototype(this, get_inst(), parent, fhk,
					 obj_name, cflags);
	fh = static_cast<RGWFileHandle*>(
	  fh_lru.insert(&prototype,
			cohort::lru::Edge::MRU,
			cohort::lru::FLAG_INITIAL));
	if (fh) {
	  fh_cache.insert_latched(fh, lat, RGWFileHandle::FHCache::FLAG_UNLOCK);
	  get<1>(fhr) |= RGWFileHandle::FLAG_CREATE;
	  if (fh->flags & RGWFileHandle::FLAG_LOCK)
	    fh->mtx.lock();
	  goto out; /* !LATCHED */
	} else {
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
      }
      lat.lock->unlock(); /* !LATCHED */
    out:
      get<0>(fhr) = fh;
      return fhr;
    }

    inline void unref(RGWFileHandle* fh) {
      (void) fh_lru.unref(fh, cohort::lru::FLAG_NONE);
    }

    inline RGWFileHandle* ref(RGWFileHandle* fh) {
      fh_lru.ref(fh, cohort::lru::FLAG_NONE);
      return fh;
    }

    int getattr(RGWFileHandle* rgw_fh, struct stat* st);

    LookupFHResult stat_bucket(RGWFileHandle* parent,
			       const char *path, uint32_t flags);

    LookupFHResult stat_leaf(RGWFileHandle* parent, const char *path,
			     uint32_t flags);

    int rename(RGWFileHandle* old_fh, RGWFileHandle* new_fh,
	       const char *old_name, const char *new_name);

    int unlink(RGWFileHandle* parent, const char *name);

    /* find existing RGWFileHandle */
    RGWFileHandle* lookup_handle(struct rgw_fh_hk fh_hk) {

      if (state.flags & FLAG_CLOSED)
	return nullptr;

      RGWFileHandle::FHCache::Latch lat;
      fh_key fhk(fh_hk);

    retry:
      RGWFileHandle* fh =
	fh_cache.find_latch(fhk.fh_hk.object /* partition selector*/,
			    fhk /* key */, lat /* serializer */,
			    RGWFileHandle::FHCache::FLAG_LOCK);
      /* LATCHED */
      if (! fh) {
	lsubdout(get_context(), rgw, 0)
	  << __func__ << " handle lookup failed <"
	  << fhk.fh_hk.bucket << "," << fhk.fh_hk.object << ">"
	  << "(need persistent handles)"
	  << dendl;
	goto out;
      }
      fh->mtx.lock();
      if (fh->flags & RGWFileHandle::FLAG_DELETED) {
	/* for now, delay briefly and retry */
	lat.lock->unlock();
	fh->mtx.unlock();
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	goto retry; /* !LATCHED */
      }
      if (! fh_lru.ref(fh, cohort::lru::FLAG_INITIAL)) {
	lat.lock->unlock();
	fh->mtx.unlock();
	goto retry; /* !LATCHED */
      }
      /* LATCHED */
    out:
      lat.lock->unlock(); /* !LATCHED */
      return fh;
    }

    CephContext* get_context() {
      return cct;
    }

    struct rgw_fs* get_fs() { return &fs; }

    uint32_t get_inst() { return fs_inst; }

    RGWUserInfo* get_user() { return &user; }

    void close();
    void gc();
  }; /* RGWLibFS */

static inline std::string make_uri(const std::string& bucket_name,
				   const std::string& object_name) {
  std::string uri("/");
  uri.reserve(bucket_name.length() + object_name.length() + 2);
  uri += bucket_name;
  uri += "/";
  uri += object_name;
  return uri;
}

/*
  read directory content (buckets)
*/

class RGWListBucketsRequest : public RGWLibRequest,
			      public RGWListBuckets /* RGWOp */
{
public:
  RGWFileHandle* rgw_fh;
  uint64_t* offset;
  void* cb_arg;
  rgw_readdir_cb rcb;
  size_t ix;

  RGWListBucketsRequest(CephContext* _cct, RGWUserInfo *_user,
			RGWFileHandle* _rgw_fh, rgw_readdir_cb _rcb,
			void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), offset(_offset),
      cb_arg(_cb_arg), rcb(_rcb), ix(0) {
    const std::string& sm = rgw_fh->find_marker(*offset);
    if (sm.size() > 0) {
      RGWListBuckets::marker =
	rgw_fh->relative_object_name();
      if (marker.back() != '/')
	marker += "/";
      marker += sm;
    }
    op = this;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {
    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    /* XXX derp derp derp */
    s->relative_uri = "/";
    s->info.request_uri = "/"; // XXX
    s->info.effective_uri = "/";
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  int get_params() {
    limit = -1; /* no limit */
    return 0;
  }

  virtual void send_response_begin(bool has_buckets) {
    sent_data = true;
  }

  virtual void send_response_data(RGWUserBuckets& buckets) {
    if (!sent_data)
      return;
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    for (const auto& iter : m) {
      boost::string_ref marker{iter.first};
      const RGWBucketEnt& ent = iter.second;
      /* call me maybe */
      this->operator()(ent.bucket.name, marker);
      ++ix;
    }
  } /* send_response_data */

  virtual void send_response_end() {
    // do nothing
  }

  int operator()(const boost::string_ref& name, const boost::string_ref& marker) {
    uint64_t off = XXH64(name.data(), name.length(), fh_key::seed);
    *offset = off;
    /* update traversal cache */
    rgw_fh->add_marker(off, marker, RGW_FS_TYPE_DIRECTORY);
    rcb(name.data(), cb_arg, off);
    return 0;
  }

  bool eof() {
    lsubdout(cct, rgw, 15) << "READDIR offset: " << *offset
			   << " is_truncated: " << is_truncated
			   << dendl;
    return !is_truncated;
  }

}; /* RGWListBucketsRequest */

/*
  read directory content (bucket objects)
*/

  class RGWReaddirRequest : public RGWLibRequest,
			    public RGWListBucket /* RGWOp */
{
public:
  RGWFileHandle* rgw_fh;
  uint64_t* offset;
  void* cb_arg;
  rgw_readdir_cb rcb;
  size_t ix;

  RGWReaddirRequest(CephContext* _cct, RGWUserInfo *_user,
		    RGWFileHandle* _rgw_fh, rgw_readdir_cb _rcb,
		    void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), offset(_offset),
      cb_arg(_cb_arg), rcb(_rcb), ix(0) {
    const std::string& sm{rgw_fh->find_marker(*offset)};
    if (sm.size() > 0) {
      RGWListBucket::marker = {rgw_fh->relative_object_name(), ""};
      if (marker.name.back() != '/')
	marker.name += "/";
      marker.name += sm;
    }
    default_max = 1000; // XXX was being omitted
    op = this;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {
    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    /* XXX derp derp derp */
    std::string uri = "/" + rgw_fh->bucket_name() + "/";
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    prefix = rgw_fh->relative_object_name();
    if (prefix.length() > 0)
      prefix += "/";
    delimiter = '/';

    return 0;
  }

  int operator()(const boost::string_ref name, const boost::string_ref marker,
		uint8_t type) {

    assert(name.length() > 0); // XXX

    /* hash offset of name in parent (short name) for NFS readdir cookie */
    uint64_t off = XXH64(name.data(), name.length(), fh_key::seed);
    *offset = off;
    /* update traversal cache */
    rgw_fh->add_marker(off, marker, type);
    rcb(name.data(), cb_arg, off); // XXX has to be legit C-style string
    return 0;
  }

  virtual int get_params() {
    max = default_max;
    return 0;
  }

  virtual void send_response() {
    struct req_state* s = get_state();
    for (const auto& iter : objs) {

      boost::string_ref sref {iter.key.name};

      lsubdout(cct, rgw, 15) << "readdir objects prefix: " << prefix
			     << " obj: " << sref << dendl;

      size_t last_del = sref.find_last_of('/');
      if (last_del != string::npos)
	sref.remove_prefix(last_del+1);

      /* leaf directory? */
      if (sref.empty())
	continue;

      lsubdout(cct, rgw, 15) << "RGWReaddirRequest "
			     << __func__ << " "
			     << "list uri=" << s->relative_uri << " "
			     << " prefix=" << prefix << " "
			     << " obj path=" << iter.key.name
			     << " (" << sref << ")" << ""
			     << dendl;

      /* call me maybe */
      this->operator()(sref, sref, RGW_FS_TYPE_FILE);
      ++ix;
    }
    for (auto& iter : common_prefixes) {

      lsubdout(cct, rgw, 15) << "readdir common prefixes prefix: " << prefix
			     << " iter first: " << iter.first
			     << " iter second: " << iter.second
			     << dendl;

      /* XXX aieee--I have seen this case! */
      if (iter.first == "/")
	continue;

      /* it's safest to modify the element in place--a suffix-modifying
       * string_ref operation is problematic since ULP rgw_file callers
       * will ultimately need a c-string */
      if (iter.first.back() == '/')
	const_cast<std::string&>(iter.first).pop_back();

      boost::string_ref sref{iter.first};

      size_t last_del = sref.find_last_of('/');
      if (last_del != string::npos)
	sref.remove_prefix(last_del+1);

      lsubdout(cct, rgw, 15) << "RGWReaddirRequest "
			     << __func__ << " "
			     << "list uri=" << s->relative_uri << " "
			     << " prefix=" << prefix << " "
			     << " cpref=" << sref
			     << dendl;

      this->operator()(sref, sref, RGW_FS_TYPE_DIRECTORY);
      ++ix;
    }
  }

  virtual void send_versioned_response() {
    send_response();
  }

  bool eof() {
    lsubdout(cct, rgw, 15) << "READDIR offset: " << *offset
			   << " next marker: " << next_marker
			   << " is_truncated: " << is_truncated
			   << dendl;
    return !is_truncated;
  }

}; /* RGWReaddirRequest */

/*
  create bucket
*/

class RGWCreateBucketRequest : public RGWLibRequest,
			       public RGWCreateBucket /* RGWOp */
{
public:
  std::string& uri;

  RGWCreateBucketRequest(CephContext* _cct, RGWUserInfo *_user,
			std::string& _uri)
    : RGWLibRequest(_cct, _user), uri(_uri) {
    op = this;
  }

  virtual bool only_bucket() { return false; }

  virtual int read_permissions(RGWOp* op_obj) {
    /* we ARE a 'create bucket' request (cf. rgw_rest.cc, ll. 1305-6) */
    return 0;
  }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "PUT";
    s->op = OP_PUT;

    /* XXX derp derp derp */
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  virtual int get_params() {
    struct req_state* s = get_state();
    RGWAccessControlPolicy_S3 s3policy(s->cct);
    /* we don't have (any) headers, so just create canned ACLs */
    int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
    policy = s3policy;
    return ret;
  }

  virtual void send_response() {
    /* TODO: something (maybe) */
  }
}; /* RGWCreateBucketRequest */

/*
  delete bucket
*/

class RGWDeleteBucketRequest : public RGWLibRequest,
			       public RGWDeleteBucket /* RGWOp */
{
public:
  std::string& uri;

  RGWDeleteBucketRequest(CephContext* _cct, RGWUserInfo *_user,
			std::string& _uri)
    : RGWLibRequest(_cct, _user), uri(_uri) {
    op = this;
  }

  virtual bool only_bucket() { return true; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "DELETE";
    s->op = OP_DELETE;

    /* XXX derp derp derp */
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  virtual void send_response() {}

}; /* RGWDeleteBucketRequest */

/*
  put object
*/
class RGWPutObjRequest : public RGWLibRequest,
			 public RGWPutObj /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;
  buffer::list& bl; /* XXX */
  size_t bytes_written;

  RGWPutObjRequest(CephContext* _cct, RGWUserInfo *_user,
		  const std::string& _bname, const std::string& _oname,
		  buffer::list& _bl)
    : RGWLibRequest(_cct, _user), bucket_name(_bname), obj_name(_oname),
      bl(_bl), bytes_written(0) {
    op = this;
  }

  virtual bool only_bucket() { return true; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED

    if (! valid_s3_object_name(obj_name))
      return -ERR_INVALID_OBJECT_NAME;

    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "PUT";
    s->op = OP_PUT;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    /* XXX required in RGWOp::execute() */
    s->content_length = bl.length();

    // woo
    s->user = user;

    return 0;
  }

  virtual int get_params() {
    struct req_state* s = get_state();
    RGWAccessControlPolicy_S3 s3policy(s->cct);
    /* we don't have (any) headers, so just create canned ACLs */
    int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
    policy = s3policy;
    return ret;
  }

  virtual int get_data(buffer::list& _bl) {
    /* XXX for now, use sharing semantics */
    _bl.claim(bl);
    uint32_t len = _bl.length();
    bytes_written += len;
    return len;
  }

  virtual void send_response() {}

  virtual int verify_params() {
    if (bl.length() > cct->_conf->rgw_max_put_size)
      return -ERR_TOO_LARGE;
    return 0;
  }

}; /* RGWPutObjRequest */

/*
  get object
*/

class RGWReadRequest : public RGWLibRequest,
		       public RGWGetObj /* RGWOp */
{
public:
  RGWFileHandle* rgw_fh;
  void *ulp_buffer;
  size_t nread;
  size_t read_resid; /* initialize to len, <= sizeof(ulp_buffer) */
  bool do_hexdump = false;

  RGWReadRequest(CephContext* _cct, RGWUserInfo *_user,
		 RGWFileHandle* _rgw_fh, uint64_t off, uint64_t len,
		 void *_ulp_buffer)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), ulp_buffer(_ulp_buffer),
      nread(0), read_resid(len) {
    op = this;

    /* fixup RGWGetObj (already know range parameters) */
    RGWGetObj::range_parsed = true;
    RGWGetObj::get_data = true; // XXX
    RGWGetObj::partial_content = true;
    RGWGetObj::ofs = off;
    RGWGetObj::end = off + len;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    /* XXX derp derp derp */
    s->relative_uri = make_uri(rgw_fh->bucket_name(),
			       rgw_fh->relative_object_name());
    s->info.request_uri = s->relative_uri; // XXX
    s->info.effective_uri = s->relative_uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  virtual int get_params() {
    return 0;
  }

  virtual int send_response_data(ceph::buffer::list& bl, off_t bl_off,
				off_t bl_len) {
    size_t bytes;
    for (auto& bp : bl.buffers()) {
      /* if for some reason bl_off indicates the start-of-data is not at
       * the current buffer::ptr, skip it and account */
      if (bl_off > bp.length()) {
	bl_off -= bp.length();
	continue;
      }
      /* read no more than read_resid */
      bytes = std::min(read_resid, size_t(bp.length()-bl_off));
      memcpy(static_cast<char*>(ulp_buffer)+nread, bp.c_str()+bl_off, bytes);
      read_resid -= bytes; /* reduce read_resid by bytes read */
      nread += bytes;
      bl_off = 0;
      /* stop if we have no residual ulp_buffer */
      if (! read_resid)
	break;
    }
    return 0;
  }

  virtual int send_response_data_error() {
    /* S3 implementation just sends nothing--there is no side effect
     * to simulate here */
    return 0;
  }

}; /* RGWReadRequest */

/*
  delete object
*/

class RGWDeleteObjRequest : public RGWLibRequest,
			    public RGWDeleteObj /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;

  RGWDeleteObjRequest(CephContext* _cct, RGWUserInfo *_user,
		      const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, _user), bucket_name(_bname), obj_name(_oname) {
    op = this;
  }

  virtual bool only_bucket() { return true; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "DELETE";
    s->op = OP_DELETE;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  virtual void send_response() {}

}; /* RGWDeleteObjRequest */

class RGWStatObjRequest : public RGWLibRequest,
			  public RGWGetObj /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;
  uint64_t _size;
  uint32_t flags;

  static constexpr uint32_t FLAG_NONE = 0x000;

  RGWStatObjRequest(CephContext* _cct, RGWUserInfo *_user,
		    const std::string& _bname, const std::string& _oname,
		    uint32_t _flags)
    : RGWLibRequest(_cct, _user), bucket_name(_bname), obj_name(_oname),
      _size(0), flags(_flags) {
    op = this;

    /* fixup RGWGetObj (already know range parameters) */
    RGWGetObj::range_parsed = true;
    RGWGetObj::get_data = false; // XXX
    RGWGetObj::partial_content = true;
    RGWGetObj::ofs = 0;
    RGWGetObj::end = UINT64_MAX;
  }

  virtual const string name() { return "stat_obj"; }
  virtual RGWOpType get_type() { return RGW_OP_STAT_OBJ; }

  time_t get_mtime() const {
    return lastmod;
  }

  /* attributes */
  uint64_t get_size() { return _size; }
  time_t ctime() { return mod_time; } // XXX
  time_t mtime() { return mod_time; }
  map<string, bufferlist>& get_attrs() { return attrs; }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    /* XXX derp derp derp */
    s->relative_uri = make_uri(bucket_name, obj_name);
    s->info.request_uri = s->relative_uri; // XXX
    s->info.effective_uri = s->relative_uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  virtual int get_params() {
    return 0;
  }

  virtual int send_response_data(ceph::buffer::list& _bl, off_t s_off,
				off_t e_off) {
    /* NOP */
    return 0;
  }

  virtual int send_response_data_error() {
    /* NOP */
    return 0;
  }

  virtual void execute() {
    RGWGetObj::execute();
    _size = get_state()->obj_size;
  }

}; /* RGWStatObjRequest */

class RGWStatBucketRequest : public RGWLibRequest,
			     public RGWStatBucket /* RGWOp */
{
public:
  std::string uri;

  RGWStatBucketRequest(CephContext* _cct, RGWUserInfo *_user,
		       const std::string& _path)
    : RGWLibRequest(_cct, _user) {
    uri = "/" + _path;
    op = this;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    /* XXX derp derp derp */
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  time_t get_ctime() const {
    return bucket.creation_time;
  }

  virtual int get_params() {
    return 0;
  }

  virtual void send_response() {
    bucket.creation_time = get_state()->bucket_info.creation_time;
  }

  bool matched() {
    return (bucket.bucket.name.length() > 0);
  }

}; /* RGWStatBucketRequest */

class RGWStatLeafRequest : public RGWLibRequest,
			   public RGWListBucket /* RGWOp */
{
public:
  RGWFileHandle* rgw_fh;
  std::string path;
  bool matched;
  bool is_dir;

  RGWStatLeafRequest(CephContext* _cct, RGWUserInfo *_user,
		     RGWFileHandle* _rgw_fh, const std::string& _path)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), path(_path),
      matched(false), is_dir(false) {
    default_max = 1000; // logical max {"foo", "foo/"}
    op = this;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    /* XXX derp derp derp */
    std::string uri = "/" + rgw_fh->bucket_name() + "/";
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    prefix = rgw_fh->relative_object_name();
    if (prefix.length() > 0)
      prefix += "/";
    prefix += path;
    delimiter = '/';

    return 0;
  }

  virtual int get_params() {
    max = default_max;
    return 0;
  }

  virtual void send_response() {
    struct req_state* s = get_state();
    // try objects
    for (const auto& iter : objs) {
      auto& name = iter.key.name;
      lsubdout(cct, rgw, 15) << "RGWStatLeafRequest "
			     << __func__ << " "
			     << "list uri=" << s->relative_uri << " "
			     << " prefix=" << prefix << " "
			     << " obj path=" << name << ""
			     << dendl;
      /* XXX is there a missing match-dir case (trailing '/')? */
      matched = true;
      return;
    }
    // try prefixes
    for (auto& iter : common_prefixes) {
      auto& name = iter.first;
      lsubdout(cct, rgw, 15) << "RGWStatLeafRequest "
			     << __func__ << " "
			     << "list uri=" << s->relative_uri << " "
			     << " prefix=" << prefix << " "
			     << " pref path=" << name << " (not chomped)"
			     << dendl;
      matched = true;
      is_dir = true;
      break;
    }
  }

  virtual void send_versioned_response() {
    send_response();
  }
}; /* RGWStatLeafRequest */

/*
  put object
*/

class RGWWriteRequest : public RGWLibContinuedReq,
			public RGWPutObj /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;
  RGWFileHandle* rgw_fh;
  RGWPutObjProcessor *processor;
  buffer::list data;
  MD5 hash;
  off_t last_off;
  off_t next_off;
  size_t bytes_written;
  bool multipart;

  RGWWriteRequest(CephContext* _cct, RGWUserInfo *_user, RGWFileHandle* _fh,
		  const std::string& _bname, const std::string& _oname)
    : RGWLibContinuedReq(_cct, _user), bucket_name(_bname), obj_name(_oname),
      rgw_fh(_fh), processor(nullptr), last_off(0), next_off(0),
      bytes_written(0), multipart(false) {

    int ret = header_init();
    if (ret == 0) {
      ret = init_from_header(get_state());
    }
    op = this;
  }

  virtual bool only_bucket() { return true; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "PUT";
    s->op = OP_PUT;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    // woo
    s->user = user;

    return 0;
  }

  virtual RGWPutObjProcessor *select_processor(RGWObjectCtx& obj_ctx,
					       bool *is_multipart) {
    struct req_state* s = get_state();
    uint64_t part_size = s->cct->_conf->rgw_obj_stripe_size;
    RGWPutObjProcessor_Atomic *processor =
      new RGWPutObjProcessor_Atomic(obj_ctx, s->bucket_info, s->bucket,
				    s->object.name, part_size, s->req_id,
				    s->bucket_info.versioning_enabled());
    processor->set_olh_epoch(olh_epoch);
    processor->set_version_id(version_id);
    return processor;
  }

  virtual int get_params() {
    struct req_state* s = get_state();
    RGWAccessControlPolicy_S3 s3policy(s->cct);
    /* we don't have (any) headers, so just create canned ACLs */
    int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
    policy = s3policy;
    return ret;
  }

  virtual int get_data(buffer::list& _bl) {
    /* XXX for now, use sharing semantics */
    uint32_t len = data.length();
    _bl.claim(data);
    bytes_written += len;
    return len;
  }

  void put_data(off_t off, buffer::list& _bl) {
    ofs = off;
    data.claim(_bl);
  }

  virtual int exec_start();
  virtual int exec_continue();
  virtual int exec_finish();

  virtual void send_response() {}

  virtual int verify_params() {
    return 0;
  }
}; /* RGWWriteRequest */

/*
  copy object
*/
class RGWCopyObjRequest : public RGWLibRequest,
			  public RGWCopyObj /* RGWOp */
{
public:
  RGWFileHandle* src_parent;
  RGWFileHandle* dst_parent;
  const std::string& src_name;
  const std::string& dst_name;

  RGWCopyObjRequest(CephContext* _cct, RGWUserInfo *_user,
		    RGWFileHandle* _src_parent, RGWFileHandle* _dst_parent,
		    const std::string& _src_name, const std::string& _dst_name)
    : RGWLibRequest(_cct, _user), src_parent(_src_parent),
      dst_parent(_dst_parent), src_name(_src_name), dst_name(_dst_name) {
    op = this;
  }

  virtual bool only_bucket() { return true; }

  virtual int op_init() {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED

    return 0;
  }

  virtual int header_init() {

    struct req_state* s = get_state();
    s->info.method = "PUT"; // XXX check
    s->op = OP_PUT;

    src_bucket_name = src_parent->bucket_name();
    // need s->src_bucket_name?
    src_object.name = src_parent->format_child_name(src_name);
    // need s->src_object?

    dest_bucket_name = dst_parent->bucket_name();
    // need s->bucket.name?
    dest_object = dst_parent->format_child_name(dst_name);
    // need s->object_name?

    if (! valid_s3_object_name(dest_object))
      return -ERR_INVALID_OBJECT_NAME;

#if 0 /* XXX needed? */
    s->relative_uri = uri;
    s->info.request_uri = uri; // XXX
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */
#endif

    // woo
    s->user = user;

    return 0;
  }

  virtual int get_params() {
    struct req_state* s = get_state();
    RGWAccessControlPolicy_S3 s3policy(s->cct);
    /* we don't have (any) headers, so just create canned ACLs */
    int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
    dest_policy = s3policy;
    return ret;
  }

  virtual void send_response() {}
  virtual void send_partial_response(off_t ofs) {}

}; /* RGWCopyObjRequest */


} /* namespace rgw */

#endif /* RGW_FILE_H */
