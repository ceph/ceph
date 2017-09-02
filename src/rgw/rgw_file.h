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
#include <boost/optional.hpp>
#include "xxhash.h"
#include "include/buffer.h"
#include "common/cohort_lru.h"
#include "common/ceph_timer.h"
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

  static inline bool operator <(const struct timespec& lhs,
				const struct timespec& rhs) {
    if (lhs.tv_sec == rhs.tv_sec)
      return lhs.tv_nsec < rhs.tv_nsec;
    else
      return lhs.tv_sec < rhs.tv_sec;
  }

  static inline bool operator ==(const struct timespec& lhs,
				 const struct timespec& rhs) {
    return ((lhs.tv_sec == rhs.tv_sec) &&
	    (lhs.tv_nsec == rhs.tv_nsec));
  }

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

    void encode(buffer::list& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(fh_hk.bucket, bl);
      ::encode(fh_hk.object, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      ::decode(fh_hk.bucket, bl);
      ::decode(fh_hk.object, bl);
      DECODE_FINISH(bl);
    }
  }; /* fh_key */

  WRITE_CLASS_ENCODER(fh_key);

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

    /* TODO: keeping just the last marker is sufficient for
     * nfs-ganesha 2.4.5; in the near future, nfs-ganesha will
     * be able to hint the name of the next dirent required,
     * from which we can directly synthesize a RADOS marker.
     * using marker_cache_t = flat_map<uint64_t, rgw_obj_key>;
     */

    struct State {
      uint64_t dev;
      size_t size;
      uint64_t nlink;
      uint32_t owner_uid; /* XXX need Unix attr */
      uint32_t owner_gid; /* XXX need Unix attr */
      mode_t unix_mode;
      struct timespec ctime;
      struct timespec mtime;
      struct timespec atime;
      State() : dev(0), size(0), nlink(1), owner_uid(0), owner_gid(0),
		ctime{0,0}, mtime{0,0}, atime{0,0} {}
    } state;

    struct file {
      RGWWriteRequest* write_req;
      file() : write_req(nullptr) {}
      ~file();
    };

    struct directory {

      static constexpr uint32_t FLAG_NONE =     0x0000;

      uint32_t flags;
      rgw_obj_key last_marker;
      struct timespec last_readdir;

      directory() : flags(FLAG_NONE), last_readdir{0,0} {}
    };

    void clear_state();

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
    static constexpr uint32_t FLAG_UNLINK_THIS = 0x0100;
    static constexpr uint32_t FLAG_LOCKED = 0x0200;
    static constexpr uint32_t FLAG_STATELESS_OPEN = 0x0400;
    static constexpr uint32_t FLAG_EXACT_MATCH = 0x0800;

#define CREATE_FLAGS(x) \
    ((x) & ~(RGWFileHandle::FLAG_CREATE|RGWFileHandle::FLAG_LOCK))

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
	state.unix_mode = RGW_RWXMODE|S_IFDIR;
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

      /* stat */
      state.dev = fs_inst;

      switch (fh.fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
	state.unix_mode = RGW_RWXMODE|S_IFDIR;
	break;
      case RGW_FS_TYPE_FILE:
	state.unix_mode = RGW_RWMODE|S_IFREG;
      default:
	break;
      }

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

    uint32_t get_owner_uid() const { return state.owner_uid; }
    uint32_t get_owner_gid() const { return state.owner_gid; }

    struct timespec get_ctime() const { return state.ctime; }
    struct timespec get_mtime() const { return state.mtime; }

    void create_stat(struct stat* st, uint32_t mask) {
      if (mask & RGW_SETATTR_UID)
	state.owner_uid = st->st_uid;

      if (mask & RGW_SETATTR_GID)
	state.owner_gid = st->st_gid;

      if (mask & RGW_SETATTR_MODE)  {
	switch (fh.fh_type) {
	case RGW_FS_TYPE_DIRECTORY:
	  state.unix_mode = st->st_mode|S_IFDIR;
	  break;
	case RGW_FS_TYPE_FILE:
	  state.unix_mode = st->st_mode|S_IFREG;
      default:
	break;
	}
      }

      if (mask & RGW_SETATTR_ATIME)
	state.atime = st->st_atim;
      if (mask & RGW_SETATTR_MTIME)
	state.mtime = st->st_mtim;
      if (mask & RGW_SETATTR_CTIME)
	state.ctime = st->st_ctim;
    }

    int stat(struct stat* st) {
      /* partial Unix attrs */
      memset(st, 0, sizeof(struct stat));
      st->st_dev = state.dev;
      st->st_ino = fh.fh_hk.object; // XXX

      st->st_uid = state.owner_uid;
      st->st_gid = state.owner_gid;

      st->st_mode = state.unix_mode;

      st->st_atim = state.atime;
      st->st_mtim = state.mtime;
      st->st_ctim = state.ctime;

      switch (fh.fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
	st->st_nlink = state.nlink;
	break;
      case RGW_FS_TYPE_FILE:
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

    std::string full_object_name(bool omit_bucket = false) const {
      std::string path;
      std::vector<const std::string*> segments;
      int reserve = 0;
      const RGWFileHandle* tfh = this;
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

    inline std::string relative_object_name() const {
      return full_object_name(true /* omit_bucket */);
    }

    inline std::string format_child_name(const std::string& cbasename) const {
      std::string child_name{relative_object_name()};
      if ((child_name.size() > 0) &&
	  (child_name.back() != '/'))
	child_name += "/";
      child_name += cbasename;
      return child_name;
    }

    inline std::string make_key_name(const char *name) const {
      std::string key_name{full_object_name()};
      if (key_name.length() > 0)
	key_name += "/";
      key_name += name;
      return key_name;
    }

    fh_key make_fhk(const std::string& name) const {
      if (depth <= 1)
	return fh_key(fhk.fh_hk.object, name.c_str());
      else {
	std::string key_name = make_key_name(name.c_str());
	return fh_key(fhk.fh_hk.bucket, key_name.c_str());
      }
    }

    void add_marker(uint64_t off, const rgw_obj_key& marker,
		    uint8_t obj_type) {
      using std::get;
      directory* d = get<directory>(&variant_type);
      if (d) {
	unique_lock guard(mtx);
	d->last_marker = marker;
      }
    }

    const rgw_obj_key* find_marker(uint64_t off) const {
      using std::get;
      if (off > 0) {
	const directory* d = get<directory>(&variant_type);
	if (d ) {
	  return &d->last_marker;
	}
      }
      return nullptr;
    }

    bool is_open() const { return flags & FLAG_OPEN; }
    bool is_root() const { return flags & FLAG_ROOT; }
    bool is_bucket() const { return flags & FLAG_BUCKET; }
    bool is_object() const { return !is_bucket(); }
    bool is_file() const { return (fh.fh_type == RGW_FS_TYPE_FILE); }
    bool is_dir() const { return (fh.fh_type == RGW_FS_TYPE_DIRECTORY); }
    bool creating() const { return flags & FLAG_CREATING; }
    bool deleted() const { return flags & FLAG_DELETED; }
    bool stateless_open() const { return flags & FLAG_STATELESS_OPEN; }
    bool has_children() const;

    int open(uint32_t gsh_flags) {
      lock_guard guard(mtx);
      if (! (flags & FLAG_OPEN)) {
	if (gsh_flags & RGW_OPEN_FLAG_V3) {
	  flags |= FLAG_STATELESS_OPEN;
	}
	flags |= FLAG_OPEN;
	return 0;
      }
      return -EPERM;
    }

    int readdir(rgw_readdir_cb rcb, void *cb_arg, uint64_t *offset, bool *eof,
		uint32_t flags);
    int write(uint64_t off, size_t len, size_t *nbytes, void *buffer);

    int commit(uint64_t offset, uint64_t length, uint32_t flags) {
      /* NFS3 and NFSv4 COMMIT implementation
       * the current atomic update strategy doesn't actually permit
       * clients to read-stable until either CLOSE (NFSv4+) or the
       * expiration of the active write timer (NFS3).  In the
       * interim, the client may send an arbitrary number of COMMIT
       * operations which must return a success result */
      return 0;
    }

    int write_finish(uint32_t flags = FLAG_NONE);
    int close();

    void open_for_create() {
      lock_guard guard(mtx);
      flags |= FLAG_CREATING;
    }

    void clear_creating() {
      lock_guard guard(mtx);
      flags &= ~FLAG_CREATING;
    }

    void inc_nlink(const uint64_t n) {
      state.nlink += n;
    }

    void set_nlink(const uint64_t n) {
      state.nlink = n;
    }

    void set_size(const size_t size) {
      state.size = size;
    }

    void set_times(real_time t) {
      state.ctime = real_clock::to_timespec(t);
      state.mtime = state.ctime;
      state.atime = state.ctime;
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

    void encode(buffer::list& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(uint32_t(fh.fh_type), bl);
      ::encode(state.dev, bl);
      ::encode(state.size, bl);
      ::encode(state.nlink, bl);
      ::encode(state.owner_uid, bl);
      ::encode(state.owner_gid, bl);
      ::encode(state.unix_mode, bl);
      for (const auto& t : { state.ctime, state.mtime, state.atime }) {
	::encode(real_clock::from_timespec(t), bl);
      }
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      uint32_t fh_type;
      ::decode(fh_type, bl);
      assert(fh.fh_type == fh_type);
      ::decode(state.dev, bl);
      ::decode(state.size, bl);
      ::decode(state.nlink, bl);
      ::decode(state.owner_uid, bl);
      ::decode(state.owner_gid, bl);
      ::decode(state.unix_mode, bl);
      ceph::real_time enc_time;
      for (auto t : { &(state.ctime), &(state.mtime), &(state.atime) }) {
	::decode(enc_time, bl);
	*t = real_clock::to_timespec(enc_time);
      }
      DECODE_FINISH(bl);
    }

    void encode_attrs(ceph::buffer::list& ux_key1,
		      ceph::buffer::list& ux_attrs1);

    void decode_attrs(const ceph::buffer::list* ux_key1,
		      const ceph::buffer::list* ux_attrs1);

    void invalidate();

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

    virtual ~RGWFileHandle();

    friend std::ostream& operator<<(std::ostream &os,
				    RGWFileHandle const &rgw_fh);

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

  WRITE_CLASS_ENCODER(RGWFileHandle);

  static inline RGWFileHandle* get_rgwfh(struct rgw_file_handle* fh) {
    return static_cast<RGWFileHandle*>(fh->fh_private);
  }

  static inline enum rgw_fh_type fh_type_of(uint32_t flags) {
    enum rgw_fh_type fh_type;
    switch(flags & RGW_LOOKUP_TYPE_FLAGS)
    {
    case RGW_LOOKUP_FLAG_DIR:
      fh_type = RGW_FS_TYPE_DIRECTORY;
      break;
    case RGW_LOOKUP_FLAG_FILE:
      fh_type = RGW_FS_TYPE_FILE;
      break;
    default:
      fh_type = RGW_FS_TYPE_NIL;
    };
    return fh_type;
  }

  typedef std::tuple<RGWFileHandle*, uint32_t> LookupFHResult;
  typedef std::tuple<RGWFileHandle*, int> MkObjResult;

  class RGWLibFS
  {
    CephContext* cct;
    struct rgw_fs fs;
    RGWFileHandle root_fh;
    rgw_fh_callback_t invalidate_cb;
    void *invalidate_arg;
    bool shutdown;

    mutable std::atomic<uint64_t> refcnt;

    RGWFileHandle::FHCache fh_cache;
    RGWFileHandle::FhLRU fh_lru;
    
    std::string uid; // should match user.user_id, iiuc

    RGWUserInfo user;
    RGWAccessKey key; // XXXX acc_key

    static atomic<uint32_t> fs_inst_counter;

    static uint32_t write_completion_interval_s;
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

    friend std::ostream& operator<<(std::ostream &os,
				    RGWLibFS::event const &ev);

    using event_vector = /* boost::small_vector<event, 16> */
      std::vector<event>;

    struct WriteCompletion
    {
      RGWFileHandle& rgw_fh;

      WriteCompletion(RGWFileHandle& _fh) : rgw_fh(_fh) {
	rgw_fh.get_fs()->ref(&rgw_fh);
      }

      void operator()() {
	rgw_fh.close(); /* will finish in-progress write */
	rgw_fh.get_fs()->unref(&rgw_fh);
      }
    };

    static ceph::timer<ceph::mono_clock> write_timer;

    struct State {
      std::mutex mtx;
      std::atomic<uint32_t> flags;
      std::deque<event> events;

      State() : flags(0) {}

      void push_event(const event& ev) {
	events.push_back(ev);
      }
    } state;

    uint32_t new_inst() {
      return ++fs_inst_counter;
    }

    friend class RGWFileHandle;
    friend class RGWLibProcess;

  public:

    static constexpr uint32_t FLAG_NONE =      0x0000;
    static constexpr uint32_t FLAG_CLOSED =    0x0001;

    struct BucketStats {
      size_t size;
      size_t size_rounded;
      real_time creation_time;
      uint64_t num_entries;
    };

    RGWLibFS(CephContext* _cct, const char *_uid, const char *_user_id,
	    const char* _key)
      : cct(_cct), root_fh(this, new_inst()), invalidate_cb(nullptr),
	invalidate_arg(nullptr), shutdown(false), refcnt(1),
	fh_cache(cct->_conf->rgw_nfs_fhcache_partitions,
		 cct->_conf->rgw_nfs_fhcache_size),
	fh_lru(cct->_conf->rgw_nfs_lru_lanes,
	       cct->_conf->rgw_nfs_lru_lane_hiwat),
	uid(_uid), key(_user_id, _key) {

      /* no bucket may be named rgw_fs_inst-(.*) */
      fsid = RGWFileHandle::root_name + "rgw_fs_inst-" +
	std::to_string(get_inst());

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

    void stop() { shutdown = true; }

    void release_evict(RGWFileHandle* fh) {
      /* remove from cache, releases sentinel ref */
      fh_cache.remove(fh->fh.fh_hk.object, fh,
		      RGWFileHandle::FHCache::FLAG_LOCK);
      /* release call-path ref */
      (void) fh_lru.unref(fh, cohort::lru::FLAG_NONE);
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
	RGWToken token;
	/* boost filters and/or string_ref may throw on invalid input */
	try {
	  token = rgw::from_base64(key.id);
	} catch(...) {
	  token = std::string("");
	}
	if (token.valid() && (ldh->auth(token.id, token.key) == 0)) {
	  /* try to store user if it doesn't already exist */
	  if (rgw_get_user_info_by_uid(store, token.id, user) < 0) {
	    int ret = rgw_store_user_info(store, user, NULL, NULL, real_time(),
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

    int register_invalidate(rgw_fh_callback_t cb, void *arg, uint32_t flags) {
      invalidate_cb = cb;
      invalidate_arg = arg;
      return 0;
    }

    /* find RGWFileHandle by id  */
    LookupFHResult lookup_fh(const fh_key& fhk,
			     const uint32_t flags = RGWFileHandle::FLAG_NONE) {
      using std::get;

      // cast int32_t(RGWFileHandle::FLAG_NONE) due to strictness of Clang 
      // the cast transfers a lvalue into a rvalue  in the ctor
      // check the commit message for the full details
      LookupFHResult fhr { nullptr, uint32_t(RGWFileHandle::FLAG_NONE) };

      RGWFileHandle::FHCache::Latch lat;
      bool fh_locked = flags & RGWFileHandle::FLAG_LOCKED;

    retry:
      RGWFileHandle* fh =
	fh_cache.find_latch(fhk.fh_hk.object /* partition selector*/,
			    fhk /* key */, lat /* serializer */,
			    RGWFileHandle::FHCache::FLAG_LOCK);
      /* LATCHED */
      if (fh) {
	if (likely(! fh_locked))
	    fh->mtx.lock(); // XXX !RAII because may-return-LOCKED
	/* need initial ref from LRU (fast path) */
	if (! fh_lru.ref(fh, cohort::lru::FLAG_INITIAL)) {
	  lat.lock->unlock();
	  if (likely(! fh_locked))
	    fh->mtx.unlock();
	  goto retry; /* !LATCHED */
	}
	/* LATCHED, LOCKED */
	if (! (flags & RGWFileHandle::FLAG_LOCK))
	  fh->mtx.unlock(); /* ! LOCKED */
      }
      lat.lock->unlock(); /* !LATCHED */
      get<0>(fhr) = fh;
      if (fh) {
	    lsubdout(get_context(), rgw, 17)
	      << __func__ << " 1 " << *fh
	      << dendl;
      }
      return fhr;
    } /* lookup_fh(const fh_key&) */

    /* find or create an RGWFileHandle */
    LookupFHResult lookup_fh(RGWFileHandle* parent, const char *name,
			     const uint32_t flags = RGWFileHandle::FLAG_NONE) {
      using std::get;

      LookupFHResult fhr { nullptr, RGWFileHandle::FLAG_NONE };

      /* mount is stale? */
      if (state.flags & FLAG_CLOSED)
	return fhr;

      RGWFileHandle::FHCache::Latch lat;
      bool fh_locked = flags & RGWFileHandle::FLAG_LOCKED;

      std::string obj_name{name};
      std::string key_name{parent->make_key_name(name)};

      lsubdout(get_context(), rgw, 10)
	<< __func__ << " lookup called on "
	<< parent->object_name() << " for " << key_name
	<< " (" << obj_name << ")"
	<< dendl;

      fh_key fhk = parent->make_fhk(key_name);

    retry:
      RGWFileHandle* fh =
	fh_cache.find_latch(fhk.fh_hk.object /* partition selector*/,
			    fhk /* key */, lat /* serializer */,
			    RGWFileHandle::FHCache::FLAG_LOCK);
      /* LATCHED */
      if (fh) {
	if (likely(! fh_locked))
	  fh->mtx.lock(); // XXX !RAII because may-return-LOCKED
	if (fh->flags & RGWFileHandle::FLAG_DELETED) {
	  /* for now, delay briefly and retry */
	  lat.lock->unlock();
	  if (likely(! fh_locked))
	    fh->mtx.unlock();
	  std::this_thread::sleep_for(std::chrono::milliseconds(20));
	  goto retry; /* !LATCHED */
	}
	/* need initial ref from LRU (fast path) */
	if (! fh_lru.ref(fh, cohort::lru::FLAG_INITIAL)) {
	  lat.lock->unlock();
	  if (likely(! fh_locked))
	    fh->mtx.unlock();
	  goto retry; /* !LATCHED */
	}
	/* LATCHED, LOCKED */
	if (! (flags & RGWFileHandle::FLAG_LOCK))
	  if (likely(! fh_locked))
	    fh->mtx.unlock(); /* ! LOCKED */
      } else {
	/* make or re-use handle */
	RGWFileHandle::Factory prototype(this, get_inst(), parent, fhk,
					 obj_name, CREATE_FLAGS(flags));
	fh = static_cast<RGWFileHandle*>(
	  fh_lru.insert(&prototype,
			cohort::lru::Edge::MRU,
			cohort::lru::FLAG_INITIAL));
	if (fh) {
	  /* lock fh (LATCHED) */
	  if (flags & RGWFileHandle::FLAG_LOCK)
	    fh->mtx.lock();
	  /* inserts, releasing latch */
	  fh_cache.insert_latched(fh, lat, RGWFileHandle::FHCache::FLAG_UNLOCK);
	  get<1>(fhr) |= RGWFileHandle::FLAG_CREATE;
	  /* ref parent (non-initial ref cannot fail on valid object) */
	  if (! parent->is_root()) {
	    (void) fh_lru.ref(parent, cohort::lru::FLAG_NONE);
	  }
	  goto out; /* !LATCHED */
	} else {
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
      }
      lat.lock->unlock(); /* !LATCHED */
    out:
      get<0>(fhr) = fh;
      if (fh) {
	    lsubdout(get_context(), rgw, 17)
	      << __func__ << " 2 " << *fh
	      << dendl;
      }
      return fhr;
    } /*  lookup_fh(RGWFileHandle*, const char *, const uint32_t) */

    inline void unref(RGWFileHandle* fh) {
      if (likely(! fh->is_root())) {
	(void) fh_lru.unref(fh, cohort::lru::FLAG_NONE);
      }
    }

    inline RGWFileHandle* ref(RGWFileHandle* fh) {
      if (likely(! fh->is_root())) {
	fh_lru.ref(fh, cohort::lru::FLAG_NONE);
      }
      return fh;
    }

    int getattr(RGWFileHandle* rgw_fh, struct stat* st);

    int setattr(RGWFileHandle* rgw_fh, struct stat* st, uint32_t mask,
		uint32_t flags);

    LookupFHResult stat_bucket(RGWFileHandle* parent, const char *path,
			       RGWLibFS::BucketStats& bs,
			       uint32_t flags);

    LookupFHResult stat_leaf(RGWFileHandle* parent, const char *path,
			     enum rgw_fh_type type = RGW_FS_TYPE_NIL,
			     uint32_t flags = RGWFileHandle::FLAG_NONE);

    int read(RGWFileHandle* rgw_fh, uint64_t offset, size_t length,
	     size_t* bytes_read, void* buffer, uint32_t flags);

    int rename(RGWFileHandle* old_fh, RGWFileHandle* new_fh,
	       const char *old_name, const char *new_name);

    MkObjResult create(RGWFileHandle* parent, const char *name, struct stat *st,
		      uint32_t mask, uint32_t flags);

    MkObjResult mkdir(RGWFileHandle* parent, const char *name, struct stat *st,
		      uint32_t mask, uint32_t flags);
    MkObjResult mkdir2(RGWFileHandle* parent, const char *name, struct stat *st,
		      uint32_t mask, uint32_t flags);

    int unlink(RGWFileHandle* rgw_fh, const char *name,
	       uint32_t flags = FLAG_NONE);

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
	fh->mtx.unlock(); /* !LOCKED */
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	goto retry; /* !LATCHED */
      }
      if (! fh_lru.ref(fh, cohort::lru::FLAG_INITIAL)) {
	lat.lock->unlock();
	fh->mtx.unlock();
	goto retry; /* !LATCHED */
      }
      /* LATCHED */
      fh->mtx.unlock(); /* !LOCKED */
    out:
      lat.lock->unlock(); /* !LATCHED */

      /* special case:  lookup root_fh */
      if (! fh) {
	if (unlikely(fh_hk == root_fh.fh.fh_hk)) {
	  fh = &root_fh;
	  ref(fh);
	}
      }

      return fh;
    }

    CephContext* get_context() {
      return cct;
    }

    struct rgw_fs* get_fs() { return &fs; }

    uint32_t get_inst() { return root_fh.state.dev; }

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
  uint32_t d_count;

  RGWListBucketsRequest(CephContext* _cct, RGWUserInfo *_user,
			RGWFileHandle* _rgw_fh, rgw_readdir_cb _rcb,
			void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), offset(_offset),
      cb_arg(_cb_arg), rcb(_rcb), ix(0), d_count(0) {
    const auto& mk = rgw_fh->find_marker(*offset);
    if (mk) {
      marker = mk->name;
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
      if (! this->operator()(ent.bucket.name, marker)) {
	/* caller cannot accept more */
	lsubdout(cct, rgw, 5) << "ListBuckets rcb failed"
			      << " dirent=" << ent.bucket.name
			      << " call count=" << ix
			      << dendl;
	return;
      }
      ++ix;
    }
  } /* send_response_data */

  virtual void send_response_end() {
    // do nothing
  }

  int operator()(const boost::string_ref& name,
		 const boost::string_ref& marker) {
    uint64_t off = XXH64(name.data(), name.length(), fh_key::seed);
    *offset = off;
    /* update traversal cache */
    rgw_fh->add_marker(off, rgw_obj_key{marker.data(), ""},
		       RGW_FS_TYPE_DIRECTORY);
    ++d_count;
    return rcb(name.data(), cb_arg, off, RGW_LOOKUP_FLAG_DIR);
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
  uint32_t d_count;

  RGWReaddirRequest(CephContext* _cct, RGWUserInfo *_user,
		    RGWFileHandle* _rgw_fh, rgw_readdir_cb _rcb,
		    void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), offset(_offset),
      cb_arg(_cb_arg), rcb(_rcb), ix(0), d_count(0) {
    const auto& mk = rgw_fh->find_marker(*offset);
    if (mk) {
      marker = *mk;
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

  int operator()(const boost::string_ref name, const rgw_obj_key& marker,
		uint8_t type) {

    assert(name.length() > 0); // XXX

    /* hash offset of name in parent (short name) for NFS readdir cookie */
    uint64_t off = XXH64(name.data(), name.length(), fh_key::seed);
    *offset = off;
    /* update traversal cache */
    rgw_fh->add_marker(off, marker, type);
    ++d_count;
    return rcb(name.data(), cb_arg, off,
	       (type == RGW_FS_TYPE_DIRECTORY) ?
	       RGW_LOOKUP_FLAG_DIR :
	       RGW_LOOKUP_FLAG_FILE);
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

      if(! this->operator()(sref, next_marker, RGW_FS_TYPE_FILE)) {
	/* caller cannot accept more */
	lsubdout(cct, rgw, 5) << "readdir rcb failed"
			      << " dirent=" << sref.data()
			      << " call count=" << ix
			      << dendl;
	return;
      }
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

      this->operator()(sref, next_marker, RGW_FS_TYPE_DIRECTORY);
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
  dir has-children predicate (bucket objects)
*/

class RGWRMdirCheck : public RGWLibRequest,
		      public RGWListBucket /* RGWOp */
{
public:
  const RGWFileHandle* rgw_fh;
  bool valid;
  bool has_children;

  RGWRMdirCheck (CephContext* _cct, RGWUserInfo *_user,
		 const RGWFileHandle* _rgw_fh)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), valid(false),
      has_children(false) {
    default_max = 2;
    op = this;
  }

  virtual bool only_bucket() override { return false; }

  virtual int op_init() override {
    // assign store, s, and dialect_handler
    RGWObjectCtx* rados_ctx
      = static_cast<RGWObjectCtx*>(get_state()->obj_ctx);
    // framework promises to call op_init after parent init
    assert(rados_ctx);
    RGWOp::init(rados_ctx->store, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() override {
    struct req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    std::string uri = "/" + rgw_fh->bucket_name() + "/";
    s->relative_uri = uri;
    s->info.request_uri = uri;
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    s->user = user;

    prefix = rgw_fh->relative_object_name();
    if (prefix.length() > 0)
      prefix += "/";
    delimiter = '/';

    return 0;
  }

  virtual int get_params() override {
    max = default_max;
    return 0;
  }

  virtual void send_response() override {
    valid = true;
    if ((objs.size() > 1) ||
	(! objs.empty() &&
	 (objs.front().key.name != prefix))) {
      has_children = true;
      return;
    }
    for (auto& iter : common_prefixes) {
      /* readdir never produces a name for this case */
      if (iter.first == "/")
	continue;
      has_children = true;
      break;
    }
  }

  virtual void send_versioned_response() {
    send_response();
  }

}; /* RGWRMdirCheck */

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

  real_time get_mtime() const {
    return lastmod;
  }

  /* attributes */
  uint64_t get_size() { return _size; }
  real_time ctime() { return mod_time; } // XXX
  real_time mtime() { return mod_time; }
  std::map<string, bufferlist>& get_attrs() { return attrs; }

  buffer::list* get_attr(const std::string& k) {
    auto iter = attrs.find(k);
    return (iter != attrs.end()) ? &(iter->second) : nullptr;
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
    /* XXX save attrs? */
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
  std::map<std::string, buffer::list> attrs;
  RGWLibFS::BucketStats& bs;

  RGWStatBucketRequest(CephContext* _cct, RGWUserInfo *_user,
		       const std::string& _path,
		       RGWLibFS::BucketStats& _stats)
    : RGWLibRequest(_cct, _user), bs(_stats) {
    uri = "/" + _path;
    op = this;
  }

  buffer::list* get_attr(const std::string& k) {
    auto iter = attrs.find(k);
    return (iter != attrs.end()) ? &(iter->second) : nullptr;
  }

  real_time get_ctime() const {
    return bucket.creation_time;
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

  virtual int get_params() {
    return 0;
  }

  virtual void send_response() {
    bucket.creation_time = get_state()->bucket_info.creation_time;
    bs.size = bucket.size;
    bs.size_rounded = bucket.size_rounded;
    bs.creation_time = bucket.creation_time;
    bs.num_entries = bucket.count;
    std::swap(attrs, get_state()->bucket_attrs);
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
  bool exact_matched;

  RGWStatLeafRequest(CephContext* _cct, RGWUserInfo *_user,
		     RGWFileHandle* _rgw_fh, const std::string& _path)
    : RGWLibRequest(_cct, _user), rgw_fh(_rgw_fh), path(_path),
      matched(false), is_dir(false), exact_matched(false) {
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
			     << " target = " << path << ""
			     << dendl;
      /* XXX is there a missing match-dir case (trailing '/')? */
      matched = true;
      if (name == path)
	exact_matched = true;
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
			     << " target = " << path << ""
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
  uint64_t timer_id;
  MD5 hash;
  off_t real_ofs;
  size_t bytes_written;
  bool multipart;
  bool eio;

  RGWWriteRequest(CephContext* _cct, RGWUserInfo *_user, RGWFileHandle* _fh,
		  const std::string& _bname, const std::string& _oname)
    : RGWLibContinuedReq(_cct, _user), bucket_name(_bname), obj_name(_oname),
      rgw_fh(_fh), processor(nullptr), real_ofs(0), bytes_written(0),
      multipart(false), eio(false) {

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
    if (off != real_ofs) {
      eio = true;
    }
    data.claim(_bl);
    real_ofs += data.length();
    ofs = off; /* consumed in exec_continue() */
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
    /* all requests have this */
    op = this;

    /* allow this request to replace selected attrs */
    attrs_mod = RGWRados::ATTRSMOD_MERGE;
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

    /* XXX and fixup key attr (could optimize w/string ref and
     * dest_object) */
    buffer::list ux_key;
    std::string key_name{dst_parent->make_key_name(dst_name.c_str())};
    fh_key fhk = dst_parent->make_fhk(key_name);
    rgw::encode(fhk, ux_key);
    emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));

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

class RGWSetAttrsRequest : public RGWLibRequest,
			   public RGWSetAttrs /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;

  RGWSetAttrsRequest(CephContext* _cct, RGWUserInfo *_user,
		     const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, _user), bucket_name(_bname), obj_name(_oname) {
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

  virtual int get_params() {
    return 0;
  }

  virtual void send_response() {}

}; /* RGWSetAttrsRequest */

} /* namespace rgw */

#endif /* RGW_FILE_H */
