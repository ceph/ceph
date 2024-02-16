// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/rados/rgw_file.h"

/* internal header */
#include <string.h>
#include <string_view>
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
#include "rgw_putobj_processor.h"
#include "rgw_aio_throttle.h"
#include "rgw_compression.h"
#include "rgw_perf_counters.h"


/* XXX
 * ASSERT_H somehow not defined after all the above (which bring
 * in common/debug.h [e.g., dout])
 */
#include "include/ceph_assert.h"


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

  inline bool operator <(const struct timespec& lhs,
			 const struct timespec& rhs) {
    if (lhs.tv_sec == rhs.tv_sec)
      return lhs.tv_nsec < rhs.tv_nsec;
    else
      return lhs.tv_sec < rhs.tv_sec;
  }

  inline bool operator ==(const struct timespec& lhs,
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
    rgw_fh_hk fh_hk {};
    uint32_t version;

    static constexpr uint64_t seed = 8675309;

    fh_key() : version(0) {}

    fh_key(const rgw_fh_hk& _hk)
      : fh_hk(_hk), version(0) {
      // nothing
    }

    fh_key(const uint64_t bk, const uint64_t ok)
      : version(0) {
      fh_hk.bucket = bk;
      fh_hk.object = ok;
    }

    fh_key(const uint64_t bk, const char *_o, const std::string& _t)
      : version(0) {
      fh_hk.bucket = bk;
      std::string to = _t + ":" + _o;
      fh_hk.object = XXH64(to.c_str(), to.length(), seed);
    }

    fh_key(const std::string& _b, const std::string& _o,
	   const std::string& _t /* tenant */)
      : version(0) {
      std::string tb = _t + ":" + _b;
      std::string to = _t + ":" + _o;
      fh_hk.bucket = XXH64(tb.c_str(), tb.length(), seed);
      fh_hk.object = XXH64(to.c_str(), to.length(), seed);
    }

    void encode(buffer::list& bl) const {
      ENCODE_START(2, 1, bl);
      encode(fh_hk.bucket, bl);
      encode(fh_hk.object, bl);
      encode((uint32_t)2, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(2, bl);
      decode(fh_hk.bucket, bl);
      decode(fh_hk.object, bl);
      if (struct_v >= 2) {
	decode(version, bl);
      }
      DECODE_FINISH(bl);
    }

    friend std::ostream& operator<<(std::ostream &os, fh_key const &fhk);

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

  typedef std::tuple<bool, bool> DecodeAttrsResult;

  class RGWFileHandle : public cohort::lru::Object
  {
    struct rgw_file_handle fh;
    std::mutex mtx;

    RGWLibFS* fs;
    RGWFileHandle* bucket;
    RGWFileHandle* parent;
    std::atomic_int64_t file_ondisk_version; // version of unix attrs, file only
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
      uint64_t size;
      uint64_t nlink;
      uint32_t owner_uid; /* XXX need Unix attr */
      uint32_t owner_gid; /* XXX need Unix attr */
      mode_t unix_mode;
      struct timespec ctime;
      struct timespec mtime;
      struct timespec atime;
      uint32_t version;
      State() : dev(0), size(0), nlink(1), owner_uid(0), owner_gid(0), unix_mode(0),
		ctime{0,0}, mtime{0,0}, atime{0,0}, version(0) {}
    } state;

    struct file {
      RGWWriteRequest* write_req;
      file() : write_req(nullptr) {}
      ~file();
    };

    // coverity[missing_lock:SUPPRESS]
    struct directory {

      static constexpr uint32_t FLAG_NONE =     0x0000;

      uint32_t flags;
      rgw_obj_key last_marker;
      struct timespec last_readdir;

      directory() : flags(FLAG_NONE), last_readdir{0,0} {}
    };

    void clear_state();
    void advance_mtime(uint32_t flags = FLAG_NONE);

    boost::variant<file, directory> variant_type;

    uint16_t depth;
    uint32_t flags;

    ceph::buffer::list etag;
    ceph::buffer::list acls;

  public:
    const static std::string root_name;

    static constexpr uint16_t MAX_DEPTH = 256;

    static constexpr uint32_t FLAG_NONE =    0x0000;
    static constexpr uint32_t FLAG_OPEN =    0x0001;
    static constexpr uint32_t FLAG_ROOT =    0x0002;
    static constexpr uint32_t FLAG_CREATE =  0x0004;
    static constexpr uint32_t FLAG_CREATING =  0x0008;
    static constexpr uint32_t FLAG_SYMBOLIC_LINK = 0x0009; // XXXX bug?
    static constexpr uint32_t FLAG_DIRECTORY = 0x0010;
    static constexpr uint32_t FLAG_BUCKET = 0x0020;
    static constexpr uint32_t FLAG_LOCK =   0x0040;
    static constexpr uint32_t FLAG_DELETED = 0x0080;
    static constexpr uint32_t FLAG_UNLINK_THIS = 0x0100;
    static constexpr uint32_t FLAG_LOCKED = 0x0200;
    static constexpr uint32_t FLAG_STATELESS_OPEN = 0x0400;
    static constexpr uint32_t FLAG_EXACT_MATCH = 0x0800;
    static constexpr uint32_t FLAG_MOUNT = 0x1000;
    static constexpr uint32_t FLAG_IN_CB = 0x2000;

#define CREATE_FLAGS(x) \
    ((x) & ~(RGWFileHandle::FLAG_CREATE|RGWFileHandle::FLAG_LOCK))

    static constexpr uint32_t RCB_MASK = \
      RGW_SETATTR_MTIME|RGW_SETATTR_CTIME|RGW_SETATTR_ATIME|RGW_SETATTR_SIZE;

    friend class RGWLibFS;

  private:
    explicit RGWFileHandle(RGWLibFS* _fs)
      : fs(_fs), bucket(nullptr), parent(nullptr), file_ondisk_version(-1),
	variant_type{directory()}, depth(0), flags(FLAG_NONE)
      {
        fh.fh_hk.bucket = 0;
        fh.fh_hk.object = 0;
	/* root */
	fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	variant_type = directory();
	/* stat */
	state.unix_mode = RGW_RWXMODE|S_IFDIR;
	/* pointer to self */
	fh.fh_private = this;
      }

    uint64_t init_fsid(std::string& uid) {
      return XXH64(uid.c_str(), uid.length(), fh_key::seed);
    }

    void init_rootfs(std::string& fsid, const std::string& object_name,
                     bool is_bucket) {
      /* fh_key */
      fh.fh_hk.bucket = XXH64(fsid.c_str(), fsid.length(), fh_key::seed);
      fh.fh_hk.object = XXH64(object_name.c_str(), object_name.length(),
			      fh_key::seed);
      fhk = fh.fh_hk;
      name = object_name;

      state.dev = init_fsid(fsid);

      if (is_bucket) {
        flags |= RGWFileHandle::FLAG_BUCKET | RGWFileHandle::FLAG_MOUNT;
        bucket = this;
        depth = 1;
      } else {
        flags |= RGWFileHandle::FLAG_ROOT | RGWFileHandle::FLAG_MOUNT;
      }
    }

    void encode(buffer::list& bl) const {
      ENCODE_START(3, 1, bl);
      encode(uint32_t(fh.fh_type), bl);
      encode(state.dev, bl);
      encode(state.size, bl);
      encode(state.nlink, bl);
      encode(state.owner_uid, bl);
      encode(state.owner_gid, bl);
      encode(state.unix_mode, bl);
      for (const auto& t : { state.ctime, state.mtime, state.atime }) {
	encode(real_clock::from_timespec(t), bl);
      }
      encode((uint32_t)2, bl);
      encode(file_ondisk_version.load(), bl);
      ENCODE_FINISH(bl);
    }

    //XXX: RGWFileHandle::decode method can only be called from
    //	   RGWFileHandle::decode_attrs, otherwise the file_ondisk_version
    //	   fied would be contaminated
    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(3, bl);
      uint32_t fh_type;
      decode(fh_type, bl);
      if ((fh.fh_type != fh_type) &&
	 (fh_type == RGW_FS_TYPE_SYMBOLIC_LINK))
        fh.fh_type = RGW_FS_TYPE_SYMBOLIC_LINK;
      decode(state.dev, bl);
      decode(state.size, bl);
      decode(state.nlink, bl);
      decode(state.owner_uid, bl);
      decode(state.owner_gid, bl);
      decode(state.unix_mode, bl);
      ceph::real_time enc_time;
      for (auto t : { &(state.ctime), &(state.mtime), &(state.atime) }) {
	decode(enc_time, bl);
	*t = real_clock::to_timespec(enc_time);
      }
      if (struct_v >= 2) {
        decode(state.version, bl);
      }
      if (struct_v >= 3) {
	int64_t fov;
	decode(fov, bl);
	file_ondisk_version = fov;
      }
      DECODE_FINISH(bl);
    }

    friend void encode(const RGWFileHandle& c, ::ceph::buffer::list &bl, uint64_t features);
    friend void decode(RGWFileHandle &c, ::ceph::bufferlist::const_iterator &p);
  public:
    RGWFileHandle(RGWLibFS* _fs, RGWFileHandle* _parent,
		  const fh_key& _fhk, std::string& _name, uint32_t _flags)
      : fs(_fs), bucket(nullptr), parent(_parent), file_ondisk_version(-1),
	name(std::move(_name)), fhk(_fhk), flags(_flags) {

      if (parent->is_root()) {
	fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	variant_type = directory();
	flags |= FLAG_BUCKET;
      } else {
	bucket = parent->is_bucket() ? parent
	  : parent->bucket;
	if (flags & FLAG_DIRECTORY) {
	  fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	  variant_type = directory();
        } else if(flags & FLAG_SYMBOLIC_LINK) {
	  fh.fh_type = RGW_FS_TYPE_SYMBOLIC_LINK;
          variant_type = file();
        } else {
	  fh.fh_type = RGW_FS_TYPE_FILE;
	  variant_type = file();
	}
      }

      depth = parent->depth + 1;

      /* save constant fhk */
      fh.fh_hk = fhk.fh_hk; /* XXX redundant in fh_hk */

      /* inherits parent's fsid */
      state.dev = parent->state.dev;

      switch (fh.fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
	state.unix_mode = RGW_RWXMODE|S_IFDIR;
	/* virtual directories are always invalid */
	advance_mtime();
	break;
      case RGW_FS_TYPE_FILE:
	state.unix_mode = RGW_RWMODE|S_IFREG;
        break;
      case RGW_FS_TYPE_SYMBOLIC_LINK:
        state.unix_mode = RGW_RWMODE|S_IFLNK;
        break;
      default:
	break;
      }

      /* pointer to self */
      fh.fh_private = this;
    }

    const std::string& get_name() const {
      return name;
    }

    const fh_key& get_key() const {
      return fhk;
    }

    directory* get_directory() {
      return boost::get<directory>(&variant_type);
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

    const ceph::buffer::list& get_etag() const { return etag; }
    const ceph::buffer::list& get_acls() const { return acls; }

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
          break;
        case RGW_FS_TYPE_SYMBOLIC_LINK:
          state.unix_mode = st->st_mode|S_IFLNK;
          break;
      default:
	break;
	}
      }

      if (mask & RGW_SETATTR_ATIME)
	state.atime = st->st_atim;

      if (mask & RGW_SETATTR_MTIME) {
	if (fh.fh_type != RGW_FS_TYPE_DIRECTORY)
	  state.mtime = st->st_mtim;
      }

      if (mask & RGW_SETATTR_CTIME)
	state.ctime = st->st_ctim;
    }

    int stat(struct stat* st, uint32_t flags = FLAG_NONE) {
      /* partial Unix attrs */
      /* FIPS zeroization audit 20191115: this memset is not security
       * related. */
      memset(st, 0, sizeof(struct stat));
      st->st_dev = state.dev;
      st->st_ino = fh.fh_hk.object; // XXX

      st->st_uid = state.owner_uid;
      st->st_gid = state.owner_gid;

      st->st_mode = state.unix_mode;

      switch (fh.fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
	/* virtual directories are always invalid */
	advance_mtime(flags);
	st->st_nlink = state.nlink;
	break;
      case RGW_FS_TYPE_FILE:
	st->st_nlink = 1;
	st->st_blksize = 4096;
	st->st_size = state.size;
	st->st_blocks = (state.size) / 512;
        break;
      case RGW_FS_TYPE_SYMBOLIC_LINK:
	st->st_nlink = 1;
	st->st_blksize = 4096;
	st->st_size = state.size;
	st->st_blocks = (state.size) / 512;
        break;
      default:
	break;
      }

#ifdef HAVE_STAT_ST_MTIMESPEC_TV_NSEC
      st->st_atimespec = state.atime;
      st->st_mtimespec = state.mtime;
      st->st_ctimespec = state.ctime;
#else
      st->st_atim = state.atime;
      st->st_mtim = state.mtime;
      st->st_ctim = state.ctime;
#endif

      return 0;
    }

    const std::string& bucket_name() const {
      if (is_root())
	return root_name;
      if (is_bucket())
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
      int pos = 1;
      path.reserve(reserve);
      for (auto& s : boost::adaptors::reverse(segments)) {
	if (pos > 1) {
	  path += "/";
	} else {
	  if (!omit_bucket &&
	      ((path.length() == 0) || (path.front() != '/')))
	    path += "/";
	}
	path += *s;
	++pos;
      }
      return path;
    }

    inline std::string relative_object_name() const {
      return full_object_name(true /* omit_bucket */);
    }

    inline std::string relative_object_name2() {
      std::string rname = full_object_name(true /* omit_bucket */);
      if (is_dir()) {
	rname += "/";
      }
      return rname;
    }

    inline std::string format_child_name(const std::string& cbasename,
                                         bool is_dir) const {
      std::string child_name{relative_object_name()};
      if ((child_name.size() > 0) &&
	  (child_name.back() != '/'))
	child_name += "/";
      child_name += cbasename;
      if (is_dir)
	child_name += "/";
      return child_name;
    }

    inline std::string make_key_name(const char *name) const {
      std::string key_name{full_object_name()};
      if (key_name.length() > 0)
	key_name += "/";
      key_name += name;
      return key_name;
    }

    fh_key make_fhk(const std::string& name);

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

    int offset_of(const std::string& name, int64_t *offset, uint32_t flags) {
      if (unlikely(! is_dir())) {
	return -EINVAL;
      }
      *offset = XXH64(name.c_str(), name.length(), fh_key::seed);
      return 0;
    }

    bool is_open() const { return flags & FLAG_OPEN; }
    bool is_root() const { return flags & FLAG_ROOT; }
    bool is_mount() const { return flags & FLAG_MOUNT; }
    bool is_bucket() const { return flags & FLAG_BUCKET; }
    bool is_object() const { return !is_bucket(); }
    bool is_file() const { return (fh.fh_type == RGW_FS_TYPE_FILE); }
    bool is_dir() const { return (fh.fh_type == RGW_FS_TYPE_DIRECTORY); }
    bool is_link() const { return (fh.fh_type == RGW_FS_TYPE_SYMBOLIC_LINK); }
    bool creating() const { return flags & FLAG_CREATING; }
    bool deleted() const { return flags & FLAG_DELETED; }
    bool stateless_open() const { return flags & FLAG_STATELESS_OPEN; }
    bool has_children() const;

    int open(uint32_t gsh_flags) {
      lock_guard guard(mtx);
      if (! is_open()) {
	if (gsh_flags & RGW_OPEN_FLAG_V3) {
	  flags |= FLAG_STATELESS_OPEN;
	}
	flags |= FLAG_OPEN;
	return 0;
      }
      return -EPERM;
    }

    typedef boost::variant<uint64_t*, const char*> readdir_offset;

    int readdir(rgw_readdir_cb rcb, void *cb_arg, readdir_offset offset,
		bool *eof, uint32_t flags);

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

    void set_times(const struct timespec &ts) {
      state.ctime = ts;
      state.mtime = state.ctime;
      state.atime = state.ctime;
    }

    void set_times(real_time t) {
      set_times(real_clock::to_timespec(t));
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

    void set_etag(const ceph::buffer::list& _etag ) {
      etag = _etag;
    }

    void set_acls(const ceph::buffer::list& _acls ) {
      acls = _acls;
    }

    void encode_attrs(ceph::buffer::list& ux_key1,
		      ceph::buffer::list& ux_attrs1,
		      bool inc_ov = true);

    DecodeAttrsResult decode_attrs(const ceph::buffer::list* ux_key1,
                                   const ceph::buffer::list* ux_attrs1);

    void invalidate();

    bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override;

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

    ~RGWFileHandle() override;

    friend std::ostream& operator<<(std::ostream &os,
				    RGWFileHandle const &rgw_fh);

    class Factory : public cohort::lru::ObjectFactory
    {
    public:
      RGWLibFS* fs;
      RGWFileHandle* parent;
      const fh_key& fhk;
      std::string& name;
      uint32_t flags;

      Factory() = delete;

      Factory(RGWLibFS* _fs, RGWFileHandle* _parent,
	      const fh_key& _fhk, std::string& _name, uint32_t _flags)
	: fs(_fs), parent(_parent), fhk(_fhk), name(_name),
	  flags(_flags) {}

      void recycle (cohort::lru::Object* o) override {
	/* re-use an existing object */
	o->~Object(); // call lru::Object virtual dtor
	// placement new!
	new (o) RGWFileHandle(fs, parent, fhk, name, flags);
      }

      cohort::lru::Object* alloc() override {
	return new RGWFileHandle(fs, parent, fhk, name, flags);
      }
    }; /* Factory */

  }; /* RGWFileHandle */

  WRITE_CLASS_ENCODER(RGWFileHandle);

  inline RGWFileHandle* get_rgwfh(struct rgw_file_handle* fh) {
    return static_cast<RGWFileHandle*>(fh->fh_private);
  }

  inline enum rgw_fh_type fh_type_of(uint32_t flags) {
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
    struct rgw_fs fs{};
    RGWFileHandle root_fh;
    rgw_fh_callback_t invalidate_cb;
    void *invalidate_arg;
    bool shutdown;

    mutable std::atomic<uint64_t> refcnt;

    RGWFileHandle::FHCache fh_cache;
    RGWFileHandle::FhLRU fh_lru;

    std::string uid; // should match user.user_id, iiuc

    std::unique_ptr<rgw::sal::User> user;
    RGWAccessKey key; // XXXX acc_key

    static std::atomic<uint32_t> fs_inst_counter;

    static uint32_t write_completion_interval_s;

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

      explicit WriteCompletion(RGWFileHandle& _fh) : rgw_fh(_fh) {
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
	    const char* _key, const char *root)
      : cct(_cct), root_fh(this), invalidate_cb(nullptr),
	invalidate_arg(nullptr), shutdown(false), refcnt(1),
	fh_cache(cct->_conf->rgw_nfs_fhcache_partitions,
		 cct->_conf->rgw_nfs_fhcache_size),
	fh_lru(cct->_conf->rgw_nfs_lru_lanes,
	       cct->_conf->rgw_nfs_lru_lane_hiwat),
	uid(_uid), key(_user_id, _key) {

      if (!root || !strcmp(root, "/")) {
        root_fh.init_rootfs(uid, RGWFileHandle::root_name, false);
      } else {
        root_fh.init_rootfs(uid, root, true);
      }

      /* pointer to self */
      fs.fs_private = this;

      /* expose public root fh */
      fs.root_fh = root_fh.get_fh();

      new_inst();
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

    int authorize(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver) {
      int ret = driver->get_user_by_access_key(dpp, key.id, null_yield, &user);
      if (ret == 0) {
	RGWAccessKey* k = user->get_info().get_key(key.id);
	if (!k || (k->key != key.key))
	  return -EINVAL;
	if (user->get_info().suspended)
	  return -ERR_USER_SUSPENDED;
      } else {
	/* try external authenticators (ldap for now) */
	rgw::LDAPHelper* ldh = g_rgwlib->get_ldh(); /* !nullptr */
	RGWToken token;
	/* boost filters and/or string_ref may throw on invalid input */
	try {
	  token = rgw::from_base64(key.id);
	} catch(...) {
	  token = std::string("");
	}
	if (token.valid() && (ldh->auth(token.id, token.key) == 0)) {
	  /* try to driver user if it doesn't already exist */
	  if (user->load_user(dpp, null_yield) < 0) {
	    int ret = user->store_user(dpp, null_yield, true);
	    if (ret < 0) {
	      lsubdout(get_context(), rgw, 10)
		<< "NOTICE: failed to driver new user's info: ret=" << ret
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

      // cast int32_t(RGWFileHandle::FLAG_NONE) due to strictness of Clang
      // the cast transfers a lvalue into a rvalue  in the ctor
      // check the commit message for the full details
      LookupFHResult fhr { nullptr, uint32_t(RGWFileHandle::FLAG_NONE) };

      /* mount is stale? */
      if (state.flags & FLAG_CLOSED)
	return fhr;

      RGWFileHandle::FHCache::Latch lat;
      bool fh_locked = flags & RGWFileHandle::FLAG_LOCKED;

      std::string obj_name{name};
      std::string key_name{parent->make_key_name(name)};
      fh_key fhk = parent->make_fhk(obj_name);

      lsubdout(get_context(), rgw, 10)
	<< __func__ << " called on "
	<< parent->object_name() << " for " << key_name
	<< " (" << obj_name << ")"
	<< " -> " << fhk
	<< dendl;

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
	RGWFileHandle::Factory prototype(this, parent, fhk,
					 obj_name, CREATE_FLAGS(flags));
	uint32_t iflags{cohort::lru::FLAG_INITIAL};
	fh = static_cast<RGWFileHandle*>(
	  fh_lru.insert(&prototype,
			cohort::lru::Edge::MRU,
			iflags));
	if (fh) {
	  /* lock fh (LATCHED) */
	  if (flags & RGWFileHandle::FLAG_LOCK)
	    fh->mtx.lock();
	  if (likely(! (iflags & cohort::lru::FLAG_RECYCLE))) {
	    /* inserts at cached insert iterator, releasing latch */
	    fh_cache.insert_latched(
	      fh, lat, RGWFileHandle::FHCache::FLAG_UNLOCK);
	  } else {
	    /* recycle step invalidates Latch */
	    fh_cache.insert(
	      fhk.fh_hk.object, fh, RGWFileHandle::FHCache::FLAG_NONE);
	    lat.lock->unlock(); /* !LATCHED */
	  }
	  get<1>(fhr) |= RGWFileHandle::FLAG_CREATE;
	  /* ref parent (non-initial ref cannot fail on valid object) */
	  if (! parent->is_mount()) {
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
      if (likely(! fh->is_mount())) {
	(void) fh_lru.unref(fh, cohort::lru::FLAG_NONE);
      }
    }

    inline RGWFileHandle* ref(RGWFileHandle* fh) {
      if (likely(! fh->is_mount())) {
	fh_lru.ref(fh, cohort::lru::FLAG_NONE);
      }
      return fh;
    }

    int getattr(RGWFileHandle* rgw_fh, struct stat* st);

    int setattr(RGWFileHandle* rgw_fh, struct stat* st, uint32_t mask,
		uint32_t flags);

    int getxattrs(RGWFileHandle* rgw_fh, rgw_xattrlist* attrs,
		  rgw_getxattr_cb cb, void *cb_arg, uint32_t flags);

    int lsxattrs(RGWFileHandle* rgw_fh, rgw_xattrstr *filter_prefix,
		 rgw_getxattr_cb cb, void *cb_arg, uint32_t flags);

    int setxattrs(RGWFileHandle* rgw_fh, rgw_xattrlist* attrs, uint32_t flags);

    int rmxattrs(RGWFileHandle* rgw_fh, rgw_xattrlist* attrs, uint32_t flags);

    void update_fh(RGWFileHandle *rgw_fh);

    LookupFHResult stat_bucket(RGWFileHandle* parent, const char *path,
			       RGWLibFS::BucketStats& bs,
			       uint32_t flags);

    LookupFHResult fake_leaf(RGWFileHandle* parent, const char *path,
			     enum rgw_fh_type type = RGW_FS_TYPE_NIL,
			     struct stat *st = nullptr, uint32_t mask = 0,
			     uint32_t flags = RGWFileHandle::FLAG_NONE);

    LookupFHResult stat_leaf(RGWFileHandle* parent, const char *path,
			     enum rgw_fh_type type = RGW_FS_TYPE_NIL,
			     uint32_t flags = RGWFileHandle::FLAG_NONE);

    int read(RGWFileHandle* rgw_fh, uint64_t offset, size_t length,
	     size_t* bytes_read, void* buffer, uint32_t flags);

    int readlink(RGWFileHandle* rgw_fh, uint64_t offset, size_t length,
	     size_t* bytes_read, void* buffer, uint32_t flags);

    int rename(RGWFileHandle* old_fh, RGWFileHandle* new_fh,
	       const char *old_name, const char *new_name);

    MkObjResult create(RGWFileHandle* parent, const char *name, struct stat *st,
		      uint32_t mask, uint32_t flags);

    MkObjResult symlink(RGWFileHandle* parent, const char *name,
               const char *link_path, struct stat *st, uint32_t mask, uint32_t flags);

    MkObjResult mkdir(RGWFileHandle* parent, const char *name, struct stat *st,
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
	if (unlikely(fhk == root_fh.fh.fh_hk)) {
	  /* lookup for root of this fs */
	  fh = &root_fh;
	  goto out;
	}
	lsubdout(get_context(), rgw, 0)
	  << __func__ << " handle lookup failed " << fhk
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
	}
      }

      return fh;
    }

    CephContext* get_context() {
      return cct;
    }

    struct rgw_fs* get_fs() { return &fs; }

    RGWFileHandle& get_fh() { return root_fh; }

    uint64_t get_fsid() { return root_fh.state.dev; }

    RGWUserInfo* get_user() { return &user->get_info(); }

    void update_user(const DoutPrefixProvider *dpp) {
      (void) g_rgwlib->get_driver()->get_user_by_access_key(dpp, key.id, null_yield, &user);
    }

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
  RGWFileHandle::readdir_offset offset;
  void* cb_arg;
  rgw_readdir_cb rcb;
  uint64_t* ioff;
  size_t ix;
  uint32_t d_count;
  bool rcb_eof; // caller forced early stop in readdir cycle

  RGWListBucketsRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
			RGWFileHandle* _rgw_fh, rgw_readdir_cb _rcb,
			void* _cb_arg, RGWFileHandle::readdir_offset& _offset)
    : RGWLibRequest(_cct, std::move(_user)), rgw_fh(_rgw_fh), offset(_offset),
      cb_arg(_cb_arg), rcb(_rcb), ioff(nullptr), ix(0), d_count(0),
      rcb_eof(false) {

    using boost::get;

    if (unlikely(!! get<uint64_t*>(&offset))) {
      ioff = get<uint64_t*>(offset);
      const auto& mk = rgw_fh->find_marker(*ioff);
      if (mk) {
	marker = mk->name;
      }
    } else {
      const char* mk = get<const char*>(offset);
      if (mk) {
	marker = mk;
      }
    }
    op = this;
  }

  bool only_bucket() override { return false; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {
    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    /* XXX derp derp derp */
    state->relative_uri = "/";
    state->info.request_uri = "/"; // XXX
    state->info.effective_uri = "/";
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  int get_params(optional_yield) override {
    limit = -1; /* no limit */
    return 0;
  }

  void send_response_begin(bool has_buckets) override {
    sent_data = true;
  }

  void send_response_data(std::span<const RGWBucketEnt> buckets) override {
    if (!sent_data)
      return;
    for (const auto& ent : buckets) {
      if (! this->operator()(ent.bucket.name, ent.bucket.name)) {
	/* caller cannot accept more */
	lsubdout(cct, rgw, 5) << "ListBuckets rcb failed"
			      << " dirent=" << ent.bucket.name
			      << " call count=" << ix
			      << dendl;
	rcb_eof = true;
	return;
      }
      ++ix;
    }
  } /* send_response_data */

  void send_response_end() override {
    // do nothing
  }

  int operator()(const std::string_view& name,
		 const std::string_view& marker) {
    uint64_t off = XXH64(name.data(), name.length(), fh_key::seed);
    if (!! ioff) {
      *ioff = off;
    }
    /* update traversal cache */
    rgw_fh->add_marker(off, rgw_obj_key{marker.data(), ""},
		       RGW_FS_TYPE_DIRECTORY);
    ++d_count;
    return rcb(name.data(), cb_arg, off, nullptr, 0, RGW_LOOKUP_FLAG_DIR);
  }

  bool eof() {
    using boost::get;

    if (unlikely(cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15))) {
      bool is_offset =
	unlikely(! get<const char*>(&offset)) ||
	!! get<const char*>(offset);
      lsubdout(cct, rgw, 15) << "READDIR offset: " <<
	((is_offset) ? offset : "(nil)")
			     << " is_truncated: " << is_truncated
			     << dendl;
    }
    return !is_truncated && !rcb_eof;
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
  RGWFileHandle::readdir_offset offset;
  void* cb_arg;
  rgw_readdir_cb rcb;
  uint64_t* ioff;
  size_t ix;
  uint32_t d_count;
  bool rcb_eof; // caller forced early stop in readdir cycle

  RGWReaddirRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		    RGWFileHandle* _rgw_fh, rgw_readdir_cb _rcb,
		    void* _cb_arg, RGWFileHandle::readdir_offset& _offset)
    : RGWLibRequest(_cct, std::move(_user)), rgw_fh(_rgw_fh), offset(_offset),
      cb_arg(_cb_arg), rcb(_rcb), ioff(nullptr), ix(0), d_count(0),
      rcb_eof(false) {

    using boost::get;

    if (unlikely(!! get<uint64_t*>(&offset))) {
      ioff = get<uint64_t*>(offset);
      const auto& mk = rgw_fh->find_marker(*ioff);
      if (mk) {
	marker = *mk;
      }
    } else {
      const char* mk = get<const char*>(offset);
      if (mk) {
	std::string tmark{rgw_fh->relative_object_name()};
	if (tmark.length() > 0)
	  tmark += "/";
	tmark += mk;
	marker = rgw_obj_key{std::move(tmark), "", ""};
      }
    }

    default_max = 1000; // XXX was being omitted
    op = this;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {
    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    /* XXX derp derp derp */
    std::string uri = "/" + rgw_fh->bucket_name() + "/";
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    prefix = rgw_fh->relative_object_name();
    if (prefix.length() > 0)
      prefix += "/";
    delimiter = '/';

    return 0;
  }

  int operator()(const std::string_view name, const rgw_obj_key& marker,
		 const ceph::real_time& t, const uint64_t fsz, uint8_t type) {

    assert(name.length() > 0); // all cases handled in callers

    /* hash offset of name in parent (short name) for NFS readdir cookie */
    uint64_t off = XXH64(name.data(), name.length(), fh_key::seed);
    if (unlikely(!! ioff)) {
      *ioff = off;
    }

    /* update traversal cache */
    rgw_fh->add_marker(off, marker, type);
    ++d_count;

    /* set c/mtime and size from bucket index entry */
    struct stat st = {};
#ifdef HAVE_STAT_ST_MTIMESPEC_TV_NSEC
    st.st_atimespec = ceph::real_clock::to_timespec(t);
    st.st_mtimespec = st.st_atimespec;
    st.st_ctimespec = st.st_atimespec;
#else
    st.st_atim = ceph::real_clock::to_timespec(t);
    st.st_mtim = st.st_atim;
    st.st_ctim = st.st_atim;
#endif
    st.st_size = fsz;

    return rcb(name.data(), cb_arg, off, &st, RGWFileHandle::RCB_MASK,
	       (type == RGW_FS_TYPE_DIRECTORY) ?
	       RGW_LOOKUP_FLAG_DIR :
	       RGW_LOOKUP_FLAG_FILE);
  }

  int get_params(optional_yield) override {
    max = default_max;
    return 0;
  }

  void send_response() override {
    req_state* state = get_state();
    auto cnow = real_clock::now();

    /* enumerate objs and common_prefixes in parallel,
     * avoiding increment on and end iterator, which is
     * undefined */

    class DirIterator
    {
      std::vector<rgw_bucket_dir_entry>& objs;
      std::vector<rgw_bucket_dir_entry>::iterator obj_iter;

      std::map<std::string, bool>& common_prefixes;
      std::map<string, bool>::iterator cp_iter;

      boost::optional<std::string_view> obj_sref;
      boost::optional<std::string_view> cp_sref;
      bool _skip_cp;

    public:

      DirIterator(std::vector<rgw_bucket_dir_entry>& objs,
		  std::map<string, bool>& common_prefixes)
	: objs(objs), common_prefixes(common_prefixes), _skip_cp(false)
	{
	  obj_iter = objs.begin();
	  parse_obj();
	  cp_iter = common_prefixes.begin();
	  parse_cp();
	}

      bool is_obj() {
	return (obj_iter != objs.end());
      }

      bool is_cp(){
	return (cp_iter != common_prefixes.end());
      }

      bool eof() {
	return ((!is_obj()) && (!is_cp()));
      }

      void parse_obj() {
	if (is_obj()) {
	  std::string_view sref{obj_iter->key.name};
	  size_t last_del = sref.find_last_of('/');
	  if (last_del != string::npos)
	    sref.remove_prefix(last_del+1);
	  obj_sref = sref;
	}
      } /* parse_obj */

      void next_obj() {
	++obj_iter;
	parse_obj();
      }

      void parse_cp() {
	if (is_cp()) {
	  /* leading-/ skip case */
	  if (cp_iter->first == "/") {
	    _skip_cp = true;
	    return;
	  } else
	    _skip_cp = false;

	  /* it's safest to modify the element in place--a suffix-modifying
	   * string_ref operation is problematic since ULP rgw_file callers
	   * will ultimately need a c-string */
	  if (cp_iter->first.back() == '/')
	    const_cast<std::string&>(cp_iter->first).pop_back();

	  std::string_view sref{cp_iter->first};
	  size_t last_del = sref.find_last_of('/');
	  if (last_del != string::npos)
	    sref.remove_prefix(last_del+1);
	  cp_sref = sref;
	} /* is_cp */
      } /* parse_cp */

      void next_cp() {
	++cp_iter;
	parse_cp();
      }

      bool skip_cp() {
	return _skip_cp;
      }

      bool entry_is_obj() {
	return (is_obj() &&
		((! is_cp()) ||
		 (obj_sref.get() < cp_sref.get())));
      }

      std::string_view get_obj_sref() {
	return obj_sref.get();
      }

      std::string_view get_cp_sref() {
	return cp_sref.get();
      }

      std::vector<rgw_bucket_dir_entry>::iterator& get_obj_iter() {
	return obj_iter;
      }

      std::map<string, bool>::iterator& get_cp_iter() {
	return cp_iter;
      }

    }; /* DirIterator */

    DirIterator di{objs, common_prefixes};

    for (;;) {

      if (di.eof()) {
	break; // done
      }

      /* assert: one of is_obj() || is_cp() holds */
      if (di.entry_is_obj()) {
	auto sref = di.get_obj_sref();
	if (sref.empty()) {
	  /* recursive list of a leaf dir (iirc), do nothing */
	} else {
	  /* send a file entry */
	  auto obj_entry = *(di.get_obj_iter());

	  lsubdout(cct, rgw, 15) << "RGWReaddirRequest "
				 << __func__ << " "
				 << "list uri=" << state->relative_uri << " "
				 << " prefix=" << prefix << " "
				 << " obj path=" << obj_entry.key.name
				 << " (" << sref << ")" << ""
				 << " mtime="
				 << real_clock::to_time_t(obj_entry.meta.mtime)
				 << " size=" << obj_entry.meta.accounted_size
				 << dendl;

	  if (! this->operator()(sref, next_marker, obj_entry.meta.mtime,
				 obj_entry.meta.accounted_size,
				 RGW_FS_TYPE_FILE)) {
	    /* caller cannot accept more */
	    lsubdout(cct, rgw, 5) << "readdir rcb caller signalled stop"
				  << " dirent=" << sref.data()
				  << " call count=" << ix
				  << dendl;
	    rcb_eof = true;
	    return;
	  }
	}
	di.next_obj(); // and advance object
      } else {
	/* send a dir entry */
	if (! di.skip_cp()) {
	  auto sref = di.get_cp_sref();

	  lsubdout(cct, rgw, 15) << "RGWReaddirRequest "
				 << __func__ << " "
				 << "list uri=" << state->relative_uri << " "
				 << " prefix=" << prefix << " "
				 << " cpref=" << sref
				 << dendl;

	  if (sref.empty()) {
	    /* null path segment--could be created in S3 but has no NFS
	     * interpretation */
	  } else {
	    if (! this->operator()(sref, next_marker, cnow, 0,
				   RGW_FS_TYPE_DIRECTORY)) {
	      /* caller cannot accept more */
	      lsubdout(cct, rgw, 5) << "readdir rcb caller signalled stop"
				    << " dirent=" << sref.data()
				    << " call count=" << ix
				    << dendl;
	      rcb_eof = true;
	      return;
	    }
	  }
	}
	di.next_cp(); // and advance common_prefixes
      } /* ! di.entry_is_obj() */
    } /* for (;;) */
  }

  virtual void send_versioned_response() {
    send_response();
  }

  bool eof() {
    using boost::get;

    if (unlikely(cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15))) {
      bool is_offset =
	unlikely(! get<const char*>(&offset)) ||
	!! get<const char*>(offset);
      lsubdout(cct, rgw, 15) << "READDIR offset: " <<
	((is_offset) ? offset : "(nil)")
			     << " next marker: " << next_marker
			     << " is_truncated: " << is_truncated
			     << dendl;
    }
    return !is_truncated && !rcb_eof;
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

  RGWRMdirCheck (CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		 const RGWFileHandle* _rgw_fh)
    : RGWLibRequest(_cct, std::move(_user)), rgw_fh(_rgw_fh), valid(false),
      has_children(false) {
    default_max = 2;
    op = this;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {
    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    std::string uri = "/" + rgw_fh->bucket_name() + "/";
    state->relative_uri = uri;
    state->info.request_uri = uri;
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    prefix = rgw_fh->relative_object_name();
    if (prefix.length() > 0)
      prefix += "/";
    delimiter = '/';

    return 0;
  }

  int get_params(optional_yield) override {
    max = default_max;
    return 0;
  }

  void send_response() override {
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
  const std::string& bucket_name;

  RGWCreateBucketRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
			std::string& _bname)
    : RGWLibRequest(_cct, std::move(_user)), bucket_name(_bname) {
    op = this;
  }

  bool only_bucket() override { return false; }

  int read_permissions(RGWOp* op_obj, optional_yield) override {
    /* we ARE a 'create bucket' request (cf. rgw_rest.cc, ll. 1305-6) */
    return 0;
  }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "PUT";
    state->op = OP_PUT;

    string uri = "/" + bucket_name;
    /* XXX derp derp derp */
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  int get_params(optional_yield) override {
    req_state* state = get_state();
    /* we don't have (any) headers, so just create default ACLs */
    policy.create_default(state->owner.id, state->owner.display_name);
    return 0;
  }

  void send_response() override {
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
  const std::string& bucket_name;

  RGWDeleteBucketRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
			std::string& _bname)
    : RGWLibRequest(_cct, std::move(_user)), bucket_name(_bname) {
    op = this;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "DELETE";
    state->op = OP_DELETE;

    string uri = "/" + bucket_name;
    /* XXX derp derp derp */
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  void send_response() override {}

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

  RGWPutObjRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		  const std::string& _bname, const std::string& _oname,
		  buffer::list& _bl)
    : RGWLibRequest(_cct, std::move(_user)), bucket_name(_bname), obj_name(_oname),
      bl(_bl), bytes_written(0) {
    op = this;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED

    int rc = valid_s3_object_name(obj_name);
    if (rc != 0)
      return rc;

    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "PUT";
    state->op = OP_PUT;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    /* XXX required in RGWOp::execute() */
    state->content_length = bl.length();

    return 0;
  }

  int get_params(optional_yield) override {
    req_state* state = get_state();
    /* we don't have (any) headers, so just create default ACLs */
    policy.create_default(state->owner.id, state->owner.display_name);
    return 0;
  }

  int get_data(buffer::list& _bl) override {
    /* XXX for now, use sharing semantics */
    _bl = std::move(bl);
    uint32_t len = _bl.length();
    bytes_written += len;
    return len;
  }

  void send_response() override {}

  int verify_params() override {
    if (bl.length() > cct->_conf->rgw_max_put_size)
      return -ERR_TOO_LARGE;
    return 0;
  }

  buffer::list* get_attr(const std::string& k) {
    auto iter = attrs.find(k);
    return (iter != attrs.end()) ? &(iter->second) : nullptr;
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

  RGWReadRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		 RGWFileHandle* _rgw_fh, uint64_t off, uint64_t len,
		 void *_ulp_buffer)
    : RGWLibRequest(_cct, std::move(_user)), rgw_fh(_rgw_fh), ulp_buffer(_ulp_buffer),
      nread(0), read_resid(len) {
    op = this;

    /* fixup RGWGetObj (already know range parameters) */
    RGWGetObj::range_parsed = true;
    RGWGetObj::get_data = true; // XXX
    RGWGetObj::partial_content = true;
    RGWGetObj::ofs = off;
    RGWGetObj::end = off + len;
  }

  bool only_bucket() override { return false; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    /* XXX derp derp derp */
    state->relative_uri = make_uri(rgw_fh->bucket_name(),
			       rgw_fh->relative_object_name());
    state->info.request_uri = state->relative_uri; // XXX
    state->info.effective_uri = state->relative_uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  int get_params(optional_yield) override {
    return 0;
  }

  int send_response_data(ceph::buffer::list& bl, off_t bl_off,
                         off_t bl_len) override {
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

  int send_response_data_error(optional_yield) override {
    /* S3 implementation just sends nothing--there is no side effect
     * to simulate here */
    return 0;
  }

  bool prefetch_data() override { return false; }

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

  RGWDeleteObjRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		      const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, std::move(_user)), bucket_name(_bname), obj_name(_oname) {
    op = this;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "DELETE";
    state->op = OP_DELETE;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  void send_response() override {}

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

  RGWStatObjRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		    const std::string& _bname, const std::string& _oname,
		    uint32_t _flags)
    : RGWLibRequest(_cct, std::move(_user)), bucket_name(_bname), obj_name(_oname),
      _size(0), flags(_flags) {
    op = this;

    /* fixup RGWGetObj (already know range parameters) */
    RGWGetObj::range_parsed = true;
    RGWGetObj::get_data = false; // XXX
    RGWGetObj::partial_content = true;
    RGWGetObj::ofs = 0;
    RGWGetObj::end = UINT64_MAX;
  }

  const char* name() const override { return "stat_obj"; }
  RGWOpType get_type() override { return RGW_OP_STAT_OBJ; }

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

  bool only_bucket() override { return false; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    /* XXX derp derp derp */
    state->relative_uri = make_uri(bucket_name, obj_name);
    state->info.request_uri = state->relative_uri; // XXX
    state->info.effective_uri = state->relative_uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  int get_params(optional_yield) override {
    return 0;
  }

  int send_response_data(ceph::buffer::list& _bl, off_t s_off,
                         off_t e_off) override {
    /* NOP */
    /* XXX save attrs? */
    return 0;
  }

  int send_response_data_error(optional_yield) override {
    /* NOP */
    return 0;
  }

  void execute(optional_yield y) override {
    RGWGetObj::execute(y);
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

  RGWStatBucketRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		       const std::string& _path,
		       RGWLibFS::BucketStats& _stats)
    : RGWLibRequest(_cct, std::move(_user)), bs(_stats) {
    uri = "/" + _path;
    op = this;
  }

  buffer::list* get_attr(const std::string& k) {
    auto iter = attrs.find(k);
    return (iter != attrs.end()) ? &(iter->second) : nullptr;
  }

  real_time get_ctime() const {
    return bucket->get_creation_time();
  }

  bool only_bucket() override { return false; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    /* XXX derp derp derp */
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  virtual int get_params() {
    return 0;
  }

  void send_response() override {
    bucket->get_creation_time() = get_state()->bucket->get_info().creation_time;
    bs.size = stats.size;
    bs.size_rounded = stats.size_rounded;
    bs.creation_time = bucket->get_creation_time();
    bs.num_entries = stats.num_objects;
    std::swap(attrs, get_state()->bucket_attrs);
  }

  bool matched() {
    return (bucket->get_name().length() > 0);
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

  RGWStatLeafRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		     RGWFileHandle* _rgw_fh, const std::string& _path)
    : RGWLibRequest(_cct, std::move(_user)), rgw_fh(_rgw_fh), path(_path),
      matched(false), is_dir(false), exact_matched(false) {
    default_max = 1000; // logical max {"foo", "foo/"}
    op = this;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;

    /* XXX derp derp derp */
    std::string uri = "/" + rgw_fh->bucket_name() + "/";
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    prefix = rgw_fh->relative_object_name();
    if (prefix.length() > 0)
      prefix += "/";
    prefix += path;
    delimiter = '/';

    return 0;
  }

  int get_params(optional_yield) override {
    max = default_max;
    return 0;
  }

  void send_response() override {
    req_state* state = get_state();
    // try objects
    for (const auto& iter : objs) {
      auto& name = iter.key.name;
      lsubdout(cct, rgw, 15) << "RGWStatLeafRequest "
			     << __func__ << " "
			     << "list uri=" << state->relative_uri << " "
			     << " prefix=" << prefix << " "
			     << " obj path=" << name << ""
			     << " target = " << path << ""
			     << dendl;
      /* XXX is there a missing match-dir case (trailing '/')? */
      matched = true;
      if (name == path) {
	exact_matched = true;
        return;
      }
    }
    // try prefixes
    for (auto& iter : common_prefixes) {
      auto& name = iter.first;
      lsubdout(cct, rgw, 15) << "RGWStatLeafRequest "
			     << __func__ << " "
			     << "list uri=" << state->relative_uri << " "
			     << " prefix=" << prefix << " "
			     << " pref path=" << name << " (not chomped)"
			     << " target = " << path << ""
			     << dendl;
      matched = true;
      /* match-dir case (trailing '/') */
      if (name == prefix + "/") {
	exact_matched = true;
        is_dir = true;
        return;
      }
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
  std::optional<rgw::BlockingAioThrottle> aio;
  std::unique_ptr<rgw::sal::Writer> processor;
  rgw::sal::DataProcessor* filter;
  boost::optional<RGWPutObj_Compress> compressor;
  CompressorRef plugin;
  buffer::list data;
  uint64_t timer_id;
  MD5 hash;
  off_t real_ofs;
  size_t bytes_written;
  bool eio;
  rgw::op_counters::CountersContainer counters;

  RGWWriteRequest(rgw::sal::Driver* driver, const RGWProcessEnv& penv,
		  std::unique_ptr<rgw::sal::User> _user,
		  RGWFileHandle* _fh, const std::string& _bname,
		  const std::string& _oname)
    : RGWLibContinuedReq(driver->ctx(), penv, std::move(_user)),
      bucket_name(_bname), obj_name(_oname),
      rgw_fh(_fh), filter(nullptr), timer_id(0), real_ofs(0),
      bytes_written(0), eio(false) {

    // in ctr this is not a virtual call
    // invoking this classes's header_init()
    (void) RGWWriteRequest::header_init();
    op = this;
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "PUT";
    state->op = OP_PUT;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  int get_params(optional_yield) override {
    req_state* state = get_state();
    /* we don't have (any) headers, so just create default ACLs */
    policy.create_default(state->owner.id, state->owner.display_name);
    return 0;
  }

  int get_data(buffer::list& _bl) override {
    /* XXX for now, use sharing semantics */
    uint32_t len = data.length();
    _bl = std::move(data);
    bytes_written += len;
    return len;
  }

  void put_data(off_t off, buffer::list& _bl) {
    if (off != real_ofs) {
      eio = true;
    }
    data = std::move(_bl);
    real_ofs += data.length();
    ofs = off; /* consumed in exec_continue() */
  }

  int exec_start() override;
  int exec_continue() override;
  int exec_finish() override;

  void send_response() override {}

  int verify_params() override {
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

  RGWCopyObjRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		    RGWFileHandle* _src_parent, RGWFileHandle* _dst_parent,
		    const std::string& _src_name, const std::string& _dst_name)
    : RGWLibRequest(_cct, std::move(_user)), src_parent(_src_parent),
      dst_parent(_dst_parent), src_name(_src_name), dst_name(_dst_name) {
    /* all requests have this */
    op = this;

    /* allow this request to replace selected attrs */
    attrs_mod = rgw::sal::ATTRSMOD_MERGE;
  }

  bool only_bucket() override { return true; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED

    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "PUT"; // XXX check
    state->op = OP_PUT;

    state->src_bucket_name = src_parent->bucket_name();
    state->bucket_name = dst_parent->bucket_name();

    std::string dest_obj_name = dst_parent->format_child_name(dst_name, false);

    int rc = valid_s3_object_name(dest_obj_name);
    if (rc != 0)
      return rc;

    state->object = RGWHandler::driver->get_object(rgw_obj_key(dest_obj_name));

    /* XXX and fixup key attr (could optimize w/string ref and
     * dest_obj_name) */
    buffer::list ux_key;
    fh_key fhk = dst_parent->make_fhk(dst_name);
    rgw::encode(fhk, ux_key);
    emplace_attr(RGW_ATTR_UNIX_KEY1, std::move(ux_key));

#if 0 /* XXX needed? */
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */
#endif

    return 0;
  }

  int get_params(optional_yield) override {
    req_state* s = get_state();
    /* we don't have (any) headers, so just create default ACLs */
    dest_policy.create_default(s->owner.id, s->owner.display_name);
    /* src_object required before RGWCopyObj::verify_permissions() */
    rgw_obj_key k = rgw_obj_key(src_name);
    s->src_object = s->bucket->get_object(k);
    s->object = s->src_object->clone(); // needed to avoid trap at rgw_op.cc:5150
    return 0;
  }

  void send_response() override {}
  void send_partial_response(off_t ofs) override {}

}; /* RGWCopyObjRequest */

class RGWGetAttrsRequest : public RGWLibRequest,
			   public RGWGetAttrs /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;

  RGWGetAttrsRequest(CephContext* _cct,
		     std::unique_ptr<rgw::sal::User> _user,
		     const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, std::move(_user)), RGWGetAttrs(),
      bucket_name(_bname), obj_name(_oname) {
    op = this;
  }

  const flat_map<std::string, std::optional<buffer::list>>& get_attrs() {
    return attrs;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    req_state* s = get_state();
    s->info.method = "GET";
    s->op = OP_GET;

    std::string uri = make_uri(bucket_name, obj_name);
    s->relative_uri = uri;
    s->info.request_uri = uri;
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    return 0;
  }

  virtual int get_params() {
    return 0;
  }

  virtual void send_response() {}

}; /* RGWGetAttrsRequest */

class RGWSetAttrsRequest : public RGWLibRequest,
			   public RGWSetAttrs /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;

  RGWSetAttrsRequest(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
		     const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, std::move(_user)), bucket_name(_bname), obj_name(_oname) {
    op = this;
  }

  const std::map<std::string, buffer::list>& get_attrs() {
    return attrs;
  }

  bool only_bucket() override { return false; }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {

    req_state* state = get_state();
    state->info.method = "PUT";
    state->op = OP_PUT;

    /* XXX derp derp derp */
    std::string uri = make_uri(bucket_name, obj_name);
    state->relative_uri = uri;
    state->info.request_uri = uri; // XXX
    state->info.effective_uri = uri;
    state->info.request_params = "";
    state->info.domain = ""; /* XXX ? */

    return 0;
  }

  int get_params(optional_yield) override {
    return 0;
  }

  void send_response() override {}

}; /* RGWSetAttrsRequest */

class RGWRMAttrsRequest : public RGWLibRequest,
			  public RGWRMAttrs /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;

  RGWRMAttrsRequest(CephContext* _cct,
		     std::unique_ptr<rgw::sal::User> _user,
		     const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, std::move(_user)), RGWRMAttrs(),
      bucket_name(_bname), obj_name(_oname) {
    op = this;
  }

  const rgw::sal::Attrs& get_attrs() {
    return attrs;
  }

  virtual bool only_bucket() { return false; }

  virtual int op_init() {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  virtual int header_init() {

    req_state* s = get_state();
    s->info.method = "DELETE";
    s->op = OP_PUT;

    std::string uri = make_uri(bucket_name, obj_name);
    s->relative_uri = uri;
    s->info.request_uri = uri;
    s->info.effective_uri = uri;
    s->info.request_params = "";
    s->info.domain = ""; /* XXX ? */

    return 0;
  }

  virtual int get_params() {
    return 0;
  }

  virtual void send_response() {}

}; /* RGWRMAttrsRequest */

/*
 * Send request to get the rados cluster stats
 */
class RGWGetClusterStatReq : public RGWLibRequest,
        public RGWGetClusterStat {
public:
  struct rados_cluster_stat_t& stats_req;
  RGWGetClusterStatReq(CephContext* _cct, std::unique_ptr<rgw::sal::User> _user,
                       rados_cluster_stat_t& _stats):
  RGWLibRequest(_cct, std::move(_user)), stats_req(_stats){
    op = this;
  }

  int op_init() override {
    // assign driver, s, and dialect_handler
    // framework promises to call op_init after parent init
    RGWOp::init(RGWHandler::driver, get_state(), this);
    op = this; // assign self as op: REQUIRED
    return 0;
  }

  int header_init() override {
    req_state* state = get_state();
    state->info.method = "GET";
    state->op = OP_GET;
    return 0;
  }

  int get_params(optional_yield) override { return 0; }
  bool only_bucket() override { return false; }
  void send_response() override {
    stats_req.kb = stats_op.kb;
    stats_req.kb_avail = stats_op.kb_avail;
    stats_req.kb_used = stats_op.kb_used;
    stats_req.num_objects = stats_op.num_objects;
  }
}; /* RGWGetClusterStatReq */


} /* namespace rgw */
