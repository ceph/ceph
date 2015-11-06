// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FILE_H
#define RGW_FILE_H

#include "include/rados/rgw_file.h"

/* internal header */
#include <string.h>

#include <atomic>
#include <mutex>
#include <boost/intrusive_ptr.hpp>
#include "xxhash.h"
#include "include/buffer.h"
#include "common/cohort_lru.h"

/* XXX
 * ASSERT_H somehow not defined after all the above (which bring
 * in common/debug.h [e.g., dout])
 */
#include "include/assert.h"

namespace rgw {

  namespace bi = boost::intrusive;

  class RGWLibFS;
  class RGWFileHandle;

  typedef boost::intrusive_ptr<RGWFileHandle> RGWFHRef;

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

  class RGWFileHandle
  {
    struct rgw_file_handle fh;
    mutable std::atomic<uint64_t> refcnt;
    RGWLibFS* fs;
    RGWFHRef parent;
    /* const */ std::string name; /* XXX file or bucket name */
    /* const */ fh_key fhk;
    uint32_t flags;

  public:
    const static string root_name;

    static constexpr uint32_t FLAG_NONE = 0x0000;
    static constexpr uint32_t FLAG_OPEN = 0x0001;
    static constexpr uint32_t FLAG_ROOT = 0x0002;

    friend class RGWLibFS;

  private:
    RGWFileHandle(RGWLibFS* _fs)
      : fs(_fs), parent(nullptr), flags(FLAG_ROOT)
      {
	/* root */
	fh.fh_type = RGW_FS_TYPE_DIRECTORY;
	/* pointer to self */
	fh.fh_private = this;
      }
    
    void init_rootfs(std::string& fsid, const std::string& object_name) {
      /* fh_key */
      fh.fh_hk.bucket = XXH64(fsid.c_str(), fsid.length(), fh_key::seed);
      fh.fh_hk.object = XXH64(object_name.c_str(), object_name.length(),
			      fh_key::seed);
    }

  public:
    RGWFileHandle(RGWLibFS* fs, RGWFileHandle* _parent, const fh_key& _fhk,
		  const char *_name)
      : parent(_parent), name(_name), fhk(_fhk), flags(FLAG_NONE) {

      fh.fh_type = parent->is_root()
	? RGW_FS_TYPE_DIRECTORY : RGW_FS_TYPE_FILE;      

      /* save constant fhk */
      fh_key fhk(parent->name, name);
      fh.fh_hk = fhk.fh_hk; /* XXX redundant in fh_hk */

      /* pointer to self */
      fh.fh_private = this;
    }

    const fh_key& get_key() const {
      return fhk;
    }

    struct rgw_file_handle* get_fh() { return &fh; }

    const std::string& bucket_name() const {
      if (is_root())
	return root_name;
      if (is_object()) {
	return parent->object_name();
      }
      return name;
    }

    const std::string& object_name() const { return name; }

    bool is_open() const { return flags & FLAG_OPEN; }
    bool is_root() const { return flags & FLAG_ROOT; }
    bool is_bucket() const { return (fh.fh_type == RGW_FS_TYPE_DIRECTORY); }
    bool is_object() const { return (fh.fh_type == RGW_FS_TYPE_FILE); }

    void open() {
      flags |= FLAG_OPEN;
    }

    void close() {
      flags &= ~FLAG_OPEN;
    }

    friend void intrusive_ptr_add_ref(const RGWFileHandle* fh) {
      fh->refcnt.fetch_add(1, std::memory_order_relaxed);
    }

    friend void intrusive_ptr_release(const RGWFileHandle* fh) {
      if (fh->refcnt.fetch_sub(1, std::memory_order_release) == 1) {
	std::atomic_thread_fence(std::memory_order_acquire);
	/* root handles are expanded in RGWLibFS */
	if (! const_cast<RGWFileHandle*>(fh)->is_root())
	  delete fh;
      }
    }

    inline void rele() {
      intrusive_ptr_release(this);
    }

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
  }; /* RGWFileHandle */

  static inline RGWFileHandle* get_rgwfh(struct rgw_file_handle* fh) {
    return static_cast<RGWFileHandle*>(fh->fh_private);
  }

  class RGWLibFS
  {
    struct rgw_fs fs;
    RGWFileHandle root_fh;

    RGWFileHandle::FHCache fh_cache;
    
    std::string uid; // should match user.user_id, iiuc

    RGWUserInfo user;
    RGWAccessKey key; // XXXX acc_key

    static atomic<uint32_t> fs_inst;
    std::string fsid;
    
  public:
    RGWLibFS(const char *_uid, const char *_user_id, const char* _key)
      : root_fh(this),  uid(_uid), key(_user_id, _key) {

      /* no bucket may be named rgw_fs_inst-(.*) */
      fsid = RGWFileHandle::root_name + "rgw_fs_inst-" +
	std::to_string(++(fs_inst));

      root_fh.init_rootfs(fsid /* bucket */, RGWFileHandle::root_name);

      /* pointer to self */
      fs.fs_private = this;

      /* expose public root fh */
      fs.root_fh = root_fh.get_fh();
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
      }
      return ret;
    }

    /* find or create an RGWFileHandle */
    RGWFileHandle* lookup_fh(RGWFileHandle* parent, const char *name) {

      RGWFileHandle::FHCache::Latch lat;
      fh_key fhk(parent->fhk.fh_hk.object, name);

      RGWFileHandle* fh =
	fh_cache.find_latch(fhk.fh_hk.object /* partition selector*/,
			    fhk /* key */, lat /* serializer */,
			    RGWFileHandle::FHCache::FLAG_LOCK);
      /* LATCHED */
      if (! fh) {
	fh = new RGWFileHandle(this, parent, fhk, name);
	intrusive_ptr_add_ref(fh); /* sentinel ref */
	fh_cache.insert_latched(fh, lat,
				RGWFileHandle::FHCache::FLAG_NONE);
      }
      intrusive_ptr_add_ref(fh); /* call path/handle ref */
      lat.lock->unlock(); /* !LATCHED */
      return fh;
    }

    /* find or create an RGWFileHandle */
    RGWFileHandle* lookup_handle(struct rgw_fh_hk fh_hk) {

      RGWFileHandle::FHCache::Latch lat;
      fh_key fhk(fh_hk);

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
      intrusive_ptr_add_ref(fh); /* call path/handle ref */
    out:
      lat.lock->unlock(); /* !LATCHED */
      return fh;
    }

    CephContext* get_context() {
      return static_cast<CephContext*>(get_fs()->fs_private);
    }

    struct rgw_fs* get_fs() { return &fs; }

    uint32_t get_inst() { return fs_inst; }

    RGWUserInfo* get_user() { return &user; }

    bool is_root(struct rgw_fs* _fs) {
      return (&fs == _fs);
    }

  }; /* RGWLibFS */

} /* namespace rgw */

/*
  read directory content (buckets)
*/

class RGWListBucketsRequest : public RGWLibRequest,
			      public RGWListBuckets_OS_Lib /* RGWOp */
{
public:
  uint64_t* offset;
  void* cb_arg;
  rgw_readdir_cb rcb;

  RGWListBucketsRequest(CephContext* _cct, RGWUserInfo *_user,
			rgw_readdir_cb _rcb, void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct, _user), offset(_offset), cb_arg(_cb_arg),
      rcb(_rcb) {
    magic = 71;
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

  int operator()(const std::string& name, const std::string& marker) {
    rcb(name.c_str(), cb_arg, (*offset)++);
    return 0;
  }

}; /* RGWListBucketsRequest */

/*
  read directory content (bucket objects)
*/

class RGWListBucketRequest : public RGWLibRequest,
			     public RGWListBucket_OS_Lib /* RGWOp */
{
public:
  std::string& uri;
  uint64_t* offset;
  void* cb_arg;
  rgw_readdir_cb rcb;

  RGWListBucketRequest(CephContext* _cct, RGWUserInfo *_user, std::string& _uri,
		      rgw_readdir_cb _rcb, void* _cb_arg, uint64_t* _offset)
    : RGWLibRequest(_cct, _user), uri(_uri), offset(_offset),
      cb_arg(_cb_arg),
      rcb(_rcb) {
    magic = 72;
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

  int operator()(const std::string& name, const std::string& marker) {
    rcb(name.c_str(), cb_arg, (*offset)++);
    return 0;
  }

}; /* RGWListBucketRequest */

/*
  create bucket
*/

class RGWCreateBucketRequest : public RGWLibRequest,
			       public RGWCreateBucket_OS_Lib /* RGWOp */
{
public:
  std::string& uri;

  RGWCreateBucketRequest(CephContext* _cct, RGWUserInfo *_user,
			std::string& _uri)
    : RGWLibRequest(_cct, _user), uri(_uri) {
    magic = 73;
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
}; /* RGWCreateBucketRequest */

/*
  delete bucket
*/

class RGWDeleteBucketRequest : public RGWLibRequest,
			       public RGWDeleteBucket_OS_Lib /* RGWOp */
{
public:
  std::string& uri;

  RGWDeleteBucketRequest(CephContext* _cct, RGWUserInfo *_user,
			std::string& _uri)
    : RGWLibRequest(_cct, _user), uri(_uri) {
    magic = 74;
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
			 public RGWPutObj_OS_Lib /* RGWOp */
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
    magic = 75;
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
    string uri = "/" + bucket_name + "/" + obj_name;
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

class RGWGetObjRequest : public RGWLibRequest,
			 public RGWGetObj_OS_Lib /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;
  buffer::list& bl;
  bool do_hexdump = false;

  RGWGetObjRequest(CephContext* _cct, RGWUserInfo *_user,
		  const std::string& _bname, const std::string& _oname,
		  uint64_t off, uint64_t len, buffer::list& _bl)
    : RGWLibRequest(_cct, _user), bucket_name(_bname), obj_name(_oname),
      bl(_bl) {
    magic = 76;
    op = this;

    /* fixup RGWGetObj (already know range parameters) */
    RGWGetObj::range_parsed = true;
    RGWGetObj::get_data = true; // XXX
    RGWGetObj::partial_content = true;
    RGWGetObj::ofs = off;
    RGWGetObj::end = off + len;
  }

  buffer::list& get_bl() { return bl; }

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
    string uri = "/" + bucket_name + "/" + obj_name;
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

  virtual int send_response_data(ceph::buffer::list& _bl, off_t s_off,
				off_t e_off) {
    /* XXX deal with offsets */
    if (do_hexdump) {
      dout(15) << __func__ << " s_off " << s_off
	       << " e_off " << e_off << " len " << _bl.length()
	       << " ";
      _bl.hexdump(*_dout);
      *_dout << dendl;
    }
    bl.claim_append(_bl);
    return 0;
  }

  virtual int send_response_data_error() {
    /* S3 implementation just sends nothing--there is no side effect
     * to simulate here */
    return 0;
  }

}; /* RGWGetObjRequest */

/*
  delete object
*/

class RGWDeleteObjRequest : public RGWLibRequest,
			    public RGWDeleteObj_OS_Lib /* RGWOp */
{
public:
  const std::string& bucket_name;
  const std::string& obj_name;

  RGWDeleteObjRequest(CephContext* _cct, RGWUserInfo *_user,
		      const std::string& _bname, const std::string& _oname)
    : RGWLibRequest(_cct, _user), bucket_name(_bname), obj_name(_oname) {
    magic = 77;
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
    string uri = "/" + bucket_name + "/" + obj_name;
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

#endif /* RGW_FILE_H */
