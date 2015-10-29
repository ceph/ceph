// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FILE_H
#define RGW_FILE_H

#include "include/rados/rgw_file.h"

/* internal header */

#include <atomic>
#include <boost/intrusive_ptr.hpp>
#include "xxhash.h"
#include "include/buffer.h"

class RGWLibFS;
class RGWFileHandle;

typedef boost::intrusive_ptr<RGWFileHandle> RGWFHRef;

class RGWFileHandle
{
  struct rgw_file_handle fh;
  mutable std::atomic<uint64_t> refcnt;
  RGWFHRef parent;
  const string name; /* XXX file or bucket name */
  uint32_t flags;

public:
  const static string root_name;

  static constexpr uint32_t FLAG_NONE = 0x0000;
  static constexpr uint32_t FLAG_OPEN = 0x0001;
  static constexpr uint32_t FLAG_ROOT = 0x0002;
  
  RGWFileHandle(RGWLibFS* fs, RGWFileHandle* _parent, const char* _name)
    : parent(_parent), name(_name), flags(FLAG_NONE) {
    if (parent) {
      fh.fh_type = parent->is_root() ?
	RGW_FS_TYPE_DIRECTORY : RGW_FS_TYPE_FILE;
      /* content-addressable hash (bucket part) */
      fh.fh_hk.bucket = parent->get_fh()->fh_hk.object;
    } else {
      /* root */
      fh.fh_type = RGW_FS_TYPE_DIRECTORY;
      /* XXX give root buckets a locally-unique bucket hash part */
      fh.fh_hk.bucket = XXH64(reinterpret_cast<char*>(fs), 8, 8675309);
      flags |= FLAG_ROOT;
    }
    /* content-addressable hash (object part) */
    fh.fh_hk.object = XXH64(name.c_str(), name.length(), 8675309 /* XXX */);
    fh.fh_private = this;
  }

  struct rgw_file_handle* get_fh() { return &fh; }

  const std::string& bucket_name() {
    if (is_root())
      return root_name;
    if (is_object()) {
      return parent->object_name();
    }
    return name;
  }

  const std::string& object_name() { return name; }

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
}; /* RGWFileHandle */

static inline RGWFileHandle* get_rgwfh(struct rgw_file_handle* fh) {
  return static_cast<RGWFileHandle*>(fh->fh_private);
}

class RGWLibFS
{
  struct rgw_fs fs;
  RGWFileHandle root_fh;

  std::string uid; // should match user.user_id, iiuc

  RGWUserInfo user;
  RGWAccessKey key;

public:
  RGWLibFS(const char *_uid, const char *_user_id, const char* _key)
    : root_fh(this, nullptr, RGWFileHandle::root_name.c_str()), uid(_uid),
      key(_user_id, _key) {
    fs.fs_private = this;
    fs.root_fh = root_fh.get_fh(); /* expose public root fh */
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
    return 0;
  }

  struct rgw_fs* get_fs() { return &fs; }
  RGWUserInfo* get_user() { return &user; }

  bool is_root(struct rgw_fs* _fs) {
    return (&fs == _fs);
  }

}; /* RGWLibFS */

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
