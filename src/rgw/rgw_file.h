// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_FILE_H
#define RGW_FILE_H

#include "include/rados/rgw_file.h"

/* internal header */

class RGWFileHandle
{
  struct rgw_file_handle fh;
public:
  RGWFileHandle() {
    fh.fh_private = this;
  }

  struct rgw_file_handle* get_fh() { return &fh; }
}; /* RGWFileHandle */

class RGWLibFS
{
  struct rgw_fs fs;

  std::string uid; // should match user.user_id, iiuc

  RGWUserInfo user;
  RGWAccessKey key;

public:
  RGWLibFS(const char *_uid, const char *_user_id, const char* _key)
    : uid(_uid), key(_user_id, _key) {
    fs.fs_private = this;
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

#endif /* RGW_FILE_H */
