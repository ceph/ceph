// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_vector_bucket.h"

#include "rgw_op.h"
#include "rgw_vector_bucket_admin.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_VectorBucketSession_Invalid : public RGWRESTOp {
  int check_caps(const RGWUserCaps& caps) override {
    return 0;
  }

  void execute(optional_yield) override {
    op_ret = -EINVAL;
  }

  const char* name() const override {return "invalid_vectorbucket_session";}
};


// base for the session ops: the handler parses and validates the request
// arguments, and hands them over as the op state
class RGWOp_VectorBucketSession : public RGWRESTOp {
protected:
  rgw::s3vector::RGWVectorBucketAdminOpState op_state;
public:
  explicit RGWOp_VectorBucketSession(rgw::s3vector::RGWVectorBucketAdminOpState _op_state)
    : op_state(std::move(_op_state)) {}
};

// class for GET /admin/vectorbucket/session?vectorbucket=<name>[&tenant=<tenant>]
class RGWOp_VectorBucketSession_Info : public RGWOp_VectorBucketSession {
public:
  using RGWOp_VectorBucketSession::RGWOp_VectorBucketSession;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::get_session_info(driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "get_vectorbucket_session"; }
};

// class for GET /admin/vectorbucket/session?uid=<user>[&max-entries=<n>&marker=<marker>]
class RGWOp_VectorBucketSession_List : public RGWOp_VectorBucketSession {
public:
  using RGWOp_VectorBucketSession::RGWOp_VectorBucketSession;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::list_sessions(driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "list_vectorbucket_session"; }
};

// class for DELETE /admin/vectorbucket/session?vectorbucket=<name>[&tenant=<tenant>]
class RGWOp_VectorBucketSession_Remove : public RGWOp_VectorBucketSession {
public:
  using RGWOp_VectorBucketSession::RGWOp_VectorBucketSession;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::remove_session(driver, op_state, this, y);
  }

  const char* name() const override { return "remove_vectorbucket_session"; }
};

RGWRESTMgr_VectorBucket::RGWRESTMgr_VectorBucket() {
  register_resource("session", new RGWRESTMgr_VectorBucketSession);
}

RGWOp* RGWHandler_VectorBucketSession::op_get() {
  std::string uid;
  bool uid_existed = false;
  RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);

  rgw::s3vector::RGWVectorBucketAdminOpState op_state;
  bool bucket_existed = false;
  RESTArgs::get_string(s, "vectorbucket", op_state.bucket_name, &op_state.bucket_name, &bucket_existed);

  // exactly one of "uid" and "vectorbucket" selects the operation
  if (bucket_existed == uid_existed) {
    return new RGWOp_VectorBucketSession_Invalid;
  }

  if (bucket_existed) {
    if (op_state.bucket_name.empty()) {
      return new RGWOp_VectorBucketSession_Invalid;
    }
    RESTArgs::get_string(s, "tenant", op_state.uid.tenant, &op_state.uid.tenant);
    return new RGWOp_VectorBucketSession_Info(std::move(op_state));
  }

  if (uid.empty()) {
    return new RGWOp_VectorBucketSession_Invalid;
  }
  op_state.uid = rgw_user(uid);
  if (RESTArgs::get_uint32(s, "max-entries", op_state.max_entries, &op_state.max_entries) < 0) {
    return new RGWOp_VectorBucketSession_Invalid;
  }
  RESTArgs::get_string(s, "marker", op_state.marker, &op_state.marker);
  return new RGWOp_VectorBucketSession_List(std::move(op_state));
}

RGWOp* RGWHandler_VectorBucketSession::op_delete() {
  std::string uid;
  bool uid_existed = false;
  RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);

  rgw::s3vector::RGWVectorBucketAdminOpState op_state;
  RESTArgs::get_string(s, "vectorbucket", op_state.bucket_name, &op_state.bucket_name);

  if (uid_existed || op_state.bucket_name.empty()) {
    return new RGWOp_VectorBucketSession_Invalid;
  }
  RESTArgs::get_string(s, "tenant", op_state.uid.tenant, &op_state.uid.tenant);
  return new RGWOp_VectorBucketSession_Remove(std::move(op_state));
}
