// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_vector_bucket.h"

#include "rgw_op.h"
#include "rgw_vector_bucket_admin.h"

#define dout_subsys ceph_subsys_rgw

static int get_vector_bucket_name(req_state* s, std::string& bucket_name, bool& existed)
{
  return RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,
                              &existed);
}

class RGWOp_VectorBucket_Invalid : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps&) override {
    return 0;
  }

  void execute(optional_yield) override
  {
    op_ret = -EINVAL;
  }

  const char* name() const override { return "invalid_vectorbucket"; }
};

static bool has_vectorbucket_selector(req_state* s, std::string* bucket_name = nullptr)
{
  bool existed = false;
  std::string local_bucket_name;
  const int ret = get_vector_bucket_name(s, local_bucket_name, existed);
  if (ret < 0) {
    return false;
  }
  if (bucket_name) {
    *bucket_name = std::move(local_bucket_name);
  }
  return existed;
}

class RGWOp_VectorBucket_Info : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override
  {
    rgw::s3vector::RGWVectorBucketAdminOpState op_state;
    std::string uid;
    bool uid_existed = false;
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);
    if (op_ret < 0 || uid_existed) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }

    std::string tenant;
    RESTArgs::get_string(s, "tenant", tenant, &tenant);
    std::string bucket_name;
    bool bucket_existed = false;
    op_ret = get_vector_bucket_name(s, bucket_name, bucket_existed);
    if (op_ret < 0 || !bucket_existed || bucket_name.empty()) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }
    bool session_enabled = false;
    bool session_existed = false;
    op_ret = RESTArgs::get_bool(s, "session", false, &session_enabled,
                                &session_existed);
    if (op_ret < 0) {
      return;
    }
    if (!session_existed || !session_enabled) {
      op_ret = 0;
      return;
    }

    op_state.uid.tenant = tenant;
    op_state.bucket_name = bucket_name;
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::get_session_info(
        driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "get_vectorbucket_info"; }
};

class RGWOp_VectorBucket_List : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override
  {
    rgw::s3vector::RGWVectorBucketAdminOpState op_state;
    std::string bucket_name;
    bool bucket_existed = false;
    op_ret = get_vector_bucket_name(s, bucket_name, bucket_existed);
    if (op_ret < 0 || bucket_existed) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }

    std::string uid;
    bool uid_existed = false;
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);
    if (op_ret < 0 || !uid_existed || uid.empty()) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }
    bool session_enabled = false;
    bool session_existed = false;
    op_ret = RESTArgs::get_bool(s, "session", false, &session_enabled,
                                &session_existed);
    if (op_ret < 0) {
      return;
    }
    if (!session_existed || !session_enabled) {
      op_ret = 0;
      return;
    }

    RESTArgs::get_uint32(s, "max-entries", op_state.max_entries,
                         &op_state.max_entries);
    RESTArgs::get_string(s, "marker", op_state.marker, &op_state.marker);
    op_state.uid = rgw_user(uid);
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::list_sessions(
        driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "list_vectorbuckets"; }
};

class RGWOp_VectorBucket_Remove : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override
  {
    rgw::s3vector::RGWVectorBucketAdminOpState op_state;
    std::string uid;
    bool uid_existed = false;
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);
    if (op_ret < 0 || uid_existed) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }

    std::string tenant;
    RESTArgs::get_string(s, "tenant", tenant, &tenant);
    std::string bucket_name;
    bool bucket_existed = false;
    op_ret = get_vector_bucket_name(s, bucket_name, bucket_existed);
    if (op_ret < 0 || !bucket_existed || bucket_name.empty()) {
      op_ret = op_ret < 0 ? op_ret : -EINVAL;
      return;
    }

    bool session_enabled = false;
    bool session_existed = false;
    op_ret = RESTArgs::get_bool(s, "session", false, &session_enabled,
                                &session_existed);
    if (op_ret < 0) {
      return;
    }
    if (!session_existed || !session_enabled) {
      op_ret = 0;
      return;
    }

    op_state.uid.tenant = tenant;
    op_state.bucket_name = bucket_name;
    op_ret = rgw::s3vector::RGWVectorBucketAdminOp::remove_session(
        driver, op_state, this, y);
  }

  const char* name() const override { return "remove_vectorbucket"; }
};

RGWOp* RGWHandler_VectorBucket::op_get()
{
  bool uid_existed = false;
  std::string uid;
  if (RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed) < 0) {
    return nullptr;
  }

  std::string bucket_name;
  const bool bucket_existed = has_vectorbucket_selector(s, &bucket_name);
  if (uid_existed == bucket_existed) {
    return new RGWOp_VectorBucket_Invalid;
  }

  return uid_existed ? static_cast<RGWOp*>(new RGWOp_VectorBucket_List)
                     : static_cast<RGWOp*>(new RGWOp_VectorBucket_Info);
}

RGWOp* RGWHandler_VectorBucket::op_delete()
{
  return new RGWOp_VectorBucket_Remove;
}
