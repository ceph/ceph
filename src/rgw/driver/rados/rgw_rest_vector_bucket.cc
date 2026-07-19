// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "driver/rados/rgw_bucket.h"
#include "rgw_op.h"
#include "rgw_rest_vector_bucket.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

int get_vector_bucket_name(req_state* s, std::string& bucket_name, bool& existed)
{
  return RESTArgs::get_string(s, "vectorbucket", bucket_name, &bucket_name,
                              &existed);
}

int require_true_bool(req_state* s, const std::string& name)
{
  bool value = false;
  bool existed = false;
  const int ret = RESTArgs::get_bool(s, name, false, &value, &existed);
  if (ret < 0) {
    return ret;
  }
  if (!existed || !value) {
    return -EINVAL;
  }
  return 0;
}

class RGWOp_VectorBucket_Session_Info : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override
  {
    RGWVectorBucketAdminOpState op_state;
    std::string uid;
    bool uid_existed = false;
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);
    if (op_ret < 0) {
      return;
    }
    if (uid_existed) {
      op_ret = -EINVAL;
      return;
    }

    std::string tenant;
    RESTArgs::get_string(s, "tenant", tenant, &tenant);
    std::string bucket_name;
    bool bucket_existed = false;
    op_ret = get_vector_bucket_name(s, bucket_name, bucket_existed);
    if (op_ret < 0) {
      return;
    }
    if (!bucket_existed || bucket_name.empty()) {
      op_ret = -EINVAL;
      return;
    }

    op_ret = require_true_bool(s, "session");
    if (op_ret < 0) {
      return;
    }

    op_state.set_tenant(tenant);
    op_state.set_bucket_name(bucket_name);
    op_state.set_fetch_session(true);
    op_ret = RGWVectorBucketAdminOp::info(driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "get_vectorbucket_session_info"; }
};

class RGWOp_VectorBucket_Session_List : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override
  {
    RGWVectorBucketAdminOpState op_state;
    std::string bucket_name;
    bool bucket_existed = false;
    op_ret = get_vector_bucket_name(s, bucket_name, bucket_existed);
    if (op_ret < 0) {
      return;
    }
    if (bucket_existed) {
      op_ret = -EINVAL;
      return;
    }

    std::string uid_str;
    bool uid_existed = false;
    op_ret = RESTArgs::get_string(s, "uid", uid_str, &uid_str, &uid_existed);
    if (op_ret < 0) {
      return;
    }
    if (!uid_existed || uid_str.empty()) {
      op_ret = -EINVAL;
      return;
    }

    op_ret = require_true_bool(s, "session");
    if (op_ret < 0) {
      return;
    }

    uint32_t max_entries = 1000;
    RESTArgs::get_uint32(s, "max-entries", max_entries, &max_entries);
    std::string marker;
    RESTArgs::get_string(s, "marker", marker, &marker);

    op_state.set_user_id(rgw_user(uid_str));
    op_state.max_entries = max_entries;
    op_state.marker = marker;
    op_state.set_fetch_session(true);
    op_ret = RGWVectorBucketAdminOp::info(driver, op_state, flusher, y, this);
  }

  const char* name() const override { return "list_vectorbucket_sessions"; }
};

class RGWOp_VectorBucket_Session_Remove : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override
  {
    RGWVectorBucketAdminOpState op_state;
    std::string uid;
    bool uid_existed = false;
    op_ret = RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed);
    if (op_ret < 0) {
      return;
    }
    if (uid_existed) {
      op_ret = -EINVAL;
      return;
    }

    std::string tenant;
    RESTArgs::get_string(s, "tenant", tenant, &tenant);
    std::string bucket_name;
    bool bucket_existed = false;
    op_ret = get_vector_bucket_name(s, bucket_name, bucket_existed);
    if (op_ret < 0) {
      return;
    }
    if (!bucket_existed || bucket_name.empty()) {
      op_ret = -EINVAL;
      return;
    }

    op_ret = require_true_bool(s, "session");
    if (op_ret < 0) {
      return;
    }

    op_state.set_tenant(tenant);
    op_state.set_bucket_name(bucket_name);
    op_state.set_fetch_session(true);
    op_ret = RGWVectorBucketAdminOp::remove_session(driver, op_state, this, y);
  }

  const char* name() const override { return "remove_vectorbucket_session"; }
};

} // anonymous namespace

RGWOp *RGWHandler_VectorBucket::op_get()
{
  bool uid_existed = false;
  std::string uid;
  if (RESTArgs::get_string(s, "uid", uid, &uid, &uid_existed) < 0) {
    return nullptr;
  }
  if (uid_existed) {
    return new RGWOp_VectorBucket_Session_List;
  }
  return new RGWOp_VectorBucket_Session_Info;
}

RGWOp *RGWHandler_VectorBucket::op_delete()
{
  return new RGWOp_VectorBucket_Session_Remove;
}
