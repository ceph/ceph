// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_op.h"
#include "driver/rados/rgw_bucket.h"
#include "rgw_process_env.h"
#include "rgw_rest_bucket.h"
#include "rgw_sal.h"

#include "include/str_list.h"

#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

class RGWOp_Bucket_Info : public RGWRESTOp {

public:
  RGWOp_Bucket_Info() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_bucket_info"; }
};

void RGWOp_Bucket_Info::execute(optional_yield y)
{
  RGWBucketAdminOpState op_state;

  bool fetch_stats;

  std::string bucket;

  string uid_str;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "stats", false, &fetch_stats);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_fetch_stats(fetch_stats);

  op_ret = RGWBucketAdminOp::info(driver, op_state, flusher, y, this);
}

class RGWOp_Get_Policy : public RGWRESTOp {

public:
  RGWOp_Get_Policy() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "get_policy"; }
};

void RGWOp_Get_Policy::execute(optional_yield y)
{
  RGWBucketAdminOpState op_state;

  std::string bucket;
  std::string object;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object(object);

  op_ret = RGWBucketAdminOp::get_policy(driver, op_state, flusher, this, y);
}

class RGWOp_Check_Bucket_Index : public RGWRESTOp {

public:
  RGWOp_Check_Bucket_Index() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "check_bucket_index"; }
};

void RGWOp_Check_Bucket_Index::execute(optional_yield y)
{
  std::string bucket;

  bool fix_index;
  bool check_objects;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "fix", false, &fix_index);
  RESTArgs::get_bool(s, "check-objects", false, &check_objects);

  op_state.set_bucket_name(bucket);
  op_state.set_fix_index(fix_index);
  op_state.set_check_objects(check_objects);

  op_ret = RGWBucketAdminOp::check_index(driver, op_state, flusher, s->yield, s);
}

class RGWOp_Bucket_Link : public RGWRESTOp {

public:
  RGWOp_Bucket_Link() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "link_bucket"; }
};

void RGWOp_Bucket_Link::execute(optional_yield y)
{
  std::string uid_str;
  std::string bucket;
  std::string bucket_id;
  std::string new_bucket_name;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "bucket-id", bucket_id, &bucket_id);
  RESTArgs::get_string(s, "new-bucket-name", new_bucket_name, &new_bucket_name);

  rgw_user uid(uid_str);
  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_bucket_id(bucket_id);
  op_state.set_new_bucket_name(new_bucket_name);

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }
  op_ret = RGWBucketAdminOp::link(driver, op_state, s, y);
}

class RGWOp_Bucket_Unlink : public RGWRESTOp {

public:
  RGWOp_Bucket_Unlink() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "unlink_bucket"; }
};

void RGWOp_Bucket_Unlink::execute(optional_yield y)
{
  std::string uid_str;
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }
  op_ret = RGWBucketAdminOp::unlink(driver, op_state, s, y);
}

class RGWOp_Bucket_Remove : public RGWRESTOp {

public:
  RGWOp_Bucket_Remove() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "remove_bucket"; }
};

void RGWOp_Bucket_Remove::execute(optional_yield y)
{
  std::string bucket_name;
  bool delete_children;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_bool(s, "purge-objects", false, &delete_children);

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "forward_request_to_master returned ret=" << op_ret << dendl;
    if (op_ret == -ENOENT) {
      /* adjust error, we want to return with NoSuchBucket and not
       * NoSuchKey */
      op_ret = -ERR_NO_SUCH_BUCKET;
    }
    return;
  }

  op_ret = driver->load_bucket(s, rgw_bucket("", bucket_name),
                               &bucket, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "get_bucket returned ret=" << op_ret << dendl;
    if (op_ret == -ENOENT) {
      op_ret = -ERR_NO_SUCH_BUCKET;
    }
    return;
  }

  op_ret = bucket->remove(s, delete_children, s->yield);
}

class RGWOp_Set_Bucket_Quota : public RGWRESTOp {

public:
  RGWOp_Set_Bucket_Quota() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "set_bucket_quota"; }
};

#define QUOTA_INPUT_MAX_LEN 1024

void RGWOp_Set_Bucket_Quota::execute(optional_yield y)
{
  bool uid_arg_existed = false;
  std::string uid_str;
  RESTArgs::get_string(s, "uid", uid_str, &uid_str, &uid_arg_existed);
  if (! uid_arg_existed) {
    op_ret = -EINVAL;
    return;
  }
  rgw_user uid(uid_str);
  bool bucket_arg_existed = false;
  std::string bucket_name;
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name, &bucket_arg_existed);
  if (! bucket_arg_existed) {
    op_ret = -EINVAL;
    return;
  }

  bool use_http_params;

  if (s->content_length > 0) {
    use_http_params = false;
  } else {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    use_http_params = (!encoding || strcmp(encoding, "chunked") != 0);
  }
  RGWQuotaInfo quota;
  if (!use_http_params) {
    bool empty;
    op_ret = get_json_input(driver->ctx(), s, quota, QUOTA_INPUT_MAX_LEN, &empty);
    if (op_ret < 0) {
      if (!empty)
        return;
      /* was probably chunked input, but no content provided, configure via http params */
      use_http_params = true;
    }
  }
  if (use_http_params) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    op_ret = driver->load_bucket(s, rgw_bucket(uid.tenant, bucket_name),
                                 &bucket, s->yield);
    if (op_ret < 0) {
      return;
    }
    RGWQuotaInfo *old_quota = &bucket->get_info().quota;
    int64_t old_max_size_kb = rgw_rounded_kb(old_quota->max_size);
    int64_t max_size_kb;
    bool has_max_size_kb = false;
    RESTArgs::get_int64(s, "max-objects", old_quota->max_objects, &quota.max_objects);
    RESTArgs::get_int64(s, "max-size", old_quota->max_size, &quota.max_size);
    RESTArgs::get_int64(s, "max-size-kb", old_max_size_kb, &max_size_kb, &has_max_size_kb);
    if (has_max_size_kb)
      quota.max_size = max_size_kb * 1024;
    RESTArgs::get_bool(s, "enabled", old_quota->enabled, &quota.enabled);
  }

  RGWBucketAdminOpState op_state;
  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket_name);
  op_state.set_quota(quota);

  op_ret = RGWBucketAdminOp::set_quota(driver, op_state, s, y);
}

class RGWOp_Sync_Bucket : public RGWRESTOp {

public:
  RGWOp_Sync_Bucket() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "sync_bucket"; }
};

void RGWOp_Sync_Bucket::execute(optional_yield y)
{
  std::string bucket;
  std::string tenant;
  bool sync_bucket;

  RGWBucketAdminOpState op_state;
  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "tenant", tenant, &tenant);
  RESTArgs::get_bool(s, "sync", true, &sync_bucket);

  op_state.set_bucket_name(bucket);
  op_state.set_tenant(tenant);
  op_state.set_sync_bucket(sync_bucket);

  op_ret = RGWBucketAdminOp::sync_bucket(driver, op_state, s, y);
}

class RGWOp_Object_Remove: public RGWRESTOp {

public:
  RGWOp_Object_Remove() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "remove_object"; }
};

void RGWOp_Object_Remove::execute(optional_yield y)
{
  std::string bucket;
  std::string object;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object(object);

  op_ret = RGWBucketAdminOp::remove_object(driver, op_state, s, y);
}


RGWOp *RGWHandler_Bucket::op_get()
{

  if (s->info.args.sub_resource_exists("policy"))
    return new RGWOp_Get_Policy;

  if (s->info.args.sub_resource_exists("index"))
    return new RGWOp_Check_Bucket_Index;

  return new RGWOp_Bucket_Info;
}

RGWOp *RGWHandler_Bucket::op_put()
{
  if (s->info.args.sub_resource_exists("quota"))
    return new RGWOp_Set_Bucket_Quota;

  if (s->info.args.sub_resource_exists("sync"))
    return new RGWOp_Sync_Bucket;
  
  return new RGWOp_Bucket_Link;
}

RGWOp *RGWHandler_Bucket::op_post()
{
  return new RGWOp_Bucket_Unlink;
}

RGWOp *RGWHandler_Bucket::op_delete()
{
  if (s->info.args.sub_resource_exists("object"))
    return new RGWOp_Object_Remove;

  return new RGWOp_Bucket_Remove;
}
