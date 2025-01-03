// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_op.h"
#include "rgw_usage.h"
#include "rgw_rest_usage.h"
#include "rgw_sal.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

class RGWOp_Usage_Get : public RGWRESTOp {

public:
  RGWOp_Usage_Get() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("usage", RGW_CAP_READ);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "get_usage"; }
};

void RGWOp_Usage_Get::execute(optional_yield y) {
  map<std::string, bool> categories;

  string uid_str;
  string bucket_name;
  string tenant;
  uint64_t start, end;
  bool show_entries;
  bool show_summary;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  std::unique_ptr<rgw::sal::User> user = driver->get_user(rgw_user(uid_str));
  std::unique_ptr<rgw::sal::Bucket> bucket;

  if (!bucket_name.empty()) {
    op_ret = driver->load_bucket(this, rgw_bucket(tenant, bucket_name),
                                 &bucket, null_yield);
    if (op_ret < 0) {
      return;
    }
  }

  RESTArgs::get_epoch(s, "start", 0, &start);
  RESTArgs::get_epoch(s, "end", (uint64_t)-1, &end);
  RESTArgs::get_bool(s, "show-entries", true, &show_entries);
  RESTArgs::get_bool(s, "show-summary", true, &show_summary);

  string cat_str;
  RESTArgs::get_string(s, "categories", cat_str, &cat_str);

  if (!cat_str.empty()) {
    list<string> cat_list;
    list<string>::iterator iter;
    get_str_list(cat_str, cat_list);
    for (iter = cat_list.begin(); iter != cat_list.end(); ++iter) {
      categories[*iter] = true;
    }
  }

  op_ret = RGWUsage::show(this, driver, user.get(), bucket.get(), start, end, show_entries, show_summary, &categories, flusher);
}

class RGWOp_Usage_Delete : public RGWRESTOp {

public:
  RGWOp_Usage_Delete() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("usage", RGW_CAP_WRITE);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "trim_usage"; }
};

void RGWOp_Usage_Delete::execute(optional_yield y) {
  string uid_str;
  string bucket_name;
  string tenant;
  uint64_t start, end;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "tenant", "", &tenant);
  std::unique_ptr<rgw::sal::User> user = driver->get_user(rgw_user(uid_str));
  std::unique_ptr<rgw::sal::Bucket> bucket;

  if (!bucket_name.empty()) {
    op_ret = driver->load_bucket(this, rgw_bucket(tenant, bucket_name),
                                 &bucket, null_yield);
    if (op_ret < 0) {
      return;
    }
  }

  RESTArgs::get_epoch(s, "start", 0, &start);
  RESTArgs::get_epoch(s, "end", (uint64_t)-1, &end);

  if (rgw::sal::User::empty(user.get()) &&
      bucket_name.empty() &&
      !start &&
      end == (uint64_t)-1) {
    bool remove_all;
    RESTArgs::get_bool(s, "remove-all", false, &remove_all);
    if (!remove_all) {
      op_ret = -EINVAL;
      return;
    }
  }

  op_ret = RGWUsage::trim(this, driver, user.get(), bucket.get(), start, end, y);
}

RGWOp *RGWHandler_Usage::op_get()
{
  return new RGWOp_Usage_Get;
}

RGWOp *RGWHandler_Usage::op_delete()
{
  return new RGWOp_Usage_Delete;
}


