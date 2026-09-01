// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/quota_ratelimit.h"
#include <iostream>
#include <string>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "rgw_account.h"
#include "rgw_user_types.h"
#include "rgw_sal.h"
#include "rgw_user.h"
using namespace rgw_admin;
using namespace std;

static const DoutPrefixProvider* g_admin_dpp;
#undef dpp
#define dpp g_admin_dpp

bool set_ratelimit_info(RGWRateLimitInfo& ratelimit, OPT command, int64_t max_read_ops, int64_t max_write_ops,
                    int64_t max_list_ops, int64_t max_delete_ops, int64_t max_read_bytes, int64_t max_write_bytes,
                    bool have_max_read_ops, bool have_max_write_ops, bool have_max_list_ops,
                    bool have_max_delete_ops, bool have_max_read_bytes, bool have_max_write_bytes)
{
  bool ratelimit_configured = true;
  switch (command) {
    case OPT::RATELIMIT_ENABLE:
    case OPT::GLOBAL_RATELIMIT_ENABLE:
      ratelimit.enabled = true;
      break;

    case OPT::RATELIMIT_SET:
    case OPT::GLOBAL_RATELIMIT_SET:
      ratelimit_configured = false;
      if (have_max_read_ops) {
        if (max_read_ops >= 0) {
          ratelimit.max_read_ops = max_read_ops;
          ratelimit_configured = true;
        }
      }
      if (have_max_write_ops) {
        if (max_write_ops >= 0) {
          ratelimit.max_write_ops = max_write_ops;
          ratelimit_configured = true;
        }
      }
      if (have_max_list_ops) {
        if (max_list_ops >= 0) {
          ratelimit.max_list_ops = max_list_ops;
          ratelimit_configured = true;
        }
      }
      if (have_max_delete_ops) {
        if (max_delete_ops >= 0) {
          ratelimit.max_delete_ops = max_delete_ops;
          ratelimit_configured = true;
        }
      }
      if (have_max_read_bytes) {
        if (max_read_bytes >= 0) {
          ratelimit.max_read_bytes = max_read_bytes;
          ratelimit_configured = true;
        }
      }
      if (have_max_write_bytes) {
        if (max_write_bytes >= 0) {
          ratelimit.max_write_bytes = max_write_bytes;
          ratelimit_configured = true;
        }
      }
      break;
    case OPT::RATELIMIT_DISABLE:
    case OPT::GLOBAL_RATELIMIT_DISABLE:
      ratelimit.enabled = false;
      break;
    default:
      break;
  }
  return ratelimit_configured;
}

void set_quota_info(RGWQuotaInfo& quota, OPT command, int64_t max_size, int64_t max_objects,
                    bool have_max_size, bool have_max_objects)
{
  switch (command) {
    case OPT::QUOTA_ENABLE:
    case OPT::GLOBAL_QUOTA_ENABLE:
      quota.enabled = true;

      // falling through on purpose

    case OPT::QUOTA_SET:
    case OPT::GLOBAL_QUOTA_SET:
      if (have_max_objects) {
        if (max_objects < 0) {
          quota.max_objects = -1;
        } else {
          quota.max_objects = max_objects;
        }
      }
      if (have_max_size) {
        if (max_size < 0) {
          quota.max_size = -1;
        } else {
          quota.max_size = rgw_rounded_kb(max_size) * 1024;
        }
      }
      break;
    case OPT::QUOTA_DISABLE:
    case OPT::GLOBAL_QUOTA_DISABLE:
      quota.enabled = false;
      break;
    default:
      break;
  }
}

int set_bucket_quota(rgw::sal::Driver* driver, OPT command,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_size, int64_t max_objects,
                     bool have_max_size, bool have_max_objects)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = driver->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                              &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket->get_info().quota, command, max_size, max_objects, have_max_size, have_max_objects);

  r = bucket->put_info(dpp, false, real_time(), null_yield);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_bucket_ratelimit(rgw::sal::Driver* driver, OPT command,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_read_ops, int64_t max_write_ops, int64_t max_list_ops,
                     int64_t max_delete_ops, int64_t max_read_bytes, int64_t max_write_bytes,
                     bool have_max_read_ops, bool have_max_write_ops, bool have_max_list_ops,
                     bool have_max_delete_ops, bool have_max_read_bytes, bool have_max_write_bytes)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = driver->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                              &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  RGWRateLimitInfo ratelimit_info;
  auto iter = bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != bucket->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  bool ratelimit_configured = set_ratelimit_info(ratelimit_info, command, max_read_ops, max_write_ops, max_list_ops,
                         max_delete_ops, max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops, have_max_list_ops,
                         have_max_delete_ops, have_max_read_bytes, have_max_write_bytes);
  if (!ratelimit_configured) {
    ldpp_dout(dpp, 0) << "ERROR: no rate limit values have been specified" << dendl;
    return -EINVAL;
  }
  bufferlist bl;
  ratelimit_info.encode(bl);
  rgw::sal::Attrs attr;
  attr[RGW_ATTR_RATELIMIT] = bl;
  r = bucket->merge_and_store_attrs(dpp, attr, null_yield);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_user_ratelimit(OPT command, std::unique_ptr<rgw::sal::User>& user,
                     int64_t max_read_ops, int64_t max_write_ops, int64_t max_list_ops,
                     int64_t max_delete_ops, int64_t max_read_bytes, int64_t max_write_bytes,
                     bool have_max_read_ops, bool have_max_write_ops, bool have_max_list_ops,
                     bool have_max_delete_ops, bool have_max_read_bytes, bool have_max_write_bytes)
{
  RGWRateLimitInfo ratelimit_info;
  user->load_user(dpp, null_yield);
  auto iter = user->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != user->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  bool ratelimit_configured = set_ratelimit_info(ratelimit_info, command, max_read_ops, max_write_ops, max_list_ops,
                         max_delete_ops, max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops, have_max_list_ops,
                         have_max_delete_ops, have_max_read_bytes, have_max_write_bytes);
  if (!ratelimit_configured) {
    ldpp_dout(dpp, 0) << "ERROR: no rate limit values have been specified" << dendl;
    return -EINVAL;
  }
  bufferlist bl;
  ratelimit_info.encode(bl);
  rgw::sal::Attrs attr;
  attr[RGW_ATTR_RATELIMIT] = bl;
  int r = user->merge_and_store_attrs(dpp, attr, null_yield);
  if (r < 0) {
    cerr << "ERROR: failed writing user instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int show_user_ratelimit(std::unique_ptr<rgw::sal::User>& user, Formatter *formatter)
{
  RGWRateLimitInfo ratelimit_info;
  user->load_user(dpp, null_yield);
  auto iter = user->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != user->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  } else {
    return -ENOENT;
  }

  formatter->open_object_section("user_ratelimit");
  encode_json("user_ratelimit", ratelimit_info, formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}

int show_bucket_ratelimit(rgw::sal::Driver* driver, const string& tenant_name,
                          const string& bucket_name, Formatter *formatter)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = driver->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                              &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  RGWRateLimitInfo ratelimit_info;
  auto iter = bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
  if (iter != bucket->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  formatter->open_object_section("bucket_ratelimit");
  encode_json("bucket_ratelimit", ratelimit_info, formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}
int set_user_bucket_quota(OPT command, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                          bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.quota.bucket_quota, command, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_bucket_quota(user_info.quota.bucket_quota);

  string err;
  int r = user.modify(dpp, op_state, null_yield, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int set_user_quota(OPT command, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                   bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.quota.user_quota, command, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_user_quota(user_info.quota.user_quota);

  string err;
  int r = user.modify(dpp, op_state, null_yield, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}



int rgw_admin_quota_ratelimit(const DoutPrefixProvider* dpp,
                              rgw::sal::Driver* driver,
                              RGWStreamFlusher& stream_flusher,
                              Formatter* formatter,
                              RGWUser& ruser,
                              RGWUserAdminOpState& user_op,
                              std::unique_ptr<rgw::sal::User>& user,
                              const rgw_admin_quota_ratelimit_options& opts)
{
  g_admin_dpp = dpp;
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& account_id = *opts.account_id;
  auto& account_name = *opts.account_name;
  auto& quota_scope = *opts.quota_scope;
  auto& ratelimit_scope = *opts.ratelimit_scope;
  int64_t max_size = opts.max_size;
  int64_t max_objects = opts.max_objects;
  int64_t max_read_ops = opts.max_read_ops;
  int64_t max_write_ops = opts.max_write_ops;
  int64_t max_list_ops = opts.max_list_ops;
  int64_t max_delete_ops = opts.max_delete_ops;
  int64_t max_read_bytes = opts.max_read_bytes;
  int64_t max_write_bytes = opts.max_write_bytes;
  bool have_max_size = opts.have_max_size;
  bool have_max_objects = opts.have_max_objects;
  bool have_max_read_ops = opts.have_max_read_ops;
  bool have_max_write_ops = opts.have_max_write_ops;
  bool have_max_list_ops = opts.have_max_list_ops;
  bool have_max_delete_ops = opts.have_max_delete_ops;
  bool have_max_read_bytes = opts.have_max_read_bytes;
  bool have_max_write_bytes = opts.have_max_write_bytes;
  int ret = 0;
  bool quota_op = (command == OPT::QUOTA_SET || command == OPT::QUOTA_ENABLE || command == OPT::QUOTA_DISABLE);

  if (quota_op) {
    if (!bucket_name.empty()) {
      if (!quota_scope.empty() && quota_scope != "bucket") {
        cerr << "ERROR: invalid quota scope specification." << std::endl;
        return EINVAL;
      }
      set_bucket_quota(driver, command, tenant, bucket_name,
                       max_size, max_objects, have_max_size, have_max_objects);
    } else if (!rgw::sal::User::empty(user)) {
      if (quota_scope == "bucket") {
        return set_user_bucket_quota(command, ruser, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else if (quota_scope == "user") {
        return set_user_quota(command, ruser, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else {
        cerr << "ERROR: invalid quota scope specification. Please specify either --quota-scope=bucket, or --quota-scope=user" << std::endl;
        return EINVAL;
      }
    } else if (!account_id.empty() || !account_name.empty()) {
      // set account quota
      rgw::account::AdminOpState op_state;
      op_state.account_id = account_id;
      op_state.tenant = tenant;
      op_state.account_name = account_name;

      if (quota_scope != "bucket" && quota_scope != "account") {
        cerr << "ERROR: invalid quota scope specification. Please specify "
            "either --quota-scope=bucket or --quota-scope=account" << std::endl;
        return EINVAL;
      }
      op_state.quota_scope = quota_scope;

      if (command == OPT::QUOTA_ENABLE) {
        op_state.quota_enabled = true;
      } else if (command == OPT::QUOTA_DISABLE) {
        op_state.quota_enabled = false;
      }
      if (have_max_objects) {
        op_state.quota_max_objects = std::max<int64_t>(-1, max_objects);
      }
      if (have_max_size) {
        if (max_size < 0) {
          op_state.quota_max_size = -1;
        } else {
          op_state.quota_max_size = rgw_rounded_kb(max_size) * 1024;
        }
      }

      std::string err_msg;
      ret = rgw::account::modify(dpp, driver, op_state, err_msg,
                                 stream_flusher, null_yield);
      if (ret < 0) {
        cerr << "ERROR: failed to set account quota with "
            << cpp_strerror(-ret) << ": " << err_msg << std::endl;
        return -ret;
      }
    } else {
      cerr << "ERROR: bucket name or uid or account is required for quota operation" << std::endl;
      return EINVAL;
    }
  }

  bool ratelimit_op_set = (command == OPT::RATELIMIT_SET || command == OPT::RATELIMIT_ENABLE || command == OPT::RATELIMIT_DISABLE);
  bool ratelimit_op_get = command == OPT::RATELIMIT_GET;
  if (ratelimit_op_set) {
    if (bucket_name.empty() && rgw::sal::User::empty(user)) {
      cerr << "ERROR: bucket name or uid is required for ratelimit operation" << std::endl;
      return EINVAL;
    }

    if (!bucket_name.empty()) {
      if (!ratelimit_scope.empty() && ratelimit_scope != "bucket") {
        cerr << "ERROR: invalid ratelimit scope specification. (bucket scope is not bucket but bucket has been specified)" << std::endl;
        return EINVAL;
      }
      return set_bucket_ratelimit(driver, command, tenant, bucket_name,
                           max_read_ops, max_write_ops, max_list_ops, max_delete_ops,
                           max_read_bytes, max_write_bytes,
                           have_max_read_ops, have_max_write_ops, have_max_list_ops, have_max_delete_ops,
                           have_max_read_bytes, have_max_write_bytes);
    } else if (!rgw::sal::User::empty(user)) {
      if (ratelimit_scope == "user") {
        return set_user_ratelimit(command, user, max_read_ops, max_write_ops, max_list_ops, max_delete_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops, have_max_list_ops, have_max_delete_ops,
                         have_max_read_bytes, have_max_write_bytes);
      } else {
        cerr << "ERROR: invalid ratelimit scope specification. Please specify either --ratelimit-scope=bucket, or --ratelimit-scope=user" << std::endl;
        return EINVAL;
      }
    }
  }

  if (ratelimit_op_get) {
    if (bucket_name.empty() && rgw::sal::User::empty(user)) {
      cerr << "ERROR: bucket name or uid is required for ratelimit operation" << std::endl;
      return EINVAL;
    }

    if (!bucket_name.empty()) {
      if (!ratelimit_scope.empty() && ratelimit_scope != "bucket") {
        cerr << "ERROR: invalid ratelimit scope specification. (bucket scope is not bucket but bucket has been specified)" << std::endl;
        return EINVAL;
      }
      return show_bucket_ratelimit(driver, tenant, bucket_name, formatter);
    } else if (!rgw::sal::User::empty(user)) {
      if (ratelimit_scope == "user") {
        int ret = show_user_ratelimit(user, formatter);
        if (ret < 0) {
          std::cerr << "ERROR: failed to get a ratelimit for user id: '" << user->get_id() << "', errno: " << cpp_strerror(-ret) << std::endl;
        }
        return ret;
      } else {
        cerr << "ERROR: invalid ratelimit scope specification. Please specify either --ratelimit-scope=bucket, or --ratelimit-scope=user" << std::endl;
        return EINVAL;
      }
    }
  }


  return 0;
}
