// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/user.h"

#include <iostream>
#include <list>
#include <string>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "driver/rados/rgw_bucket.h"
#include "include/utime.h"
#include "rgw_account.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_sal.h"
#include "rgw_user.h"

using ceph::Formatter;
using namespace std;

namespace {

void show_user_info(RGWUserInfo& info, Formatter* formatter)
{
  encode_json("user_info", info, formatter);
  formatter->flush(std::cout);
  std::cout << std::endl;
}

void show_policy_arns(const boost::container::flat_set<std::string>& arns,
                      Formatter* formatter)
{
  formatter->open_array_section("AttachedPolicies");
  for (const auto& arn : arns) {
    formatter->dump_string("PolicyArn", arn);
  }
  formatter->close_section();
}

int init_bucket(const DoutPrefixProvider* dpp,
                rgw::sal::Driver* driver,
                const std::string& tenant_name,
                const std::string& bucket_name,
                const std::string& bucket_id,
                std::unique_ptr<rgw::sal::Bucket>* out_bucket)
{
  rgw_bucket b{tenant_name, bucket_name, bucket_id};
  return driver->load_bucket(dpp, b, out_bucket, null_yield);
}

} // anonymous namespace

int rgw_admin_user_mutate(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          Formatter* formatter,
                          RGWUser& ruser,
                          RGWUserAdminOpState& user_op,
                          std::unique_ptr<rgw::sal::User>& user,
                          const rgw_admin_user_mutate_options& opts,
                          std::string& err_msg)
{
  int ret = 0;
  bool output_user_info = true;
  RGWUserInfo info;

  switch (opts.command) {
  case rgw_admin::OPT::USER_INFO:
    if (rgw::sal::User::empty(user) && opts.access_key->empty()) {
      cerr << "ERROR: --uid or --access-key required" << std::endl;
      return EINVAL;
    }
    break;
  case rgw_admin::OPT::USER_CREATE:
    if (!user_op.has_existing_user() && (opts.generate_key != 0)) {
      user_op.set_generate_key(); // generate a new key by default
    }
    ret = ruser.add(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      if (ret == -ERR_INVALID_TENANT_NAME)
	ret = -EINVAL;

      return -ret;
    }
    if (!opts.subuser->empty()) {
      ret = ruser.subusers.add(dpp, user_op, null_yield, &err_msg);
      if (ret < 0) {
        cerr << "could not create subuser: " << err_msg << std::endl;
        return -ret;
      }
    }
    break;
  case rgw_admin::OPT::USER_RM:
    ret = ruser.remove(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove user: " << err_msg << std::endl;
      return -ret;
    }

    output_user_info = false;
    break;
  case rgw_admin::OPT::USER_RENAME:
    if (opts.yes_i_really_mean_it) {
      user_op.set_overwrite_new_user(true);
    }
    ret = ruser.rename(user_op, null_yield, dpp, &err_msg);
    if (ret < 0) {
      if (ret == -EEXIST) {
        err_msg += ". to overwrite this user, add --yes-i-really-mean-it";
      }
      cerr << "could not rename user: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case rgw_admin::OPT::USER_ENABLE:
  case rgw_admin::OPT::USER_SUSPEND:
  case rgw_admin::OPT::USER_MODIFY:
    ret = ruser.modify(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not modify user: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case rgw_admin::OPT::SUBUSER_CREATE:
    ret = ruser.subusers.add(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case rgw_admin::OPT::SUBUSER_MODIFY:
    ret = ruser.subusers.modify(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not modify subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case rgw_admin::OPT::SUBUSER_RM:
    ret = ruser.subusers.remove(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case rgw_admin::OPT::KEY_CREATE:
    ret = ruser.keys.add(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not create key: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case rgw_admin::OPT::KEY_RM:
    ret = ruser.keys.remove(dpp, user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove key: " << err_msg << std::endl;
      return -ret;
    }
    break;
  default:
    return EINVAL;
  }

  if (output_user_info) {
    ret = ruser.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }
    show_user_info(info, formatter);
  }

  return 0;
}

int rgw_admin_user_query(const DoutPrefixProvider* dpp,
                         rgw::sal::Driver* driver,
                         Formatter* formatter,
                         RGWFormatterFlusher& stream_flusher,
                         std::unique_ptr<rgw::sal::User>& user,
                         std::unique_ptr<rgw::sal::Bucket>& bucket,
                         const rgw_admin_user_query_options& opts)
{
  const std::string& tenant = *opts.tenant;
  const std::string& bucket_name = *opts.bucket_name;
  const std::string& bucket_id = *opts.bucket_id;
  const std::string& account_id = *opts.account_id;
  const std::string& account_name = *opts.account_name;
  const std::string& path_prefix = *opts.path_prefix;
  const std::string& marker = *opts.marker;
  const std::string& policy_arn = *opts.policy_arn;
  int max_entries = opts.max_entries;
  bool max_entries_specified = opts.max_entries_specified;
  int ret = 0;

  switch (opts.command) {
  case rgw_admin::OPT::USER_CHECK:
    check_bad_owner_bucket_mapping(driver, user->get_id(),
                                   user->get_display_name(), user->get_tenant(),
                                   opts.fix, null_yield, dpp);
    break;
  case rgw_admin::OPT::USER_STATS: {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    if (opts.reset_stats) {
      if (!bucket_name.empty()) {
	cerr << "ERROR: --reset-stats does not work on buckets and "
	  "bucket specified" << std::endl;
	return EINVAL;
      }
      if (opts.sync_stats) {
	cerr << "ERROR: sync-stats includes the reset-stats functionality, "
	  "so at most one of the two should be specified" << std::endl;
	return EINVAL;
      }
      ret = driver->reset_stats(dpp, null_yield, user->get_id());
      if (ret < 0) {
	cerr << "ERROR: could not reset user stats: " << cpp_strerror(-ret) <<
	  std::endl;
	return -ret;
      }
    }

    if (opts.sync_stats) {
      if (!bucket_name.empty()) {
        ret = init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
        if (ret < 0) {
          cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        ret = bucket->sync_owner_stats(dpp, null_yield, nullptr);
        if (ret < 0) {
          cerr << "ERROR: could not sync bucket stats: " <<
	    cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      } else {
        ret = rgw_sync_all_stats(dpp, null_yield, driver,
                                 user->get_id(), user->get_tenant());
        if (ret < 0) {
          cerr << "ERROR: could not sync user stats: " <<
	    cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      }
    }

    ret = user->load_user(dpp, null_yield);
    if (ret < 0) {
      cerr << "User has not been initialized or user does not exist" << std::endl;
      return -ret;
    }

    const RGWUserInfo& info = user->get_info();
    rgw_owner owner = info.user_id;
    if (!info.account_id.empty()) {
      cerr << "Reading stats for user account " << info.account_id << std::endl;
      owner = info.account_id;
    }

    constexpr bool omit_utilized_stats = false;
    RGWStorageStats stats(omit_utilized_stats);
    ceph::real_time last_stats_sync;
    ceph::real_time last_stats_update;
    ret = driver->load_stats(dpp, null_yield, owner, stats,
                             last_stats_sync, last_stats_update);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "User has not been initialized or user does not exist" << std::endl;
      } else {
        cerr << "ERROR: can't read user: " << cpp_strerror(ret) << std::endl;
      }
      return -ret;
    }

    {
      Formatter::ObjectSection os(*formatter, "result");
      encode_json("stats", stats, formatter);
      utime_t last_sync_ut(last_stats_sync);
      encode_json("last_stats_sync", last_sync_ut, formatter);
      utime_t last_update_ut(last_stats_update);
      encode_json("last_stats_update", last_update_ut, formatter);
    }
    formatter->flush(std::cout);
    break;
  }
  case rgw_admin::OPT::USER_POLICY_ATTACH: {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    if (policy_arn.empty()) {
      cerr << "policy arn is empty" << std::endl;
      return EINVAL;
    }
    ret = user->load_user(dpp, null_yield);
    if (ret < 0) {
      return -ret;
    }
    if (user->get_info().account_id.empty()) {
      std::cerr << "Managed policies are only supported for account users" << std::endl;
      return EINVAL;
    }

    try {
      if (!rgw::IAM::get_managed_policy(g_ceph_context, policy_arn)) {
        cerr << "unrecognized policy arn " << policy_arn << std::endl;
        return ENOENT;
      }
    } catch (rgw::IAM::PolicyParseException& e) {
      cerr << "failed to parse managed policy: " << e.what() << std::endl;
      return EINVAL;
    }

    rgw::IAM::ManagedPolicies policies;
    auto& attrs = user->get_attrs();
    if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) {
      decode(policies, it->second);
    }
    const bool inserted = policies.arns.insert(policy_arn).second;
    if (!inserted) {
      cout << "That managed policy is already attached." << std::endl;
      return EEXIST;
    }

    bufferlist in_bl;
    encode(policies, in_bl);
    attrs[RGW_ATTR_MANAGED_POLICY] = in_bl;

    ret = user->store_user(dpp, null_yield, false);
    if (ret < 0) {
      return -ret;
    }
    cout << "Managed policy attached successfully" << std::endl;
    return 0;
  }
  case rgw_admin::OPT::USER_POLICY_DETACH: {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    if (policy_arn.empty()) {
      cerr << "policy arn is empty" << std::endl;
      return EINVAL;
    }
    ret = user->load_user(dpp, null_yield);
    if (ret < 0) {
      return -ret;
    }

    rgw::IAM::ManagedPolicies policies;
    auto& attrs = user->get_attrs();
    if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) {
      decode(policies, it->second);
    }

    auto i = policies.arns.find(policy_arn);
    if (i == policies.arns.end()) {
      cout << "That managed policy is not attached." << std::endl;
      return ENOENT;
    }
    policies.arns.erase(i);

    bufferlist in_bl;
    encode(policies, in_bl);
    attrs[RGW_ATTR_MANAGED_POLICY] = in_bl;

    ret = user->store_user(dpp, null_yield, false);
    if (ret < 0) {
      return -ret;
    }
    cout << "Managed policy detached successfully" << std::endl;
    return 0;
  }
  case rgw_admin::OPT::USER_POLICY_LIST_ATTACHED: {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: uid not specified" << std::endl;
      return -EINVAL;
    }
    ret = user->load_user(dpp, null_yield);
    if (ret < 0) {
      return -ret;
    }

    rgw::IAM::ManagedPolicies policies;
    auto& attrs = user->get_attrs();
    if (auto it = attrs.find(RGW_ATTR_MANAGED_POLICY); it != attrs.end()) {
      decode(policies, it->second);
    }

    show_policy_arns(policies.arns, formatter);
    formatter->flush(std::cout);
    return 0;
  }
  case rgw_admin::OPT::USER_LIST: {
    if (!account_id.empty() || !account_name.empty()) {
      rgw::account::AdminOpState op_state;
      op_state.account_id = account_id;
      op_state.tenant = tenant;
      op_state.account_name = account_name;

      std::string err_msg;
      ret = rgw::account::list_users(
          dpp, driver, op_state, path_prefix, marker,
          max_entries_specified, max_entries, opts.account_root,
          err_msg, stream_flusher, null_yield);
      if (ret < 0)  {
        cerr << "ERROR: " << err_msg << std::endl;
        return -ret;
      }
      return 0;
    }

    void *handle = nullptr;
    int max = 1000;
    ret = driver->meta_list_keys_init(dpp, "user", marker, &handle);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool truncated = false;
    uint64_t count = 0;

    if (max_entries_specified) {
      formatter->open_object_section("result");
    }
    formatter->open_array_section("keys");

    uint64_t left = 0;
    do {
      list<string> keys;
      left = (max_entries_specified ? max_entries - count : max);
      ret = driver->meta_list_keys_next(dpp, handle, left, keys, &truncated);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (ret != -ENOENT) {
	for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
	  formatter->dump_string("key", *iter);
          ++count;
	}
	formatter->flush(std::cout);
      }
    } while (truncated && left > 0);

    formatter->close_section();

    if (max_entries_specified) {
      encode_json("truncated", truncated, formatter);
      encode_json("count", count, formatter);
      if (truncated) {
        encode_json("marker", driver->meta_get_marker(handle), formatter);
      }
      formatter->close_section();
    }
    formatter->flush(std::cout);

    driver->meta_list_keys_complete(handle);
    break;
  }
  default:
    return EINVAL;
  }
  return 0;
}
