// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/account.h"

#include <list>
#include <string>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/scope_guard.h"
#include "rgw_account.h"
#include "rgw_sal.h"
#include "radosgw-admin/util.h"

using ceph::Formatter;

namespace {

constexpr int DEFAULT_MAX_KEYS = 1000;

rgw::account::AdminOpState make_op_state(const rgw_admin_account_options& o)
{
  return rgw::account::AdminOpState{
    .account_id = o.account_id,
    .tenant = o.tenant,
    .account_name = o.account_name,
    .email = o.user_email,
    .max_users = o.max_users,
    .max_roles = o.max_roles,
    .max_groups = o.max_groups,
    .max_access_keys = o.max_access_keys,
    .max_buckets = o.max_buckets,
    .purge_data = o.purge_data,
  };
}

int handle_account_op(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      RGWStreamFlusher& stream_flusher,
                      const rgw_admin_account_options& o)
{
  auto op_state = make_op_state(o);
  std::string err_msg;
  int ret = 0;

  switch (o.command) {
  case rgw_admin::OPT::ACCOUNT_CREATE:
    ret = rgw::account::create(dpp, driver, op_state, err_msg,
                               stream_flusher, null_yield);
    if (ret < 0) {
      return rgw_admin::report_error("failed to create account", ret, err_msg);
    }
    break;

  case rgw_admin::OPT::ACCOUNT_MODIFY:
    ret = rgw::account::modify(dpp, driver, op_state, err_msg,
                               stream_flusher, null_yield);
    if (ret < 0) {
      return rgw_admin::report_error("failed to modify account", ret, err_msg);
    }
    break;

  case rgw_admin::OPT::ACCOUNT_GET:
    ret = rgw::account::info(dpp, driver, op_state, err_msg,
                             stream_flusher, null_yield);
    if (ret < 0) {
      return rgw_admin::report_error("failed to read account", ret, err_msg);
    }
    break;

  case rgw_admin::OPT::ACCOUNT_STATS:
    ret = rgw::account::stats(dpp, driver, op_state,
                              o.sync_stats, o.reset_stats, err_msg,
                              stream_flusher, null_yield);
    if (ret < 0) {
      return rgw_admin::report_error("failed to read account stats", ret, err_msg);
    }
    break;

  case rgw_admin::OPT::ACCOUNT_RM:
    ret = rgw::account::remove(dpp, driver, op_state, err_msg,
                               stream_flusher, null_yield);
    if (ret < 0) {
      return rgw_admin::report_error("failed to remove account", ret, err_msg);
    }
    break;

  default:
    return EINVAL;
  }

  return 0;
}

int handle_account_list(const DoutPrefixProvider* dpp,
                        rgw::sal::Driver* driver,
                        RGWStreamFlusher& stream_flusher,
                        const rgw_admin_account_options& o)
{
  if (o.max_entries && o.max_entries < 0) {
    return rgw_admin::report_error("invalid max entries", -EINVAL);
  }

  void* handle = nullptr;
  int ret = driver->meta_list_keys_init(dpp, "account", o.marker, &handle);
  if (ret < 0) {
    return rgw_admin::report_error("can't get key", ret);
  }

  auto handle_guard = make_scope_guard([&] {
    driver->meta_list_keys_complete(handle);
  });

  bool truncated = false;
  uint64_t count = 0;
  Formatter* formatter = stream_flusher.get_formatter();
  const bool limit_specified = o.max_entries.has_value();

  if (limit_specified) {
    formatter->open_object_section("result");
  }
  formatter->open_array_section("keys");

  do {
    std::list<std::string> keys;
    const uint64_t left = limit_specified
        ? static_cast<uint64_t>(*o.max_entries) - count
        : static_cast<uint64_t>(DEFAULT_MAX_KEYS);

    if (left == 0) {
      break;
    }

    ret = driver->meta_list_keys_next(dpp, handle, left, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      return rgw_admin::report_error("failed to list account keys", ret);
    }
    if (ret != -ENOENT) {
      for (const auto& key : keys) {
        formatter->dump_string("key", key);
        ++count;
      }
      formatter->flush(std::cout);
    }
  } while (truncated &&
           (!limit_specified ||
            count < static_cast<uint64_t>(*o.max_entries)));

  formatter->close_section(); // keys

  if (limit_specified) {
    encode_json("truncated", truncated, formatter);
    encode_json("count", count, formatter);
    if (truncated) {
      encode_json("marker", driver->meta_get_marker(handle), formatter);
    }
    formatter->close_section();
  }
  formatter->flush(std::cout);

  return 0;
}

} // anonymous namespace

int rgw_admin_account(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      RGWStreamFlusher& stream_flusher,
                      const rgw_admin_account_options& o)
{
  switch (o.command) {
  case rgw_admin::OPT::ACCOUNT_CREATE:
  case rgw_admin::OPT::ACCOUNT_MODIFY:
  case rgw_admin::OPT::ACCOUNT_GET:
  case rgw_admin::OPT::ACCOUNT_STATS:
  case rgw_admin::OPT::ACCOUNT_RM:
    return handle_account_op(dpp, driver, stream_flusher, o);

  case rgw_admin::OPT::ACCOUNT_LIST:
    return handle_account_list(dpp, driver, stream_flusher, o);

  default:
    return EINVAL;
  }
}