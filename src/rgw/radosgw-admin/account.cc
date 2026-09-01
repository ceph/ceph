// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/account.h"

#include <iostream>
#include <list>
#include <string>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "rgw_account.h"
#include "rgw_sal.h"

using ceph::Formatter;

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
  case rgw_admin::OPT::ACCOUNT_RM: {
    auto op_state = rgw::account::AdminOpState{
      .account_id = std::string(o.account_id),
      .tenant = std::string(o.tenant),
      .account_name = std::string(o.account_name),
      .email = std::string(o.user_email),
      .max_users = o.max_users ? *o.max_users : std::nullopt,
      .max_roles = o.max_roles ? *o.max_roles : std::nullopt,
      .max_groups = o.max_groups ? *o.max_groups : std::nullopt,
      .max_access_keys = o.max_access_keys ? *o.max_access_keys : std::nullopt,
      .max_buckets = o.max_buckets ? *o.max_buckets : std::nullopt,
      .purge_data = o.purge_data,
    };

    std::string err_msg;
    int ret = 0;

    if (o.command == rgw_admin::OPT::ACCOUNT_CREATE) {
      ret = rgw::account::create(dpp, driver, op_state, err_msg,
                                 stream_flusher, null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: failed to create account with " << cpp_strerror(-ret)
            << ": " << err_msg << std::endl;
        return -ret;
      }
    }

    if (o.command == rgw_admin::OPT::ACCOUNT_MODIFY) {
      ret = rgw::account::modify(dpp, driver, op_state, err_msg,
                                 stream_flusher, null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: failed to modify account with " << cpp_strerror(-ret)
            << ": " << err_msg << std::endl;
        return -ret;
      }
    }

    if (o.command == rgw_admin::OPT::ACCOUNT_GET) {
      ret = rgw::account::info(dpp, driver, op_state, err_msg,
                               stream_flusher, null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: failed to read account with " << cpp_strerror(-ret)
            << ": " << err_msg << std::endl;
        return -ret;
      }
    }

    if (o.command == rgw_admin::OPT::ACCOUNT_STATS) {
      ret = rgw::account::stats(dpp, driver, op_state,
                                o.sync_stats, o.reset_stats, err_msg,
                                stream_flusher, null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: failed to read account stats with " << cpp_strerror(-ret)
            << ": " << err_msg << std::endl;
        return -ret;
      }
    }

    if (o.command == rgw_admin::OPT::ACCOUNT_RM) {
      ret = rgw::account::remove(dpp, driver, op_state, err_msg,
                                 stream_flusher, null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: failed to remove account with " << cpp_strerror(-ret)
            << ": " << err_msg << std::endl;
        return -ret;
      }
    }
    return 0;
  }

  case rgw_admin::OPT::ACCOUNT_LIST: {
    void* handle = nullptr;
    int max = 1000;
    int ret = driver->meta_list_keys_init(dpp, "account", std::string(o.marker), &handle);
    if (ret < 0) {
      std::cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool truncated = false;
    uint64_t count = 0;
    Formatter* formatter = stream_flusher.get_formatter();

    if (o.max_entries_specified) {
      formatter->open_object_section("result");
    }
    formatter->open_array_section("keys");

    uint64_t left = 0;
    do {
      std::list<std::string> keys;
      left = (o.max_entries_specified ? o.max_entries - count : max);
      ret = driver->meta_list_keys_next(dpp, handle, left, keys, &truncated);
      if (ret < 0 && ret != -ENOENT) {
        std::cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (ret != -ENOENT) {
        for (const auto& key : keys) {
          formatter->dump_string("key", key);
          ++count;
        }
        formatter->flush(std::cout);
      }
    } while (truncated && left > 0);

    formatter->close_section();

    if (o.max_entries_specified) {
      encode_json("truncated", truncated, formatter);
      encode_json("count", count, formatter);
      if (truncated) {
        encode_json("marker", driver->meta_get_marker(handle), formatter);
      }
      formatter->close_section();
    }
    formatter->flush(std::cout);

    driver->meta_list_keys_complete(handle);
    return 0;
  }

  default:
    return EINVAL;
  }
}
