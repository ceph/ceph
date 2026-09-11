// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/usage.h"

#include <iostream>
#include <string>

#include "include/utime.h"
#include "common/errno.h"
#include "rgw_sal.h"
#include "rgw_usage.h"

using ceph::Formatter;
using namespace std;

namespace {

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

int rgw_admin_usage(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    ceph::Formatter* formatter,
                    RGWFormatterFlusher& stream_flusher,
                    std::unique_ptr<rgw::sal::User>& user,
                    std::unique_ptr<rgw::sal::Bucket>& bucket,
                    rgw_admin_usage_options& opts)
{
  const std::string& tenant = opts.tenant;
  const std::string& bucket_name = opts.bucket_name;
  const std::string& bucket_id = opts.bucket_id;
  const std::string& start_date = opts.start_date;
  const std::string& end_date = opts.end_date;

  switch (opts.command) {
  case rgw_admin::OPT::USAGE_SHOW: {
    uint64_t start_epoch = 0;
    uint64_t end_epoch = (uint64_t)-1;
    int ret;

    if (!start_date.empty()) {
      ret = utime_t::parse_date(start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }
    if (!end_date.empty()) {
      ret = utime_t::parse_date(end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }

    if (!bucket_name.empty()) {
      ret = init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
	cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }
    ret = RGWUsage::show(dpp, driver, user.get(), bucket.get(), start_epoch,
			 end_epoch, opts.show_log_entries, opts.show_log_sum,
                         &opts.categories, stream_flusher);
    if (ret < 0) {
      cerr << "ERROR: failed to show usage" << std::endl;
      return 1;
    }
    return 0;
  }
  case rgw_admin::OPT::USAGE_TRIM: {
    if (rgw::sal::User::empty(user) && bucket_name.empty() &&
	start_date.empty() && end_date.empty() && !opts.yes_i_really_mean_it) {
      cerr << "usage trim without user/date/bucket specified will remove *all* users data" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }
    int ret;
    uint64_t start_epoch = 0;
    uint64_t end_epoch = (uint64_t)-1;

    if (!start_date.empty()) {
      ret = utime_t::parse_date(start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }

    if (!end_date.empty()) {
      ret = utime_t::parse_date(end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }

    if (!bucket_name.empty()) {
      ret = init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
	cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }
    ret = RGWUsage::trim(dpp, driver, user.get(), bucket.get(), start_epoch,
                         end_epoch, null_yield);
    if (ret < 0) {
      cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
      return 1;
    }
    return 0;
  }
  case rgw_admin::OPT::USAGE_CLEAR: {
    if (!opts.yes_i_really_mean_it) {
      cerr << "usage clear would remove *all* users usage data for all time" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }

    int ret = RGWUsage::clear(dpp, driver, null_yield);
    if (ret < 0) {
      return ret;
    }
    return 0;
  }
  default:
    return EINVAL;
  }
}
