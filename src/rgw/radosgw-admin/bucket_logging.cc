// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/bucket_logging.h"
#include <iostream>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_bucket_logging.h"
#include "driver/rados/rgw_bl_rados.h"
#include "rgw_common.h"
#include "radosgw-admin/bucket.h"


using namespace rgw_admin;
using namespace std;

namespace {



} // anonymous namespace

int rgw_admin_bucket_logging(const DoutPrefixProvider* dpp,
                                   rgw::sal::Driver* driver,
                                   ceph::Formatter* formatter,
                                   std::unique_ptr<rgw::sal::Bucket>& bucket,
                                   const rgw_admin_bucket_logging_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& bucket_id = *opts.bucket_id;
  int ret = 0;

  if (command == OPT::BUCKET_LOGGING_FLUSH) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    rgw::bucketlogging::configuration configuration;
    std::unique_ptr<rgw::sal::Bucket> target_bucket;
    ret =  rgw::bucketlogging::get_target_and_conf_from_source(dpp, driver, bucket.get(), tenant, configuration, target_bucket, null_yield);
    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: failed to get target bucket and logging conf from source bucket '"
        << bucket_name << "': " << cpp_strerror(-ret) << std::endl;
      return -ret;
    } else if (ret == -ENODATA) {
      cerr << "ERROR: bucket '" << bucket_name << "' does not have logging enabled" << std::endl;
      return 0;
    }

    // make sure that the logging source attribute is up-to-date
    if (ret = rgw::bucketlogging::update_bucket_logging_sources(dpp, target_bucket, bucket->get_key(), true, null_yield); ret < 0) {
      cerr << "WARNING: failed to update logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
        << "' in logging target '" << target_bucket->get_key() << "'. error: " << cpp_strerror(ret) << std::endl;
    }

    std::string obj_name;
    RGWObjVersionTracker objv_tracker;
    ret = target_bucket->get_logging_object_name(obj_name, configuration.target_prefix, null_yield, dpp, &objv_tracker);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: failed to get pending logging object name from target bucket '" << configuration.target_bucket <<
        "'. error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    std::string old_obj;
    const auto region = driver->get_zone()->get_zonegroup().get_api_name();
    ret = rgw::bucketlogging::rollover_logging_object(configuration, target_bucket, obj_name, dpp, region, bucket.get(), null_yield, true, &objv_tracker, false, &old_obj);
    if (ret < 0) {
      cerr << "ERROR: failed to flush pending logging object '" << obj_name << "' to target bucket '" << configuration.target_bucket
        << "'. error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    cout << "flushed pending logging object '" << old_obj
      << "' to target bucket '" << configuration.target_bucket << "'" << std::endl;
    return 0;
  }

  if (command == OPT::BUCKET_LOGGING_INFO) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    const auto& bucket_attrs = bucket->get_attrs();
    auto iter = bucket_attrs.find(RGW_ATTR_BUCKET_LOGGING);
    if (iter != bucket_attrs.end()) {
      rgw::bucketlogging::configuration configuration;
      try {
        configuration.enabled = true;
        decode(configuration, iter->second);
      } catch (buffer::error& err) {
        cerr << "ERROR: failed to decode logging attribute '" << RGW_ATTR_BUCKET_LOGGING
          << "'. error: " << err.what() << std::endl;
        return  EINVAL;
      }
      encode_json("logging", configuration, formatter);
      formatter->flush(cout);
    }
    iter = bucket_attrs.find(RGW_ATTR_BUCKET_LOGGING_SOURCES);
    if (iter != bucket_attrs.end()) {
      rgw::bucketlogging::source_buckets sources;
      try {
        decode(sources, iter->second);
      } catch (buffer::error& err) {
        cerr << "ERROR: failed to decode logging sources attribute '" << RGW_ATTR_BUCKET_LOGGING_SOURCES
          << "'. error: " << err.what() << std::endl;
        return  EINVAL;
      }
      encode_json("logging_sources", sources, formatter);
      formatter->flush(cout);
    }

    return 0;
  }

#ifdef WITH_RADOSGW_RADOS
  if (command == OPT::BUCKET_LOGGING_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (driver->get_name() != "rados") {
      cerr << "ERROR: this command is only available with the RADOS driver." << std::endl;
      return EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    rgw::bucketlogging::configuration configuration;
    std::unique_ptr<rgw::sal::Bucket> target_bucket;
    ret =  rgw::bucketlogging::get_target_and_conf_from_source(dpp,
         driver, bucket.get(), tenant, configuration, target_bucket, null_yield);
    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: failed to get target bucket and logging conf from source bucket '"
        << bucket_name << "': " << cpp_strerror(-ret) << std::endl;
      return -ret;
    } else if (ret == -ENODATA) {
      cerr << "ERROR: bucket '" << bucket_name << "' does not have logging enabled" << std::endl;
      return 0;
    }
    std::string target_prefix = configuration.target_prefix;
    std::set<std::string> entries;

    ret = rgw::bucketlogging::list_pending_commit_objects(dpp,
        static_cast<rgw::sal::RadosStore*>(driver), target_bucket.get(),
        target_prefix, entries, null_yield);

    if (ret < 0) {
      cerr << "ERROR: failed to get pending log entries for bucket '" << bucket_name
           << "': " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    formatter->open_array_section("pending_logs");
    for (auto &entry: entries) {
        formatter->dump_string("log", entry);
    }
    formatter->close_section(); // objs
    formatter->flush(cout);
    return 0;
  }
#endif

  return 0;
}

