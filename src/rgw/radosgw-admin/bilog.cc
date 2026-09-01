// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/bilog.h"
#include <iostream>
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_zone.h"
#include "rgw_trim_bilog.h"
#include "rgw_coroutine.h"
#include "rgw_http_client.h"
#include "radosgw-admin/bucket.h"


using namespace rgw_admin;
using namespace std;

namespace {



} // anonymous namespace

int rgw_admin_bilog(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    ceph::Formatter* formatter,
                    std::unique_ptr<rgw::sal::Bucket>& bucket,
                    const rgw_admin_bilog_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& bucket_id = *opts.bucket_id;
  auto& marker = *opts.marker;
  auto& start_marker = *opts.start_marker;
  auto& end_marker = *opts.end_marker;
  auto& gen = *opts.gen;
  int max_entries = opts.max_entries;
  int shard_id = opts.shard_id;
  bool yes_i_really_mean_it = opts.yes_i_really_mean_it;
  int ret = 0;

  if (command == OPT::BILOG_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    const auto& logs = bucket->get_info().layout.logs;
    auto log_layout = std::reference_wrapper{logs.back()};
    if (gen) {
      auto i = std::find_if(logs.begin(), logs.end(), rgw::matches_gen(*gen));
      if (i == logs.end()) {
        cerr << "ERROR: no log layout with gen=" << *gen << std::endl;
        return ENOENT;
      }
      log_layout = *i;
    }

    do {
      list<rgw_bi_log_entry> entries;
      ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->bilog_rados->log_list(dpp, null_yield, bucket->get_info(), log_layout, shard_id, marker, max_entries - count, entries, &truncated);
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (list<rgw_bi_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_bi_log_entry& entry = *iter;
        encode_json("entry", entry, formatter);

        marker = entry.id;
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }
  if (command == OPT::BILOG_TRIM) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    if (!gen) {
      gen = 0;
    }
    ret = bilog_trim(dpp, null_yield, static_cast<rgw::sal::RadosStore*>(driver),
		     bucket->get_info(), *gen,
		     shard_id, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::BILOG_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<int, string> markers;
    const auto& logs = bucket->get_info().layout.logs;
    auto log_layout = std::reference_wrapper{logs.back()};
    if (gen) {
      auto i = std::find_if(logs.begin(), logs.end(), rgw::matches_gen(*gen));
      if (i == logs.end()) {
        cerr << "ERROR: no log layout with gen=" << *gen << std::endl;
        return ENOENT;
      }
      log_layout = *i;
    }

    ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->bilog_rados->get_log_status(dpp, bucket->get_info(), log_layout, shard_id,
						    &markers, null_yield);
    if (ret < 0) {
      cerr << "ERROR: get_bi_log_status(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("entries");
    encode_json("markers", markers, formatter);
    formatter->dump_string("current_time",
			   to_iso_8601(ceph::real_clock::now(),
				       iso_8601_format::YMDhms));
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::BILOG_AUTOTRIM) {
    // The background sync-log-trim thread only runs bucket trim on zones whose
    // sync module exports data. Non-exporting zones (e.g. archive) deliberately
    // forbid bucket-instance removal. Likewise, here, we add the same guard for
    // user triggered auto-trim.
    if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->sync_module_exports_data() &&
        !yes_i_really_mean_it) {
      cerr << "This zone's sync module does not export data (e.g. an archive zone). "
              "bilog autotrim can remove bucket instance metadata that this zone type "
              "is meant to retain.\n"
              "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EPERM;
    }

    RGWCoroutinesManager crs(driver->ctx(), driver->get_cr_registry());
    RGWHTTPManager http(driver->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    rgw::BucketTrimConfig config;
    configure_bucket_trim(driver->ctx(), config);

    rgw::BucketTrimManager trim(static_cast<rgw::sal::RadosStore*>(driver), config);
    ret = trim.init();
    if (ret < 0) {
      cerr << "trim manager init failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
    ret = crs.run(dpp, trim.create_admin_bucket_trim_cr(&http));
    if (ret < 0) {
      cerr << "automated bilog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  return 0;
}

