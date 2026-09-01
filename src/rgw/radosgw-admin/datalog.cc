// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/datalog.h"

#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "common/async/context_pool.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "driver/rados/rgw_log_backing.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw/async_utils.h"
#include "rgw_coroutine.h"
#include "rgw_datalog.h"
#include "rgw_http_client.h"
#include "rgw_trim_datalog.h"

using namespace rgw_admin;
using namespace std;

int rgw_admin_datalog(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      ceph::async::io_context_pool& context_pool,
                      ceph::Formatter* formatter,
                      const rgw_admin_datalog_options& opts)
{
  auto& command = opts.command;
  auto& marker = *opts.marker;
  auto& start_marker = *opts.start_marker;
  auto& end_marker = *opts.end_marker;
  auto& start_date = *opts.start_date;
  auto& end_date = *opts.end_date;
  auto& opt_log_type = *opts.opt_log_type;
  auto& count = *opts.count;
  int max_entries = opts.max_entries;
  int shard_id = opts.shard_id;
  bool specified_shard_id = opts.specified_shard_id;
  bool extra_info = opts.extra_info;
  int ret = 0;

  if (command == OPT::DATALOG_SEMAPHORE_LIST) {
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)
      ->svc()->datalog_rados;
    std::optional<int> shard;
    if (specified_shard_id) {
      shard = shard_id;
    }
    std::string err;
    ret = rgw::run_coro(dpp, context_pool,
                        datalog->admin_sem_list(shard, max_entries, marker,
                                                cout, *formatter),
                        &err);
    if (ret < 0) {
      std::cerr << "datalog semaphore list: " << err << std::endl;
      return ret;
    }
  }

  if (command == OPT::DATALOG_SEMAPHORE_RESET) {
    if (marker.empty()) {
      std::cerr << "Specify the semaphore key with --marker." << std::endl;
      return -EINVAL;
    }
    std::string errstr;
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)
      ->svc()->datalog_rados;
    ret = rgw::run_coro(dpp, context_pool,
                        datalog->admin_sem_reset(marker, count.value_or(0)),
                        &errstr);
    if (ret < 0) {
      std::cerr << "datalog semaphore reset: " << errstr << std::endl;
      return ret;
    }
  }

  if (command == OPT::DATALOG_LIST) {
    formatter->open_array_section("entries");
    bool truncated = false;
    int entry_count = 0;
    if (max_entries < 0) {
      max_entries = 1000;
    }
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      std::cerr << "end-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      if (marker.empty()) {
        marker = start_marker;
      } else {
        std::cerr << "start-marker and marker not both allowed." << std::endl;
        return -EINVAL;
      }
    }

    auto datalog_svc = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    RGWDataChangesLogMarker log_marker;

    std::string errstr;
    do {
      std::vector<rgw_data_change_log_entry> entries;
      if (specified_shard_id) {
        ret = rgw::run_coro(
          dpp,
          context_pool,
          datalog_svc->list_entries(dpp, shard_id, max_entries - entry_count,
                                    marker),
          std::tie(entries, marker, truncated),
          &errstr);
      } else {
        ret = rgw::run_coro(
          dpp,
          context_pool,
          datalog_svc->list_entries(dpp, max_entries - entry_count, log_marker),
          std::tie(entries, log_marker, truncated),
          &errstr);
      }
      if (ret < 0) {
        cerr << "ERROR: datalog_svc->list_entries(): " << errstr << ": "
             << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      entry_count += entries.size();

      for (const auto& entry : entries) {
        if (!extra_info) {
          encode_json("entry", entry.entry, formatter);
        } else {
          encode_json("entry", entry, formatter);
        }
      }
      formatter->flush(cout);
    } while (truncated && entry_count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::DATALOG_STATUS) {
    int i = (specified_shard_id ? shard_id : 0);

    formatter->open_array_section("entries");
    for (; i < driver->ctx()->_conf->rgw_data_log_num_shards; i++) {
      std::string errstr;
      RGWDataChangesLogInfo info;

      int r = rgw::run_coro(dpp, context_pool,
                            static_cast<rgw::sal::RadosStore*>(driver)->svc()->
                            datalog_rados->get_info(dpp, i),
                            info, &errstr);

      if (r < 0) {
        std::cerr << "datalog status: " << errstr << std::endl;
        return -r;
      }

      ::encode_json("info", info, formatter);

      if (specified_shard_id) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::DATALOG_AUTOTRIM) {
    RGWCoroutinesManager crs(driver->ctx(), driver->get_cr_registry());
    RGWHTTPManager http(driver->ctx(), crs.get_completion_mgr());
    ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = driver->ctx()->_conf->rgw_data_log_num_shards;
    std::vector<std::string> markers(num_shards);
    ret = crs.run(dpp, create_admin_data_log_trim_cr(dpp, static_cast<rgw::sal::RadosStore*>(driver), &http, num_shards, markers));
    if (ret < 0) {
      cerr << "automated datalog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::DATALOG_TRIM) {
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      std::cerr << "start-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      if (marker.empty()) {
        marker = end_marker;
      } else {
        std::cerr << "end-marker and marker not both allowed." << std::endl;
        return -EINVAL;
      }
    }

    if (!specified_shard_id) {
      cerr << "ERROR: requires a --shard-id" << std::endl;
      return EINVAL;
    }

    if (marker.empty()) {
      cerr << "ERROR: requires a --marker" << std::endl;
      return EINVAL;
    }

    std::string errstr;
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    ret = rgw::run_coro(dpp, context_pool,
                        datalog->trim_entries(dpp, shard_id, marker),
                        &errstr);

    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: trim_entries(): " << errstr << std::endl;
      return -ret;
    }
  }

  if (command == OPT::DATALOG_TYPE) {
    if (!opt_log_type) {
      std::cerr << "log-type not specified." << std::endl;
      return -EINVAL;
    }
    if (opt_log_type == log_type::omap) {
      std::cerr << "omap datalogs are deprecated. You cannot convert to them." << std::endl;
      return -EINVAL;
    }
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    std::string errstr;
    ret = rgw::run_coro(dpp, context_pool,
                        datalog->change_format(dpp, log_type::fifo),
                        &errstr);
    if (ret < 0) {
      cerr << "ERROR: change_format(): " << errstr << std::endl;
      return -ret;
    }
  }

  if (command == OPT::DATALOG_PRUNE) {
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    std::optional<uint64_t> through;
    std::string errstr;
    ret = rgw::run_coro(dpp, context_pool,
                        datalog->trim_generations(dpp, through),
                        &errstr);

    if (ret < 0) {
      cerr << "ERROR: trim_generations(): " << errstr << std::endl;
      return -ret;
    }

    if (through) {
      std::cout << "Pruned " << *through << " empty generations." << std::endl;
    } else {
      std::cout << "No empty generations." << std::endl;
    }
  }

  return 0;
}
