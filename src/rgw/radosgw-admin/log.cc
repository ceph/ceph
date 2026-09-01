// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/log.h"

#include <chrono>
#include <iostream>
#include <string>

#include "common/errno.h"
#include "common/Formatter.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_formats.h"
#include "rgw_log.h"
#include "rgw_sal.h"

using namespace rgw_admin;
using namespace std;

int rgw_admin_log(const DoutPrefixProvider* dpp,
                  rgw::sal::Driver* driver,
                  Formatter* formatter,
                  const rgw_admin_log_options& o)
{
  auto command = o.command;
  const auto& date = *o.date;
  const auto& object = *o.object;
  const auto& bucket_name = *o.bucket_name;
  const auto& bucket_id = *o.bucket_id;
  const bool show_log_entries = o.show_log_entries;
  const bool show_log_sum = o.show_log_sum;
  const bool skip_zero_entries = o.skip_zero_entries;

  auto* rados = static_cast<rgw::sal::RadosStore*>(driver)->getRados();

  if (command == OPT::LOG_LIST) {
    if (date.size() && date.size() != 10) {
      cerr << "bad date format for '" << date << "', expect YYYY-MM-DD" << std::endl;
      return EINVAL;
    }

    formatter->reset();
    formatter->open_array_section("logs");
    RGWAccessHandle h;
    int r = rados->log_list_init(dpp, date, &h);
    if (r == -ENOENT) {
      // no logs.
    } else {
      if (r < 0) {
        cerr << "log list: error " << r << std::endl;
        return -r;
      }
      while (true) {
        string name;
        r = rados->log_list_next(h, &name);
        if (r == -ENOENT) {
          break;
        }
        if (r < 0) {
          cerr << "log list: error " << r << std::endl;
          return -r;
        }
        formatter->dump_string("object", name);
      }
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
    return 0;
  }

  if (command == OPT::LOG_SHOW || command == OPT::LOG_RM) {
    if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id.empty())) {
      cerr << "specify an object or a date, bucket and bucket-id" << std::endl;
      exit(1);
    }

    string oid;
    if (!object.empty()) {
      oid = object;
    } else {
      oid = date;
      oid += "-";
      oid += bucket_id;
      oid += "-";
      oid += bucket_name;
    }

    if (command == OPT::LOG_SHOW) {
      RGWAccessHandle h;

      int r = rados->log_show_init(dpp, oid, &h);
      if (r < 0) {
        cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
        return -r;
      }

      formatter->reset();
      formatter->open_object_section("log");

      struct rgw_log_entry entry;

      r = rados->log_show_next(dpp, h, &entry);
      if (r < 0) {
        cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
        return -r;
      }
      formatter->dump_string("bucket_id", entry.bucket_id);
      formatter->dump_string("bucket_owner", to_string(entry.bucket_owner));
      formatter->dump_string("bucket", entry.bucket);

      uint64_t agg_time = 0;
      uint64_t agg_bytes_sent = 0;
      uint64_t agg_bytes_received = 0;
      uint64_t total_entries = 0;

      if (show_log_entries) {
        formatter->open_array_section("log_entries");
      }

      do {
        using namespace std::chrono;
        uint64_t total_time = duration_cast<milliseconds>(entry.total_time).count();

        agg_time += total_time;
        agg_bytes_sent += entry.bytes_sent;
        agg_bytes_received += entry.bytes_received;
        total_entries++;

        if (skip_zero_entries && entry.bytes_sent == 0 &&
            entry.bytes_received == 0) {
          goto next;
        }

        if (show_log_entries) {
          rgw_format_ops_log_entry(entry, formatter);
          formatter->flush(cout);
        }
next:
        r = rados->log_show_next(dpp, h, &entry);
      } while (r > 0);

      if (r < 0) {
        cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
        return -r;
      }
      if (show_log_entries) {
        formatter->close_section();
      }

      if (show_log_sum) {
        formatter->open_object_section("log_sum");
        formatter->dump_int("bytes_sent", agg_bytes_sent);
        formatter->dump_int("bytes_received", agg_bytes_received);
        formatter->dump_int("total_time", agg_time);
        formatter->dump_int("total_entries", total_entries);
        formatter->close_section();
      }
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
    if (command == OPT::LOG_RM) {
      int r = rados->log_remove(dpp, oid);
      if (r < 0) {
        cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
        return -r;
      }
    }
    return 0;
  }

  return EINVAL;
}
