// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/metadata.h"

#include <iostream>
#include <list>
#include <string>

#include "common/ceph_json.h"
#include "driver/rados/rgw_sal_rados.h"
#include "radosgw-admin/admin_io.h"

using namespace rgw_admin;
using namespace std;

int rgw_admin_metadata(const DoutPrefixProvider* dpp,
                       rgw::sal::Driver* driver,
                       ceph::Formatter* formatter,
                       const rgw_admin_metadata_options& opts)
{
  auto& command = opts.command;
  auto& metadata_key = *opts.metadata_key;
  auto& marker = *opts.marker;
  auto& infile = *opts.infile;
  int max_entries = opts.max_entries;
  bool max_entries_specified = opts.max_entries_specified;

    if (command == OPT::METADATA_GET) {
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->get(metadata_key, formatter, null_yield, dpp);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    formatter->flush(cout);
  }

    if (command == OPT::METADATA_PUT) {
    bufferlist bl;
    int ret = rgw_admin_read_input(infile, bl);
    if (ret < 0) {
      cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->put(metadata_key, bl, null_yield, dpp, RGWMDLogSyncType::APPLY_ALWAYS, false);
    if (ret < 0) {
      cerr << "ERROR: can't put key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::METADATA_RM) {
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->remove(metadata_key, null_yield, dpp);
    if (ret < 0) {
      cerr << "ERROR: can't remove key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::METADATA_LIST) {
    void *handle;
    int max = 1000;
    int ret = driver->meta_list_keys_init(dpp, metadata_key, marker, &handle);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool truncated;
    uint64_t count = 0;

    if (max_entries_specified) {
      formatter->open_object_section("result");
    }
    formatter->open_array_section("keys");

    uint64_t left;
    do {
      list<string> keys;
      left = (max_entries_specified ? max_entries - count : max);
      ret = driver->meta_list_keys_next(dpp, handle, left, keys, &truncated);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      } if (ret != -ENOENT) {
	for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
	  formatter->dump_string("key", *iter);
          ++count;
	}
	formatter->flush(cout);
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
    formatter->flush(cout);

    driver->meta_list_keys_complete(handle);
  }

  return 0;
}
