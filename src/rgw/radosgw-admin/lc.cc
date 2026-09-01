// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/lc.h"
#include <iostream>
#include "common/ceph_json.h"
#include "rgw_lc.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "driver/rados/rgw_bucket.h"
#include "radosgw-admin/bucket.h"


using namespace rgw_admin;
using namespace std;

namespace {



} // anonymous namespace

int rgw_admin_lc(const DoutPrefixProvider* dpp,
                 rgw::sal::Driver* driver,
                 ceph::Formatter* formatter,
                 RGWStreamFlusher& stream_flusher,
                 RGWBucketAdminOpState& bucket_op,
                 std::unique_ptr<rgw::sal::Bucket>& bucket,
                 const rgw_admin_lc_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& bucket_id = *opts.bucket_id;
  int max_entries = opts.max_entries;
  int ret = 0;

  if (command == OPT::LC_LIST) {
    formatter->open_array_section("lifecycle_list");
    vector<rgw::sal::LCEntry> bucket_lc_map;
    string marker;
    int index{0};
#define MAX_LC_LIST_ENTRIES 100
    if (max_entries < 0) {
      max_entries = MAX_LC_LIST_ENTRIES;
    }
    RGWLC* lc = driver->get_rgwlc();
    do {
      int ret = lc->list_lc_progress(marker, max_entries, bucket_lc_map, index);
      if (ret < 0) {
        cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret)
	     << std::endl;
        return 1;
      }
      for (const auto& entry : bucket_lc_map) {
        formatter->open_object_section("bucket_lc_info");
        formatter->dump_string("bucket", entry.bucket);
	char exp_buf[100];
        time_t t = entry.start_time;
	if (std::strftime(
	      exp_buf, sizeof(exp_buf),
	      "%a, %d %b %Y %T %Z", std::gmtime(&t))) {
	  formatter->dump_string("started", exp_buf);
	}
        formatter->dump_string("status", LC_STATUS[entry.status]);
        formatter->close_section(); // objs
        formatter->flush(cout);
      }
    } while (!bucket_lc_map.empty());

    formatter->close_section(); //lifecycle list
    formatter->flush(cout);
  }


  if (command == OPT::LC_GET) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    RGWLifecycleConfiguration config;
    ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto aiter = bucket->get_attrs().find(RGW_ATTR_LC);
    if (aiter == bucket->get_attrs().end()) {
      return -ENOENT;
    }

    bufferlist::const_iterator iter{&aiter->second};
    try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      cerr << "ERROR: decode life cycle config failed" << std::endl;
      return -EIO;
    }

    encode_json("result", config, formatter);
    formatter->flush(cout);
  }

#ifdef WITH_RADOSGW_RADOS
  if (command == OPT::LC_PROCESS) {
    if ((! bucket_name.empty()) ||
	(! bucket_id.empty())) {
        int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
	if (ret < 0) {
	  cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret)
	       << std::endl;
	  return ret;
	}
    }

    int ret =
      static_cast<rgw::sal::RadosStore*>(driver)->getRados()->process_lc(bucket);
    if (ret < 0) {
      cerr << "ERROR: lc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }
#endif

  if (command == OPT::LC_RESHARD_FIX) {
    ret = RGWBucketAdminOp::fix_lc_shards(driver, bucket_op, stream_flusher, dpp, null_yield);
    if (ret < 0) {
      cerr << "ERROR: fixing lc shards: " << cpp_strerror(-ret) << std::endl;
    }

  }

  return 0;
}

