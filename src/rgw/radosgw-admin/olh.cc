// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/olh.h"
#include <iostream>
#include "common/ceph_json.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "radosgw-admin/bucket.h"


using namespace rgw_admin;
using namespace std;

namespace {



} // anonymous namespace

int rgw_admin_olh(const DoutPrefixProvider* dpp,
                  rgw::sal::Driver* driver,
                  ceph::Formatter* formatter,
                  std::unique_ptr<rgw::sal::Bucket>& bucket,
                  const rgw_admin_olh_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& bucket_id = *opts.bucket_id;
  auto& object = *opts.object;
  int ret = 0;

  if (command == OPT::OLH_GET || command == OPT::OLH_READLOG) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
  }

  if (command == OPT::OLH_GET) {
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWOLHInfo olh;
    rgw_obj obj(bucket->get_key(), object);
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_olh(dpp, bucket->get_info(), obj, &olh, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("olh", olh, formatter);
    formatter->flush(cout);
  }

  if (command == OPT::OLH_READLOG) {
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
    bool is_truncated;

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);

    ret = obj->load_obj_state(dpp, null_yield);
    if (ret < 0) {
      return -ret;
    }

    RGWObjState& state = static_cast<rgw::sal::RadosObject*>(obj.get())->get_state();

    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bucket_index_read_olh_log(dpp, bucket->get_info(), state, obj->get_obj(), 0, &log, &is_truncated, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("is_truncated", is_truncated, formatter);
    encode_json("log", log, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  return 0;
}

