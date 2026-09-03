// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>
#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_gc_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string marker;
  int shard_id = 0;
  bool specified_shard_id = false;
  bool include_all = false;
  bool bypass_gc = false;
};


int rgw_admin_gc(const DoutPrefixProvider* dpp,
                 rgw::sal::Driver* driver,
                 ceph::Formatter* formatter,
                 const rgw_admin_gc_options& opts);

