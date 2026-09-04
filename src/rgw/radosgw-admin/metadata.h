// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_metadata_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string metadata_key;
  std::string marker;
  std::string infile;
  std::optional<int> max_entries;
};

int rgw_admin_metadata(const DoutPrefixProvider* dpp,
                       rgw::sal::Driver* driver,
                       ceph::Formatter* formatter,
                       rgw_admin_metadata_options& opts);
