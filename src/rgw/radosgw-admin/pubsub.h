// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class User; }

struct rgw_admin_pubsub_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string account_id;
  std::string bucket_name;
  std::string bucket_id;
  std::string topic_name;
  std::string notification_id;
  std::string marker;
  std::optional<int> max_entries;
};

int rgw_admin_pubsub(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     const rgw::SiteConfig& site,
                     rgw::sal::User* user,
                     ceph::Formatter* formatter,
                     const rgw_admin_pubsub_options& opts);
