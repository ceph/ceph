// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWStreamFlusher;
namespace rgw::sal { class Driver; }

struct rgw_admin_restore_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string bucket_name;
  std::string object;
  std::optional<std::string> restore_status_filter;
};

int rgw_admin_restore(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      RGWStreamFlusher& stream_flusher,
                      const rgw_admin_restore_options& opts);
