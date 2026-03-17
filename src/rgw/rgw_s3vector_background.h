// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>

namespace rgw::sal {
  class Driver;
}
class DoutPrefixProvider;

namespace rgw::s3vector {
  bool init(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  void shutdown();
  // update whenever new vectors are added to an index
  bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name);
}

