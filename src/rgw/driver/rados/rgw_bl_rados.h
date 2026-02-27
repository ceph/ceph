// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>

// forward declarations
namespace rgw { class SiteConfig; }
namespace rgw::sal {
  class RadosStore;
}
class DoutPrefixProvider;

namespace rgw::bucketlogging {

  // initialize the bucket logging commit manager
  bool init(const DoutPrefixProvider* dpp, rgw::sal::RadosStore* store,
            const rgw::SiteConfig& site);

  // shutdown the bucket logging commit manager
  void shutdown();

  int add_commit_target_entry(const DoutPrefixProvider* dpp,
                              rgw::sal::RadosStore* store,
                              const rgw::sal::Bucket* log_bucket,
                              const std::string& prefix,
                              const std::string& obj_name,
                              const std::string& tail_obj_name,
                              const rgw_pool& temp_data_pool,
                              optional_yield y);

int list_pending_commit_objects(const DoutPrefixProvider* dpp,
                                rgw::sal::RadosStore* store,
                                const rgw::sal::Bucket* log_bucket,
                                const std::string& prefix,
                                std::set<std::string>& entries,
                                optional_yield y);

}

