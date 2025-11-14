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
                              rgw::sal::Bucket *log_bucket,
                              const std::string& prefix,
                              const std::string& obj_name,
                              const std::string& tail_obj_name,
                              rgw_pool temp_data_pool,
                              optional_yield y);

  struct commit_log_entry_t{
    std::string logging_obj_name;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(logging_obj_name, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(logging_obj_name, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->dump_string("objectName", logging_obj_name);
    }
  };
  WRITE_CLASS_ENCODER(commit_log_entry_t)

}

