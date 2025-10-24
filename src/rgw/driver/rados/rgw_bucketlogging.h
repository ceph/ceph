// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include "common/ceph_time.h"
#include "include/common_fwd.h"
#include "common/async/yield_context.h"
#include "rgw_common.h"

// forward declarations
namespace rgw { class SiteConfig; }
namespace rgw::sal {
  class RadosStore;
  class RGWObject;
}
class DoutPrefixProvider;
class RGWRados;
struct rgw_obj_key;

namespace rgw::bucket_logging {

  // initialize the bucket logging commit manager
  bool init(const DoutPrefixProvider* dpp, rgw::sal::RadosStore* store,
            const rgw::SiteConfig& site);

  // shutdown the bucket logging commit manager
  void shutdown();

/*
  const DoutPrefixProvider* const dpp;
  rgw::sal::RadosStore* const store;
  const req_state* const s;
  size_t size;
  rgw::sal::Object* const object;
  rgw::sal::Object* const src_object; // may differ from object
  rgw::sal::Bucket* bucket;
  const std::string* const object_name;
  boost::optional<const RGWObjTags&> tagset;
  meta_map_t x_meta_map; // metadata cached by value
  bool metadata_fetched_from_attributes;
  const std::string user_id;
  const std::string user_tenant;
  const std::string req_id;
  optional_yield yield;
*/

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

