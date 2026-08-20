// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <string>

#include "common/async/yield_context.h"
#include "rgw_basic_types.h"

namespace rgw::sal {
class Driver;
}

class DoutPrefixProvider;
class RGWFormatterFlusher;

namespace rgw::s3vector {

struct RGWVectorBucketAdminOpState {
  rgw_user uid;
  std::string bucket_name;
  std::string marker;
  uint32_t max_entries{1000};
};

class RGWVectorBucketAdminOp {
public:
  static int list_sessions(rgw::sal::Driver* driver,
                           RGWVectorBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher,
                           optional_yield y,
                           const DoutPrefixProvider* dpp);

  static int get_session_info(rgw::sal::Driver* driver,
                              RGWVectorBucketAdminOpState& op_state,
                              RGWFormatterFlusher& flusher,
                              optional_yield y,
                              const DoutPrefixProvider* dpp);

  static int remove_session(rgw::sal::Driver* driver,
                            RGWVectorBucketAdminOpState& op_state,
                            const DoutPrefixProvider* dpp,
                            optional_yield y);
};

} // namespace rgw::s3vector
