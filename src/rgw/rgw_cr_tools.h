#ifndef CEPH_RGW_CR_TOOLS_H
#define CEPH_RGW_CR_TOOLS_H

#include "rgw_cr_rados.h"


struct rgw_user_create_params {
  std::string uid;
  std::string display_name;
  std::string email;
  std::string access_key;
  std::string secret_key;
  std::string key_type; /* "swift" or "s3" */
  std::string caps;
  std::string tenant_name;

  bool generate_key{true};
  bool suspended{false};
  std::optional<int32_t> max_buckets;
  bool system{false};
  bool exclusive{false};
  bool apply_quota{true};
};

using RGWUserCreateCR = RGWSimpleWriteOnlyAsyncCR<rgw_user_create_params>;



#endif
