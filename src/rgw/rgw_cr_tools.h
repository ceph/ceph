#ifndef CEPH_RGW_CR_TOOLS_H
#define CEPH_RGW_CR_TOOLS_H

#include "rgw_cr_rados.h"


struct rgw_user_create_params {
  rgw_user user;
  std::string display_name;
  std::string email;
  std::string access_key;
  std::string secret_key;
  std::string key_type; /* "swift" or "s3" */
  std::string caps;

  bool generate_key{true};
  bool suspended{false};
  std::optional<int32_t> max_buckets;
  bool system{false};
  bool exclusive{false};
  bool apply_quota{true};
};

using RGWUserCreateCR = RGWSimpleWriteOnlyAsyncCR<rgw_user_create_params>;

struct rgw_get_user_info_params {
  rgw_user user;
};

using RGWGetUserInfoCR = RGWSimpleAsyncCR<rgw_get_user_info_params, RGWUserInfo>;

struct rgw_get_bucket_info_params {
  string tenant;
  string bucket_name;
};

struct rgw_get_bucket_info_result {
  ceph::real_time mtime;
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
};

using RGWGetBucketInfoCR = RGWSimpleAsyncCR<rgw_get_bucket_info_params, rgw_get_bucket_info_result>;

struct rgw_bucket_create_local_params {
  shared_ptr<RGWUserInfo> user_info;
  std::string bucket_name;
  std::string placement_rule;
};

using RGWBucketCreateLocalCR = RGWSimpleWriteOnlyAsyncCR<rgw_bucket_create_local_params>;



#endif
