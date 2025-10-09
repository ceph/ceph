// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_cache.h"
#include "rgw_meta_sync_status.h"
#include "rgw_data_sync.h"
#include "rgw_multi.h"
#include "rgw_bucket_encryption.h"

#include "common/Formatter.h"

using namespace std;

static string shadow_ns = RGW_OBJ_NS_SHADOW;

list<obj_version> obj_version::generate_test_instances()
{
  list<obj_version> o;

  obj_version v;
  v.ver = 5;
  v.tag = "tag";

  o.push_back(std::move(v));
  o.emplace_back();
  return o;
}

std::list<RGWBucketEncryptionConfig> RGWBucketEncryptionConfig::generate_test_instances()
{
  std::list<RGWBucketEncryptionConfig> o;

  auto bc = RGWBucketEncryptionConfig("aws:kms", "some:key", true);
  o.push_back(std::move(bc));

  bc = RGWBucketEncryptionConfig("AES256");
  o.push_back(std::move(bc));

  o.emplace_back();
  return o;
}
