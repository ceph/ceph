// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

void obj_version::generate_test_instances(list<obj_version*>& o)
{
  obj_version *v = new obj_version;
  v->ver = 5;
  v->tag = "tag";

  o.push_back(v);
  o.push_back(new obj_version);
}

void RGWBucketEncryptionConfig::generate_test_instances(std::list<RGWBucketEncryptionConfig*>& o)
{
  auto *bc = new RGWBucketEncryptionConfig("aws:kms", "some:key", true);
  o.push_back(bc);

  bc = new RGWBucketEncryptionConfig("AES256");
  o.push_back(bc);

  o.push_back(new RGWBucketEncryptionConfig);
}
