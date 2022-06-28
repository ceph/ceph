// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <map>
#include <sstream>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/scope_guard.h"

#include "rgw_datalog.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_tag_s3.h"

#include "include/types.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_op.h"
#include "rgw_bucket_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_bucket.h"
#include "services/svc_bucket_sync.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_user.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"

#include "include/rados/librados.hpp"
// until everything is moved from rgw_common
#include "rgw_common.h"
#include "rgw_reshard.h"
#include "rgw_lc.h"
#include "rgw_bucket_layout.h"

// stolen from src/cls/version/cls_version.cc
#define VERSION_ATTR "ceph.objclass.version"

#include "cls/user/cls_user_types.h"

#include "rgw_sal_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

// seconds for timeout during RGWBucket::check_object_index
constexpr uint64_t BUCKET_TAG_QUICK_TIMEOUT = 30;

using namespace std;

void init_bucket(rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id)
{
  b->tenant = t;
  b->name = n;
  b->marker = m;
  b->bucket_id = id;
  b->explicit_placement.data_pool = rgw_pool(dp);
  b->explicit_placement.index_pool = rgw_pool(ip);
}

// parse key in format: [tenant/]name:instance[:shard_id]
int rgw_bucket_parse_bucket_key(CephContext *cct, const string& key,
                                rgw_bucket *bucket, int *shard_id)
{
  std::string_view name{key};
  std::string_view instance;

  // split tenant/name
  auto pos = name.find('/');
  if (pos != string::npos) {
    auto tenant = name.substr(0, pos);
    bucket->tenant.assign(tenant.begin(), tenant.end());
    name = name.substr(pos + 1);
  } else {
    bucket->tenant.clear();
  }

  // split name:instance
  pos = name.find(':');
  if (pos != string::npos) {
    instance = name.substr(pos + 1);
    name = name.substr(0, pos);
  }
  bucket->name.assign(name.begin(), name.end());

  // split instance:shard
  pos = instance.find(':');
  if (pos == string::npos) {
    bucket->bucket_id.assign(instance.begin(), instance.end());
    if (shard_id) {
      *shard_id = -1;
    }
    return 0;
  }

  // parse shard id
  auto shard = instance.substr(pos + 1);
  string err;
  auto id = strict_strtol(shard.data(), 10, &err);
  if (!err.empty()) {
    if (cct) {
      ldout(cct, 0) << "ERROR: failed to parse bucket shard '"
          << instance.data() << "': " << err << dendl;
    }
    return -EINVAL;
  }

  if (shard_id) {
    *shard_id = id;
  }
  instance = instance.substr(0, pos);
  bucket->bucket_id.assign(instance.begin(), instance.end());
  return 0;
}

/*
 * Note that this is not a reversal of parse_bucket(). That one deals
 * with the syntax we need in metadata and such. This one deals with
 * the representation in RADOS pools. We chose '/' because it's not
 * acceptable in bucket names and thus qualified buckets cannot conflict
 * with the legacy or S3 buckets.
 */
std::string rgw_make_bucket_entry_name(const std::string& tenant_name,
                                       const std::string& bucket_name) {
  std::string bucket_entry;

  if (bucket_name.empty()) {
    bucket_entry.clear();
  } else if (tenant_name.empty()) {
    bucket_entry = bucket_name;
  } else {
    bucket_entry = tenant_name + "/" + bucket_name;
  }

  return bucket_entry;
}

/*
 * Tenants are separated from buckets in URLs by a colon in S3.
 * This function is not to be used on Swift URLs, not even for COPY arguments.
 */
void rgw_parse_url_bucket(const string &bucket, const string& auth_tenant,
                          string &tenant_name, string &bucket_name) {

  int pos = bucket.find(':');
  if (pos >= 0) {
    /*
     * N.B.: We allow ":bucket" syntax with explicit empty tenant in order
     * to refer to the legacy tenant, in case users in new named tenants
     * want to access old global buckets.
     */
    tenant_name = bucket.substr(0, pos);
    bucket_name = bucket.substr(pos + 1);
  } else {
    tenant_name = auth_tenant;
    bucket_name = bucket;
  }
}

