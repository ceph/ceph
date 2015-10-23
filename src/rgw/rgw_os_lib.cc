// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_user.h"
#include "rgw_os_lib.h"
#include "rgw_file.h"


/* static */
int RGWHandler_Lib::init_from_header(struct req_state *s)
{
  string req;
  string first;

  const char *req_name = s->relative_uri.c_str();
  const char *p;

  /* skip request_params parsing, rgw_file should not be
   * seeing any */
  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse();

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

  req = req_name;
  int pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  if (s->bucket_name.empty()) {
    s->bucket_name = std::move(first);
    if (pos >= 0) {
      // XXX ugh, another copy
      string encoded_obj_str = req.substr(pos+1);
      s->object = rgw_obj_key(encoded_obj_str, s->info.args.get("versionId"));
    }
  } else {
    s->object = rgw_obj_key(req_name, s->info.args.get("versionId"));
  }
  return 0;
} /* init_from_header */

/* RGWOps */

void RGWListBuckets_OS_Lib::send_response_begin(bool has_buckets)
{
  sent_data = true;
}

void RGWListBuckets_OS_Lib::send_response_data(RGWUserBuckets& buckets)
{
  if (!sent_data)
    return;

  // XXX if necessary, we can remove the need for dynamic_cast
  RGWListBucketsRequest* req
    = dynamic_cast<RGWListBucketsRequest*>(this);

  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  for (const auto& iter : m) {
    const std::string& marker = iter.first; // XXX may need later
    const RGWBucketEnt& ent = iter.second;
    /* call me maybe */
    req->operator()(ent.bucket.name, marker); // XXX attributes
  }
} /* send_response_data */

void RGWListBuckets_OS_Lib::send_response_end()
{
  // do nothing
}

int RGWListBucket_OS_Lib::get_params()
{
  // XXX S3
  list_versions = s->info.args.exists("versions");
  prefix = s->info.args.get("prefix");
  if (!list_versions) {
    marker = s->info.args.get("marker");
  } else {
    marker.name = s->info.args.get("key-marker");
    marker.instance = s->info.args.get("version-id-marker");
  }
  max_keys = s->info.args.get("max-keys");
  op_ret = parse_max_keys();
  if (op_ret < 0) {
    return op_ret;
  }
#if 0
  delimiter = s->info.args.get("delimiter");
  encoding_type = s->info.args.get("encoding-type");
#endif
  return 0;
}

void RGWListBucket_OS_Lib::send_response()
{
  // XXX if necessary, we can remove the need for dynamic_cast
  RGWListBucketRequest* req
    = dynamic_cast<RGWListBucketRequest*>(this);

  for (const auto& iter : objs) {
    /* call me maybe */
    req->operator()(iter.key.name, iter.key.name); // XXX attributes
  }
}

int RGWCreateBucket_OS_Lib::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  /* we don't have (any) headers, so just create canned ACLs */
  int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
  policy = s3policy;
  return ret;
}

void RGWCreateBucket_OS_Lib::send_response()
{
  /* TODO: something (maybe) */
}
