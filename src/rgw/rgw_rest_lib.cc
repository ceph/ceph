// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_user.h"
#include "rgw_rest_lib.h"

/* XXX going away ! */

/* static */
int RGWHandler_ObjStore_Lib::init_from_header(struct req_state *s)
{
  string req;
  string first;

  const char *req_name = s->relative_uri.c_str();
  const char *p;

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

void RGWListBuckets_ObjStore_Lib::send_response_begin(bool has_buckets)
{
  sent_data = true;
}

void RGWListBuckets_ObjStore_Lib::send_response_data(RGWUserBuckets& buckets)
{
  if (!sent_data)
    return;

  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  for (const auto& iter : m) {
    const RGWBucketEnt& ent = iter.second;
    const std::string& marker = iter.first; // XXX may need later
    // XXXX (void) cb(ent.bucket.name, marker); // XXX attributes
  }
} /* send_response_data */

void RGWListBuckets_ObjStore_Lib::send_response_end()
{
  // do nothing
}

int RGWListBucket_ObjStore_Lib::get_params()
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

void RGWListBucket_ObjStore_Lib::send_response()
{
  for (const auto& iter : objs) {
    // XXXX cb(iter.key.name, iter.key.name); // XXX marker, attributes
  }
}
