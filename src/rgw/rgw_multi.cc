// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_xml.h"
#include "rgw_multi.h"
#include "rgw_op.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

bool RGWMultiPart::xml_end(const char *el)
{
  RGWMultiPartNumber *num_obj = static_cast<RGWMultiPartNumber *>(find_first("PartNumber"));
  RGWMultiETag *etag_obj = static_cast<RGWMultiETag *>(find_first("ETag"));

  if (!num_obj || !etag_obj)
    return false;

  string s = num_obj->get_data();
  if (s.empty())
    return false;

  num = atoi(s.c_str());

  s = etag_obj->get_data();
  etag = s;

  return true;
}

bool RGWMultiCompleteUpload::xml_end(const char *el) {
  XMLObjIter iter = find("Part");
  RGWMultiPart *part = static_cast<RGWMultiPart *>(iter.get_next());
  while (part) {
    int num = part->get_num();
    string etag = part->get_etag();
    parts[num] = etag;
    part = static_cast<RGWMultiPart *>(iter.get_next());
  }
  return true;
}

RGWMultiXMLParser::~RGWMultiXMLParser() {}

XMLObj *RGWMultiXMLParser::alloc_obj(const char *el) {
  XMLObj *obj = NULL;
  // CompletedMultipartUpload is incorrect but some versions of some libraries use it, see PR #41700
  if (strcmp(el, "CompleteMultipartUpload") == 0 ||
      strcmp(el, "CompletedMultipartUpload") == 0 ||
      strcmp(el, "MultipartUpload") == 0) {
    obj = new RGWMultiCompleteUpload();
  } else if (strcmp(el, "Part") == 0) {
    obj = new RGWMultiPart();
  } else if (strcmp(el, "PartNumber") == 0) {
    obj = new RGWMultiPartNumber();
  } else if (strcmp(el, "ETag") == 0) {
    obj = new RGWMultiETag();
  }

  return obj;
}

bool is_v2_upload_id(const string& upload_id)
{
  const char *uid = upload_id.c_str();

  return (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX, sizeof(MULTIPART_UPLOAD_ID_PREFIX) - 1) == 0) ||
         (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX_LEGACY, sizeof(MULTIPART_UPLOAD_ID_PREFIX_LEGACY) - 1) == 0);
}

void RGWUploadPartInfo::generate_test_instances(list<RGWUploadPartInfo*>& o)
{
  RGWUploadPartInfo *i = new RGWUploadPartInfo;
  i->num = 1;
  i->size = 10 * 1024 * 1024;
  i->etag = "etag";
  o.push_back(i);
  o.push_back(new RGWUploadPartInfo);
}

void RGWUploadPartInfo::dump(Formatter *f) const
{
  encode_json("num", num, f);
  encode_json("size", size, f);
  encode_json("etag", etag, f);
  utime_t ut(modified);
  encode_json("modified", ut, f);
  encode_json("past_prefixes", past_prefixes, f);
}

