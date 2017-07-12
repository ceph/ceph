// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MULTI_H
#define CEPH_RGW_MULTI_H

#include <map>
#include "rgw_xml.h"
#include "rgw_rados.h"

#define MP_META_SUFFIX ".meta"
#define MULTIPART_UPLOAD_ID_PREFIX_LEGACY "2/"
#define MULTIPART_UPLOAD_ID_PREFIX "2~" // must contain a unique char that may not come up in gen_rand_alpha()

class RGWMultiCompleteUpload : public XMLObj
{
public:
  RGWMultiCompleteUpload() {}
  ~RGWMultiCompleteUpload() override {}
  bool xml_end(const char *el) override;

  std::map<int, string> parts;
};

class RGWMultiPart : public XMLObj
{
  string etag;
  int num;
public:
  RGWMultiPart() : num(0) {}
  ~RGWMultiPart() override {}
  bool xml_end(const char *el) override;

  string& get_etag() { return etag; }
  int get_num() { return num; }
};

class RGWMultiPartNumber : public XMLObj
{
public:
  RGWMultiPartNumber() {}
  ~RGWMultiPartNumber() override {}
};

class RGWMultiETag : public XMLObj
{
public:
  RGWMultiETag() {}
  ~RGWMultiETag() override {}
};

class RGWMultiXMLParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override;
public:
  RGWMultiXMLParser() {}
  ~RGWMultiXMLParser() override {}
};

class MultipartMetaFilter : public RGWAccessListFilter {
public:
  MultipartMetaFilter() {}
  bool filter(string& name, string& key) override {
    int len = name.size();
    if (len < 6)
      return false;

    size_t pos = name.find(MP_META_SUFFIX, len - 5);
    if (pos == string::npos)
      return false;

    pos = name.rfind('.', pos - 1);
    if (pos == string::npos)
      return false;

    key = name.substr(0, pos);

    return true;
  }
};

extern bool is_v2_upload_id(const string& upload_id);

extern int list_multipart_parts(RGWRados *store, RGWBucketInfo& bucket_info, CephContext *cct,
                                const string& upload_id,
                                string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted = false);

extern int list_multipart_parts(RGWRados *store, struct req_state *s,
                                const string& upload_id,
                                string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted = false);

extern int abort_multipart_upload(RGWRados *store, CephContext *cct, RGWObjectCtx *obj_ctx,
                                RGWBucketInfo& bucket_info, RGWMPObj& mp_obj);

extern int list_bucket_multiparts(RGWRados *store, RGWBucketInfo& bucket_info,
                                string& prefix, string& marker, string& delim,
                                int& max_uploads, vector<rgw_bucket_dir_entry> *objs,
                                map<string, bool> *common_prefixes, bool *is_truncated);

extern int abort_bucket_multiparts(RGWRados *store, CephContext *cct, RGWBucketInfo& bucket_info,
                                string& prefix, string& delim);
#endif
