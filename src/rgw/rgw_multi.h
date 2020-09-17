// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_MULTI_H
#define CEPH_RGW_MULTI_H

#include <map>
#include "rgw_xml.h"
#include "rgw_common.h"

namespace rgw { namespace sal {
  class RGWRadosStore;
} }

#define MULTIPART_UPLOAD_ID_PREFIX_LEGACY "2/"
#define MULTIPART_UPLOAD_ID_PREFIX "2~" // must contain a unique char that may not come up in gen_rand_alpha()

class RGWMPObj;

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

extern bool is_v2_upload_id(const string& upload_id);

extern int list_multipart_parts(rgw::sal::RGWRadosStore *store, RGWBucketInfo& bucket_info,
				CephContext *cct,
                                const string& upload_id,
                                const string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted = false);

extern int list_multipart_parts(rgw::sal::RGWRadosStore *store, struct req_state *s,
                                const string& upload_id,
                                const string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted = false);

extern int abort_multipart_upload(rgw::sal::RGWRadosStore* store, CephContext *cct, RGWObjectCtx *obj_ctx, RGWBucketInfo& bucket_info, RGWMPObj& mp_obj);

extern void cleanup_multipart_reuploads(rgw::sal::RGWRadosStore *store,
					CephContext *cct,
					RGWObjectCtx *obj_ctx,
					RGWBucketInfo& bucket_info,
					RGWUploadPartInfo& obj_part,
					const rgw_obj& meta_obj);

extern int list_bucket_multiparts(rgw::sal::RGWRadosStore *store, RGWBucketInfo& bucket_info,
				  const string& prefix,
				  const string& marker,
				  const string& delim,
				  const int& max_uploads,
				  vector<rgw_bucket_dir_entry> *objs,
				  map<string, bool> *common_prefixes, bool *is_truncated);

extern int abort_bucket_multiparts(rgw::sal::RGWRadosStore *store, CephContext *cct, RGWBucketInfo& bucket_info,
                                string& prefix, string& delim);
#endif
