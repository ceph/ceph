// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MULTI_H
#define CEPH_RGW_MULTI_H

#include <map>
#include "rgw_xml.h"
#include "rgw_rados.h"

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

/**
 * A filter to a) test whether an object name is a multipart meta
 * object, and b) filter out just the key used to determine the bucket
 * index shard.
 *
 * Objects for multipart meta have names adorned with an upload id and
 * other elements -- specifically a ".", MULTIPART_UPLOAD_ID_PREFIX,
 * unique id, and MP_META_SUFFIX. This filter will return true when
 * the name provided is such. It will also extract the key used for
 * bucket index shard calculation from the adorned name.
 */
class MultipartMetaFilter : public RGWAccessListFilter {
public:
  MultipartMetaFilter() {}

  /**
   * @param name [in] The object name as it appears in the bucket index.
   * @param key [out] An output parameter that will contain the bucket
   *        index key if this entry is in the form of a multipart meta object.
   * @return true if the name provided is in the form of a multipart meta
   *         object, false otherwise
   */
  bool filter(const string& name, string& key) override;
}; // class MultipartMetaFilter

extern bool is_v2_upload_id(const string& upload_id);

extern int list_multipart_parts(RGWRados *store, RGWBucketInfo& bucket_info,
				CephContext *cct,
                                const string& upload_id,
                                const string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted = false);

extern int list_multipart_parts(RGWRados *store, struct req_state *s,
                                const string& upload_id,
                                const string& meta_oid, int num_parts,
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
