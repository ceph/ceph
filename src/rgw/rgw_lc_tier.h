// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_LC_TIER_H
#define CEPH_RGW_LC_TIER_H

#include "rgw_lc.h"
#include "rgw_cr_rados.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_coroutine.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#define DEFAULT_MULTIPART_SYNC_PART_SIZE (32 * 1024 * 1024)
#define MULTIPART_MIN_POSSIBLE_PART_SIZE (5 * 1024 * 1024)

struct RGWLCCloudTierCtx {
  CephContext *cct;

  /* Source */
  rgw_bucket_dir_entry& o;
  rgw::sal::RGWRadosStore *store;
  RGWBucketInfo& bucket_info;

  rgw_obj obj;
  RGWObjectCtx& rctx;

  /* Remote */
  std::shared_ptr<RGWRESTConn> conn;
  string target_bucket_name;
  string target_storage_class;
  RGWHTTPManager *http_manager;

  map<string, RGWTierACLMapping> acl_mappings;
  uint64_t multipart_min_part_size;
  uint64_t multipart_sync_threshold;

  RGWLCCloudTierCtx(CephContext* _cct, rgw_bucket_dir_entry& _o,
            rgw::sal::RGWRadosStore* _store, RGWBucketInfo &_binfo, rgw_obj _obj,
            RGWObjectCtx& _rctx, std::shared_ptr<RGWRESTConn> _conn, string _bucket,
            string _storage_class, RGWHTTPManager *_http)
            : cct(_cct), o(_o), store(_store), bucket_info(_binfo),
              obj(_obj), rctx(_rctx), conn(_conn), target_bucket_name(_bucket),
              target_storage_class(_storage_class), http_manager(_http) {}
};

class RGWLCCloudTierCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;
  bufferlist out_bl;
  int retcode;
  bool bucket_created = false;
  struct CreateBucketResult {
    string code;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Code", code, obj);
    }
  } result;

  public:
    RGWLCCloudTierCR(RGWLCCloudTierCtx& _tier_ctx) :
          RGWCoroutine(_tier_ctx.cct), tier_ctx(_tier_ctx) {}

    int operate() override;
};

struct rgw_lc_multipart_part_info {
  int part_num{0};
  uint64_t ofs{0};
  uint64_t size{0};
  string etag;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(part_num, bl);
    encode(ofs, bl);
    encode(size, bl);
    encode(etag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(part_num, bl);
    decode(ofs, bl);
    decode(size, bl);
    decode(etag, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_multipart_part_info)

struct rgw_lc_obj_properties {
  ceph::real_time mtime;
  string etag;
  uint64_t versioned_epoch{0};
  map<string, RGWTierACLMapping>& target_acl_mappings;
  string target_storage_class;

  rgw_lc_obj_properties(ceph::real_time _mtime, string _etag,
                        uint64_t _versioned_epoch, map<string,
                        RGWTierACLMapping>& _t_acl_mappings,
                        string _t_storage_class) :
                        mtime(_mtime), etag(_etag),
                        versioned_epoch(_versioned_epoch),
                        target_acl_mappings(_t_acl_mappings),
                        target_storage_class(_t_storage_class) {}
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(mtime, bl);
    encode(etag, bl);
    encode(versioned_epoch, bl);
    encode(target_acl_mappings, bl);
    encode(target_storage_class, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(mtime, bl);
    decode(etag, bl);
    decode(versioned_epoch, bl);
    decode(target_acl_mappings, bl);
    decode(target_storage_class, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_obj_properties)

struct rgw_lc_multipart_upload_info {
  string upload_id;
  uint64_t obj_size;
  uint32_t part_size{0};
  uint32_t num_parts{0};

  int cur_part{0};
  uint64_t cur_ofs{0};

  std::map<int, rgw_lc_multipart_part_info> parts;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(upload_id, bl);
    encode(obj_size, bl);
    encode(part_size, bl);
    encode(num_parts, bl);
    encode(cur_part, bl);
    encode(cur_ofs, bl);
    encode(parts, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(upload_id, bl);
    decode(obj_size, bl);
    decode(part_size, bl);
    decode(num_parts, bl);
    decode(cur_part, bl);
    decode(cur_ofs, bl);
    decode(parts, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_multipart_upload_info)

#endif
