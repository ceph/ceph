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
#include "rgw_sal_rados.h"

#define DEFAULT_MULTIPART_SYNC_PART_SIZE (32 * 1024 * 1024)
#define MULTIPART_MIN_POSSIBLE_PART_SIZE (5 * 1024 * 1024)

struct RGWLCCloudTierCtx {
  CephContext *cct;
  const DoutPrefixProvider *dpp;

  /* Source */
  rgw_bucket_dir_entry& o;
  rgw::sal::Store *store;
  RGWBucketInfo& bucket_info;
  string storage_class;

  std::unique_ptr<rgw::sal::Object>* obj;
  RGWObjectCtx& rctx;

  /* Remote */
  std::shared_ptr<RGWRESTConn> conn;
  string target_bucket_name;
  string target_storage_class;
  RGWHTTPManager *http_manager;

  map<string, RGWTierACLMapping> acl_mappings;
  uint64_t multipart_min_part_size;
  uint64_t multipart_sync_threshold;

  bool is_multipart_upload{false};

  RGWLCCloudTierCtx(CephContext* _cct, const DoutPrefixProvider *_dpp,
            rgw_bucket_dir_entry& _o, rgw::sal::Store* _store,
            RGWBucketInfo &_binfo, std::unique_ptr<rgw::sal::Object>* _obj,
            RGWObjectCtx& _rctx, std::shared_ptr<RGWRESTConn> _conn, string _bucket,
            string _storage_class, RGWHTTPManager *_http)
            : cct(_cct), dpp(_dpp), o(_o), store(_store), bucket_info(_binfo),
              obj(_obj), rctx(_rctx), conn(_conn), target_bucket_name(_bucket),
              target_storage_class(_storage_class), http_manager(_http) {}
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
  ceph::real_time mtime;
  string etag;
  uint32_t part_size{0};
  uint32_t num_parts{0};

  int cur_part{0};
  uint64_t cur_ofs{0};

  std::map<int, rgw_lc_multipart_part_info> parts;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(upload_id, bl);
    encode(obj_size, bl);
    encode(mtime, bl);
    encode(etag, bl);
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
    decode(mtime, bl);
    decode(etag, bl);
    decode(part_size, bl);
    decode(num_parts, bl);
    decode(cur_part, bl);
    decode(cur_ofs, bl);
    decode(parts, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_lc_multipart_upload_info)

class RGWLCStreamGetCRF : public RGWStreamReadHTTPResourceCRF
{
  RGWRESTConn::get_obj_params req_params;

  CephContext *cct;
  RGWHTTPManager *http_manager;
  rgw_lc_obj_properties obj_properties;
  std::shared_ptr<RGWRESTConn> conn;
  rgw::sal::Object* dest_obj;
  string etag;
  RGWRESTStreamRWRequest *in_req;
  map<string, string> headers;

  public:
  RGWLCStreamGetCRF(CephContext *_cct,
      RGWCoroutinesEnv *_env,
      RGWCoroutine *_caller,
      RGWHTTPManager *_http_manager,
      const rgw_lc_obj_properties&  _obj_properties,
      std::shared_ptr<RGWRESTConn> _conn,
      rgw::sal::Object* _dest_obj) :
    RGWStreamReadHTTPResourceCRF(_cct, _env, _caller, _http_manager, _dest_obj->get_key()),
                                 cct(_cct), http_manager(_http_manager), obj_properties(_obj_properties),
                                 conn(_conn), dest_obj(_dest_obj) {}
  int init(const DoutPrefixProvider *dpp);
  int is_already_tiered();
};

class RGWLCCloudTierCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;
  bufferlist out_bl;
  int retcode;
  struct CreateBucketResult {
    string code;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Code", code, obj);
    }
  } result;

  public:
    RGWLCCloudTierCR(RGWLCCloudTierCtx& _tier_ctx) :
          RGWCoroutine(_tier_ctx.cct), tier_ctx(_tier_ctx) {}

    int operate(const DoutPrefixProvider *dpp) override;
};

class RGWLCCloudCheckCR : public RGWCoroutine {
  RGWLCCloudTierCtx& tier_ctx;
  bufferlist bl;
  bool need_retry{false};
  int retcode;
  bool *already_tiered;
  rgw_lc_obj_properties obj_properties;
  RGWBucketInfo b;
  string target_obj_name;
  int ret = 0;
  std::unique_ptr<rgw::sal::Bucket> dest_bucket;
  std::unique_ptr<rgw::sal::Object> dest_obj;
  std::unique_ptr<RGWLCStreamGetCRF> get_crf;

  public:
    RGWLCCloudCheckCR(RGWLCCloudTierCtx& _tier_ctx, bool *_al_ti) :
          RGWCoroutine(_tier_ctx.cct), tier_ctx(_tier_ctx), already_tiered(_al_ti),
          obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
                         tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
                         tier_ctx.target_storage_class){}

    int operate(const DoutPrefixProvider *dpp) override;
};

#endif
