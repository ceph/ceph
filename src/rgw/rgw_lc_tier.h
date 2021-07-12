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

  std::unique_ptr<rgw::sal::Object> *obj;
  RGWObjectCtx& rctx;

  /* Remote */
  std::shared_ptr<RGWRESTConn> conn;
  string target_bucket_name;
  string target_storage_class;

  map<string, RGWTierACLMapping> acl_mappings;
  uint64_t multipart_min_part_size;
  uint64_t multipart_sync_threshold;

  bool is_multipart_upload{false};

  RGWLCCloudTierCtx(CephContext* _cct, const DoutPrefixProvider *_dpp,
      rgw_bucket_dir_entry& _o, rgw::sal::Store *_store,
      RGWBucketInfo &_binfo, std::unique_ptr<rgw::sal::Object> *_obj,
      RGWObjectCtx& _rctx, std::shared_ptr<RGWRESTConn> _conn, string _bucket,
      string _storage_class) :
    cct(_cct), dpp(_dpp), o(_o), store(_store), bucket_info(_binfo),
    obj(_obj), rctx(_rctx), conn(_conn), target_bucket_name(_bucket),
    target_storage_class(_storage_class) {}
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

/* To GET/HEAD object from cloud endpoint */
class RGWLCCloudStreamGet
{
  RGWRESTConn::get_obj_params req_params;

  const DoutPrefixProvider *dpp;
  bool head; /* if Needed only headers */
  rgw_lc_obj_properties obj_properties;
  std::shared_ptr<RGWRESTConn> conn;
  rgw::sal::Object *dest_obj;
  string etag;
  RGWRESTStreamRWRequest *in_req;
  map<string, string> headers;

  int retcode;

  public:
  RGWLCCloudStreamGet(const DoutPrefixProvider *_dpp,
      bool _head,
      const rgw_lc_obj_properties&  _obj_properties,
      std::shared_ptr<RGWRESTConn> _conn,
      rgw::sal::Object *_dest_obj) :
    dpp(_dpp), head(_head), obj_properties(_obj_properties),
    conn(_conn), dest_obj(_dest_obj) {}
  int init(const DoutPrefixProvider *dpp);
  int is_already_tiered();
};

/* Read object locally & also initialize dest rest obj based on read attrs */
class RGWLCStreamRead
{
  CephContext *cct;
  const DoutPrefixProvider *dpp;
  map<string, bufferlist> attrs;
  uint64_t obj_size;
  std::unique_ptr<rgw::sal::Object> *obj;
  const real_time &mtime;

  bool multipart;
  uint64_t m_part_size;
  off_t m_part_off;
  off_t m_part_end;

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op;
  off_t ofs;
  off_t end;
  rgw_rest_obj rest_obj;

  int retcode;

  public:
  RGWLCStreamRead(CephContext *_cct, const DoutPrefixProvider *_dpp,
      RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::Object> *_obj,
      const real_time &_mtime) :
    cct(_cct), dpp(_dpp), obj(_obj), mtime(_mtime),
    read_op((*obj)->get_read_op(&obj_ctx)) {}

  ~RGWLCStreamRead() {};
  int set_range(off_t _ofs, off_t _end);
  int get_range(off_t &_ofs, off_t &_end);
  rgw_rest_obj get_rest_obj();
  void set_multipart(uint64_t part_size, off_t part_off, off_t part_end);
  int init();
  int init_rest_obj();
  int read(off_t ofs, off_t end, RGWGetDataCB *out_cb);
};

/* Send PUT op to remote endpoint */
class RGWLCCloudStreamPut
{
  const DoutPrefixProvider *dpp;
  rgw_lc_obj_properties obj_properties;
  std::shared_ptr<RGWRESTConn> conn;
  rgw::sal::Object *dest_obj;
  string etag;
  RGWRESTStreamS3PutObj *out_req{nullptr};

  struct multipart_info {
    bool is_multipart{false};
    string upload_id;
    int part_num{0};
    uint64_t part_size;
  } multipart;

  int retcode;

  public:
  RGWLCCloudStreamPut(const DoutPrefixProvider *_dpp,
      const rgw_lc_obj_properties&  _obj_properties,
      std::shared_ptr<RGWRESTConn> _conn,
      rgw::sal::Object *_dest_obj) :
    dpp(_dpp), obj_properties(_obj_properties), conn(_conn), dest_obj(_dest_obj) {
    }
  int init();
  static bool keep_attr(const string& h);
  static void init_send_attrs(const DoutPrefixProvider *dpp, const rgw_rest_obj& rest_obj,
      const rgw_lc_obj_properties& obj_properties,
      map<string, string> *attrs);
  void send_ready(const DoutPrefixProvider *dpp, const rgw_rest_obj& rest_obj);
  void handle_headers(const map<string, string>& headers);
  bool get_etag(string *petag);
  void set_multipart(const string& upload_id, int part_num, uint64_t part_size);
  int send();
  RGWGetDataCB *get_cb();
  int complete_request();
};

/* Read local copy and write to Cloud endpoint */
class RGWLCCloudStreamRW {
  const DoutPrefixProvider *dpp;
  string url;
  std::shared_ptr<RGWLCStreamRead> readf;
  std::shared_ptr<RGWLCCloudStreamPut> writef;
  bufferlist bl;
  bool need_retry{false};
  bool sent_attrs{false};
  uint64_t total_read{0};
  int ret{0};
  off_t ofs;
  off_t end;
  uint64_t read_len = 0;
  rgw_rest_obj rest_obj;
  int retcode;

  public:
  RGWLCCloudStreamRW(const DoutPrefixProvider *_dpp,
      std::shared_ptr<RGWLCStreamRead>& _readf,
      shared_ptr<RGWLCCloudStreamPut>& _writef) : dpp(_dpp),
  readf(_readf), writef(_writef) {}
  ~RGWLCCloudStreamRW() {}

  int process(const DoutPrefixProvider *dpp);
};

class RGWLCCloudStreamObjToPlain {
  RGWLCCloudTierCtx& tier_ctx;

  std::shared_ptr<RGWLCStreamRead> readf;
  std::shared_ptr<RGWLCCloudStreamPut> writef;
  std::unique_ptr<RGWLCCloudStreamRW> rwf;

  std::unique_ptr<rgw::sal::Bucket> dest_bucket;
  std::unique_ptr<rgw::sal::Object> dest_obj;

  rgw_lc_obj_properties obj_properties;
  RGWBucketInfo b;
  string target_obj_name;

  rgw::sal::Object *o;

  int retcode;

  public:
  RGWLCCloudStreamObjToPlain(RGWLCCloudTierCtx& _tier_ctx)
    : tier_ctx(_tier_ctx),
    obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
        tier_ctx.target_storage_class){}
  int process();
};

class RGWLCCloudStreamObjToMultipartPart {
  RGWLCCloudTierCtx& tier_ctx;

  string upload_id;

  rgw_lc_multipart_part_info part_info;

  string *petag;
  std::shared_ptr<RGWLCStreamRead> readf;
  std::shared_ptr<RGWLCCloudStreamPut> writef;
  std::unique_ptr<RGWLCCloudStreamRW> rwf;

  std::unique_ptr<rgw::sal::Bucket> dest_bucket;
  std::unique_ptr<rgw::sal::Object> dest_obj;

  rgw_lc_obj_properties obj_properties;
  RGWBucketInfo b;
  string target_obj_name;
  off_t end;

  int retcode;

  public:
  RGWLCCloudStreamObjToMultipartPart(RGWLCCloudTierCtx& _tier_ctx, const string& _upload_id,
      const rgw_lc_multipart_part_info& _part_info,
      string *_petag) : tier_ctx(_tier_ctx),
  upload_id(_upload_id), part_info(_part_info), petag(_petag),
  obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
      tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
      tier_ctx.target_storage_class){}
  int process();
};

class RGWLCCloudAbortMultipart {
  const DoutPrefixProvider *dpp;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  string upload_id;

  int retcode;
  bufferlist out_bl;

  public:
  RGWLCCloudAbortMultipart(const DoutPrefixProvider *_dpp,
      RGWRESTConn *_dest_conn, const rgw_obj& _dest_obj,
      const string& _upload_id) :
    dpp(_dpp),
    dest_conn(_dest_conn), dest_obj(_dest_obj),
    upload_id(_upload_id) {}
  int process();
};

class RGWLCCloudInitMultipart {
  const DoutPrefixProvider *dpp;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  uint64_t obj_size;
  map<string, string> attrs;

  bufferlist out_bl;

  string *upload_id;

  struct InitMultipartResult {
    string bucket;
    string key;
    string upload_id;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("UploadId", upload_id, obj);
    }
  } result;

  int retcode;

  public:
  RGWLCCloudInitMultipart(const DoutPrefixProvider *_dpp,
      RGWRESTConn *_dest_conn, const rgw_obj& _dest_obj,
      uint64_t _obj_size, const map<string, string>& _attrs,
      string *_upload_id) : dpp(_dpp),
  dest_conn(_dest_conn),
  dest_obj(_dest_obj), obj_size(_obj_size),
  attrs(_attrs), upload_id(_upload_id) {}
  int process();
};

class RGWLCCloudCompleteMultipart {
  const DoutPrefixProvider *dpp;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  bufferlist out_bl;

  string upload_id;

  int retcode;

  struct CompleteMultipartReq {
    map<int, rgw_lc_multipart_part_info> parts;

    explicit CompleteMultipartReq(const map<int, rgw_lc_multipart_part_info>& _parts) : parts(_parts) {}

    void dump_xml(Formatter *f) const {
      for (const auto& p : parts) {
        f->open_object_section("Part");
        encode_xml("PartNumber", p.first, f);
        encode_xml("ETag", p.second.etag, f);
        f->close_section();
      };
    }
  } req_enc;

  struct CompleteMultipartResult {
    string location;
    string bucket;
    string key;
    string etag;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Location", bucket, obj);
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("ETag", etag, obj);
    }
  } result;

  public:
  RGWLCCloudCompleteMultipart(const DoutPrefixProvider *_dpp,
      RGWRESTConn *_dest_conn, const rgw_obj& _dest_obj,
      string _upload_id, const map<int, rgw_lc_multipart_part_info>& _parts) :
    dpp(_dpp), 
    dest_conn(_dest_conn), dest_obj(_dest_obj), upload_id(_upload_id),
    req_enc(_parts) {}

  int process();
};

class RGWLCCloudStreamAbortMultipartUpload {
  RGWLCCloudTierCtx& tier_ctx;
  const rgw_obj dest_obj;
  const rgw_raw_obj status_obj;

  string upload_id;
  std::unique_ptr<RGWLCCloudAbortMultipart> abort_mp;

  int retcode;

  public:

  RGWLCCloudStreamAbortMultipartUpload(RGWLCCloudTierCtx& _tier_ctx,
      const rgw_obj& _dest_obj, const rgw_raw_obj& _status_obj,
      const string& _upload_id) :
    tier_ctx(_tier_ctx), dest_obj(_dest_obj), status_obj(_status_obj),
    upload_id(_upload_id) {}
  int process();
};


class RGWLCCloudStreamObjToMultipart {
  RGWLCCloudTierCtx& tier_ctx;
  RGWRESTConn *source_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  uint64_t obj_size;
  string src_etag;
  rgw_rest_obj rest_obj;

  rgw_lc_multipart_upload_info status;
  std::shared_ptr<RGWLCStreamRead> readf;

  map<string, string> new_attrs;

  rgw_raw_obj status_obj;

  rgw_lc_obj_properties obj_properties;
  RGWBucketInfo b;
  string target_obj_name;
  rgw_bucket target_bucket;
  rgw::sal::RadosStore *rados;

  int retcode;

  public:
  RGWLCCloudStreamObjToMultipart(RGWLCCloudTierCtx& _tier_ctx)
    : tier_ctx(_tier_ctx),
    obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
        tier_ctx.target_storage_class){}
  int process();
};

/* Verify if object already exists in the cloud endpoint */
class RGWLCCloudCheckObj {
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
  std::unique_ptr<RGWLCCloudStreamGet> getf;

  public:
  RGWLCCloudCheckObj(RGWLCCloudTierCtx& _tier_ctx, bool *_al_ti) :
    tier_ctx(_tier_ctx), already_tiered(_al_ti),
    obj_properties(tier_ctx.o.meta.mtime, tier_ctx.o.meta.etag,
        tier_ctx.o.versioned_epoch, tier_ctx.acl_mappings,
        tier_ctx.target_storage_class){}

  int process();
};

/* Transition object to cloud endpoint */
class RGWLCCloudTier {
  RGWLCCloudTierCtx& tier_ctx;
  bufferlist out_bl;
  int retcode;
  std::unique_ptr<RGWLCCloudCheckObj> chk_cloud;
  std::unique_ptr<RGWLCCloudStreamObjToPlain> cloud_tier_plain;
  std::unique_ptr<RGWLCCloudStreamObjToMultipart> cloud_tier_mp;
  struct CreateBucketResult {
    string code;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Code", code, obj);
    }
  } result;

  public:
  RGWLCCloudTier(RGWLCCloudTierCtx& _tier_ctx) : tier_ctx(_tier_ctx) {}
  int process();
};

#endif
