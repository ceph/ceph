
#pragma once

#include "include/encoding.h"

#include "rgw_sync_info.h"
#include "rgw_common.h"

#include "cls/rgw/cls_rgw_types.h"

namespace ceph {
  class Formatter;
}

namespace rgw { namespace sal {
  class RGWRadosStore;
} }

static std::string SIP_BUCKET_OP_CREATE_OBJ    = "create_obj";
static std::string SIP_BUCKET_OP_DELETE_OBJ    = "delete_obj";
static std::string SIP_BUCKET_OP_SET_CURRENT   = "set_current";
static std::string SIP_BUCKET_OP_CREATE_DM     = "create_dm";
static std::string SIP_BUCKET_OP_UNKNOWN       = "unknown";

/* rgw specific */
static std::string SIP_BUCKET_OP_SYNC_STOP     = "sync_stop";
static std::string SIP_BUCKET_OP_SYNC_START    = "sync_start";
static std::string SIP_BUCKET_OP_CANCEL        = "cancel";


struct siprovider_bucket_entry_info : public SIProvider::EntryInfoBase {
  string get_data_type() const override {
    return "bucket";
  }

  struct Info {
    std::string object;
    std::string instance;
    ceph::real_time timestamp;
    std::optional<uint64_t> versioned_epoch;
    std::string op;
    std::string owner;
    std::string owner_display_name;
    std::string instance_tag;
    bool complete{true};

    std::set<std::string> sync_trace;


    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(object, bl);
      encode(instance, bl);
      encode(timestamp, bl);
      encode(versioned_epoch, bl);
      encode(op, bl);
      encode(owner, bl);
      encode(owner_display_name, bl);
      encode(instance_tag, bl);
      encode(complete, bl);
      encode(sync_trace, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(object, bl);
      decode(instance, bl);
      decode(timestamp, bl);
      decode(versioned_epoch, bl);
      decode(op, bl);
      decode(owner, bl);
      decode(owner_display_name, bl);
      decode(instance_tag, bl);
      decode(complete, bl);
      decode(sync_trace, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const;
    void decode_json(JSONObj *obj);

    static const std::string& to_sip_op(RGWModifyOp rgw_op);
    static RGWModifyOp from_sip_op(const string& op, std::optional<uint64_t> versioned_epoch);
  } info;

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    info.encode(bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
     DECODE_START(1, bl);
     info.decode(bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const override;
  void decode_json(JSONObj *obj) override;
};
WRITE_CLASS_ENCODER(siprovider_bucket_entry_info::Info)
WRITE_CLASS_ENCODER(siprovider_bucket_entry_info)

class SIProvider_BucketFull : public SIProvider_SingleStage
{

  std::string to_marker(const cls_rgw_obj_key& k) const;
  SIProvider::Entry create_entry(rgw_bucket_dir_entry& be) const;

  int do_get_cur_state(const DoutPrefixProvider *dpp,
                       int shard_id, std::string *marker, ceph::real_time *timestamp,
                       bool *disabled, optional_yield y) const  override {
    marker->clear(); /* full data, no current incremental state */
    *timestamp = ceph::real_time();
    *disabled = false;
    return 0;
  }

protected:
  int do_fetch(const DoutPrefixProvider *dpp, int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(const DoutPrefixProvider *dpp, int shard_id, std::string *marker, ceph::real_time *timestamp) const override {
    marker->clear();
    *timestamp = ceph::real_time();
    return 0;
  }

  int do_trim(const DoutPrefixProvider *dpp, int shard_id, const std::string& marker) override {
    return 0;
  }

  rgw::sal::RGWRadosStore *store;
  RGWBucketInfo bucket_info;

public:
  SIProvider_BucketFull(CephContext *_cct,
                        rgw::sal::RGWRadosStore *_store,
                        RGWBucketInfo& _bucket_info) : SIProvider_SingleStage(_cct,
									     "bucket.full",
                                                                             _bucket_info.bucket.get_key(),
                                                                             std::make_shared<SITypeHandlerProvider_Default<siprovider_bucket_entry_info> >(),
                                                                             std::nullopt, /* stage id */
									     SIProvider::StageType::FULL,
									     _bucket_info.layout.current_index.layout.normal.num_shards,
                                                                             false),
                                                       store(_store),
                                                       bucket_info(_bucket_info) {
  }
};

class RGWBucketCtl;

class RGWSIPGen_BucketFull : public RGWSIPGenerator
{
  CephContext *cct;
  rgw::sal::RGWRadosStore *store;

  struct {
    RGWBucketCtl *bucket;
  } ctl;

public:
  RGWSIPGen_BucketFull(CephContext *_cct,
                       rgw::sal::RGWRadosStore *_store,
                       RGWBucketCtl *_bucket_ctl) : cct(_cct),
                                                    store(_store) {
    ctl.bucket = _bucket_ctl;
  }

  SIProviderRef get(const DoutPrefixProvider *dpp,
                    std::optional<std::string> instance) override;
};

class SIProvider_BucketInc : public SIProvider_SingleStage
{

  SIProvider::Entry create_entry(rgw_bi_log_entry& be) const;

protected:
  int do_fetch(const DoutPrefixProvider *dpp,
               int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(const DoutPrefixProvider *dpp,
                          int shard_id, std::string *marker, ceph::real_time *timestamp) const override {
    marker->clear();
    *timestamp = ceph::real_time();
    return 0;
  }

  int do_get_cur_state(const DoutPrefixProvider *dpp,
                       int shard_id, std::string *marker, ceph::real_time *timestamp,
                       bool *disabled, optional_yield y) const override;

  int do_trim(const DoutPrefixProvider *dpp,
              int shard_id, const std::string& marker) override;

  rgw::sal::RGWRadosStore *store;

  RGWBucketInfo bucket_info;

public:
  SIProvider_BucketInc(CephContext *_cct,
                       rgw::sal::RGWRadosStore *_store,
                       RGWBucketInfo& _bucket_info) : SIProvider_SingleStage(_cct,
									     "bucket.inc",
                                                                             _bucket_info.bucket.get_key(),
                                                                             std::make_shared<SITypeHandlerProvider_Default<siprovider_bucket_entry_info> >(),
                                                                             std::nullopt, /* stage id */
									     SIProvider::StageType::INC,
									     _bucket_info.layout.current_index.layout.normal.num_shards,
                                                                             !_bucket_info.datasync_flag_enabled()),
                                                       store(_store),
                                                       bucket_info(_bucket_info) {
  }
};

class RGWSIPGen_BucketInc : public RGWSIPGenerator
{
  CephContext *cct;
  rgw::sal::RGWRadosStore *store;

  struct {
    RGWBucketCtl *bucket;
  } ctl;

public:
  RGWSIPGen_BucketInc(CephContext *_cct,
                      rgw::sal::RGWRadosStore *_store,
                      RGWBucketCtl *_bucket_ctl) : cct(_cct),
                                                   store(_store) {
    ctl.bucket = _bucket_ctl;
  }

  SIProviderRef get(const DoutPrefixProvider *dpp,
                    std::optional<std::string> instance) override;
};

class RGWSIPGen_BucketContainer : public RGWSIPGenerator
{
  CephContext *cct;
  rgw::sal::RGWRadosStore *store;

  struct {
    RGWBucketCtl *bucket;
  } ctl;

public:
  RGWSIPGen_BucketContainer(CephContext *_cct,
                            rgw::sal::RGWRadosStore *_store,
                            RGWBucketCtl *_bucket_ctl) : cct(_cct),
                                                         store(_store) {
    ctl.bucket = _bucket_ctl;
  }

  SIProviderRef get(const DoutPrefixProvider *dpp,
                    std::optional<std::string> instance) override;
};

