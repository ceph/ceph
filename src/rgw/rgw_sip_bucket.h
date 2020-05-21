
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


struct siprovider_bucket_entry_info : public SIProvider::EntryInfoBase {
  rgw_bi_log_entry entry;

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
     DECODE_START(1, bl);
     decode(entry, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const override;
};
WRITE_CLASS_ENCODER(siprovider_bucket_entry_info)

class SIProvider_BucketFull : public SIProvider_SingleStage,
                              public SITypedProviderDefaultHandler<siprovider_bucket_entry_info>
{

  std::string to_marker(const cls_rgw_obj_key& k) const;
  SIProvider::Entry create_entry(rgw_bucket_dir_entry& be) const;

  int do_get_cur_state(int shard_id, std::string *marker) const  override {
    marker->clear(); /* full data, no current incremental state */
    return 0;
  }

protected:
  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int do_trim( int shard_id, const std::string& marker) override {
    return 0;
  }

  rgw::sal::RGWRadosStore *store;
  RGWBucketInfo bucket_info;

public:
  SIProvider_BucketFull(CephContext *_cct,
                        rgw::sal::RGWRadosStore *_store,
                        RGWBucketInfo& _bucket_info) : SIProvider_SingleStage(_cct,
									     "bucket.full",
									     SIProvider::StageType::FULL,
									     _bucket_info.layout.current_index.layout.normal.num_shards),
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

  SIProviderRef get(std::optional<std::string> instance) override;
};

class SIProvider_BucketInc : public SIProvider_SingleStage,
                              public SITypedProviderDefaultHandler<siprovider_bucket_entry_info>
{

  SIProvider::Entry create_entry(rgw_bi_log_entry& be) const;

protected:
  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int do_get_cur_state(int shard_id, std::string *marker) const override;

  int do_trim( int shard_id, const std::string& marker) override;

  rgw::sal::RGWRadosStore *store;

  RGWBucketInfo bucket_info;

public:
  SIProvider_BucketInc(CephContext *_cct,
                       rgw::sal::RGWRadosStore *_store,
                       RGWBucketInfo& _bucket_info) : SIProvider_SingleStage(_cct,
									     "bucket.inc",
									     SIProvider::StageType::FULL,
									     _bucket_info.layout.current_index.layout.normal.num_shards),
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

  SIProviderRef get(std::optional<std::string> instance) override;
};

