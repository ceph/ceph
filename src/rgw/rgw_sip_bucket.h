
#pragma once

#include "include/encoding.h"

#include "rgw_sync_info.h"
#include "rgw_common.h"

#include "cls/rgw/cls_rgw_types.h"

namespace rgw { namespace sal {
  class RGWRadosStore;
} }


struct siprovider_bucket_entry_info {
  rgw_bi_log_entry entry;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(entry, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(siprovider_bucket_entry_info)

class SIProvider_BucketFull : public SIProvider_SingleStage {

  std::string to_marker(const cls_rgw_obj_key& k) const;
  SIProvider::Entry create_entry(rgw_bucket_dir_entry& be) const;

protected:
  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
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

