
#pragma once

#include "include/encoding.h"

#include "rgw_sync_info.h"

namespace ceph {
  class Formatter;
}

class RGWMetadataManager;

struct siprovider_data_info : public SIProvider::EntryInfoBase {
  std::string id;
  std::optional<ceph::real_time> timestamp;

  siprovider_data_info() {}
  siprovider_data_info(const string& _id,
                       std::optional<ceph::real_time> _ts) : id(_id), timestamp(_ts) {}

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
     DECODE_START(1, bl);
     decode(id, bl);
     decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const override;
};
WRITE_CLASS_ENCODER(siprovider_data_info)

static inline SIProvider::Entry siprovider_data_create_entry(const std::string& k,
                                                             std::optional<ceph::real_time> timestamp,
                                                             const std::string& m)
{
  siprovider_data_info data_info = { k, timestamp };
  SIProvider::Entry e;
  e.key = m;
  data_info.encode(e.data);
  return e;
}

class RGWDatadataManager;

class SIProvider_DataFull : public SIProvider_SingleStage,
                            public SITypedProviderDefaultHandler<siprovider_data_info>
{
  struct {
    RGWMetadataManager *mgr;
  } meta;

protected:
  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int do_get_cur_state(int shard_id, std::string *marker) const {
    marker->clear(); /* full data, no current incremental state */
    return 0;
  }


  int do_trim(int shard_id, const std::string& marker) override {
    return 0;
  }

public:
  SIProvider_DataFull(CephContext *_cct,
                      RGWMetadataManager *meta_mgr) : SIProvider_SingleStage(_cct,
									     "data.full",
									     SIProvider::StageType::FULL,
									     1) {
    meta.mgr = meta_mgr;
  }

  int init() {
    return 0;
  }

};

class RGWDataChangesLog;

class SIProvider_DataInc : public SIProvider_SingleStage,
                           public SITypedProviderDefaultHandler<siprovider_data_info>
{
  struct {
    RGWDataChangesLog *datalog;
  } svc;

  RGWDataChangesLog *data_log{nullptr};

protected:
  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(int shard_id, std::string *marker) const override;
  int do_get_cur_state(int shard_id, std::string *marker) const;

  int do_trim( int shard_id, const std::string& marker) override;
public:
  SIProvider_DataInc(CephContext *_cct,
                     RGWSI_DataLog *_datalog_svc);

  int init();
};
