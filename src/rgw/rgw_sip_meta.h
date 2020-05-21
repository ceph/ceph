
#pragma once

#include "include/encoding.h"

#include "rgw_sync_info.h"

namespace ceph {
  class Formatter;
}

struct siprovider_meta_info : public SIProvider::EntryInfoBase {
  std::string section;
  std::string id;

  siprovider_meta_info() {}
  siprovider_meta_info(const string& _section, const string& _id) : section(_section),
                                                                    id(_id) {}

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    encode(section, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) override {
     DECODE_START(1, bl);
     decode(section, bl);
     decode(id, bl);
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const override;
};
WRITE_CLASS_ENCODER(siprovider_meta_info)

class RGWMetadataManager;

class SIProvider_MetaFull : public SIProvider_SingleStage,
                            public SITypedProviderDefaultHandler<siprovider_meta_info>
{
  struct {
    RGWMetadataManager *mgr;
  } meta;

  std::list<std::string> sections;
  std::map<std::string, std::string> next_section_map;

  void append_section_from_set(std::set<std::string>& all_sections, const std::string& name);
  void rearrange_sections();
  int get_all_sections();

  int next_section(const std::string& section, string *next);

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
  SIProvider_MetaFull(CephContext *_cct,
                      RGWMetadataManager *meta_mgr) : SIProvider_SingleStage(_cct,
									     "meta.full",
									     SIProvider::StageType::FULL,
									     1) {
    meta.mgr = meta_mgr;
  }

  int init();

  int next_meta_section(const std::string& cur_section, std::string *next) const;

  std::string to_marker(const std::string& section, const std::string& k) const;

  SIProvider::Entry create_entry(const std::string& section,
                                 const std::string& k) const {
    siprovider_meta_info meta_info = { section, k };
    SIProvider::Entry e;
    e.key = to_marker(section, k);
    meta_info.encode(e.data);
    return e;
  }
};

class RGWSI_MDLog;
class RGWMetadataLog;

class SIProvider_MetaInc : public SIProvider_SingleStage,
                           public SITypedProviderDefaultHandler<siprovider_meta_info>
{
  RGWSI_MDLog *mdlog;
  string period_id;

  RGWMetadataLog *meta_log{nullptr};

protected:
  int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int do_get_start_marker(int shard_id, std::string *marker) const override;
  int do_get_cur_state(int shard_id, std::string *marker) const;

  int do_trim( int shard_id, const std::string& marker) override;
public:
  SIProvider_MetaInc(CephContext *_cct,
                     RGWSI_MDLog *_mdlog,
                     const string& _period_id);

  int init();
};

