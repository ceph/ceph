
#pragma once

#include "include/encoding.h"

#include "rgw_sync_info.h"


struct siprovider_meta_info {
  std::string section;
  std::string id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(section, bl);
    encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(section, bl);
     decode(id, bl);
     DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(siprovider_meta_info)

class RGWMetadataManager;

class SIProvider_MetaFull : public SIProvider {
  CephContext *cct;
  struct {
    RGWMetadataManager *mgr;
  } meta;

  std::list<std::string> sections;
  std::map<std::string, std::string> next_section_map;

  void append_section_from_set(std::set<std::string>& all_sections, const std::string& name);
  void rearrange_sections();
  int get_all_sections();

  int next_section(const std::string& section, string *next);

public:
  SIProvider_MetaFull(CephContext *_cct,
                      RGWMetadataManager *meta_mgr) : cct(_cct) {
    meta.mgr = meta_mgr;
  }

  int init();

  Info get_info() const override {
    return { Type::FULL, 1 };
  }

  int fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int get_start_marker(int shard_id, std::string *marker) const override {
    marker->clear();
    return 0;
  }

  int get_cur_state(int shard_id, std::string *marker) const {
    marker->clear(); /* full data, no current incremental state */
    return 0;
  }

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

class SIProvider_MetaInc : public SIProvider {
  RGWSI_MDLog *mdlog;
  string period_id;

  RGWMetadataLog *meta_log{nullptr};

public:
  SIProvider_MetaInc(CephContext *_cct,
                     RGWSI_MDLog *_mdlog,
                     const string& _period_id) : cct(_cct),
                                                 mdlog(_mdlog),
                                                 period_id(_period_id) {}

  int init();

  Info get_info() const override;

  int fetch(int shard_id, std::string marker, int max, fetch_result *result) override;

  int get_start_marker(int shard_id, std::string *marker) const override;
  int get_cur_state(int shard_id, std::string *marker) const;
};

