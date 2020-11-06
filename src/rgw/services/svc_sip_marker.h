// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"


class SIProvider;
using SIProviderRef = std::shared_ptr<SIProvider>;

namespace ceph {
  class Formatter;
}


class RGWSI_SIP_Marker : public RGWServiceInstance
{
public:

  using stage_id_t = string;

  struct target_marker_info {
    string pos;
    ceph::real_time mtime;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(pos, bl);
      encode(mtime, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(pos, bl);
      decode(mtime, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const;
  };

  struct stage_shard_info {
    map<string, target_marker_info> targets;
    std::optional<std::string> min_targets_pos;
    std::optional<std::string> min_source_pos;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      encode(targets, bl);
      encode(min_targets_pos, bl);
      encode(min_source_pos, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(targets, bl);
      decode(min_targets_pos, bl);
      decode(min_source_pos, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const;
  };

  struct SetParams {
    std::string target_id;
    std::string marker;
    ceph::real_time mtime;
    bool check_exists{false};

    void dump(Formatter *f) const;
    void decode_json(JSONObj *obj);
  };

  class Handler {
  public:
    virtual ~Handler() {}

    struct modify_result {
      bool modified{false};
      std::optional<std::string> min_pos;

      void dump(Formatter *f) const;
    };

    virtual int set_marker(const stage_id_t& sid,
                           int shard_id,
                           const RGWSI_SIP_Marker::SetParams& params,
                           modify_result *result) = 0;

    virtual int remove_target(const string& target_id,
                              const stage_id_t& sid,
                              int shard_id,
                              modify_result *result) = 0;

    virtual int set_min_source_pos(const stage_id_t& sid,
                                   int shard_id,
                                   const std::string& pos) = 0;

    virtual int get_min_targets_pos(const stage_id_t& sid,
                                    int shard_id,
                                    std::optional<std::string> *pos) = 0;

    virtual int get_info(const stage_id_t& sid,
                         int shard_id,
                         stage_shard_info *info) = 0;

    virtual int remove_info(const stage_id_t& sid,
                            int shard_id) = 0;
  };

  using HandlerRef = std::shared_ptr<Handler>;

  RGWSI_SIP_Marker(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_SIP_Marker() {}

  virtual HandlerRef get_handler(SIProviderRef& sip) = 0;

  static std::string create_target_id(const rgw_zone_id& zid,
                                      std::optional<std::string> bucket_id);
  static void parse_target_id(const std::string& target_id,
                              rgw_zone_id *zid,
                              std::string *bucket_id);

};
WRITE_CLASS_ENCODER(RGWSI_SIP_Marker::target_marker_info)
WRITE_CLASS_ENCODER(RGWSI_SIP_Marker::stage_shard_info)
