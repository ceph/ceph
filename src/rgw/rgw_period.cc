// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sync.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"

using namespace std;
using namespace rgw_zone_defaults;

std::string period_latest_epoch_info_oid = ".latest_epoch";
std::string period_info_oid_prefix = "periods.";

#define FIRST_EPOCH 1

const string& RGWPeriod::get_info_oid_prefix() const
{
  return period_info_oid_prefix;
}

bool RGWPeriod::find_zone(const DoutPrefixProvider *dpp,
                          const rgw_zone_id& zid,
                          RGWZoneGroup *pzonegroup,
                          optional_yield y) const
{
  RGWZoneGroup zg;
  RGWZone zone;

  bool found = period_map.find_zone_by_id(zid, &zg, &zone);
  if (found) {
    *pzonegroup = zg;
  }

  return found;
}

rgw_pool RGWPeriod::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_period_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_PERIOD_ROOT_POOL);
  }
  return rgw_pool(cct->_conf->rgw_period_root_pool);
}

void RGWPeriod::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("epoch", epoch , f);
  encode_json("predecessor_uuid", predecessor_uuid, f);
  encode_json("sync_status", sync_status, f);
  encode_json("period_map", period_map, f);
  encode_json("master_zonegroup", master_zonegroup, f);
  encode_json("master_zone", master_zone, f);
  encode_json("period_config", period_config, f);
  encode_json("realm_id", realm_id, f);
  encode_json("realm_epoch", realm_epoch, f);
}

void RGWPeriod::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
  JSONDecoder::decode_json("predecessor_uuid", predecessor_uuid, obj);
  JSONDecoder::decode_json("sync_status", sync_status, obj);
  JSONDecoder::decode_json("period_map", period_map, obj);
  JSONDecoder::decode_json("master_zonegroup", master_zonegroup, obj);
  JSONDecoder::decode_json("master_zone", master_zone, obj);
  JSONDecoder::decode_json("period_config", period_config, obj);
  JSONDecoder::decode_json("realm_id", realm_id, obj);
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

