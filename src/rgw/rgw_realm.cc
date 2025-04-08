// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <optional>

#include "common/errno.h"

#include "rgw_zone.h"
#include "rgw_realm_watcher.h"
#include "rgw_meta_sync_status.h"
#include "rgw_sal_config.h"
#include "rgw_string.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw_zone_defaults {

std::string realm_info_oid_prefix = "realms.";
std::string realm_names_oid_prefix = "realms_names.";
std::string default_realm_info_oid = "default.realm";
std::string RGW_DEFAULT_REALM_ROOT_POOL = "rgw.root";

}

using namespace std;
using namespace rgw_zone_defaults;

RGWRemoteMetaLog::~RGWRemoteMetaLog()
{
  delete error_logger;
}

rgw_pool RGWRealm::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_realm_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_REALM_ROOT_POOL);
  }
  return rgw_pool(cct->_conf->rgw_realm_root_pool);
}

const string& RGWRealm::get_info_oid_prefix(bool old_format) const
{
  return realm_info_oid_prefix;
}

string RGWRealm::get_control_oid() const
{
  return get_info_oid_prefix() + id + ".control";
}


int RGWRealm::find_zone(const DoutPrefixProvider *dpp,
                        const rgw_zone_id& zid,
                        RGWPeriod *pperiod,
                        RGWZoneGroup *pzonegroup,
                        bool *pfound,
                        rgw::sal::ConfigStore* cfgstore,
                        optional_yield y) const
{
  auto& found = *pfound;

  found = false;

  RGWPeriod period;
  int r = cfgstore->read_period(dpp, y, current_period, std::nullopt, period);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "WARNING: period init failed: " << cpp_strerror(-r) << " ... skipping" << dendl;
    return r;
  }

  found = period.find_zone(dpp, zid, pzonegroup, y);
  if (found) {
    *pperiod = period;
  }
  return 0;
}

void RGWRealm::generate_test_instances(list<RGWRealm*> &o)
{
  RGWRealm *z = new RGWRealm;
  o.push_back(z);
  o.push_back(new RGWRealm);
}

void RGWRealm::dump(Formatter *f) const
{
  encode_json("id", id , f);
  encode_json("name", name , f);
  encode_json("current_period", current_period, f);
  encode_json("epoch", epoch, f);
}


void RGWRealm::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("current_period", current_period, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
}

