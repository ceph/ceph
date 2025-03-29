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

RGWRealm::~RGWRealm() {}

RGWRemoteMetaLog::~RGWRemoteMetaLog()
{
  delete error_logger;
}

string RGWRealm::get_predefined_id(CephContext *cct) const {
  return cct->_conf.get_val<string>("rgw_realm_id");
}

const string& RGWRealm::get_predefined_name(CephContext *cct) const {
  return cct->_conf->rgw_realm;
}

rgw_pool RGWRealm::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_realm_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_REALM_ROOT_POOL);
  }
  return rgw_pool(cct->_conf->rgw_realm_root_pool);
}

const string RGWRealm::get_default_oid(bool old_format) const
{
  if (cct->_conf->rgw_default_realm_info_oid.empty()) {
    return default_realm_info_oid;
  }
  return cct->_conf->rgw_default_realm_info_oid;
}

const string& RGWRealm::get_names_oid_prefix() const
{
  return realm_names_oid_prefix;
}

const string& RGWRealm::get_info_oid_prefix(bool old_format) const
{
  return realm_info_oid_prefix;
}

int RGWRealm::set_current_period(const DoutPrefixProvider *dpp, RGWPeriod& period, optional_yield y)
{
  // update realm epoch to match the period's
  if (epoch > period.get_realm_epoch()) {
    ldpp_dout(dpp, 0) << "ERROR: set_current_period with old realm epoch "
        << period.get_realm_epoch() << ", current epoch=" << epoch << dendl;
    return -EINVAL;
  }
  if (epoch == period.get_realm_epoch() && current_period != period.get_id()) {
    ldpp_dout(dpp, 0) << "ERROR: set_current_period with same realm epoch "
        << period.get_realm_epoch() << ", but different period id "
        << period.get_id() << " != " << current_period << dendl;
    return -EINVAL;
  }

  epoch = period.get_realm_epoch();
  current_period = period.get_id();

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  auto cfgstore = DriverManager::create_config_store(dpp, config_store_type);
  std::unique_ptr<rgw::sal::RealmWriter> writer;
  int ret = cfgstore->create_realm(dpp, y, false, *this, &writer);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: realm create: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = rgw::realm_set_current_period(dpp, y, cfgstore.get(), *writer, *this, period);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: period update: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = rgw::reflect_period(dpp, y, cfgstore.get(), period);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: period.reflect(): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
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
                        optional_yield y) const
{
  auto& found = *pfound;

  found = false;

  string period_id;
  epoch_t epoch = 0;

  RGWPeriod period(period_id, epoch);
  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  auto cfgstore = DriverManager::create_config_store(dpp, config_store_type);
  int r = cfgstore->read_period(dpp, y, period_id, epoch, period);
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

