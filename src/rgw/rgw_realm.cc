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

int RGWRealm::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  int ret = RGWSystemMetaObj::create(dpp, y, exclusive);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR creating new realm object " << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  // create the control object for watch/notify
  ret = create_control(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR creating control for new realm " << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  RGWPeriod period;
  if (current_period.empty()) {
    /* create new period for the realm */
    ret = period.init(dpp, cct, sysobj_svc, id, y, false);
    if (ret < 0 ) {
      return ret;
    }
    ret = period.create(dpp, y, true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: creating new period for realm " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  } else {
    period = RGWPeriod(current_period, 0);
    int ret = period.init(dpp, cct, sysobj_svc, id, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to init period " << current_period << dendl;
      return ret;
    }
  }
  ret = set_current_period(dpp, period, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed set current period " << current_period << dendl;
    return ret;
  }
  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  ret = set_as_default(dpp, y, true);
  if (ret < 0 && ret != -EEXIST) {
    ldpp_dout(dpp, 0) << "WARNING: failed to set realm as default realm, ret=" << ret << dendl;
  }

  return 0;
}

int RGWRealm::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = RGWSystemMetaObj::delete_obj(dpp, y);
  if (ret < 0) {
    return ret;
  }
  return delete_control(dpp, y);
}

int RGWRealm::create_control(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  auto pool = rgw_pool{get_pool(cct)};
  auto oid = get_control_oid();
  bufferlist bl;
  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWRealm::delete_control(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto pool = rgw_pool{get_pool(cct)};
  auto obj = rgw_raw_obj{pool, get_control_oid()};
  auto sysobj = sysobj_svc->get_obj(obj);
  return sysobj.wop().remove(dpp, y);
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

  int ret = update(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: period update: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = period.reflect(dpp, y);
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

int RGWRealm::notify_zone(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y)
{
  rgw_pool pool{get_pool(cct)};
  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj{pool, get_control_oid()});
  int ret = sysobj.wn().notify(dpp, bl, 0, nullptr, y);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RGWRealm::notify_new_period(const DoutPrefixProvider *dpp, const RGWPeriod& period, optional_yield y)
{
  bufferlist bl;
  using ceph::encode;
  // push the period to dependent zonegroups/zones
  encode(RGWRealmNotify::ZonesNeedPeriod, bl);
  encode(period, bl);
  // reload the gateway with the new period
  encode(RGWRealmNotify::Reload, bl);

  return notify_zone(dpp, bl, y);
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
  int r = period.init(dpp, cct, sysobj_svc, get_id(), y);
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
  RGWSystemMetaObj::dump(f);
  encode_json("current_period", current_period, f);
  encode_json("epoch", epoch, f);
}


void RGWRealm::decode_json(JSONObj *obj)
{
  RGWSystemMetaObj::decode_json(obj);
  JSONDecoder::decode_json("current_period", current_period, obj);
  JSONDecoder::decode_json("epoch", epoch, obj);
}

