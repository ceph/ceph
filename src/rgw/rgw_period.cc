// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "rgw_sync.h"

using namespace std;
using namespace rgw_zone_defaults;

std::string period_latest_epoch_info_oid = ".latest_epoch";
std::string period_info_oid_prefix = "periods.";

#define FIRST_EPOCH 1

int RGWPeriod::init(const DoutPrefixProvider *dpp, 
                    CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
		    optional_yield y, bool setup_obj)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;

  if (!setup_obj)
    return 0;

  if (id.empty()) {
    RGWRealm realm(realm_id);
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 4) << "RGWPeriod::init failed to init realm  id " << realm_id << " : " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }
    id = realm.get_current_period();
    realm_id = realm.get_id();
  }

  if (!epoch) {
    int ret = use_latest_epoch(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to use_latest_epoch period id " << id << " realm id " << realm_id
	   << " : " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return read_info(dpp, y);
}

int RGWPeriod::init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
		    const string& period_realm_id, optional_yield y, bool setup_obj)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;

  realm_id = period_realm_id;

  if (!setup_obj)
    return 0;

  return init(dpp, _cct, _sysobj_svc, y, setup_obj);
}

const string& RGWPeriod::get_latest_epoch_oid() const
{
  if (cct->_conf->rgw_period_latest_epoch_info_oid.empty()) {
    return period_latest_epoch_info_oid;
  }
  return cct->_conf->rgw_period_latest_epoch_info_oid;
}

const string& RGWPeriod::get_info_oid_prefix() const
{
  return period_info_oid_prefix;
}

const string RGWPeriod::get_period_oid_prefix() const
{
  return get_info_oid_prefix() + id;
}

const string RGWPeriod::get_period_oid() const
{
  std::ostringstream oss;
  oss << get_period_oid_prefix();
  // skip the epoch for the staging period
  if (id != get_staging_id(realm_id))
    oss << "." << epoch;
  return oss.str();
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

int RGWPeriod::set_latest_epoch(const DoutPrefixProvider *dpp, 
                                optional_yield y,
				epoch_t epoch, bool exclusive,
                                RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool(cct));
  bufferlist bl;

  RGWPeriodLatestEpochInfo info;
  info.epoch = epoch;

  using ceph::encode;
  encode(info, bl);

  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWPeriod::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  bufferlist bl;

  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj{pool, get_period_oid()});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed reading obj info from " << pool << ":" << get_period_oid() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << get_period_oid() << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  string oid = get_period_oid();
  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);

  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWPeriod::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  int ret;
  
  /* create unique id */
  uuid_d new_uuid;
  char uuid_str[37];
  new_uuid.generate_random();
  new_uuid.print(uuid_str);
  id = uuid_str;

  epoch = FIRST_EPOCH;

  period_map.id = id;

  ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing info for " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = set_latest_epoch(dpp, y, epoch);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: setting latest epoch " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWPeriod::reflect(const DoutPrefixProvider *dpp, optional_yield y)
{
  for (auto& iter : period_map.zonegroups) {
    RGWZoneGroup& zg = iter.second;
    zg.reinit_instance(cct, sysobj_svc);
    int r = zg.write(dpp, false, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to store zonegroup info for zonegroup=" << iter.first << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    if (zg.is_master_zonegroup()) {
      // set master as default if no default exists
      r = zg.set_as_default(dpp, y, true);
      if (r == 0) {
        ldpp_dout(dpp, 1) << "Set the period's master zonegroup " << zg.get_id()
            << " as the default" << dendl;
      }
    }
  }

  int r = period_config.write(dpp, sysobj_svc, realm_id, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to store period config: "
        << cpp_strerror(-r) << dendl;
    return r;
  }
  return 0;
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

int RGWPeriod::update_latest_epoch(const DoutPrefixProvider *dpp, epoch_t epoch, optional_yield y)
{
  static constexpr int MAX_RETRIES = 20;

  for (int i = 0; i < MAX_RETRIES; i++) {
    RGWPeriodLatestEpochInfo info;
    RGWObjVersionTracker objv;
    bool exclusive = false;

    // read existing epoch
    int r = read_latest_epoch(dpp, info, y, &objv);
    if (r == -ENOENT) {
      // use an exclusive create to set the epoch atomically
      exclusive = true;
      ldpp_dout(dpp, 20) << "creating initial latest_epoch=" << epoch
          << " for period=" << id << dendl;
    } else if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to read latest_epoch" << dendl;
      return r;
    } else if (epoch <= info.epoch) {
      r = -EEXIST; // fail with EEXIST if epoch is not newer
      ldpp_dout(dpp, 10) << "found existing latest_epoch " << info.epoch
          << " >= given epoch " << epoch << ", returning r=" << r << dendl;
      return r;
    } else {
      ldpp_dout(dpp, 20) << "updating latest_epoch from " << info.epoch
          << " -> " << epoch << " on period=" << id << dendl;
    }

    r = set_latest_epoch(dpp, y, epoch, exclusive, &objv);
    if (r == -EEXIST) {
      continue; // exclusive create raced with another update, retry
    } else if (r == -ECANCELED) {
      continue; // write raced with a conflicting version, retry
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to write latest_epoch" << dendl;
      return r;
    }
    return 0; // return success
  }

  return -ECANCELED; // fail after max retries
}

int RGWPeriod::read_latest_epoch(const DoutPrefixProvider *dpp, 
                                 RGWPeriodLatestEpochInfo& info,
				 optional_yield y,
                                 RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool(cct));
  bufferlist bl;
  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "error read_lastest_epoch " << pool << ":" << oid << dendl;
    return ret;
  }
  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::use_latest_epoch(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWPeriodLatestEpochInfo info;
  int ret = read_latest_epoch(dpp, info, y);
  if (ret < 0) {
    return ret;
  }

  epoch = info.epoch;

  return 0;
}

