// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"

#include "rgw_zone.h"
#include "rgw_realm_watcher.h"
#include "rgw_meta_sync_status.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rgw_zone_defaults;

std::string period_latest_epoch_info_oid = ".latest_epoch";
std::string period_info_oid_prefix = "periods.";

#define FIRST_EPOCH 1

int RGWPeriod::init(const DoutPrefixProvider *dpp, CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
		    const string& period_realm_id, optional_yield y,
		    const string& period_realm_name, bool setup_obj)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;

  realm_id = period_realm_id;
  realm_name = period_realm_name;

  if (!setup_obj)
    return 0;

  return init(dpp, _cct, _sysobj_svc, y, setup_obj);
}


int RGWPeriod::init(const DoutPrefixProvider *dpp, 
                    CephContext *_cct, RGWSI_SysObj *_sysobj_svc,
		    optional_yield y, bool setup_obj)
{
  cct = _cct;
  sysobj_svc = _sysobj_svc;

  if (!setup_obj)
    return 0;

  if (id.empty()) {
    RGWRealm realm(realm_id, realm_name);
    int ret = realm.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 4) << "RGWPeriod::init failed to init realm " << realm_name << " id " << realm_id << " : " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }
    id = realm.get_current_period();
    realm_id = realm.get_id();
  }

  if (!epoch) {
    int ret = use_latest_epoch(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to use_latest_epoch period id " << id << " realm " << realm_name  << " id " << realm_id
	   << " : " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return read_info(dpp, y);
}


int RGWPeriod::get_zonegroup(RGWZoneGroup& zonegroup,
                             const string& zonegroup_id) const
{
  map<string, RGWZoneGroup>::const_iterator iter;
  if (!zonegroup_id.empty()) {
    iter = period_map.zonegroups.find(zonegroup_id);
  } else {
    iter = period_map.zonegroups.find("default");
  }
  if (iter != period_map.zonegroups.end()) {
    zonegroup = iter->second;
    return 0;
  }

  return -ENOENT;
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

int RGWPeriod::read_latest_epoch(const DoutPrefixProvider *dpp, 
                                 RGWPeriodLatestEpochInfo& info,
				 optional_yield y,
                                 RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool(cct));
  bufferlist bl;
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
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

int RGWPeriod::get_latest_epoch(const DoutPrefixProvider *dpp, epoch_t& latest_epoch, optional_yield y)
{
  RGWPeriodLatestEpochInfo info;

  int ret = read_latest_epoch(dpp, info, y);
  if (ret < 0) {
    return ret;
  }

  latest_epoch = info.epoch;

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

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
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

int RGWPeriod::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  // delete the object for each period epoch
  for (epoch_t e = 1; e <= epoch; e++) {
    RGWPeriod p{get_id(), e};
    rgw_raw_obj oid{pool, p.get_period_oid()};
    auto obj_ctx = sysobj_svc->init_obj_ctx();
    auto sysobj = sysobj_svc->get_obj(obj_ctx, oid);
    int ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
          << ": " << cpp_strerror(-ret) << dendl;
    }
  }

  // delete the .latest_epoch object
  rgw_raw_obj oid{pool, get_period_oid_prefix() + get_latest_epoch_oid()};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, oid);
  int ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
        << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int RGWPeriod::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, get_period_oid()});
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

int RGWPeriod::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  string oid = get_period_oid();
  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

rgw_pool RGWPeriod::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_period_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_PERIOD_ROOT_POOL);
  }
  return rgw_pool(cct->_conf->rgw_period_root_pool);
}

int RGWPeriod::add_zonegroup(const DoutPrefixProvider *dpp, const RGWZoneGroup& zonegroup, optional_yield y)
{
  if (zonegroup.realm_id != realm_id) {
    return 0;
  }
  int ret = period_map.update(zonegroup, cct);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: updating period map: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_info(dpp, false, y);
}

int RGWPeriod::update(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto zone_svc = sysobj_svc->get_zone_svc();
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm_id << " period " << get_id() << dendl;
  list<string> zonegroups;
  int ret = zone_svc->list_zonegroups(dpp, zonegroups);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to list zonegroups: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  // clear zone short ids of removed zones. period_map.update() will add the
  // remaining zones back
  period_map.short_zone_ids.clear();

  for (auto& iter : zonegroups) {
    RGWZoneGroup zg(string(), iter);
    ret = zg.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: zg.init() failed: " << cpp_strerror(-ret) << dendl;
      continue;
    }

    if (zg.realm_id != realm_id) {
      ldpp_dout(dpp, 20) << "skipping zonegroup " << zg.get_name() << " zone realm id " << zg.realm_id << ", not on our realm " << realm_id << dendl;
      continue;
    }

    if (zg.master_zone.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name() << " should have a master zone " << dendl;
      return -EINVAL;
    }

    if (zg.zones.find(zg.master_zone) == zg.zones.end()) {
      ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name()
                   << " has a non existent master zone "<< dendl;
      return -EINVAL;
    }

    if (zg.is_master_zonegroup()) {
      master_zonegroup = zg.get_id();
      master_zone = zg.master_zone;
    }

    int ret = period_map.update(zg, cct);
    if (ret < 0) {
      return ret;
    }
  }

  ret = period_config.read(dpp, sysobj_svc, realm_id, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read period config: "
        << cpp_strerror(ret) << dendl;
    return ret;
  }
  return 0;
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

void RGWPeriod::fork()
{
  ldout(cct, 20) << __func__ << " realm " << realm_id << " period " << id << dendl;
  predecessor_uuid = id;
  id = get_staging_id(realm_id);
  period_map.reset();
  realm_epoch++;
}

static int read_sync_status(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, rgw_meta_sync_status *sync_status)
{
  // initialize a sync status manager to read the status
  RGWMetaSyncStatusManager mgr(store, store->svc()->rados->get_async_processor());
  int r = mgr.init(dpp);
  if (r < 0) {
    return r;
  }
  r = mgr.read_sync_status(dpp, sync_status);
  mgr.stop();
  return r;
}

int RGWPeriod::update_sync_status(const DoutPrefixProvider *dpp,
                                  rgw::sal::Store* store, /* for now */
				  const RGWPeriod &current_period,
                                  std::ostream& error_stream,
                                  bool force_if_stale)
{
  rgw_meta_sync_status status;
  int r = read_sync_status(dpp, static_cast<rgw::sal::RadosStore*>(store), &status);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "period failed to read sync status: "
        << cpp_strerror(-r) << dendl;
    return r;
  }

  std::vector<std::string> markers;

  const auto current_epoch = current_period.get_realm_epoch();
  if (current_epoch != status.sync_info.realm_epoch) {
    // no sync status markers for the current period
    ceph_assert(current_epoch > status.sync_info.realm_epoch);
    const int behind = current_epoch - status.sync_info.realm_epoch;
    if (!force_if_stale && current_epoch > 1) {
      error_stream << "ERROR: This zone is " << behind << " period(s) behind "
          "the current master zone in metadata sync. If this zone is promoted "
          "to master, any metadata changes during that time are likely to "
          "be lost.\n"
          "Waiting for this zone to catch up on metadata sync (see "
          "'radosgw-admin sync status') is recommended.\n"
          "To promote this zone to master anyway, add the flag "
          "--yes-i-really-mean-it." << std::endl;
      return -EINVAL;
    }
    // empty sync status markers - other zones will skip this period during
    // incremental metadata sync
    markers.resize(status.sync_info.num_shards);
  } else {
    markers.reserve(status.sync_info.num_shards);
    for (auto& i : status.sync_markers) {
      auto& marker = i.second;
      // filter out markers from other periods
      if (marker.realm_epoch != current_epoch) {
        marker.marker.clear();
      }
      markers.emplace_back(std::move(marker.marker));
    }
  }

  std::swap(sync_status, markers);
  return 0;
}

int RGWPeriod::commit(const DoutPrefixProvider *dpp,
		      rgw::sal::Store* store,
		      RGWRealm& realm, const RGWPeriod& current_period,
                      std::ostream& error_stream, optional_yield y,
		      bool force_if_stale)
{
  auto zone_svc = sysobj_svc->get_zone_svc();
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm.get_id() << " period " << current_period.get_id() << dendl;
  // gateway must be in the master zone to commit
  if (master_zone != zone_svc->get_zone_params().get_id()) {
    error_stream << "Cannot commit period on zone "
        << zone_svc->get_zone_params().get_id() << ", it must be sent to "
        "the period's master zone " << master_zone << '.' << std::endl;
    return -EINVAL;
  }
  // period predecessor must match current period
  if (predecessor_uuid != current_period.get_id()) {
    error_stream << "Period predecessor " << predecessor_uuid
        << " does not match current period " << current_period.get_id()
        << ". Use 'period pull' to get the latest period from the master, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // realm epoch must be 1 greater than current period
  if (realm_epoch != current_period.get_realm_epoch() + 1) {
    error_stream << "Period's realm epoch " << realm_epoch
        << " does not come directly after current realm epoch "
        << current_period.get_realm_epoch() << ". Use 'realm pull' to get the "
        "latest realm and period from the master zone, reapply your changes, "
        "and try again." << std::endl;
    return -EINVAL;
  }
  // did the master zone change?
  if (master_zone != current_period.get_master_zone()) {
    // store the current metadata sync status in the period
    int r = update_sync_status(dpp, store, current_period, error_stream, force_if_stale);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update metadata sync status: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    // create an object with a new period id
    r = create(dpp, y, true);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to create new period: " << cpp_strerror(-r) << dendl;
      return r;
    }
    // set as current period
    r = realm.set_current_period(dpp, *this, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update realm's current period: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 4) << "Promoted to master zone and committed new period "
        << id << dendl;
    realm.notify_new_period(dpp, *this, y);
    return 0;
  }
  // period must be based on current epoch
  if (epoch != current_period.get_epoch()) {
    error_stream << "Period epoch " << epoch << " does not match "
        "predecessor epoch " << current_period.get_epoch()
        << ". Use 'period pull' to get the latest epoch from the master zone, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // set period as next epoch
  set_id(current_period.get_id());
  set_epoch(current_period.get_epoch() + 1);
  set_predecessor(current_period.get_predecessor());
  realm_epoch = current_period.get_realm_epoch();
  // write the period to rados
  int r = store_info(dpp, false, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to store period: " << cpp_strerror(-r) << dendl;
    return r;
  }
  // set as latest epoch
  r = update_latest_epoch(dpp, epoch, y);
  if (r == -EEXIST) {
    // already have this epoch (or a more recent one)
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to set latest epoch: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = reflect(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to update local objects: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 4) << "Committed new epoch " << epoch
      << " for period " << id << dendl;
  realm.notify_new_period(dpp, *this, y);
  return 0;
}

void RGWPeriod::generate_test_instances(list<RGWPeriod*> &o)
{
  RGWPeriod *z = new RGWPeriod;
  o.push_back(z);
  o.push_back(new RGWPeriod);
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
  encode_json("realm_name", realm_name, f);
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
  JSONDecoder::decode_json("realm_name", realm_name, obj);
  JSONDecoder::decode_json("realm_epoch", realm_epoch, obj);
}

