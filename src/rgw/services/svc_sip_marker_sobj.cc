#include "svc_sip_marker_sobj.h"
#include "svc_sys_obj.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_sync_info.h"

#define dout_subsys ceph_subsys_rgw

static string stage_shard_info_oid_prefix = "sip.target-markers";


void RGWSI_SIP_Marker_SObj::init(RGWSI_Zone *_zone_svc,
                                 RGWSI_SysObj *_sysobj_svc)
{
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
}

class RGWSI_SIP_Marker_SObj_Handler : public RGWSI_SIP_Marker::Handler {
  friend class Shard;

  using stage_shard_info = RGWSI_SIP_Marker::stage_shard_info;

  CephContext *cct;

  struct {
    RGWSI_Zone *zone;
    RGWSI_SysObj *sysobj;
  } svc;

  SIProviderRef sip;

  string oid_prefix;

  class ShardObj {
    CephContext *cct;
    RGWSysObjectCtx obj_ctx;
    rgw_raw_obj obj;
    RGWSysObj sysobj;

  public:
    ShardObj(RGWSI_SysObj *_sysobj_svc,
             const rgw_raw_obj& _obj) : cct(_sysobj_svc->ctx()),
                                        obj_ctx(_sysobj_svc->init_obj_ctx()),
                                        obj(_obj),
                                        sysobj(obj_ctx.get_obj(obj)) {}

    int read(stage_shard_info *result, RGWObjVersionTracker *ot, optional_yield y);
    int write(const stage_shard_info& info, RGWObjVersionTracker *ot, optional_yield y);
    int remove(RGWObjVersionTracker *ot, optional_yield y);
  };

  rgw_raw_obj shard_obj(const RGWSI_SIP_Marker::stage_id_t& sid, int shard_id) const {
    char buf[oid_prefix.size() + sid.size() + 32];
    snprintf(buf, sizeof(buf), "%s.%s.%d", oid_prefix.c_str(), sid.c_str(), shard_id);
    return rgw_raw_obj(svc.zone->get_zone_params().log_pool,
                       buf);

  }

public:
  RGWSI_SIP_Marker_SObj_Handler(CephContext *_cct,
                                RGWSI_Zone *_zone_svc,
                                RGWSI_SysObj *_sysobj_svc,
                                SIProviderRef& _sip) : cct(_cct),
                                                       sip(_sip) {
    svc.zone = _zone_svc;
    svc.sysobj = _sysobj_svc;

    oid_prefix = stage_shard_info_oid_prefix + "." + sip->get_id() + ".";
  }


  int set_marker(const string& target_id,
                 const RGWSI_SIP_Marker::stage_id_t& sid,
                 int shard_id,
                 const std::string& marker,
                 const ceph::real_time& mtime,
                 bool init_target,
                 RGWSI_SIP_Marker::Handler::modify_result *result) override {

#define NUM_RACE_RETRY 10
    ShardObj sobj(svc.sysobj, shard_obj(sid, shard_id));
    stage_shard_info sinfo;

    int i;

    for (i = 0; i < NUM_RACE_RETRY; ++i) {
      RGWObjVersionTracker objv_tracker;
      int r = sobj.read(&sinfo, &objv_tracker, null_yield);
      if (r < 0 && r != -ENOENT) {
        ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to read shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
        return r;
      }

      RGWSI_SIP_Marker::target_marker_info *marker_info;

      auto citer = sinfo.targets.find(target_id);
      if (citer == sinfo.targets.end()) {
        if (!init_target) {
          ldout(cct, 20) << __func__ << "(): couldn't find target (target_id=" << target_id << ")" << dendl;
          return -ENOENT;
        }
        marker_info = &sinfo.targets[target_id];
      } else {
        marker_info = &citer->second;
      }

      if  (marker < sinfo.min_source_pos) {
        ldout(cct, 20) << __func__ << "(): can't set marker: target is too far behind: min_source_pos=" << sinfo.min_source_pos << " target: id=" << target_id << " marker=" << marker << dendl;
        return -ERANGE;
      }

      if (marker <= marker_info->pos) { /* can a target marker go backwards? */
        result->modified = false;
        break;
      }

      marker_info->pos = marker;
      marker_info->mtime = mtime;

      if (sinfo.targets.size() > 1) {
        string min = std::move(marker);

        for (auto& iter : sinfo.targets) {
          if (iter.second.pos < min) {
            min = iter.second.pos;
          }
        }
        sinfo.min_targets_pos = std::move(min);
      } else {
        sinfo.min_targets_pos = std::move(marker);
      }

      r = sobj.write(sinfo, &objv_tracker, null_yield);
      if (r >= 0) {
        break;
      }

      if (r != -ECANCELED) {
        return r;
      }
    }

    if (i == NUM_RACE_RETRY) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to write shard_info (racing writes) for too many times. Likely a bug!" << dendl;
      return -EIO;
    }

    result->modified = true;
    result->min_pos = sinfo.min_targets_pos;

    return 0;
  }

  int remove_target(const string& target_id,
                    const SIProvider::stage_id_t& sid,
                    int shard_id,
                    RGWSI_SIP_Marker::Handler::modify_result *result) override {
    ShardObj sobj(svc.sysobj, shard_obj(sid, shard_id));
    stage_shard_info sinfo;

    int i;

    result->modified = false;

    for (i = 0; i < NUM_RACE_RETRY; ++i) {
      RGWObjVersionTracker objv_tracker;
      int r = sobj.read(&sinfo, &objv_tracker, null_yield);
      if (r == -ENOENT) {
        return 0;
      }
      if (r < 0) {
        ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to read shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
        return r;
      }

      auto citer = sinfo.targets.find(target_id);
      if (citer != sinfo.targets.end()) {
        sinfo.targets.erase(citer);
        result->modified = true;
      }

      string min;

      if (sinfo.targets.size() > 0) {
        auto iter = sinfo.targets.begin();

        min = iter->second.pos;

        while (++iter != sinfo.targets.end()) {
          if (iter->second.pos < min) {
            min = iter->second.pos;
          }
        }
      }

      if (sinfo.min_targets_pos != min) {
        result->modified |= true;
        sinfo.min_targets_pos = std::move(min);
      }

      if (!result->modified) {
        break;
      }

      r = sobj.write(sinfo, &objv_tracker, null_yield);
      if (r >= 0) {
        break;
      }

      if (r != -ECANCELED) {
        return r;
      }
    }

    if (i == NUM_RACE_RETRY) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to write shard_info (racing writes) for too many times. Likely a bug!" << dendl;
      return -EIO;
    }

    result->min_pos = sinfo.min_targets_pos;

    return 0;
  }

  int set_min_source_pos(const RGWSI_SIP_Marker::stage_id_t& sid,
                         int shard_id,
                         const std::string& pos) override {
    ShardObj sobj(svc.sysobj, shard_obj(sid, shard_id));
    stage_shard_info sinfo;

    int i;

    for (i = 0; i < NUM_RACE_RETRY; ++i) {
      RGWObjVersionTracker objv_tracker;
      int r = sobj.read(&sinfo, &objv_tracker, null_yield);
      if (r < 0 && r != -ENOENT) {
        ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to read shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
        return r;
      }

      if (sinfo.min_source_pos == pos) {
        /* nothing changed */
        return 0;
      }

      sinfo.min_source_pos = pos;

      if (sinfo.min_source_pos > sinfo.min_targets_pos) {
        /* need to remove any target that is too far behind */

        auto iter = sinfo.targets.begin();
        while (iter != sinfo.targets.end()) {
          auto prev = iter++;

          auto& target = iter->second;
          if (target.pos < sinfo.min_source_pos) {
            ldout(cct, 20) << __func__ << "(): removing target fell behind tracking shard: sip=" << sip->get_id()
              << " sid=" << sid << " target=" << prev->first << " min_source_pos=" << sinfo.min_source_pos << " target.pos=" << target.pos << dendl;

            sinfo.targets.erase(prev);
          }
        }

        if (sinfo.targets.empty()) {
          sinfo.min_targets_pos.clear();
        }

        r = sobj.write(sinfo, &objv_tracker, null_yield);
        if (r >= 0) {
          break;
        }

        if (r != -ECANCELED) {
          return r;
        }
      }
    }

    if (i == NUM_RACE_RETRY) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to write shard_info (racing writes) for too many times. Likely a bug!" << dendl;
      return -EIO;
    }

    return 0;
  }

  int get_min_targets_pos(const RGWSI_SIP_Marker::stage_id_t& sid,
                          int shard_id,
                          std::optional<std::string> *pos) override {
    stage_shard_info sinfo;
    int r = get_info(sid, shard_id, &sinfo);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to read shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
      return r;
    }

    *pos = sinfo.min_targets_pos;

    return 0;
  }

  int get_info(const RGWSI_SIP_Marker::stage_id_t& sid,
               int shard_id,
               stage_shard_info *info) override {
    ShardObj sobj(svc.sysobj, shard_obj(sid, shard_id));

    int r = sobj.read(info, nullptr, null_yield);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to read shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
      return r;
    }

    return 0;
  }

  int remove_info(const SIProvider::stage_id_t& sid,
                  int shard_id) override {
    ShardObj sobj(svc.sysobj, shard_obj(sid, shard_id));

    int r = sobj.remove(nullptr, null_yield);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to remove shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
      return r;
    }

    return 0;
  }
};

int RGWSI_SIP_Marker_SObj_Handler::ShardObj::read(stage_shard_info *result,
                                                  RGWObjVersionTracker *ot,
                                                  optional_yield y)
{
  bufferlist bl;
  int r = sysobj.rop()
    .set_objv_tracker(ot) /* forcing read of current version */
    .read(&bl, y);
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 0) << "ERROR: failed reading stage shard markers data (obj=" << obj << "), r=" << r << dendl;
    return r;
  }

  if (r >= 0) {
    auto iter = bl.cbegin();
    try {
      decode(*result, iter);
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to decode entries, ignoring" << dendl;
    }
  } else {
    *result = stage_shard_info();
  }

  return 0;
}

int RGWSI_SIP_Marker_SObj_Handler::ShardObj::write(const stage_shard_info& info,
                                                   RGWObjVersionTracker *ot,
                                                   optional_yield y)
{
  bufferlist bl;
  encode(info, bl);

  int r = sysobj.wop()
    .set_objv_tracker(ot) /* forcing read of current version */
    .write(bl, y);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed writing stage shard markers data (obj=" << obj << "), r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWSI_SIP_Marker_SObj_Handler::ShardObj::remove(RGWObjVersionTracker *ot, optional_yield y)
{
  int r = sysobj.wop()
    .set_objv_tracker(ot) /* forcing read of current version */
    .remove(y);
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 0) << "ERROR: failed removing stage shard markers data (obj=" << obj << "), r=" << r << dendl;
    return r;
  }

  return 0;
}

RGWSI_SIP_Marker::HandlerRef RGWSI_SIP_Marker_SObj::get_handler(SIProviderRef& sip)
{
  return RGWSI_SIP_Marker::HandlerRef(new RGWSI_SIP_Marker_SObj_Handler(cct, svc.zone, svc.sysobj, sip));
}
