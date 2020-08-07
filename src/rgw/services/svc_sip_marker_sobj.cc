#include "svc_sip_marker_sobj.h"
#include "svc_sys_obj.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_sync_info.h"

#define dout_subsys ceph_subsys_rgw

static string stage_shard_info_oid_prefix = "sip.client-markers";


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


  int set_marker(const string& client_id,
                 const RGWSI_SIP_Marker::stage_id_t& sid,
                 int shard_id,
                 const std::string& marker,
                 const ceph::real_time& mtime,
                 bool init_client,
                 RGWSI_SIP_Marker::Handler::set_result *result) override {

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

      RGWSI_SIP_Marker::client_marker_info *marker_info;

      auto citer = sinfo.clients.find(client_id);
      if (citer == sinfo.clients.end()) {
        if (!init_client) {
          ldout(cct, 20) << __func__ << "(): couldn't find client (client_id=" << client_id << ")" << dendl;
          return -ENOENT;
        }
        marker_info = &sinfo.clients[client_id];
      } else {
        marker_info = &citer->second;
      }

      if (marker <= marker_info->pos) { /* can a client marker go backwards? */
        result->modified = false;
        break;
      }

      marker_info->pos = marker;
      marker_info->mtime = mtime;

      if (sinfo.clients.size() > 1) {
        string min = std::move(marker);

        for (auto& iter : sinfo.clients) {
          if (iter.second.pos < min) {
            min = iter.second.pos;
          }
        }
        sinfo.min_clients_pos = std::move(min);
      } else {
        sinfo.min_clients_pos = std::move(marker);
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
    result->min_pos = sinfo.min_clients_pos;

    return 0;
  }

  int set_low_pos(const RGWSI_SIP_Marker::stage_id_t& sid,
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

      if (sinfo.low_pos == pos) {
        /* nothing changed */
        return 0;
      }

      sinfo.low_pos = pos;

      if (sinfo.low_pos > sinfo.min_clients_pos) {
        /* need to remove any client that is too far behind */

        auto iter = sinfo.clients.begin();
        while (iter != sinfo.clients.end()) {
          auto prev = iter++;

          auto& client = iter->second;
          if (client.pos < sinfo.low_pos) {
            ldout(cct, 20) << __func__ << "(): removing client fell behind tracking shard: sip=" << sip->get_id()
              << " sid=" << sid << " client=" << prev->first << " low_pos=" << sinfo.low_pos << " client.pos=" << client.pos << dendl;

            sinfo.clients.erase(prev);
          }
        }

        if (sinfo.clients.empty()) {
          sinfo.min_clients_pos.clear();
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

  int get_min_clients_pos(const RGWSI_SIP_Marker::stage_id_t& sid,
                          int shard_id,
                          std::optional<std::string> *pos) override {
    stage_shard_info sinfo;
    int r = get_info(sid, shard_id, &sinfo);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to read shard info (sid=" << sid << ", shard_id=" << shard_id << "), r=" << r << dendl;
      return r;
    }

    *pos = sinfo.min_clients_pos;

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


RGWSI_SIP_Marker::HandlerRef RGWSI_SIP_Marker_SObj::get_handler(SIProviderRef& sip)
{
  return RGWSI_SIP_Marker::HandlerRef(new RGWSI_SIP_Marker_SObj_Handler(cct, svc.zone, svc.sysobj, sip));
}
