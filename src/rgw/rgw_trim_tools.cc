#include "rgw_trim_tools.h"
#include "rgw_coroutine.h"
#include "rgw_cr_sip.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#include "services/svc_sip_marker.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "trim: ")

class RGWTrimSIPMgrImpl : public RGWTrimSIPMgr
{
  friend class InitCR;
  friend class RGWTrimGetSIPTargetsInfo;

  rgw::sal::RGWRadosStore *store;
  string sip_name;
  std::optional<string> sip_instance;

  SIProviderRef sip;
  SIProvider::stage_id_t sid;
  SIProvider::StageInfo stage_info;
  RGWSI_SIP_Marker::HandlerRef marker_handler;

  std::optional<SIProviderCRMgr_Local> sip_cr;

  class InitCR : public RGWCoroutine {
    RGWTrimSIPMgrImpl *mgr;
    rgw::sal::RGWRadosStore *store;
  public:
    InitCR(RGWTrimSIPMgrImpl *_mgr) : RGWCoroutine(_mgr->ctx()),
                                      mgr(_mgr),
                                      store(mgr->store) {}

    int operate() override {
      reenter(this) {
        mgr->sip = store->ctl()->si.mgr->find_sip(mgr->sip_name, mgr->sip_instance);
        if (!mgr->sip) {
          return set_cr_error(-ENOENT);
        }
        mgr->sip_cr.emplace(store->svc()->sip_marker,
                            store->svc()->rados->get_async_processor(),
                            mgr->sip);

        mgr->sid = mgr->sip->get_first_stage();

        yield call(mgr->sip_cr->get_stage_info_cr(mgr->sid, &mgr->stage_info));
        if (retcode < 0) {
          ldout(cct, 0) << "ERROR: could not get stage info for sid=" << mgr->sid << ": ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        mgr->marker_handler = store->svc()->sip_marker->get_handler(mgr->sip);
        if (!mgr->marker_handler) {
          ldout(cct, 0) << "ERROR: can't get sip marker handler" << dendl;
          return set_cr_error(-EIO);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

public:
  RGWTrimSIPMgrImpl(rgw::sal::RGWRadosStore *_store,
                    const string& _sip_name,
                    std::optional<string> _sip_instance) : store(_store),
                                                           sip_name(_sip_name),
                                                           sip_instance(_sip_instance) {}

  CephContext *ctx() {
    return store->ctx();
  }

  RGWCoroutine *init_cr() override {
    return new InitCR(this);
  }

  RGWCoroutine *get_targets_info_cr(std::vector<std::string> *min_shard_markers,
                                    std::set<string> *sip_targets,
                                    std::set<rgw_zone_id> *target_zones) override;

  RGWCoroutine *set_min_source_pos_cr(int shard_id, const string& pos) override {
    return sip_cr->set_min_source_pos_cr(sid, shard_id, pos);
  }

};

class RGWTrimGetSIPTargetsInfo : public RGWCoroutine
{
  RGWTrimSIPMgrImpl *mgr;
  rgw::sal::RGWRadosStore *store;

  std::vector<RGWSI_SIP_Marker::stage_shard_info> sip_shards_info;

  int ret;
  int i;

  std::vector<std::string> *min_shard_markers;
  std::set<string> *sip_targets;
  std::set<rgw_zone_id> *target_zones;

public:
  RGWTrimGetSIPTargetsInfo(RGWTrimSIPMgrImpl *_mgr,
                           std::vector<std::string> *_min_shard_markers,
                           std::set<string> *_sip_targets,
                           std::set<rgw_zone_id> *_target_zones) : RGWCoroutine(_mgr->ctx()),
                                                                   mgr(_mgr),
                                                                   min_shard_markers(_min_shard_markers),
                                                                   sip_targets(_sip_targets),
                                                                   target_zones(_target_zones) {}

  int operate() override;
};

int RGWTrimGetSIPTargetsInfo::operate()
{
  reenter(this) {
    if (!mgr->sip_cr) {
      ldout(cct, 0) << "WARNING: could not find sip handler for " << mgr->sip_name << dendl;

      /* caller will do legacy handling anyway */

      return set_cr_done();
    }

    sip_shards_info.resize(mgr->stage_info.num_shards);

#define TRIM_SPAWN_WINDOW 16
    for (i = 0; i < mgr->stage_info.num_shards; ++i) {
      yield_spawn_window(mgr->sip_cr->get_marker_info_cr(mgr->marker_handler,
                                                    mgr->sid, i,
                                                    &sip_shards_info[i]),
                         TRIM_SPAWN_WINDOW,
                         [&](uint64_t stack_id, int ret) {
                         if (ret < 0 &&
                             ret != -ENOENT) {
                         ldout(cct, 0) << "failed to fetch markers info for sip " << mgr->sip_name << " sid=" << mgr->sid << ": ret=" << ret << dendl;
                         return ret;
                         }
                         return 0;
                         });
    }

    drain_all_cb([&](uint64_t stack_id, int ret) {
                 if (ret < 0 &&
                     ret != -ENOENT) {
                 ldout(cct, 0) << "failed to fetch markers info for sip " << mgr->sip_name << " sid=" << mgr->sid << ": ret=" << ret << dendl;
                 return ret;
                 }
                 return 0;
                 });

    i = 0;

    for (auto& info : sip_shards_info) {
      if (!info.min_targets_pos.empty()) {
        (*min_shard_markers)[i] = info.min_targets_pos;
      }
      for (auto& entry : info.targets) {
        if (sip_targets) {
          sip_targets->insert(entry.first);
        }
        if (target_zones) {
          rgw_zone_id zid;
          RGWSI_SIP_Marker::parse_target_id(entry.first, &zid, nullptr);
          target_zones->insert(zid);
        }
      }

      ++i;
    }

    return set_cr_done();
  }

  return 0;
}


RGWCoroutine *RGWTrimSIPMgrImpl::get_targets_info_cr(std::vector<std::string> *min_shard_markers,
                                                     std::set<string> *sip_targets,
                                                     std::set<rgw_zone_id> *target_zones)
{
  return new RGWTrimGetSIPTargetsInfo(this,
                                      min_shard_markers,
                                      sip_targets,
                                      target_zones);
}

RGWTrimSIPMgr *RGWTrimTools::get_trim_sip_mgr(rgw::sal::RGWRadosStore *store,
                                              const std::string& sip_name,
                                              std::optional<std::string> sip_instance)
{
  return new RGWTrimSIPMgrImpl(store, sip_name, sip_instance);
}

