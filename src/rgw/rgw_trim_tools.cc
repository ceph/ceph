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

class RGWTrimGetSIPTargetsInfo : public RGWCoroutine
{
  rgw::sal::RGWRadosStore *store;

  string sip_name;
  std::optional<string> sip_instance;

  SIProviderRef sip;
  SIProvider::stage_id_t sid;
  SIProvider::StageInfo stage_info;
  RGWSI_SIP_Marker::HandlerRef marker_handler;

  std::optional<SIProviderCRMgr_Local> sip_cr;

  std::vector<RGWSI_SIP_Marker::stage_shard_info> sip_shards_info;

  int ret;
  int i;

  std::vector<std::string> *min_shard_markers;
  std::set<string> *sip_targets;

public:
  RGWTrimGetSIPTargetsInfo(rgw::sal::RGWRadosStore *_store,
                           const string& _sip_name,
                           std::optional<string> _sip_instance,
                           std::vector<std::string> *_min_shard_markers,
                           std::set<string> *_sip_targets) : RGWCoroutine(_store->ctx()),
                                                             store(_store),
                                                             sip_name(_sip_name),
                                                             sip_instance(_sip_instance),
                                                             min_shard_markers(_min_shard_markers),
                                                             sip_targets(_sip_targets) {}

  int operate() override;
};

int RGWTrimGetSIPTargetsInfo::operate()
{
  reenter(this) {
    sip = store->ctl()->si.mgr->find_sip(sip_name, sip_instance);
    if (!sip) {
      ldout(cct, 0) << "WARNING: could not find sip handler for " << sip_name << dendl;

      /* caller will do legacy handling anyway */

      return set_cr_done();
    }

    sip_cr.emplace(store->svc()->sip_marker,
                   store->svc()->rados->get_async_processor(),
                   sip);

    sid = sip->get_first_stage();

    ret = sip->get_stage_info(sid, &stage_info);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: could not get stage info for sid=" << sid << ": ret=" << ret << dendl;
      return set_cr_error(ret);
    }

    marker_handler = store->svc()->sip_marker->get_handler(sip);
    if (!marker_handler) {
      ldout(cct, 0) << "ERROR: can't get sip marker handler" << dendl;
      return set_cr_error(-EIO);
    }

    sip_shards_info.resize(stage_info.num_shards);

#define TRIM_SPAWN_WINDOW 16
    for (i = 0; i < stage_info.num_shards; ++i) {
      yield_spawn_window(sip_cr->get_marker_info_cr(marker_handler,
                                                    sid, i,
                                                    &sip_shards_info[i]),
                         TRIM_SPAWN_WINDOW,
                         [&](uint64_t stack_id, int ret) {
                         if (ret < 0 &&
                             ret != -ENOENT) {
                         ldout(cct, 0) << "failed to fetch markers info for sip " << sip_name << " sid=" << sid << ": ret=" << ret << dendl;
                         return ret;
                         }
                         return 0;
                         });
    }

    drain_all_cb([&](uint64_t stack_id, int ret) {
                 if (ret < 0 &&
                     ret != -ENOENT) {
                 ldout(cct, 0) << "failed to fetch markers info for sip " << sip_name << " sid=" << sid << ": ret=" << ret << dendl;
                 return ret;
                 }
                 return 0;
                 });

    i = 0;

    for (auto& info : sip_shards_info) {
      (*min_shard_markers)[i] = info.min_targets_pos;
      for (auto& entry : info.targets) {
        sip_targets->insert(entry.first);
      }

      ++i;
    }

    return set_cr_done();
  }
  return 0;
}

RGWCoroutine* RGWTrimTools::get_sip_targets_info_cr(rgw::sal::RGWRadosStore *store,
                                                    const std::string& sip_name,
                                                    std::optional<std::string> sip_instance,
                                                    std::vector<std::string> *min_shard_markers,
                                                    std::set<std::string> *sip_targets)
{
  return new RGWTrimGetSIPTargetsInfo(store, sip_name, sip_instance,
                                      min_shard_markers, sip_targets);
}

