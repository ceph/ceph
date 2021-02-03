#include "common/debug.h"

#include "rgw_sip_rest.h"
#include "rgw_cr_sip.h"

#define dout_subsys ceph_subsys_rgw


SIProvider_REST::SIProvider_REST(const DoutPrefixProvider *_dpp,
                                 RGWCoroutinesManager *_cr_mgr,
                                 RGWRESTConn *_conn,
                                 RGWHTTPManager *_http_manager,
                                 const string& _remote_provider_name,
                                 std::optional<string> _instance) : SIProviderCommon(_dpp->get_cct(), string("rest:") + _remote_provider_name, _instance),
                                                                    cr_mgr(_cr_mgr),
                                                                    conn(_conn),
                                                                    http_manager(_http_manager),
                                                                    remote_provider_name(_remote_provider_name),
                                                                    instance(_instance),
                                                                    proxy_type_provider(this) {
  sip_cr_mgr.reset(new SIProviderCRMgr_REST(_dpp, conn, http_manager));
  sip_cr_mgri.reset(sip_cr_mgr->alloc_instance(_remote_provider_name,
                                               &proxy_type_provider, instance));
}

SIProvider_REST::SIProvider_REST(const DoutPrefixProvider *_dpp,
                                 RGWCoroutinesManager *_cr_mgr,
                                 RGWRESTConn *_conn,
                                 RGWHTTPManager *_http_manager,
                                 const string& _data_type,
                                 SIProvider::StageType _stage_type,
                                 std::optional<string> _instance) : SIProviderCommon(_dpp->get_cct(),
                                                                                     string("rest:") + _data_type + "." + SIProvider::stage_type_to_str(_stage_type),
                                                                                     _instance),
                                                                    cr_mgr(_cr_mgr),
                                                                    conn(_conn),
                                                                    http_manager(_http_manager),
                                                                    data_type(_data_type),
                                                                    stage_type(_stage_type),
                                                                    instance(_instance),
                                                                    proxy_type_provider(this) {
  sip_cr_mgr.reset(new SIProviderCRMgr_REST(_dpp, conn, http_manager));
  sip_cr_mgri.reset(sip_cr_mgr->alloc_instance(_data_type,
                                               _stage_type,
                                               &proxy_type_provider, instance));
}

int SIProvider_REST::init(const DoutPrefixProvider *dpp)
{
  return cr_mgr->run(sip_cr_mgri->init_cr());
}

SIProvider_REST::~SIProvider_REST() {
}

SIProvider::stage_id_t SIProvider_REST::get_first_stage(const DoutPrefixProvider *dpp)
{
  std::vector<SIProvider::stage_id_t> stages = get_stages(dpp);
  if (stages.empty()) {
    return SIProvider::stage_id_t();
  }
  return stages[0];
}

SIProvider::stage_id_t SIProvider_REST::get_last_stage(const DoutPrefixProvider *dpp)
{
  std::vector<SIProvider::stage_id_t> stages = get_stages(dpp);
  if (stages.empty()) {
    return SIProvider::stage_id_t();
  }
  return stages.back();
}

int SIProvider_REST::get_next_stage(const DoutPrefixProvider *dpp,
                                    const SIProvider::stage_id_t& sid, SIProvider::stage_id_t *next_sid)
{
  int r = cr_mgr->run(sip_cr_mgri->get_next_stage_cr(sid, next_sid));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch data from remote sip: r=" << r << dendl;
    return r;
  }
  return 0;
}

std::vector<SIProvider::stage_id_t> SIProvider_REST::get_stages(const DoutPrefixProvider *dpp)
{
  std::vector<SIProvider::stage_id_t> stages;
  int r = cr_mgr->run(sip_cr_mgri->get_stages_cr(&stages));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch stages from remote sip: r=" << r << dendl;
    /* continue -- no stages */
  }

  return stages;
}

int SIProvider_REST::get_stage_info(const DoutPrefixProvider *dpp,
                                    const SIProvider::stage_id_t& sid, SIProvider::StageInfo *sinfo)
{
  int r = cr_mgr->run(sip_cr_mgri->get_stage_info_cr(sid, sinfo));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  return 0;
}

int SIProvider_REST::fetch(const DoutPrefixProvider *dpp,
                           const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  int r = cr_mgr->run(sip_cr_mgri->fetch_cr(sid, shard_id, marker, max, result));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch data from remote sip: r=" << r << dendl;
    return r;
  }
  return 0;
}

int SIProvider_REST::get_start_marker(const DoutPrefixProvider *dpp,
                                      const SIProvider::stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp)
{
  rgw_sip_pos pos;
  int r = cr_mgr->run(sip_cr_mgri->get_start_marker_cr(sid, shard_id, &pos));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  if (marker) {
    *marker = std::move(pos.marker);
  }
  if (timestamp) {
    *timestamp = std::move(pos.timestamp);
  }
  return 0;
}

int SIProvider_REST::get_cur_state(const DoutPrefixProvider *dpp,
                                   const SIProvider::stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp,
                                   bool *disabled, optional_yield y)
{
  rgw_sip_pos pos;
  int r = cr_mgr->run(sip_cr_mgri->get_cur_state_cr(sid, shard_id, &pos, disabled));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  if (marker) {
    *marker = std::move(pos.marker);
  }
  if (timestamp) {
    *timestamp = std::move(pos.timestamp);
  }
  return 0;
}

int SIProvider_REST::trim(const DoutPrefixProvider *dpp,
                          const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker)
{
  int r = cr_mgr->run(sip_cr_mgri->trim_cr(sid, shard_id, marker));
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  return 0;
}

