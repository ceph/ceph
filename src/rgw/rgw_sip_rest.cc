#include "common/debug.h"

#include "rgw_sip_rest.h"
#include "rgw_cr_sip.h"

#define dout_subsys ceph_subsys_rgw


SIProvider_REST::SIProvider_REST(CephContext *_cct,
                                 RGWCoroutinesManager *_cr_mgr,
                                 RGWRESTConn *_conn,
                                 RGWHTTPManager *_http_manager,
                                 const string& _remote_provider_name,
                                 std::optional<string> _instance) : SIProviderCommon(_cct, string("rest:") + _remote_provider_name, _instance),
                                                                    cr_mgr(_cr_mgr),
                                                                    conn(_conn),
                                                                    http_manager(_http_manager),
                                                                    remote_provider_name(_remote_provider_name),
                                                                    instance(_instance),
                                                                    proxy_type_provider(this) {
  sip_cr_mgr.reset(new SIProviderCRMgr_REST(cct, conn, http_manager,
                                            remote_provider_name,
                                            &proxy_type_provider, instance));
}

SIProvider_REST::~SIProvider_REST() {
}

SIProvider::stage_id_t SIProvider_REST::get_first_stage()
{
  std::vector<SIProvider::stage_id_t> stages = get_stages();
  if (stages.empty()) {
    return SIProvider::stage_id_t();
  }
  return stages[0];
}

SIProvider::stage_id_t SIProvider_REST::get_last_stage()
{
  std::vector<SIProvider::stage_id_t> stages = get_stages();
  if (stages.empty()) {
    return SIProvider::stage_id_t();
  }
  return stages.back();
}

int SIProvider_REST::get_next_stage(const SIProvider::stage_id_t& sid, SIProvider::stage_id_t *next_sid)
{
  int r = cr_mgr->run(sip_cr_mgr->get_next_stage_cr(sid, next_sid));
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch data from remote sip: r=" << r << dendl;
    return r;
  }
  return 0;
}

std::vector<SIProvider::stage_id_t> SIProvider_REST::get_stages()
{
  std::vector<SIProvider::stage_id_t> stages;
  int r = cr_mgr->run(sip_cr_mgr->get_stages_cr(&stages));
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch stages from remote sip: r=" << r << dendl;
    /* continue -- no stages */
  }

  return stages;
}

int SIProvider_REST::get_stage_info(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *sinfo)
{
  int r = cr_mgr->run(sip_cr_mgr->get_stage_info_cr(sid, sinfo));
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  return 0;
}

int SIProvider_REST::fetch(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  int r = cr_mgr->run(sip_cr_mgr->fetch_cr(sid, shard_id, marker, max, result));
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch data from remote sip: r=" << r << dendl;
    return r;
  }
  return 0;
}

int SIProvider_REST::get_start_marker(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker)
{
  int r = cr_mgr->run(sip_cr_mgr->get_start_marker_cr(sid, shard_id, marker));
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  return 0;
}

int SIProvider_REST::get_cur_state(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker)
{
  int r = cr_mgr->run(sip_cr_mgr->get_cur_state_cr(sid, shard_id, marker));
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to fetch stages: r=" << r << dendl;
    return r;
  }
  return 0;
}

int SIProvider_REST::trim(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker)
{
#warning FIXME
  return -ENOTSUP;
}

