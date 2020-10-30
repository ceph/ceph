#include "rgw_coroutine.h"
#include "rgw_sync_info.h"

#include "services/svc_sip_marker.h"

#pragma once

class SIProviderCRMgr
{
  class GetNextStageCR : public RGWCoroutine {
    SIProviderCRMgr *mgr;

    SIProvider::stage_id_t sid;
    SIProvider::stage_id_t *next_sid;

    std::vector<SIProvider::stage_id_t> stages;
  public:
    GetNextStageCR(SIProviderCRMgr *_mgr,
                   SIProvider::stage_id_t _sid,
                   SIProvider::stage_id_t *_next_sid) : RGWCoroutine(_mgr->ctx()),
                                                        mgr(_mgr),
                                                        sid(_sid),
                                                        next_sid(_next_sid) {}

    int operate() override;
  };

protected:
  CephContext *cct;
public:
  SIProviderCRMgr(CephContext *_cct) : cct(_cct) {}
  virtual ~SIProviderCRMgr() {}

  CephContext *ctx() {
    return cct;
  }

  virtual RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) = 0;
  virtual RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) = 0;
  virtual RGWCoroutine *get_info_cr(SIProvider::Info *info) = 0;
  virtual RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) = 0;
  virtual RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) = 0;
  virtual RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) = 0;
  virtual RGWCoroutine *trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker) = 0;
  virtual RGWCoroutine *update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                         const RGWSI_SIP_Marker::SetParams& params) = 0;

  virtual RGWCoroutine *get_next_stage_cr(const SIProvider::stage_id_t& sid, SIProvider::stage_id_t *next_sid) {
    return new GetNextStageCR(this, sid, next_sid);
  }
};

class RGWAsyncRadosProcessor;

class SIProviderCRMgr_Local : public SIProviderCRMgr
{
  struct {
    RGWSI_SIP_Marker *sip_marker;
  } svc;

  RGWAsyncRadosProcessor *async_rados;
  SIProviderRef provider;
public:
  SIProviderCRMgr_Local(RGWSI_SIP_Marker *_sip_marker_svc,
                        RGWAsyncRadosProcessor *_async_rados,
                        SIProviderRef& _provider);

  RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) override;
  RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) override;
  RGWCoroutine *get_info_cr(SIProvider::Info *info) override;
  RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) override;
  RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) override;
  RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) override;
  RGWCoroutine *trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker) override;
  RGWCoroutine *update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                 const RGWSI_SIP_Marker::SetParams& params) override;

  /* local only ops */

  RGWCoroutine *get_marker_info_cr(RGWSI_SIP_Marker::HandlerRef& marker_handler,
                                   const SIProvider::stage_id_t& sid, int shard_id,
                                   RGWSI_SIP_Marker::stage_shard_info *info);

};

class RGWRESTConn;
class RGWHTTPManager;

class SIProviderCRMgr_REST : public SIProviderCRMgr
{
  friend class GetStagesCR;
  friend struct SIProviderRESTCRs;

  RGWAsyncRadosProcessor *async_rados;
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;

  string path_prefix = "/admin/sip";

  string remote_provider_name;
  std::optional<string> instance;

  SIProvider::TypeHandlerProvider *type_provider;

public:
  SIProviderCRMgr_REST(CephContext *_cct,
                       RGWRESTConn *_conn,
                       RGWHTTPManager *_http_manager,
                       const string& _remote_provider_name,
                       SIProvider::TypeHandlerProvider *_type_provider,
                       std::optional<string> _instance) : SIProviderCRMgr(_cct),
                                                          conn(_conn),
                                                          http_manager(_http_manager),
                                                          remote_provider_name(_remote_provider_name),
                                                          instance(_instance.value_or(string())),
                                                          type_provider(_type_provider) {}

  RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) override;
  RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) override;
  RGWCoroutine *get_info_cr(SIProvider::Info *info) override;
  RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) override;
  RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) override;
  RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) override;
  RGWCoroutine *trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker) override;
  RGWCoroutine *update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                 const RGWSI_SIP_Marker::SetParams& params) override;

  SIProvider::TypeHandler *get_type_handler();
};

