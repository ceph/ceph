#include "rgw_coroutine.h"
#include "rgw_sync_info.h"

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
  virtual RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) = 0;
  virtual RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker) = 0;
  virtual RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker) = 0;

  virtual RGWCoroutine *get_next_stage_cr(const SIProvider::stage_id_t& sid, SIProvider::stage_id_t *next_sid) {
    return new GetNextStageCR(this, sid, next_sid);
  }
};

class RGWAsyncRadosProcessor;

class SIProviderCRMgr_Local : public SIProviderCRMgr
{
  RGWAsyncRadosProcessor *async_rados;
  SIProviderRef provider;
public:
  SIProviderCRMgr_Local(CephContext *_cct,
                        RGWAsyncRadosProcessor *_async_rados,
                        SIProviderRef& _provider) : SIProviderCRMgr(_cct),
                                                    async_rados(_async_rados),
                                                    provider(_provider) {}

  RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) override;
  RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) override;
  RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) override;
  RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker) override;
  RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker) override;
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

  SIProvider *local_provider;

public:
  SIProviderCRMgr_REST(CephContext *_cct,
                       RGWRESTConn *_conn,
                       RGWHTTPManager *_http_manager,
                       const string& _remote_provider_name,
                       SIProvider *_local_provider,
                       std::optional<string> _instance) : SIProviderCRMgr(_cct),
                                                          conn(_conn),
                                                          http_manager(_http_manager),
                                                          remote_provider_name(_remote_provider_name),
                                                          instance(_instance.value_or(string())),
                                                          local_provider(_local_provider) {}

  RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) override;
  RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) override;
  RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) override;
  RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker) override;
  RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string *marker) override;
};

