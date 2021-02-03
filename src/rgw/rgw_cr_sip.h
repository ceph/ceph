#include "rgw_coroutine.h"
#include "rgw_sync_info.h"

#include "services/svc_sip_marker.h"

#pragma once

class SIProviderCRMgr
{
protected:
  const DoutPrefixProvider *dpp;
  CephContext *cct;

public:
  SIProviderCRMgr(const DoutPrefixProvider *_dpp) : dpp(_dpp),
                                              cct(_dpp->get_cct()) {}
  virtual ~SIProviderCRMgr() {}

  const DoutPrefixProvider *get_dpp() {
    return dpp;
  }

  CephContext *ctx() {
    return cct;
  }

  class Instance {
  protected:
    const DoutPrefixProvider *dpp;
    CephContext *cct;

    class GetNextStageCR : public RGWCoroutine {
      SIProviderCRMgr::Instance *mgri;

      SIProvider::stage_id_t sid;
      SIProvider::stage_id_t *next_sid;

      std::vector<SIProvider::stage_id_t> stages;
      public:
      GetNextStageCR(SIProviderCRMgr::Instance *_mgri,
                     SIProvider::stage_id_t _sid,
                     SIProvider::stage_id_t *_next_sid) : RGWCoroutine(_mgri->ctx()),
                                                          mgri(_mgri),
                                                          sid(_sid),
                                                          next_sid(_next_sid) {}

      int operate() override;
    };

    SIProvider::Info info;

  public:
    Instance(const DoutPrefixProvider *_dpp) : dpp(_dpp),
                                               cct(_dpp->get_cct()) {}
    virtual ~Instance() {}

    const DoutPrefixProvider *get_dpp() {
      return dpp;
    }

    CephContext *ctx() {
      return cct;
    }

    const SIProvider::Info& get_info() {
      return info;
    }

    virtual RGWCoroutine *init_cr() = 0;
    virtual RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) = 0;
    virtual RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) = 0;
    virtual RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) = 0;
    virtual RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) = 0;
    virtual RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos, bool *disabled) = 0;
    virtual RGWCoroutine *trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker) = 0;
    virtual RGWCoroutine *update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                           const RGWSI_SIP_Marker::SetParams& params) = 0;

    virtual RGWCoroutine *get_next_stage_cr(const SIProvider::stage_id_t& sid, SIProvider::stage_id_t *next_sid) {
      return new GetNextStageCR(this, sid, next_sid);
    }
  };

  virtual RGWCoroutine *list_cr(std::vector<std::string> *providers) = 0;
};

class RGWAsyncRadosProcessor;
class SIProviderCRMgrInstance_Local;

class SIProviderCRMgr_Local : public SIProviderCRMgr
{
  friend class SIProviderCRMgrInstance_Local;

  struct {
    RGWSI_SIP_Marker *sip_marker;
  } svc;

  struct {
    struct {
        RGWSIPManager *mgr;
    } si;
  } ctl;

  RGWAsyncRadosProcessor *async_rados;
public:
  SIProviderCRMgr_Local(const DoutPrefixProvider *_dpp,
                        RGWSI_SIP_Marker *_sip_marker_svc,
                        RGWSIPManager *_si_mgr,
                        RGWAsyncRadosProcessor *_async_rados) : SIProviderCRMgr(_dpp),
                                                                async_rados(_async_rados) {
    svc.sip_marker = _sip_marker_svc;
    ctl.si.mgr = _si_mgr;
  }

  SIProviderCRMgrInstance_Local *alloc_instance(SIProviderRef& _provider);

  RGWCoroutine *list_cr(std::vector<std::string> *providers) override;
};

class SIProviderCRMgrInstance_Local : public SIProviderCRMgr::Instance
{
  friend class SIProviderCRMgr_Local;

  SIProviderCRMgr_Local *mgr;
  SIProviderRef provider;

  SIProviderCRMgrInstance_Local(SIProviderCRMgr_Local *_mgr,
                                SIProviderRef& _provider);

public:
  RGWCoroutine *init_cr() override;
  RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) override;
  RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) override;
  RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) override;
  RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) override;
  RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos, bool *disabled) override;
  RGWCoroutine *trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker) override;
  RGWCoroutine *update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                 const RGWSI_SIP_Marker::SetParams& params) override;

  /* local only ops */

  RGWCoroutine *get_marker_info_cr(RGWSI_SIP_Marker::HandlerRef& marker_handler,
                                   const SIProvider::stage_id_t& sid, int shard_id,
                                   RGWSI_SIP_Marker::stage_shard_info *info);
  RGWCoroutine *set_min_source_pos_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                      const string& pos);
};

class RGWRESTConn;
class RGWHTTPManager;
class SIProviderCRMgrInstance_REST;

class SIProviderCRMgr_REST : public SIProviderCRMgr
{
  friend class SIProviderCRMgrInstance_REST;
  friend struct SIProviderRESTCRs;

  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;

  string path_prefix = "/admin/sip";
public:
  SIProviderCRMgr_REST(const DoutPrefixProvider *_dpp,
                         RGWRESTConn *_conn,
                         RGWHTTPManager *_http_manager) : SIProviderCRMgr(_dpp),
                                                          conn(_conn),
                                                          http_manager(_http_manager) {}

  SIProviderCRMgrInstance_REST *alloc_instance(const string& remote_provider_name,
                                               SIProvider::TypeHandlerProvider *type_provider,
                                               std::optional<string> instance);
  SIProviderCRMgrInstance_REST *alloc_instance(const string& data_type,
                                               SIProvider::StageType stage_type,
                                               SIProvider::TypeHandlerProvider *type_provider,
                                               std::optional<string> instance);

  RGWCoroutine *list_cr(std::vector<std::string> *providers) override;
};

class SIProviderCRMgrInstance_REST : public SIProviderCRMgr::Instance
{
  friend class SIProviderCRMgr_REST;
  friend class GetStagesCR;
  friend struct SIProviderRESTCRs;

  SIProviderCRMgr_REST *mgr;

  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;

  string path_prefix;

  std::optional<string> remote_provider_name;
  std::optional<string> data_type;
  std::optional<SIProvider::StageType> stage_type;
  std::optional<string> instance;

  SIProvider::TypeHandlerProvider *type_provider;

  SIProviderCRMgrInstance_REST(SIProviderCRMgr_REST *_mgr,
                               const string& _remote_provider_name,
                               SIProvider::TypeHandlerProvider *_type_provider,
                               std::optional<string> _instance) : SIProviderCRMgr::Instance(_mgr->get_dpp()),
                                                          mgr(_mgr),
                                                          conn(_mgr->conn),
                                                          http_manager(_mgr->http_manager),
                                                          path_prefix(_mgr->path_prefix),
                                                          remote_provider_name(_remote_provider_name),
                                                          instance(_instance.value_or(string())),
                                                          type_provider(_type_provider) {}
  SIProviderCRMgrInstance_REST(SIProviderCRMgr_REST *_mgr,
                               const string& _data_type,
                               SIProvider::StageType _stage_type,
                               SIProvider::TypeHandlerProvider *_type_provider,
                               std::optional<string> _instance) : SIProviderCRMgr::Instance(_mgr->dpp),
                                                          mgr(_mgr),
                                                          conn(_mgr->conn),
                                                          http_manager(_mgr->http_manager),
                                                          path_prefix(_mgr->path_prefix),
                                                          data_type(_data_type),
                                                          stage_type(_stage_type),
                                                          instance(_instance.value_or(string())),
                                                          type_provider(_type_provider) {}

public:
  RGWCoroutine *init_cr();
  RGWCoroutine *get_stages_cr(std::vector<SIProvider::stage_id_t> *stages) override;
  RGWCoroutine *get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info) override;
  RGWCoroutine *fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result) override;
  RGWCoroutine *get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos) override;
  RGWCoroutine *get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos, bool *disabled) override;
  RGWCoroutine *trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const std::string& marker) override;
  RGWCoroutine *update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                 const RGWSI_SIP_Marker::SetParams& params) override;

  SIProvider::TypeHandler *get_type_handler();
};

