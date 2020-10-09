#include "rgw_sync_info.h"

#pragma once

class RGWCoroutinesManager;
class RGWRESTConn;
class RGWHTTPManager;
class SIProviderCRMgr_REST;

class SIProvider_REST : public SIProviderCommon
{
  RGWCoroutinesManager *cr_mgr;
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  std::string remote_provider_name;
  std::optional<std::string> instance;

  std::unique_ptr<SIProviderCRMgr_REST> sip_cr_mgr;

  /* this is needed so that we can initialize sip_cr_mgr with type
   * provider at constructor.
   */
  class TypeProvider : public SIProvider::TypeHandlerProvider {
    SIProvider_REST *sip;

  public:
    TypeProvider(SIProvider_REST *_sip) : sip(_sip) {}
    SIProvider::TypeHandler *get_type_handler() override {
      return sip->get_type_handler();
    }
  } proxy_type_provider;

public:
  SIProvider_REST(CephContext *_cct,
                  RGWCoroutinesManager *_cr_mgr,
                  RGWRESTConn *_conn,
                  RGWHTTPManager *_http_manager,
                  const std::string& _remote_provider_name,
                  std::optional<std::string> _instance);
  virtual ~SIProvider_REST();

  stage_id_t get_first_stage() override;
  stage_id_t get_last_stage() override;
  int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) override;
  std::vector<stage_id_t> get_stages() override;
  int get_stage_info(const stage_id_t& sid, SIProvider::StageInfo *sinfo) override;
  int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;
  int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;
  int trim(const stage_id_t& sid, int shard_id, const std::string& marker) override;
};

class SIProvider_REST_SingleType : public SIProvider_REST
{
  SIProvider::TypeHandlerProviderRef type_provider;

public:
  SIProvider_REST_SingleType(CephContext *_cct,
                             RGWCoroutinesManager *_cr_mgr,
                             RGWRESTConn *_conn,
                             RGWHTTPManager *_http_manager,
                             const std::string& _remote_provider_name,
                             std::optional<std::string> _instance,
                             SIProvider::TypeHandlerProviderRef _type_provider) : SIProvider_REST(_cct, _cr_mgr, _conn,
                                                                                                  _http_manager, _remote_provider_name,
                                                                                                  _instance), type_provider(_type_provider) {}
  SIProvider::TypeHandlerProvider *get_type_provider() override {
    return type_provider.get();
  }
};

