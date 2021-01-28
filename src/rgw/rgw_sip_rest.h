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
  std::optional<std::string> remote_provider_name;
  std::optional<std::string> data_type;
  std::optional<SIProvider::StageType> stage_type;
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
  SIProvider_REST(const DoutPrefixProvider *_dpp,
                  RGWCoroutinesManager *_cr_mgr,
                  RGWRESTConn *_conn,
                  RGWHTTPManager *_http_manager,
                  const std::string& _remote_provider_name,
                  std::optional<std::string> _instance);

  SIProvider_REST(const DoutPrefixProvider *_dpp,
                  RGWCoroutinesManager *_cr_mgr,
                  RGWRESTConn *_conn,
                  RGWHTTPManager *_http_manager,
                  const std::string& _data_type,
                  SIProvider::StageType _stage_type,
                  std::optional<std::string> _instance);

  virtual ~SIProvider_REST();

  int init(const DoutPrefixProvider *dpp) override;
  stage_id_t get_first_stage(const DoutPrefixProvider *dpp) override;
  stage_id_t get_last_stage(const DoutPrefixProvider *dpp) override;
  int get_next_stage(const DoutPrefixProvider *dpp,
                     const stage_id_t& sid, stage_id_t *next_sid) override;
  std::vector<stage_id_t> get_stages(const DoutPrefixProvider *dpp) override;
  int get_stage_info(const DoutPrefixProvider *dpp,
                     const stage_id_t& sid, SIProvider::StageInfo *sinfo) override;
  int fetch(const DoutPrefixProvider *dpp,
            const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const DoutPrefixProvider *dpp,
                       const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;
  int get_cur_state(const DoutPrefixProvider *dpp,
                    const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp, bool *disabled, optional_yield y) override;
  int trim(const DoutPrefixProvider *dpp,
           const stage_id_t& sid, int shard_id, const std::string& marker) override;
};

class SIProvider_REST_SingleType : public SIProvider_REST
{
  SIProvider::TypeHandlerProviderRef type_provider;

public:
  SIProvider_REST_SingleType(const DoutPrefixProvider *_dpp,
                             RGWCoroutinesManager *_cr_mgr,
                             RGWRESTConn *_conn,
                             RGWHTTPManager *_http_manager,
                             const std::string& _remote_provider_name,
                             std::optional<std::string> _instance,
                             SIProvider::TypeHandlerProviderRef _type_provider) : SIProvider_REST(_dpp, _cr_mgr, _conn,
                                                                                                  _http_manager, _remote_provider_name,
                                                                                                  _instance), type_provider(_type_provider) {}
  SIProvider_REST_SingleType(const DoutPrefixProvider *_dpp,
                             RGWCoroutinesManager *_cr_mgr,
                             RGWRESTConn *_conn,
                             RGWHTTPManager *_http_manager,
                             const std::string& _data_type,
                             SIProvider::StageType _stage_type,
                             std::optional<std::string> _instance,
                             SIProvider::TypeHandlerProviderRef _type_provider) : SIProvider_REST(_dpp, _cr_mgr, _conn,
                                                                                                  _http_manager, _data_type, _stage_type,
                                                                                                  _instance), type_provider(_type_provider) {}
  SIProvider::TypeHandlerProvider *get_type_provider() override {
    return type_provider.get();
  }
};

