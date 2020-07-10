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

  int get_stages(vector<stage_id_t> *stages);

public:
  SIProvider_REST(CephContext *_cct,
                  RGWCoroutinesManager *_cr_mgr,
                  RGWRESTConn *_conn,
                  RGWHTTPManager *_http_manager,
                  const std::string& _remote_provider_name,
                  std::optional<std::string> _instance);

  stage_id_t get_first_stage() override;
  stage_id_t get_last_stage() override;
  int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) override;
  std::vector<stage_id_t> get_stages() override;
  int get_stage_info(const stage_id_t& sid, SIProvider::StageInfo *sinfo) override;
  int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker) override;
  int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker) override;
  int trim(const stage_id_t& sid, int shard_id, const std::string& marker) override;
};

