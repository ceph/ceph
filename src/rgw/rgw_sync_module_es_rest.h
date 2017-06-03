#ifndef CEPH_RGW_SYNC_MODULE_ES_REST_H
#define CEPH_RGW_SYNC_MODULE_ES_REST_H

#include "rgw_rest.h"

class RGWElasticSyncModuleInstance;

class RGWRESTMgr_MDSearch_S3 : public RGWRESTMgr {
public:
  explicit RGWRESTMgr_MDSearch_S3() {}

  RGWHandler_REST *get_handler(struct req_state* s,
                               const rgw::auth::StrategyRegistry& auth_registry,
                               const std::string& frontend_prefix) override;
};

#endif
