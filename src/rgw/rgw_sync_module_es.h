#ifndef CEPH_RGW_SYNC_MODULE_ES_H
#define CEPH_RGW_SYNC_MODULE_ES_H

#include "rgw_sync_module.h"

class RGWElasticSyncModule : public RGWSyncModule {
public:
  RGWElasticSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif
