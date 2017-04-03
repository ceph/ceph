#ifndef CEPH_RGW_SYNC_MODULE_LOG_H
#define CEPH_RGW_SYNC_MODULE_LOG_H

#include "rgw_sync_module.h"

class RGWLogSyncModule : public RGWSyncModule {
public:
  RGWLogSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif
