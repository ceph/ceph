#ifndef CEPH_RGW_SYNC_MODULE_CUSTOM_H
#define CEPH_RGW_SYNC_MODULE_CUSTOM_H

#include "rgw_sync_module.h"

class RGWCustomSyncModule : public RGWSyncModule {
public:
  RGWCustomSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, 
                      map<std::string, std::string, ltstr_nocase>& config, 
                      RGWSyncModuleInstanceRef *instance) override;
};

#endif
