#ifndef RGW_SYNC_MODULE_AWS_H
#define RGW_SYNC_MODULE_AWS_H

#include "rgw_sync_module.h"

class RGWAWSSyncModule : public RGWSyncModule {
 public:
  RGWAWSSyncModule() {}
  bool supports_data_export() override { return false;}
  int create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif /* RGW_SYNC_MODULE_AWS_H */
