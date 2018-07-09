#ifndef CEPH_RGW_SYNC_MODULE_PUBSUB_H
#define CEPH_RGW_SYNC_MODULE_PUBSUB_H

#include "rgw_sync_module.h"

class RGWPSSyncModule : public RGWSyncModule {
public:
  RGWPSSyncModule() {}
  bool supports_data_export() override {
    return false;
  }
  int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

class RGWPSDataSyncModule;
class RGWRESTConn;

class RGWPSSyncModuleInstance : public RGWSyncModuleInstance {
  std::unique_ptr<RGWPSDataSyncModule> data_handler;
  JSONFormattable effective_conf;
public:
  RGWPSSyncModuleInstance(CephContext *cct, const JSONFormattable& config);
  RGWDataSyncModule *get_data_handler() override;
  RGWRESTMgr *get_rest_filter(int dialect, RGWRESTMgr *orig) override;
  bool supports_user_writes() override {
    return true;
  }
  const JSONFormattable& get_effective_conf() {
    return effective_conf;
  }
};

#endif
