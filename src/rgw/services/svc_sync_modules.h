#ifndef CEPH_RGW_SERVICES_SYNC_MODULES_H
#define CEPH_RGW_SERVICES_SYNC_MODULES_H


#include "rgw/rgw_service.h"
#include "rgw/rgw_sync_module.h"

class RGWSI_Zone;

class RGWSyncModulesManager;

class RGWSI_SyncModules : public RGWServiceInstance
{
  RGWSyncModulesManager *sync_modules_manager{nullptr};
  RGWSyncModuleInstanceRef sync_module;

  struct Svc {
    RGWSI_Zone *zone{nullptr};
  } svc;

public:
  RGWSI_SyncModules(CephContext *cct): RGWServiceInstance(cct) {}
  ~RGWSI_SyncModules();

  RGWSyncModulesManager *get_manager() {
    return sync_modules_manager;
  }

  void init(RGWSI_Zone *zone_svc);
  int do_start() override;

  RGWSyncModuleInstanceRef& get_sync_module() { return sync_module; }
};

#endif

