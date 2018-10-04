#ifndef CEPH_RGW_SERVICES_SYNC_MODULES_H
#define CEPH_RGW_SERVICES_SYNC_MODULES_H


#include "rgw/rgw_service.h"


class RGWSyncModulesManager;

class RGWSI_SyncModules : public RGWServiceInstance
{
  RGWSyncModulesManager *sync_modules_manager{nullptr};

public:
  RGWSI_SyncModules(CephContext *cct): RGWServiceInstance(cct) {}
  ~RGWSI_SyncModules();

  RGWSyncModulesManager *get_manager() {
    return sync_modules_manager;
  }

  void init();
};

#endif

