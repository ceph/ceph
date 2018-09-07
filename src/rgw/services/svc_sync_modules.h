#ifndef CEPH_RGW_SERVICES_SYNC_MODULES_H
#define CEPH_RGW_SERVICES_SYNC_MODULES_H


#include "rgw/rgw_service.h"


class RGWSyncModulesManager;

class RGWS_SyncModules : public RGWService
{
public:
  RGWS_SyncModules(CephContext *cct) : RGWService(cct, "sync_modules") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_SyncModules : public RGWServiceInstance
{
  RGWSyncModulesManager *sync_modules_manager;

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;
  int init() override;

public:
  RGWSI_SyncModules(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}
  ~RGWSI_SyncModules();

  RGWSyncModulesManager *get_manager() {
    return sync_modules_manager;
  }
};

#endif

