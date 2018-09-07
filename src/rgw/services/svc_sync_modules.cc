#include "svc_sync_modules.h"

#include "rgw/rgw_sync_module.h"

int RGWS_SyncModules::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_SyncModules(this, cct));
  return 0;
}

std::map<string, RGWServiceInstance::dependency> RGWSI_SyncModules::get_deps()
{
  return std::map<string, RGWServiceInstance::dependency>();
}

int RGWSI_SyncModules::load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  return 0;
}

int RGWSI_SyncModules::init()
{
  sync_modules_manager = new RGWSyncModulesManager();
  rgw_register_sync_modules(sync_modules_manager);
  return 0;
}

RGWSI_SyncModules::~RGWSI_SyncModules()
{
  delete sync_modules_manager;
}

