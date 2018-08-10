#include "rgw_service.h"

#include "services/svc_rados.h"



RGWServiceInstance::~RGWServiceInstance()
{
  if (svc) {
    svc->svc_registry->remove_instance(this);
  }
}

void RGWServiceRegistry::register_all(CephContext *cct)
{
  services["rados"] = make_shared<RGWS_RADOS>(cct);
}

bool RGWServiceRegistry::find(const string& name, RGWServiceRef *svc)
{
  auto iter = services.find(name);
  if (iter == services.end()) {
    return false;
  }

  *svc = iter->second;
  return true;
}

int RGWServiceRegistry::instantiate(RGWServiceRegistryRef& registry, RGWServiceRef& svc, JSONFormattable& conf) {
  auto self_ref = shared_from_this();
  RGWServiceInstanceRef instance_ref;
  int r = svc->create_instance(conf, &instance_ref);
  if (r < 0) {
    return r;
  }
  instance_ref->svc = svc;
  instance_ref->svc_id = ++max_registry_id;

  r = instance_ref->init(conf);
  if (r < 0) {
    return r;
  }

  if (instance_ref->svc_instance.empty()) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", (long long)instance_ref->svc_id);
    instance_ref->svc_instance = buf;
  }

  instance_info& iinfo = instances[instance_ref->svc_id];
  iinfo.id = instance_ref->svc_id;
  iinfo.title = instance_ref->get_title();
  iinfo.conf = conf;

  return 0;
}

void RGWServiceRegistry::remove_instance(RGWServiceInstance *instance) {
  instances.erase(instance->svc_id);
}
