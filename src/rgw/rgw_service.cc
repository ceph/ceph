#include "rgw_service.h"

#include "services/svc_finisher.h"
#include "services/svc_notify.h"
#include "services/svc_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_sys_obj_core.h"

#define dout_subsys ceph_subsys_rgw

RGWServiceInstance::~RGWServiceInstance()
{
  if (svc) {
    shutdown();
    svc->svc_registry->remove_instance(this);
  }
}

void RGWServiceRegistry::register_all(CephContext *cct)
{
  services["finisher"] = make_shared<RGWS_Finisher>(cct);
  services["notify"] = make_shared<RGWS_Notify>(cct);
  services["rados"] = make_shared<RGWS_RADOS>(cct);
  services["zone"] = make_shared<RGWS_Zone>(cct);
  services["zone_utils"] = make_shared<RGWS_ZoneUtils>(cct);
  services["quota"] = make_shared<RGWS_Quota>(cct);
  services["sync_modules"] = make_shared<RGWS_SyncModules>(cct);
  services["sys_obj"] = make_shared<RGWS_SysObj>(cct);
  services["sys_obj_cache"] = make_shared<RGWS_SysObj_Cache>(cct);
  services["sys_obj_core"] = make_shared<RGWS_SysObj_Core>(cct);
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

string RGWServiceRegistry::get_conf_id(const string& service_type, const string& conf)
{
  return service_type + ":" + conf;
}

int RGWServiceRegistry::get_instance(RGWServiceRef& svc,
                                     const string& conf,
                                     RGWServiceInstanceRef *ref) {
  auto self_ref = shared_from_this();
  RGWServiceInstanceRef instance_ref;

  string conf_id = get_conf_id(svc->type(), conf);

  auto iter = instances_by_conf.find(conf_id);
  if (iter != instances_by_conf.end()) {
    *ref = iter->second.ref;
    return 0;
  }
  int r = svc->create_instance(conf, &instance_ref);
  if (r < 0) {
    return r;
  }
  instance_ref->svc = svc;
  instance_ref->svc_id = ++max_registry_id;

  map<string, RGWServiceInstanceRef> dep_refs;

  instance_info& iinfo = instances[instance_ref->svc_id];
  iinfo.conf_id = get_conf_id(svc->type(), conf);
  iinfo.id = instance_ref->svc_id;
  iinfo.title = instance_ref->get_title();
  iinfo.conf = conf;
  iinfo.ref = instance_ref;

  instances_by_conf[iinfo.conf_id] = iinfo;

  auto deps = instance_ref->get_deps();
  for (auto iter : deps) {
    auto& dep_id = iter.first;
    auto& dep = iter.second;
    RGWServiceInstanceRef dep_ref;
    r = get_instance(dep.name, dep.conf, &dep_ref);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: cannot satisfy dependency for service " << svc->type() << ": " << dep.name << dendl;
      return r;
    }
    dep_refs[dep_id] = dep_ref;
  }

  r = instance_ref->load(conf, dep_refs);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: service instance load return error: service=" << svc->type() << " r=" << r << dendl;
    return r;
  }

  if (instance_ref->svc_instance.empty()) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", (long long)instance_ref->svc_id);
    instance_ref->svc_instance = buf;
  }

  *ref = iinfo.ref;

  r = instance_ref->init();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: service instance init return error: service=" << svc->type() << " r=" << r << dendl;
    return r;
  }

  return 0;
}

void RGWServiceRegistry::remove_instance(RGWServiceInstance *instance) {
  auto iter = instances.find(instance->svc_id);
  if (iter == instances.end()) {
    return;
  }
  instances_by_conf.erase(iter->second.conf_id);
  instances.erase(iter);
}
