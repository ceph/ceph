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

#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw


int RGWServices_Shared::init(CephContext *cct,
                             bool have_cache)
{
  finisher = std::make_shared<RGWSI_Finisher>(cct);
  notify = std::make_shared<RGWSI_Notify>(cct);
  rados = std::make_shared<RGWSI_RADOS>(cct);
  zone = std::make_shared<RGWSI_Zone>(cct);
  zone_utils = std::make_shared<RGWSI_ZoneUtils>(cct);
  quota = std::make_shared<RGWSI_Quota>(cct);
  sync_modules = std::make_shared<RGWSI_SyncModules>(cct);
  sysobj = std::make_shared<RGWSI_SysObj>(cct);
  sysobj_core = std::make_shared<RGWSI_SysObj_Core>(cct);

  if (have_cache) {
    sysobj_cache = std::make_shared<RGWSI_SysObj_Cache>(cct);
  }

  finisher->init();
  notify->init(zone, rados, finisher);
  rados->init();
  zone->init(sysobj, rados, sync_modules);
  zone_utils->init(rados, zone);
  quota->init(zone);
  sync_modules->init();
  sysobj_core->core_init(rados, zone);
  if (have_cache) {
    sysobj_cache->init(rados, zone, notify);
    auto _cache = std::static_pointer_cast<RGWSI_SysObj_Core>(sysobj_cache);
    sysobj->init(rados, _cache);
  } else {
    sysobj->init(rados, sysobj_core);
  }


  int r = finisher->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start finisher service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = notify->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start notify service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = rados->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start rados service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start zone service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone_utils->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start zone_utils service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = quota->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start quota service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  r = sysobj_core->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start sysobj_core service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  if (have_cache) {
    r = sysobj_cache->start();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to start sysobj_cache service (" << cpp_strerror(-r) << dendl;
      return r;
    }
  }

  r = sysobj->start();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to start sysobj service (" << cpp_strerror(-r) << dendl;
    return r;
  }

  /* cache or core services will be started by sysobj */

  return  0;
}


int RGWServices::init(CephContext *cct, bool have_cache)
{
  int r = _svc.init(cct, have_cache);
  if (r < 0) {
    return r;
  }

  finisher = _svc.finisher.get();
  notify = _svc.notify.get();
  rados = _svc.rados.get();
  zone = _svc.zone.get();
  zone_utils = _svc.zone_utils.get();
  quota = _svc.quota.get();
  sync_modules = _svc.sync_modules.get();
  sysobj = _svc.sysobj.get();
  cache = _svc.sysobj_cache.get();
  core = _svc.sysobj_core.get();

  return 0;
}

int RGWServiceInstance::start()
{
  if (start_state != StateInit) {
    return 0;
  }

  start_state = StateStarting;; /* setting started prior to do_start() on purpose so that circular
                                   references can call start() on each other */

  int r = do_start();
  if (r < 0) {
    return r;
  }

  start_state = StateStarted;

  return 0;
}
