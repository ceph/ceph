#ifndef CEPH_RGW_SERVICE_H
#define CEPH_RGW_SERVICE_H


#include <string>
#include <vector>
#include <memory>

#include "rgw/rgw_common.h"

struct RGWServices_Shared;

class RGWServiceInstance
{
  friend struct RGWServices_Shared;

protected:
  CephContext *cct;

  enum StartState {
    StateInit = 0,
    StateStarting = 1,
    StateStarted = 2,
  } start_state{StateInit};

  virtual void shutdown() {}
  virtual int do_start() {
    return 0;
  }
public:
  RGWServiceInstance(CephContext *_cct) : cct(_cct) {}
  virtual ~RGWServiceInstance() {}

  int start();
  bool is_started() {
    return (start_state == StateStarted);
  }

  CephContext *ctx() {
    return cct;
  }
};

class RGWSI_Finisher;
class RGWSI_Notify;
class RGWSI_RADOS;
class RGWSI_Zone;
class RGWSI_ZoneUtils;
class RGWSI_Quota;
class RGWSI_SyncModules;
class RGWSI_SysObj;
class RGWSI_SysObj_Core;
class RGWSI_SysObj_Cache;

struct RGWServices_Shared
{
  std::shared_ptr<RGWSI_Finisher> finisher;
  std::shared_ptr<RGWSI_Notify> notify;
  std::shared_ptr<RGWSI_RADOS> rados;
  std::shared_ptr<RGWSI_Zone> zone;
  std::shared_ptr<RGWSI_ZoneUtils> zone_utils;
  std::shared_ptr<RGWSI_Quota> quota;
  std::shared_ptr<RGWSI_SyncModules> sync_modules;
  std::shared_ptr<RGWSI_SysObj> sysobj;
  std::shared_ptr<RGWSI_SysObj_Core> sysobj_core;
  std::shared_ptr<RGWSI_SysObj_Cache> sysobj_cache;

  int init(CephContext *cct, bool have_cache);
};


struct RGWServices
{
  RGWServices_Shared _svc;

  RGWSI_Finisher *finisher{nullptr};
  RGWSI_Notify *notify{nullptr};
  RGWSI_RADOS *rados{nullptr};
  RGWSI_Zone *zone{nullptr};
  RGWSI_ZoneUtils *zone_utils{nullptr};
  RGWSI_Quota *quota{nullptr};
  RGWSI_SyncModules *sync_modules{nullptr};
  RGWSI_SysObj *sysobj{nullptr};
  RGWSI_SysObj_Cache *cache{nullptr};
  RGWSI_SysObj_Core *core{nullptr};

  int init(CephContext *cct, bool have_cache);
};


#endif
