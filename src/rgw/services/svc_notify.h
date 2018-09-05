#ifndef CEPH_RGW_SERVICES_NOTIFY_H
#define CEPH_RGW_SERVICES_NOTIFY_H


#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_Zone;

class RGWWatcher;

class RGWS_Notify : public RGWService
{
public:
  RGWS_Notify(CephContext *cct) : RGWService(cct, "quota") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_Notify : public RGWServiceInstance
{
  std::shared_ptr<RGWSI_Zone> zone_svc;
  std::shared_ptr<RGWSI_RADOS> rados_svc;

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

  Mutex watchers_lock{"watchers_lock"};
  rgw_pool control_pool;

  int num_watchers{0};
  RGWWatcher **watchers{nullptr};
  std::set<int> watchers_set;
  vector<RGWSI_RADOS::Obj> notify_objs;

  double inject_notify_timeout_probability{0};
  unsigned max_notify_retries{0};

  friend class RGWWatcher;

  string get_control_oid(int i);
  RGWSI_RADOS::Obj pick_control_obj(const string& key);

  int init_watch();
  void finalize_watch();

  int init() override;
  void shutdown() override;

  int watch(RGWSI_RADOS::Obj& obj, uint64_t *watch_handle, librados::WatchCtx2 *ctx);
  int aio_watch(const string& oid, uint64_t *watch_handle, librados::WatchCtx2 *ctx, librados::AioCompletion *c);
  int unwatch(RGWSI_RADOS::Obj& obj, uint64_t watch_handle);
  void add_watcher(int i);
  void remove_watcher(int i);
public:
  RGWSI_Notify(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

};

#endif

