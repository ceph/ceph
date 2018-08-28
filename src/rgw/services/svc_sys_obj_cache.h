
#ifndef CEPH_RGW_SERVICES_SYS_OBJ_CACHE_H
#define CEPH_RGW_SERVICES_SYS_OBJ_CACHE_H


#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_SysObj_Cache : public RGWSI_SysObj_Core
{
protected:
  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

  int raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
               map<string, bufferlist> *attrs, bufferlist *first_chunk,
               RGWObjVersionTracker *objv_tracker) override;

  int read(RGWSysObjectCtxBase& obj_ctx,
           GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           map<string, bufferlist> *attrs,
           boost::optional<obj_version>) override;

  int get_attr(rgw_raw_obj& obj, const char *name, bufferlist *dest) override;

  int set_attrs(rgw_raw_obj& obj, 
                map<string, bufferlist>& attrs,
                RGWObjVersionTracker *objv_tracker);

  int remove(RGWSysObjectCtxBase& obj_ctx,
             RGWObjVersionTracker *objv_tracker,
             rgw_raw_obj& obj) override;

  int write(rgw_raw_obj& obj,
            real_time *pmtime,
            map<std::string, bufferlist>& attrs,
            bool exclusive,
            const bufferlist& data,
            RGWObjVersionTracker *objv_tracker,
            real_time set_mtime) override;

  int write_data(rgw_raw_obj& obj,
                 const bufferlist& bl,
                 bool exclusive,
                 RGWObjVersionTracker *objv_tracker);

  int distribute_cache(const string& normal_name, rgw_raw_obj& obj, ObjectCacheInfo& obj_info, int op);

  int watch_cb(uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl);

public:
  RGWSI_SysObj_Cache(RGWService *svc, CephContext *cct): RGW_SysObj_Core(svc, cct) {}

  int call_list(const std::optional<std::string>& filter, Formatter* f);
  int call_inspect(const std::string& target, Formatter* f);
  int call_erase(const std::string& target);
  int call_zap();
};

#endif
