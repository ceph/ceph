#ifndef CEPH_RGW_SERVICES_SYS_OBJ_CORE_H
#define CEPH_RGW_SERVICES_SYS_OBJ_CORE_H


#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_Zone;

struct rgw_cache_entry_info;

struct RGWSysObjState {
  rgw_raw_obj obj;
  bool has_attrs{false};
  bool exists{false};
  uint64_t size{0};
  ceph::real_time mtime;
  uint64_t epoch{0};
  bufferlist obj_tag;
  bool has_data{false};
  bufferlist data;
  bool prefetch_data{false};
  uint64_t pg_ver{0};

  /* important! don't forget to update copy constructor */

  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrset;
  RGWSysObjState() {}
  RGWSysObjState(const RGWSysObjState& rhs) : obj (rhs.obj) {
    has_attrs = rhs.has_attrs;
    exists = rhs.exists;
    size = rhs.size;
    mtime = rhs.mtime;
    epoch = rhs.epoch;
    if (rhs.obj_tag.length()) {
      obj_tag = rhs.obj_tag;
    }
    has_data = rhs.has_data;
    if (rhs.data.length()) {
      data = rhs.data;
    }
    prefetch_data = rhs.prefetch_data;
    pg_ver = rhs.pg_ver;
    objv_tracker = rhs.objv_tracker;
  }
};

class RGWSysObjectCtxBase {
  std::map<rgw_raw_obj, RGWSysObjState> objs_state;
  RWLock lock;

public:
  explicit RGWSysObjectCtxBase() : lock("RGWSysObjectCtxBase") {}

  RGWSysObjectCtxBase(const RGWSysObjectCtxBase& rhs) : objs_state(rhs.objs_state),
                                                  lock("RGWSysObjectCtxBase") {}
  RGWSysObjectCtxBase(const RGWSysObjectCtxBase&& rhs) : objs_state(std::move(rhs.objs_state)),
                                                   lock("RGWSysObjectCtxBase") {}

  RGWSysObjState *get_state(const rgw_raw_obj& obj) {
    RGWSysObjState *result;
    std::map<rgw_raw_obj, RGWSysObjState>::iterator iter;
    lock.get_read();
    assert (!obj.empty());
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock();
    } else {
      lock.unlock();
      lock.get_write();
      result = &objs_state[obj];
      lock.unlock();
    }
    return result;
  }

  void set_atomic(rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
  }
  void set_prefetch_data(rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }
  void invalidate(rgw_raw_obj& obj) {
    RWLock::WLocker wl(lock);
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    objs_state.erase(iter);
  }
};

class RGWSI_SysObj_Core : public RGWServiceInstance
{
  friend class RGWSI_SysObj;

protected:
  std::shared_ptr<RGWSI_RADOS> rados_svc;
  std::shared_ptr<RGWSI_Zone> zone_svc;

  struct GetObjState {
    RGWSI_RADOS::Obj rados_obj;
    bool has_rados_obj{false};
    uint64_t last_ver{0};

    GetObjState() {}

    int get_rados_obj(RGWSI_RADOS *rados_svc,
                      RGWSI_Zone *zone_svc,
                      rgw_raw_obj& obj,
                      RGWSI_RADOS::Obj **pobj);
  };


  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

  int get_rados_obj(RGWSI_Zone *zone_svc, rgw_raw_obj& obj, RGWSI_RADOS::Obj *pobj);

  virtual int raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                       map<string, bufferlist> *attrs, bufferlist *first_chunk,
                       RGWObjVersionTracker *objv_tracker);

  virtual int read(RGWSysObjectCtxBase& obj_ctx,
                   GetObjState& read_state,
                   RGWObjVersionTracker *objv_tracker,
                   rgw_raw_obj& obj,
                   bufferlist *bl, off_t ofs, off_t end,
                   map<string, bufferlist> *attrs,
                   rgw_cache_entry_info *cache_info,
                   boost::optional<obj_version>);

  virtual int remove(RGWSysObjectCtxBase& obj_ctx,
                     RGWObjVersionTracker *objv_tracker,
                     rgw_raw_obj& obj);

  virtual int write(rgw_raw_obj& obj,
                    real_time *pmtime,
                    map<std::string, bufferlist>& attrs,
                    bool exclusive,
                    const bufferlist& data,
                    RGWObjVersionTracker *objv_tracker,
                    real_time set_mtime);

  virtual int write_data(rgw_raw_obj& obj,
                         const bufferlist& bl,
                         bool exclusive,
                         RGWObjVersionTracker *objv_tracker);

  virtual int get_attr(rgw_raw_obj& obj, const char *name, bufferlist *dest);

  virtual int set_attrs(rgw_raw_obj& obj, 
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist> *rmattrs,
                        RGWObjVersionTracker *objv_tracker);

  virtual int omap_get_all(rgw_raw_obj& obj, std::map<string, bufferlist> *m);
  virtual int omap_get_vals(rgw_raw_obj& obj,
                            const string& marker,
                            uint64_t count,
                            std::map<string, bufferlist> *m,
                            bool *pmore);
  virtual int omap_set(rgw_raw_obj& obj, const std::string& key, bufferlist& bl, bool must_exist = false);
  virtual int omap_set(rgw_raw_obj& obj, const map<std::string, bufferlist>& m, bool must_exist = false);
  virtual int omap_del(rgw_raw_obj& obj, const std::string& key);

  /* wrappers */
  int get_system_obj_state_impl(RGWSysObjectCtxBase *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker);
  int get_system_obj_state(RGWSysObjectCtxBase *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker);

  int stat(RGWSysObjectCtxBase& obj_ctx,
           GetObjState& state,
           rgw_raw_obj& obj,
           map<string, bufferlist> *attrs,
           real_time *lastmod,
           uint64_t *obj_size,
           RGWObjVersionTracker *objv_tracker);

public:
  RGWSI_SysObj_Core(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

};

#endif
