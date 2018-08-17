#ifndef CEPH_RGW_SERVICES_SYS_OBJ_H
#define CEPH_RGW_SERVICES_SYS_OBJ_H


#include "rgw/rgw_service.h"

#include "svc_rados.h"


class RGWSI_Zone;
class RGWSI_SysObj;

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

template <class T, class S>
class RGWSysObjectCtxImpl {
  RGWSI_SysObj *sysobj_svc;
  std::map<T, S> objs_state;
  RWLock lock;

public:
  explicit RGWSysObjectCtxImpl(RGWSI_SysObj *_sysobj_svc) : sysobj_svc(_sysobj_svc), lock("RGWSysObjectCtxImpl") {}

  RGWSysObjectCtxImpl(const RGWSysObjectCtxImpl& rhs) : sysobj_svc(rhs.sysobj_svc),
                                                  objs_state(rhs.objs_state),
                                                  lock("RGWSysObjectCtxImpl") {}
  RGWSysObjectCtxImpl(const RGWSysObjectCtxImpl&& rhs) : sysobj_svc(rhs.sysobj_svc),
                                                   objs_state(std::move(rhs.objs_state)),
                                                   lock("RGWSysObjectCtxImpl") {}

  S *get_state(const T& obj) {
    S *result;
    typename std::map<T, S>::iterator iter;
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

  void set_atomic(T& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].is_atomic = true;
  }
  void set_prefetch_data(T& obj) {
    RWLock::WLocker wl(lock);
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }
  void invalidate(T& obj) {
    RWLock::WLocker wl(lock);
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    bool is_atomic = iter->second.is_atomic;
    bool prefetch_data = iter->second.prefetch_data;
  
    objs_state.erase(iter);

    if (is_atomic || prefetch_data) {
      auto& s = objs_state[obj];
      s.is_atomic = is_atomic;
      s.prefetch_data = prefetch_data;
    }
  }
};

using RGWSysObjectCtx = RGWSysObjectCtxImpl<rgw_raw_obj, RGWSysObjState>;

class RGWS_SysObj : public RGWService
{
public:
  RGWS_SysObj(CephContext *cct) : RGWService(cct, "sys_obj") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_SysObj : public RGWServiceInstance
{
public:
  class Obj {
    friend class Read;

    RGWSI_SysObj *sysobj_svc;
    RGWSysObjectCtx& ctx;
    rgw_raw_obj obj;

    RGWSI_RADOS *get_rados_svc();

  public:
    Obj(RGWSI_SysObj *_sysobj_svc,
        RGWSysObjectCtx& _ctx,
        const rgw_raw_obj& _obj) : sysobj_svc(_sysobj_svc),
                                   ctx(_ctx),
                                   obj(_obj) {}

    void invalidate_state();

    RGWSysObjectCtx& get_ctx() {
      return ctx;
    }

    rgw_raw_obj& get_obj() {
      return obj;
    }

    struct Read {
      Obj& source;

      struct GetObjState {
        RGWSI_RADOS::Obj rados_obj;
        bool has_rados_obj{false};
        uint64_t last_ver{0};

        GetObjState() {}

        int get_rados_obj(RGWSI_SysObj *sysobj_svc, rgw_raw_obj& obj, RGWSI_RADOS::Obj **pobj);
      } state;
      
      struct StatParams {
        RGWObjVersionTracker *objv_tracker{nullptr};
        ceph::real_time *lastmod{nullptr};
        uint64_t *obj_size{nullptr};
        map<string, bufferlist> *attrs{nullptr};

        StatParams& set_last_mod(ceph::real_time *_lastmod) {
          lastmod = _lastmod;
          return *this;
        }
        StatParams& set_obj_size(uint64_t *_obj_size) {
          obj_size = _obj_size;
          return *this;
        }
        StatParams& set_attrs(map<string, bufferlist> *_attrs) {
          attrs = _attrs;
          return *this;
        }
      } stat_params;

      struct ReadParams {
        RGWObjVersionTracker *objv_tracker{nullptr};
        map<string, bufferlist> *attrs{nullptr};
        boost::optional<obj_version> refresh_version{boost::none};

        ReadParams& set_attrs(map<string, bufferlist> *_attrs) {
          attrs = _attrs;
          return *this;
        }
        ReadParams& set_obj_tracker(RGWObjVersionTracker *_objv_tracker) {
          objv_tracker = _objv_tracker;
          return *this;
        }
        ReadParams& set_refresh_version(const obj_version& rf) {
          refresh_version = rf;
          return *this;
        }
      } read_params;

      Read(Obj& _source) : source(_source) {}

      int stat();
      int read(int64_t ofs, int64_t end, bufferlist *pbl);
      int read(bufferlist *pbl) {
        return read(0, -1, pbl);
      }
      int get_attr(std::string_view name, bufferlist *dest);
    };
  };

  friend class Obj;
  friend class Obj::Read;

private:
  std::shared_ptr<RGWSI_RADOS> rados_svc;
  std::shared_ptr<RGWSI_Zone> zone_svc;

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

  int get_rados_obj(RGWSI_Zone *zone_svc, rgw_raw_obj& obj, RGWSI_RADOS::Obj *pobj);

  int get_system_obj_state_impl(RGWSysObjectCtx *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker);
  int get_system_obj_state(RGWSysObjectCtx *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker);

  int raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
               map<string, bufferlist> *attrs, bufferlist *first_chunk,
               RGWObjVersionTracker *objv_tracker);

  int stat(RGWSysObjectCtx& obj_ctx,
           RGWSI_SysObj::Obj::Read::GetObjState& state,
           rgw_raw_obj& obj,
           map<string, bufferlist> *attrs,
           real_time *lastmod,
           uint64_t *obj_size,
           RGWObjVersionTracker *objv_tracker);

  int read(RGWSysObjectCtx& obj_ctx,
           Obj::Read::GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           map<string, bufferlist> *attrs,
           boost::optional<obj_version>);

  int get_attr(rgw_raw_obj& obj, std::string_view name, bufferlist *dest);

public:
  RGWSI_SysObj(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

  RGWSysObjectCtx&& init_obj_ctx() {
    return std::move(RGWSysObjectCtx(this));
  }

  Obj&& get_obj(RGWSysObjectCtx& obj_ctx, const rgw_raw_obj& obj) {
    return std::move(Obj(this, obj_ctx, obj));
  }

};

using RGWSysObj = RGWSI_SysObj::Obj;

#endif

