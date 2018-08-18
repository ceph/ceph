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
class RGWSysObjectCtxImpl;

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
    friend class ROp;

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

    struct ROp {
      Obj& source;

      struct GetObjState {
        RGWSI_RADOS::Obj rados_obj;
        bool has_rados_obj{false};
        uint64_t last_ver{0};

        GetObjState() {}

        int get_rados_obj(RGWSI_RADOS *rados_svc,
                          RGWSI_Zone *zone_svc,
                          rgw_raw_obj& obj,
                          RGWSI_RADOS::Obj **pobj);
      } state;
      
      RGWObjVersionTracker *objv_tracker{nullptr};
      map<string, bufferlist> *attrs{nullptr};
      boost::optional<obj_version> refresh_version{boost::none};
      ceph::real_time *lastmod{nullptr};
      uint64_t *obj_size{nullptr};

      ROp& set_last_mod(ceph::real_time *_lastmod) {
        lastmod = _lastmod;
        return *this;
      }

      ROp& set_obj_size(uint64_t *_obj_size) {
        obj_size = _obj_size;
        return *this;
      }

      ROp& set_attrs(map<string, bufferlist> *_attrs) {
        attrs = _attrs;
        return *this;
      }

      ROp& set_refresh_version(const obj_version& rf) {
        refresh_version = rf;
        return *this;
      }

      ROp(Obj& _source) : source(_source) {}

      int stat();
      int read(int64_t ofs, int64_t end, bufferlist *pbl);
      int read(bufferlist *pbl) {
        return read(0, -1, pbl);
      }
      int get_attr(const char *name, bufferlist *dest);
    };

    struct WOp {
      Obj& source;

      RGWObjVersionTracker *objv_tracker{nullptr};
      map<string, bufferlist> attrs;
      ceph::real_time mtime;
      ceph::real_time *pmtime;
      bool exclusive{false};

      WOp& set_attrs(map<string, bufferlist>&& _attrs) {
        attrs = _attrs;
        return *this;
      }

      WOp& set_mtime(const ceph::real_time& _mtime) {
        mtime = _mtime;
        return *this;
      }

      WOp& set_pmtime(ceph::real_time *_pmtime) {
        pmtime = _pmtime;
        return *this;
      }

      WOp& set_exclusive(bool _exclusive = true) {
        exclusive = _exclusive;
        return *this;
      }

      WOp(Obj& _source) : source(_source) {}

      int remove();
      int write(bufferlist& bl);
    };

    struct OmapOp {
      Obj& source;

      bool must_exist{false};

      OmapOp& set_must_exist(bool _must_exist = true) {
        must_exist = _must_exist;
        return *this;
      }

      OmapOp(Obj& _source) : source(_source) {}

      int get_all(std::map<string, bufferlist> *m);
      int get_vals(const string& marker,
                        uint64_t count,
                        std::map<string, bufferlist> *m,
                        bool *pmore);
      int set(const std::string& key, bufferlist& bl);
      int set(const map<std::string, bufferlist>& m);
      int del(const std::string& key);
    };
    ROp rop() {
      return ROp(*this);
    }

    WOp wop() {
      return WOp(*this);
    }

    OmapOp omap() {
      return OmapOp(*this);
    }
  };

  class Pool {
    friend class Op;

    RGWSI_SysObj *sysobj_svc;
    rgw_pool pool;

    RGWSI_RADOS *get_rados_svc();

  public:
    Pool(RGWSI_SysObj *_sysobj_svc,
         const rgw_pool& _pool) : sysobj_svc(_sysobj_svc),
                                  pool(_pool) {}

    rgw_pool& get_pool() {
      return pool;
    }

    struct Op {
      Pool& source;

      Op(Pool& _source) : source(_source) {}

      int list_prefixed_objs(const std::string& prefix, std::list<std::string> *result);
    };

    Op op() {
      return Op(*this);
    }
  };

  friend class Obj;
  friend class Obj::ROp;
  friend class Obj::WOp;
  friend class Pool;
  friend class Pool::Op;

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
           RGWSI_SysObj::Obj::ROp::GetObjState& state,
           rgw_raw_obj& obj,
           map<string, bufferlist> *attrs,
           real_time *lastmod,
           uint64_t *obj_size,
           RGWObjVersionTracker *objv_tracker);

  int read(RGWSysObjectCtx& obj_ctx,
           Obj::ROp::GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           map<string, bufferlist> *attrs,
           boost::optional<obj_version>);

  int get_attr(rgw_raw_obj& obj, const char *name, bufferlist *dest);

  int omap_get_all(rgw_raw_obj& obj, std::map<string, bufferlist> *m);
  int omap_get_vals(rgw_raw_obj& obj,
                    const string& marker,
                    uint64_t count,
                    std::map<string, bufferlist> *m,
                    bool *pmore);
  int omap_set(rgw_raw_obj& obj, const std::string& key, bufferlist& bl, bool must_exist = false);
  int omap_set(rgw_raw_obj& obj, const map<std::string, bufferlist>& m, bool must_exist = false);
  int omap_del(rgw_raw_obj& obj, const std::string& key);

  int remove(RGWSysObjectCtx& obj_ctx,
             RGWObjVersionTracker *objv_tracker,
             rgw_raw_obj& obj);

  int write(rgw_raw_obj& obj,
            real_time *pmtime,
            map<std::string, bufferlist>& attrs,
            bool exclusive,
            const bufferlist& data,
            RGWObjVersionTracker *objv_tracker,
            real_time set_mtime);
public:
  RGWSI_SysObj(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

  RGWSysObjectCtx&& init_obj_ctx();
  Obj&& get_obj(RGWSysObjectCtx& obj_ctx, const rgw_raw_obj& obj);

  Pool&& get_pool(const rgw_pool& pool) {
    return std::move(Pool(this, pool));
  }

};

using RGWSysObj = RGWSI_SysObj::Obj;

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
    objs_state.erase(iter);
  }

  RGWSI_SysObj::Obj&& get_obj(const rgw_raw_obj& obj) {
    return sysobj_svc->get_obj(*this, obj);
  }
};

#endif

