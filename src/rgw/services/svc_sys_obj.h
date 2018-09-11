#ifndef CEPH_RGW_SERVICES_SYS_OBJ_H
#define CEPH_RGW_SERVICES_SYS_OBJ_H


#include "rgw/rgw_service.h"

#include "svc_rados.h"
#include "svc_sys_obj_core.h"


class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSysObjectCtx;

struct rgw_cache_entry_info;

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

    RGWSI_SysObj_Core *core_svc;
    RGWSysObjectCtx& ctx;
    rgw_raw_obj obj;

  public:
    Obj(RGWSI_SysObj_Core *_core_svc,
        RGWSysObjectCtx& _ctx,
        const rgw_raw_obj& _obj) : core_svc(_core_svc),
                                   ctx(_ctx),
                                   obj(_obj) {}

    void invalidate();

    RGWSysObjectCtx& get_ctx() {
      return ctx;
    }

    rgw_raw_obj& get_obj() {
      return obj;
    }

    struct ROp {
      Obj& source;

      RGWSI_SysObj_Core::GetObjState state;
      
      RGWObjVersionTracker *objv_tracker{nullptr};
      map<string, bufferlist> *attrs{nullptr};
      boost::optional<obj_version> refresh_version{boost::none};
      ceph::real_time *lastmod{nullptr};
      uint64_t *obj_size{nullptr};
      rgw_cache_entry_info *cache_info{nullptr};

      ROp& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

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

      ROp& set_refresh_version(boost::optional<obj_version>& rf) {
        refresh_version = rf;
        return *this;
      }

      ROp& set_cache_info(rgw_cache_entry_info *ci) {
        cache_info = ci;
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
      ceph::real_time *pmtime{nullptr};
      bool exclusive{false};

      WOp& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      WOp& set_attrs(map<string, bufferlist>& _attrs) {
        attrs = _attrs;
        return *this;
      }

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

      int write_data(bufferlist& bl); /* write data only */
      int write_attrs(); /* write attrs only */
      int write_attr(const char *name, bufferlist& bl); /* write attrs only */
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

    struct WNOp {
      Obj& source;

      WNOp(Obj& _source) : source(_source) {}

      int notify(bufferlist& bl,
		 uint64_t timeout_ms,
		 bufferlist *pbl);
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

    WNOp wn() {
      return WNOp(*this);
    }
  };

  class Pool {
    friend class Op;

    RGWSI_RADOS *rados_svc;
    RGWSI_SysObj_Core *core_svc;
    rgw_pool pool;

  public:
    Pool(RGWSI_RADOS *_rados_svc,
	 RGWSI_SysObj_Core *_core_svc,
         const rgw_pool& _pool) : rados_svc(_rados_svc),
                                  core_svc(_core_svc),
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

protected:
  std::shared_ptr<RGWSI_RADOS> rados_svc;
  std::shared_ptr<RGWSI_SysObj_Core> core_svc;

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

public:
  RGWSI_SysObj(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

  RGWSysObjectCtx init_obj_ctx();
  Obj get_obj(RGWSysObjectCtx& obj_ctx, const rgw_raw_obj& obj);

  Pool get_pool(const rgw_pool& pool) {
    return Pool(rados_svc.get(), core_svc.get(), pool);
  }

  RGWSI_Zone *get_zone_svc();
};

using RGWSysObj = RGWSI_SysObj::Obj;

class RGWSysObjectCtx : public RGWSysObjectCtxBase
{
  RGWSI_SysObj *sysobj_svc;
public:
  RGWSysObjectCtx(RGWSI_SysObj *_sysobj_svc) : sysobj_svc(_sysobj_svc) {}

  RGWSI_SysObj::Obj get_obj(const rgw_raw_obj& obj) {
    return sysobj_svc->get_obj(*this, obj);
  }
};

#endif

