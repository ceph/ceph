// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/static_ptr.h"

#include "rgw/rgw_service.h"

#include "svc_rados.h"
#include "svc_sys_obj_core_types.h"
#include "common/expected.h"


class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSysObjectCtx;

struct rgw_cache_entry_info;

class RGWSI_SysObj : public RGWServiceInstance
{
  friend struct RGWServices_Def;

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

      RGWSI_SysObj_Obj_GetObjState state;

      RGWObjVersionTracker *objv_tracker{nullptr};
      boost::container::flat_map<std::string, ceph::buffer::list>* attrs{nullptr};
      bool raw_attrs{false};
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

      ROp& set_attrs(boost::container::flat_map<std::string,
                                                ceph::buffer::list>* _attrs) {
        attrs = _attrs;
        return *this;
      }

      ROp& set_raw_attrs(bool ra) {
	raw_attrs = ra;
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

      ROp(Obj& _source);

      boost::system::error_code stat(optional_yield y);
      boost::system::error_code read(int64_t ofs, int64_t end, bufferlist *pbl, optional_yield y);
      boost::system::error_code read(bufferlist *pbl, optional_yield y) {
        return read(0, -1, pbl, y);
      }
      boost::system::error_code get_attr(const char *name, bufferlist *dest, optional_yield y);
    };

    struct WOp {
      Obj& source;

      RGWObjVersionTracker *objv_tracker{nullptr};
      boost::container::flat_map<std::string, ceph::buffer::list> attrs;
      ceph::real_time mtime;
      ceph::real_time *pmtime{nullptr};
      bool exclusive{false};

      WOp& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      WOp& set_attrs(boost::container::flat_map<std::string, ceph::buffer::list>& _attrs) {
        attrs = _attrs;
        return *this;
      }

      WOp& set_attrs(boost::container::flat_map<std::string, ceph::buffer::list>&& _attrs) {
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

      boost::system::error_code remove(optional_yield y);
      boost::system::error_code write(bufferlist& bl, optional_yield y);

      boost::system::error_code write_data(bufferlist& bl, optional_yield y); /* write data only */
      boost::system::error_code write_attrs(optional_yield y); /* write attrs only */
      boost::system::error_code write_attr(const char *name, bufferlist& bl,
                                           optional_yield y); /* write attrs only */
    };

    struct OmapOp {
      Obj& source;

      bool must_exist{false};

      OmapOp& set_must_exist(bool _must_exist = true) {
        must_exist = _must_exist;
        return *this;
      }

      OmapOp(Obj& _source) : source(_source) {}

      boost::system::error_code
      get_all(boost::container::flat_map<std::string, ceph::buffer::list> *m,
              optional_yield y);
      boost::system::error_code
      get_vals(const std::string& marker, uint64_t count,
               boost::container::flat_map<std::string, ceph::buffer::list> *m,
               bool *pmore, optional_yield y);
      boost::system::error_code set(const std::string& key, bufferlist& bl,
                                    optional_yield y);
      boost::system::error_code
      set(const boost::container::flat_map<std::string, ceph::buffer::list>& m,
          optional_yield y);
      boost::system::error_code del(const std::string& key, optional_yield y);
    };

    struct WNOp {
      Obj& source;

      WNOp(Obj& _source) : source(_source) {}

      boost::system::error_code notify(bufferlist& bl,
                                       std::optional<std::chrono::milliseconds> timeout,
                                       bufferlist *pbl, optional_yield y);
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
    friend class RGWSI_SysObj_Core;

    RGWSI_SysObj_Core* core_svc;
    RGWSI_RADOS::Pool pool;

  public:
    Pool(RGWSI_SysObj_Core *_core_svc,
	 RGWSI_RADOS::Pool&& pool) : core_svc(_core_svc),
				   pool(std::move(pool)) {}

    CephContext* cct() const {
      return pool.get_cct();
    }

    rgw_pool get_pool() const {
      return pool.get_rgw_pool();
    }

    struct Op {
      static constexpr auto dout_subsys = ceph_subsys_rgw;
      RGWSI_RADOS::Pool::List ctx;

      Op(Pool* pool, RGWAccessListFilter filter,
	 std::optional<std::string> marker)
	: ctx(pool->pool.list(std::move(filter))) {
	if (marker) {
	  auto b = ctx.set_marker(*marker);
	  if (!b) {
	    ldout(pool->cct(), -1) << __func__
				   << " ERROR: failed to convert cursor "
				   << *marker << dendl;
	  }
	}
      }

      boost::system::error_code get_next(int max, std::vector<string> *oids,
					 bool *is_truncated);
      std::string get_marker();
    };

    boost::system::error_code list_prefixed_objs(
      const std::string& prefix,
      std::function<void(std::string_view)> cb);
    boost::system::error_code list_prefixed_objs(
      const std::string& prefix,
      std::vector<std::string>* oids);

    template <typename Container>
    boost::system::error_code list_prefixed_objs(std::string_view prefix,
						 Container *result) {
      return list_prefixed_objs(prefix, [&](const string& val) {
        result->push_back(val);
      });
    }

    Op op(RGWAccessListFilter filter,
	  std::optional<std::string> marker = nullopt) {
      return Op{this, filter, marker};
    }
  };

  friend class Obj;
  friend struct Obj::ROp;
  friend struct Obj::WOp;

protected:
  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_SysObj_Core *core_svc{nullptr};

  void init(RGWSI_RADOS *_rados_svc,
            RGWSI_SysObj_Core *_core_svc) {
    rados_svc = _rados_svc;
    core_svc = _core_svc;
  }

public:
  RGWSI_SysObj(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc) {}

  RGWSysObjectCtx init_obj_ctx();
  Obj get_obj(RGWSysObjectCtx& obj_ctx, const rgw_raw_obj& obj);

  tl::expected<Pool, boost::system::error_code>
  get_pool(const rgw_pool& pool, optional_yield y) {
    return Pool(core_svc, TRY(rados_svc->pool(pool, y)));
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
