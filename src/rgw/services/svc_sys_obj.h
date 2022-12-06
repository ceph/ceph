// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/static_ptr.h"

#include "rgw_service.h"

#include "svc_sys_obj_types.h"
#include "svc_sys_obj_core_types.h"


class RGWSI_Zone;
class RGWSI_SysObj;

struct rgw_cache_entry_info;

class RGWSI_SysObj : public RGWServiceInstance
{
  friend struct RGWServices_Def;

public:
  class Obj {
    friend class ROp;

    RGWSI_SysObj_Core *core_svc;
    rgw_raw_obj obj;

  public:
    Obj(RGWSI_SysObj_Core *_core_svc, const rgw_raw_obj& _obj)
        : core_svc(_core_svc), obj(_obj) {}

    rgw_raw_obj& get_obj() {
      return obj;
    }

    struct ROp {
      Obj& source;

      ceph::static_ptr<RGWSI_SysObj_Obj_GetObjState, sizeof(RGWSI_SysObj_Core_GetObjState)> state;
      
      RGWObjVersionTracker *objv_tracker{nullptr};
      std::map<std::string, bufferlist> *attrs{nullptr};
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

      ROp& set_attrs(std::map<std::string, bufferlist> *_attrs) {
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

      int stat(optional_yield y, const DoutPrefixProvider *dpp);
      int read(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, bufferlist *pbl, optional_yield y);
      int read(const DoutPrefixProvider *dpp, bufferlist *pbl, optional_yield y) {
        return read(dpp, 0, -1, pbl, y);
      }
      int get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist *dest, optional_yield y);
    };

    struct WOp {
      Obj& source;

      RGWObjVersionTracker *objv_tracker{nullptr};
      std::map<std::string, bufferlist> attrs;
      ceph::real_time mtime;
      ceph::real_time *pmtime{nullptr};
      bool exclusive{false};

      WOp& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
        objv_tracker = _objv_tracker;
        return *this;
      }

      WOp& set_attrs(const std::map<std::string, bufferlist>& _attrs) {
        attrs = _attrs;
        return *this;
      }

      WOp& set_attrs(std::map<std::string, bufferlist>&& _attrs) {
        attrs = std::move(_attrs);
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

      int remove(const DoutPrefixProvider *dpp, optional_yield y);
      int write(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y);

      int write_data(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y); /* write data only */
      int write_attrs(const DoutPrefixProvider *dpp, optional_yield y); /* write attrs only */
      int write_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& bl,
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

      int get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m, optional_yield y);
      int get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
                   std::map<std::string, bufferlist> *m,
                   bool *pmore, optional_yield y);
      int set(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& bl, optional_yield y);
      int set(const DoutPrefixProvider *dpp, const std::map<std::string, bufferlist>& m, optional_yield y);
      int del(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y);
    };

    struct WNOp {
      Obj& source;

      WNOp(Obj& _source) : source(_source) {}

      int notify(const DoutPrefixProvider *dpp, bufferlist& bl, uint64_t timeout_ms, bufferlist *pbl,
                 optional_yield y);
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

    RGWSI_SysObj_Core *core_svc;
    rgw_pool pool;

  protected:
    using ListImplInfo = RGWSI_SysObj_Pool_ListInfo;

    struct ListCtx {
      ceph::static_ptr<ListImplInfo, sizeof(RGWSI_SysObj_Core_PoolListImplInfo)> impl; /* update this if creating new backend types */
    };

  public:
    Pool(RGWSI_SysObj_Core *_core_svc,
         const rgw_pool& _pool) : core_svc(_core_svc),
                                  pool(_pool) {}

    rgw_pool& get_pool() {
      return pool;
    }

    struct Op {
      Pool& source;
      ListCtx ctx;

      Op(Pool& _source) : source(_source) {}

      int init(const DoutPrefixProvider *dpp, const std::string& marker, const std::string& prefix);
      int get_next(const DoutPrefixProvider *dpp, int max, std::vector<std::string> *oids, bool *is_truncated);
      int get_marker(std::string *marker);
    };

    int list_prefixed_objs(const DoutPrefixProvider *dpp, const std::string& prefix, std::function<void(const std::string&)> cb);

    template <typename Container>
    int list_prefixed_objs(const DoutPrefixProvider *dpp, const std::string& prefix,
                           Container *result) {
      return list_prefixed_objs(dpp, prefix, [&](const std::string& val) {
        result->push_back(val);
      });
    }

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
  librados::Rados* rados{nullptr};
  RGWSI_SysObj_Core *core_svc{nullptr};

  void init(librados::Rados* rados_,
            RGWSI_SysObj_Core *_core_svc) {
    rados = rados_;
    core_svc = _core_svc;
  }

public:
  RGWSI_SysObj(CephContext *cct): RGWServiceInstance(cct) {}

  Obj get_obj(const rgw_raw_obj& obj);

  Pool get_pool(const rgw_pool& pool) {
    return Pool(core_svc, pool);
  }

  RGWSI_Zone *get_zone_svc();
};

using RGWSysObj = RGWSI_SysObj::Obj;
