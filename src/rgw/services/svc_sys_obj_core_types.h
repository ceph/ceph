// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once


#include "rgw/rgw_service.h"

#include "svc_rados.h"
#include "svc_sys_obj_types.h"



struct RGWSI_SysObj_Core_GetObjState : public RGWSI_SysObj_Obj_GetObjState {
  RGWSI_RADOS::Obj rados_obj;
  bool has_rados_obj{false};
  uint64_t last_ver{0};

  RGWSI_SysObj_Core_GetObjState() {}

  int get_rados_obj(RGWSI_RADOS *rados_svc,
                    RGWSI_Zone *zone_svc,
                    const rgw_raw_obj& obj,
                    RGWSI_RADOS::Obj **pobj);
};

struct RGWSI_SysObj_Core_PoolListImplInfo : public RGWSI_SysObj_Pool_ListInfo {
  RGWSI_RADOS::Pool pool;
  RGWSI_RADOS::Pool::List op;
  RGWAccessListFilterPrefix filter;

  RGWSI_SysObj_Core_PoolListImplInfo(const string& prefix) : op(pool.op()), filter(prefix) {}
};

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
  ceph::shared_mutex lock = ceph::make_shared_mutex("RGWSysObjectCtxBase");

public:
  RGWSysObjectCtxBase() = default;

  RGWSysObjectCtxBase(const RGWSysObjectCtxBase& rhs) : objs_state(rhs.objs_state) {}
  RGWSysObjectCtxBase(RGWSysObjectCtxBase&& rhs) : objs_state(std::move(rhs.objs_state)) {}

  RGWSysObjState *get_state(const rgw_raw_obj& obj) {
    RGWSysObjState *result;
    std::map<rgw_raw_obj, RGWSysObjState>::iterator iter;
    lock.lock_shared();
    assert (!obj.empty());
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock_shared();
    } else {
      lock.unlock_shared();
      lock.lock();
      result = &objs_state[obj];
      lock.unlock();
    }
    return result;
  }

  void set_prefetch_data(rgw_raw_obj& obj) {
    std::unique_lock wl{lock};
    assert (!obj.empty());
    objs_state[obj].prefetch_data = true;
  }
  void invalidate(const rgw_raw_obj& obj) {
    std::unique_lock wl{lock};
    auto iter = objs_state.find(obj);
    if (iter == objs_state.end()) {
      return;
    }
    objs_state.erase(iter);
  }
};

