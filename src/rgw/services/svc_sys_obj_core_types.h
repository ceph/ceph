// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "common/shunique_lock.h"


#include "rgw/rgw_service.h"

#include "svc_rados.h"


struct RGWSI_SysObj_Obj_GetObjState {
  boost::optional<RGWSI_RADOS::Obj> rados_obj;
  uint64_t last_ver{0};

  RGWSI_SysObj_Obj_GetObjState() {}
  boost::system::error_code get_rados_obj(RGWSI_RADOS *rados_svc,
                                          RGWSI_Zone *zone_svc,
                                          const rgw_raw_obj& obj,
                                          RGWSI_RADOS::Obj **pobj,
                                          optional_yield y);
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

  boost::container::flat_map<std::string, ceph::buffer::list> attrset;
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
    ceph::shunique_lock l(lock, ceph::acquire_shared);
    assert(!obj.empty());
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      l.unlock();
    } else {
      l.unlock();
      l.lock();
      result = &objs_state[obj];
      l.unlock();
    }
    return result;
  }

  void set_prefetch_data(rgw_raw_obj& obj) {
    std::unique_lock wl{lock};
    assert(!obj.empty());
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
