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

  int get_rados_obj(const DoutPrefixProvider *dpp,
                    RGWSI_RADOS *rados_svc,
                    RGWSI_Zone *zone_svc,
                    const rgw_raw_obj& obj,
                    RGWSI_RADOS::Obj **pobj);
};

struct RGWSI_SysObj_Core_PoolListImplInfo : public RGWSI_SysObj_Pool_ListInfo {
  RGWSI_RADOS::Pool pool;
  RGWSI_RADOS::Pool::List op;
  RGWAccessListFilterPrefix filter;

  RGWSI_SysObj_Core_PoolListImplInfo(const std::string& prefix) : op(pool.op()), filter(prefix) {}
};
