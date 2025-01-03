// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once


#include "rgw_tools.h"
#include "rgw_service.h"

#include "svc_sys_obj_types.h"



struct RGWSI_SysObj_Core_GetObjState : public RGWSI_SysObj_Obj_GetObjState {
  rgw_rados_ref rados_obj;
  bool has_rados_obj{false};
  uint64_t last_ver{0};

  RGWSI_SysObj_Core_GetObjState() {}

  int get_rados_obj(const DoutPrefixProvider *dpp,
                    librados::Rados* rados_svc,
                    RGWSI_Zone *zone_svc,
                    const rgw_raw_obj& obj,
                    rgw_rados_ref** pobj);
};

struct RGWSI_SysObj_Core_PoolListImplInfo : public RGWSI_SysObj_Pool_ListInfo {
  librados::IoCtx pool;
  rgw::AccessListFilter filter;
  std::string marker;

  RGWSI_SysObj_Core_PoolListImplInfo(const std::string& prefix,
                                     const std::string& marker)
    : filter(rgw::AccessListFilterPrefix(prefix)), marker(marker) {}
};
