

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

#include "rgw/rgw_service.h"

#include "svc_cls.h"
#include "svc_meta_be.h"
#include "svc_meta_be_sobj.h"
#include "svc_sys_obj.h"


using RGWSI_MBOTP_Handler_Module  = RGWSI_MBSObj_Handler_Module;
using RGWSI_MetaBackend_Handler_OTP  = RGWSI_MetaBackend_Handler_SObj;

using otp_devices_list_t = std::list<rados::cls::otp::otp_info_t>;

struct RGWSI_MBOTP_GetParams : public RGWSI_MetaBackend::GetParams {
  otp_devices_list_t *pdevices{nullptr};
};

struct RGWSI_MBOTP_PutParams : public RGWSI_MetaBackend::PutParams {
  otp_devices_list_t devices;
};

using RGWSI_MBOTP_RemoveParams = RGWSI_MBSObj_RemoveParams;

class RGWSI_MetaBackend_OTP : public RGWSI_MetaBackend_SObj
{
  RGWSI_Cls *cls_svc{nullptr};

public:
  struct Context_OTP : public RGWSI_MetaBackend_SObj::Context_SObj {
    otp_devices_list_t devices;
  };

  RGWSI_MetaBackend_OTP(CephContext *cct);
  virtual ~RGWSI_MetaBackend_OTP();

  RGWSI_MetaBackend::Type get_type() {
    return MDBE_OTP;
  }

  static std::string get_meta_key(const rgw_user& user);

  void init(RGWSI_SysObj *_sysobj_svc,
            RGWSI_MDLog *_mdlog_svc,
	    RGWSI_Cls *_cls_svc) {
    RGWSI_MetaBackend_SObj::init(_sysobj_svc, _mdlog_svc);
    cls_svc = _cls_svc;
  }

  RGWSI_MetaBackend_Handler *alloc_be_handler() override;
  RGWSI_MetaBackend::Context *alloc_ctx() override;

  int call_with_get_params(ceph::real_time *pmtime, std::function<int(RGWSI_MetaBackend::GetParams&)> cb) override;

  int get_entry(RGWSI_MetaBackend::Context *ctx,
                const std::string& key,
                RGWSI_MetaBackend::GetParams& _params,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y,
                const DoutPrefixProvider *dpp,
                bool get_raw_attrs=false);
  int put_entry(const DoutPrefixProvider *dpp, 
                RGWSI_MetaBackend::Context *ctx,
                const std::string& key,
                RGWSI_MetaBackend::PutParams& _params,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);
};


