
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

#include "cls/otp/cls_otp_types.h"

#include "rgw/rgw_service.h"

#include "svc_otp_types.h"
#include "svc_meta_be_sobj.h"

class RGWSI_Zone;

using otp_devices_list_t = std::list<rados::cls::otp::otp_info_t>;

struct RGWSI_MBOTP_GetParams : public RGWSI_MetaBackend::GetParams {
  otp_devices_list_t *pdevices{nullptr};
};

struct RGWSI_MBOTP_PutParams : public RGWSI_MetaBackend::PutParams {
  otp_devices_list_t devices;
};

using RGWSI_MBOTP_RemoveParams = RGWSI_MBSObj_RemoveParams;

class RGWSI_OTP : public RGWServiceInstance
{
  RGWSI_OTP_BE_Handler be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;

public:
  struct Svc {
    RGWSI_OTP *otp{nullptr};
    RGWSI_Zone *zone{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
  } svc;

  RGWSI_OTP(CephContext *cct);
  ~RGWSI_OTP();

  RGWSI_OTP_BE_Handler& get_be_handler() {
    return be_handler;
  }

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_Meta *_meta_svc,
            RGWSI_MetaBackend *_meta_be_svc);

  int read_all(RGWSI_OTP_BE_Ctx& ctx,
               const std::string& key,
               otp_devices_list_t *devices,
               real_time *pmtime,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y,
               const DoutPrefixProvider *dpp);
  int read_all(RGWSI_OTP_BE_Ctx& ctx,
               const rgw_user& uid,
               otp_devices_list_t *devices,
               real_time *pmtime,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y,
               const DoutPrefixProvider *dpp);
  int store_all(const DoutPrefixProvider *dpp, 
                RGWSI_OTP_BE_Ctx& ctx,
                const std::string& key,
                const otp_devices_list_t& devices,
                real_time mtime,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);
  int store_all(const DoutPrefixProvider *dpp, 
                RGWSI_OTP_BE_Ctx& ctx,
                const rgw_user& uid,
                const otp_devices_list_t& devices,
                real_time mtime,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);
  int remove_all(const DoutPrefixProvider *dpp, 
                 RGWSI_OTP_BE_Ctx& ctx,
                 const std::string& key,
                 RGWObjVersionTracker *objv_tracker,
                 optional_yield y);
  int remove_all(const DoutPrefixProvider *dpp, 
                 RGWSI_OTP_BE_Ctx& ctx,
                 const rgw_user& uid,
                 RGWObjVersionTracker *objv_tracker,
                 optional_yield y);
};


