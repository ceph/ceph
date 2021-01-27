
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
#include "svc_meta_be_otp.h"

class RGWSI_Zone;

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
               const string& key,
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
  int store_all(RGWSI_OTP_BE_Ctx& ctx,
                const string& key,
                const otp_devices_list_t& devices,
                real_time mtime,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);
  int store_all(RGWSI_OTP_BE_Ctx& ctx,
                const rgw_user& uid,
                const otp_devices_list_t& devices,
                real_time mtime,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);
  int remove_all(RGWSI_OTP_BE_Ctx& ctx,
                 const string& key,
                 RGWObjVersionTracker *objv_tracker,
                 optional_yield y);
  int remove_all(RGWSI_OTP_BE_Ctx& ctx,
                 const rgw_user& uid,
                 RGWObjVersionTracker *objv_tracker,
                 optional_yield y);
};


