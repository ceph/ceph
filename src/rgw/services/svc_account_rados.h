// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#pragma once

#include "svc_account.h"

class RGWSI_RADOS;
class RGWSI_Zone;
class RGWSI_SysObj;
class RGWSI_SysObj_Cache;
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWSI_MetaBackend_Handler;

class RGWSI_Account_Rados : public RGWSI_Account
{
public:
  struct Svc {
    RGWSI_Zone *zone {nullptr};
    RGWSI_MetaBackend *meta_be {nullptr};
  } svc;

  RGWSI_Account_Rados(CephContext *cct);
  ~RGWSI_Account_Rados() = default;

  int store_account_info(const DoutPrefixProvider *dpp,
			 RGWSI_MetaBackend::Context *ctx,
			 const RGWAccountInfo& info,
			 RGWObjVersionTracker * const objv_tracker,
			 const real_time& mtime,
			 bool exclusive,
			 std::map<std::string, bufferlist> * const pattrs,
			 optional_yield y) override;

private:
  RGWSI_MetaBackend_Handler *be_handler;
};
