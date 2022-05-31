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

#include "rgw/rgw_service.h"
#include "svc_meta_be.h"

class RGWAccountInfo;
class RGWSI_MetaBackend_Handler;

class RGWSI_Account : public RGWServiceInstance
{
public:
  RGWSI_Account(CephContext *cct);
  virtual ~RGWSI_Account();

  static string get_meta_key(const RGWAccountInfo& info);

  virtual RGWSI_MetaBackend_Handler *get_be_handler() = 0;

  virtual int read_account_info(RGWSI_MetaBackend::Context *ctx,
				const std::string& account_id,
				RGWAccountInfo *info,
				RGWObjVersionTracker * const objv_tracker,
				real_time * const pmtime,
				map<std::string, bufferlist> * const pattrs,
				optional_yield y) = 0;

  virtual int remove_account_info(RGWSI_MetaBackend::Context *ctx,
				  const std::string& account_id,
				  RGWObjVersionTracker *objv_tracker,
				  optional_yield y) = 0;

  virtual int add_user(const RGWAccountInfo& info,
		       const rgw_user& rgw_user,
		       optional_yield y) = 0;

  virtual int remove_user(const RGWAccountInfo& info,
                          const rgw_user& rgw_user,
                          optional_yield y) = 0;

  virtual int store_account_info(RGWSI_MetaBackend::Context *ctx,
				 const RGWAccountInfo& info,
				 RGWObjVersionTracker * const objv_tracker,
				 const real_time& mtime,
				 bool exclusive,
				 map<std::string, bufferlist> * const pattrs,
				 optional_yield y) = 0;

  virtual int list_users(const RGWAccountInfo& info,
                         const std::string& marker,
                         bool* more,
                         vector<rgw_user>& results,
                         optional_yield y) = 0;

};