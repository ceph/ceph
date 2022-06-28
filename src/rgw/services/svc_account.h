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

#include <map>
#include <string>
#include <vector>
#include "rgw/rgw_service.h"
#include "svc_meta_be.h"

class RGWAccountInfo;
class RGWSI_MetaBackend_Handler;

class RGWSI_Account : public RGWServiceInstance
{
public:
  RGWSI_Account(CephContext *cct);
  virtual ~RGWSI_Account();

  static std::string get_meta_key(const RGWAccountInfo& info);

  // metadata key for RGWAccountNameToId
  static std::string get_name_meta_key(std::string_view tenant,
                                       std::string_view name);

  virtual RGWSI_MetaBackend_Handler *get_be_handler() = 0;

  virtual int read_account_by_name(const DoutPrefixProvider *dpp,
                                   RGWSI_MetaBackend::Context *ctx,
                                   std::string_view tenant,
                                   std::string_view name,
                                   RGWAccountInfo& info,
                                   RGWObjVersionTracker& objv,
                                   real_time* pmtime,
                                   std::map<std::string, bufferlist>* pattrs,
                                   optional_yield y) = 0;

  virtual int read_account_info(const DoutPrefixProvider *dpp,
				RGWSI_MetaBackend::Context *ctx,
				std::string_view account_id,
				RGWAccountInfo& info,
				RGWObjVersionTracker& objv,
				real_time* pmtime,
				std::map<std::string, bufferlist>*  pattrs,
				optional_yield y) = 0;

  virtual int remove_account_info(const DoutPrefixProvider* dpp,
				  RGWSI_MetaBackend::Context* ctx,
				  const RGWAccountInfo& info,
				  RGWObjVersionTracker& objv,
				  optional_yield y) = 0;

  virtual int add_user(const DoutPrefixProvider *dpp,
		       const RGWAccountInfo& info,
		       const rgw_user& rgw_user,
		       optional_yield y) = 0;

  virtual int remove_user(const DoutPrefixProvider *dpp,
                          const RGWAccountInfo& info,
                          const rgw_user& rgw_user,
                          optional_yield y) = 0;

  virtual int store_account_info(const DoutPrefixProvider *dpp,
				 RGWSI_MetaBackend::Context *ctx,
				 const RGWAccountInfo& info,
				 const RGWAccountInfo* old_info,
				 RGWObjVersionTracker& objv,
				 const real_time& mtime, bool exclusive,
				 std::map<std::string, bufferlist>* pattrs,
				 optional_yield y) = 0;

  virtual int list_users(const DoutPrefixProvider *dpp,
                         const RGWAccountInfo& info,
                         const std::string& marker,
                         bool* more,
                         std::vector<rgw_user>& results,
                         optional_yield y) = 0;

};
