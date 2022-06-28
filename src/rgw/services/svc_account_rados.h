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

#include "rgw_service.h"

struct RGWAccountInfo;

class RGWSI_Account_RADOS : public RGWServiceInstance
{
public:
  struct Svc {
    RGWSI_Zone* zone = nullptr;
    RGWSI_MDLog* mdlog = nullptr;
    RGWSI_SysObj* sysobj = nullptr;
    RGWSI_RADOS* rados = nullptr;
  } svc;

  RGWSI_Account_RADOS(RGWSI_Zone* zone_svc,
                      RGWSI_MDLog* mdlog_svc,
                      RGWSI_SysObj* sysobj_svc,
                      RGWSI_RADOS* rados_svc);
  ~RGWSI_Account_RADOS() = default;

  const rgw_pool& account_pool() const;
  const rgw_pool& account_name_pool() const;

  int store_account_info(const DoutPrefixProvider *dpp,
			 RGWSysObjectCtx& obj_ctx,
			 const RGWAccountInfo& info,
			 const RGWAccountInfo* old_info,
			 RGWObjVersionTracker& objv,
			 const real_time& mtime, bool exclusive,
			 std::map<std::string, bufferlist>* pattrs,
			 optional_yield y);

  int read_account_by_name(const DoutPrefixProvider *dpp,
                           RGWSysObjectCtx& obj_ctx,
                           std::string_view tenant,
                           std::string_view name,
                           RGWAccountInfo& info,
                           RGWObjVersionTracker& objv,
                           real_time* pmtime,
                           std::map<std::string, bufferlist>* pattrs,
                           optional_yield y);

  int read_account_info(const DoutPrefixProvider *dpp,
			RGWSysObjectCtx& obj_ctx,
			std::string_view account_id,
			RGWAccountInfo& info,
			RGWObjVersionTracker& objv,
			real_time* pmtime,
			std::map<std::string, bufferlist>* pattrs,
			optional_yield y);

  int remove_account_info(const DoutPrefixProvider* dpp,
                          RGWSysObjectCtx& obj_ctx,
                          const RGWAccountInfo& info,
                          RGWObjVersionTracker& objv,
                          optional_yield y);

  int add_user(const DoutPrefixProvider *dpp,
               const RGWAccountInfo& info,
               const rgw_user& rgw_user,
               optional_yield y);

  int remove_user(const DoutPrefixProvider *dpp,
                  const RGWAccountInfo& info,
                  const rgw_user& rgw_user,
                  optional_yield y);

  int list_users(const DoutPrefixProvider *dpp,
                 const RGWAccountInfo& info,
                 const std::string& marker,
                 bool *more,
                 std::vector<rgw_user>& results,
                 optional_yield y);

  rgw_raw_obj get_account_user_obj(const std::string& account_id) const;
};
