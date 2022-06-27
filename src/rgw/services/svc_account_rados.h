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
class RGWSI_Meta;
class RGWSI_SyncModules;
class RGWSI_MetaBackend_Handler;

class RGWSI_Account_RADOS : public RGWSI_Account
{
public:
  struct Svc {
    RGWSI_Zone *zone {nullptr};
    RGWSI_Meta *meta {nullptr};
    RGWSI_MetaBackend *meta_be {nullptr};
    RGWSI_RADOS *rados{nullptr};
  } svc;

  RGWSI_Account_RADOS(CephContext *cct);
  ~RGWSI_Account_RADOS() = default;

  RGWSI_MetaBackend_Handler *get_be_handler() override {
    return be_handler;
  }

  int do_start(optional_yield y, const DoutPrefixProvider *dpp) override;

  void init(RGWSI_Zone *_zone_svc,
	    RGWSI_Meta *_meta_svc,
	    RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_RADOS *_rados_svc);

  int store_account_info(const DoutPrefixProvider *dpp,
			 RGWSI_MetaBackend::Context *ctx,
			 const RGWAccountInfo& info,
			 RGWObjVersionTracker& objv,
			 const real_time& mtime, bool exclusive,
			 std::map<std::string, bufferlist>* pattrs,
			 optional_yield y) override;

  int read_account_info(const DoutPrefixProvider *dpp,
			RGWSI_MetaBackend::Context *ctx,
			const std::string& account_id,
			RGWAccountInfo& info,
			RGWObjVersionTracker& objv,
			real_time* pmtime,
			std::map<std::string, bufferlist>* pattrs,
			optional_yield y) override;

  int remove_account_info(const DoutPrefixProvider* dpp,
                          RGWSI_MetaBackend::Context* ctx,
                          const std::string& account_id,
                          RGWObjVersionTracker& objv,
                          optional_yield y) override;

  int add_user(const DoutPrefixProvider *dpp,
               const RGWAccountInfo& info,
               const rgw_user& rgw_user,
               optional_yield y) override;

  int remove_user(const DoutPrefixProvider *dpp,
                  const RGWAccountInfo& info,
                  const rgw_user& rgw_user,
                  optional_yield y) override;

  int list_users(const DoutPrefixProvider *dpp,
                 const RGWAccountInfo& info,
                 const std::string& marker,
                 bool *more,
                 std::vector<rgw_user>& results,
                 optional_yield y) override;

  rgw_raw_obj get_account_user_obj(const std::string& account_id) const;
private:
  RGWSI_MetaBackend_Handler *be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;
};
