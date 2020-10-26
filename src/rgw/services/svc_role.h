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
#include "rgw/rgw_role.h"
#include "svc_meta_be.h"

class RGWSI_Role: public RGWServiceInstance
{
 public:
  RGWSI_Role(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_Role() {}

  virtual RGWSI_MetaBackend_Handler* get_be_handler() = 0;
  static std::string get_role_meta_key(const std::string& role_id);
  static std::string get_role_name_meta_key(const std::string& role_name, const std::string& tenant);
  static std::string get_role_path_meta_key(const std::string& path, const std::string& role_id, const std::string& tenant);

  virtual int create(RGWSI_MetaBackend::Context *ctx,
		     rgw::sal::RGWRole& role,
		     RGWObjVersionTracker * const objv_tracker,
		     const real_time& pmtime,
		     bool exclusive,
		     std::map<std::string, bufferlist> * pattrs,
		     optional_yield y,
			 const DoutPrefixProvider *dpp) = 0;

  virtual int store_info(RGWSI_MetaBackend::Context *ctx,
			 const rgw::sal::RGWRole& role,
			 RGWObjVersionTracker * const objv_tracker,
			 const real_time& mtime,
			 bool exclusive,
			 std::map<std::string, bufferlist> * pattrs,
			 optional_yield y,
             const DoutPrefixProvider *dpp) = 0;

  virtual int store_name(RGWSI_MetaBackend::Context *ctx,
			 const std::string& role_id,
			 const std::string& name,
			 const std::string& tenant,
			 RGWObjVersionTracker * const objv_tracker,
			 const real_time& mtime,
			 bool exclusive,
			 optional_yield y,
             const DoutPrefixProvider *dpp) = 0;

  virtual int store_path(RGWSI_MetaBackend::Context *ctx,
			 const std::string& role_id,
			 const std::string& path,
			 const std::string& tenant,
			 RGWObjVersionTracker * const objv_tracker,
			 const real_time &mtime,
			 bool exclusive,
			 optional_yield y,
             const DoutPrefixProvider *dpp) = 0;

  virtual int read_info(RGWSI_MetaBackend::Context *ctx,
			const std::string& role_id,
			rgw::sal::RGWRole *role,  // out param
			RGWObjVersionTracker * const objv_tracker,
			real_time * const pmtime,
			std::map<std::string, bufferlist> * pattrs,
			optional_yield y,
            const DoutPrefixProvider *dpp) = 0;

  virtual int read_name(RGWSI_MetaBackend::Context *ctx,
			const std::string& name,
			const std::string& tenant,
			std::string& role_id,
			RGWObjVersionTracker * const objv_tracker,
			real_time * const pmtime,
			optional_yield y,
            const DoutPrefixProvider *dpp) = 0;

  virtual int read_path(RGWSI_MetaBackend::Context *ctx,
			std::string& path,
			RGWObjVersionTracker * const objv_tracker,
			real_time * const pmtime,
			optional_yield y,
            const DoutPrefixProvider *dpp) = 0;

  virtual int delete_info(RGWSI_MetaBackend::Context *ctx,
			   const std::string& id,
			  RGWObjVersionTracker * const objv_tracker,
			  optional_yield y,
			  const DoutPrefixProvider *dpp) = 0;

  // deletes a role from the system and the supporting objects
  virtual int delete_role(RGWSI_MetaBackend::Context *ctx,
			  const rgw::sal::RGWRole& info,
			  RGWObjVersionTracker * const objv_tracker,
			  optional_yield y,
			  const DoutPrefixProvider *dpp) = 0;

  virtual int delete_name(RGWSI_MetaBackend::Context *ctx,
			  const std::string& name,
			  const std::string& tenant,
			  RGWObjVersionTracker * const objv_tracker,
			  optional_yield y,
			  const DoutPrefixProvider *dpp) = 0;

  virtual int delete_path(RGWSI_MetaBackend::Context *ctx,
			  const std::string& role_id,
			  const std::string& path,
			  const std::string& tenant,
			  RGWObjVersionTracker * const objv_tracker,
			  optional_yield y,
			  const DoutPrefixProvider *dpp) = 0;

  virtual int list_roles_by_path_prefix(RGWSI_MetaBackend::Context *ctx,
								const std::string& path,
								const std::string& tenant,
								std::vector<rgw::sal::RGWRole>& roles,  // out param
								optional_yield y,
								const DoutPrefixProvider *dpp) = 0;

};

static const std::string role_name_oid_prefix = "role_names.";
static const std::string role_oid_prefix = "roles.";
static const std::string role_path_oid_prefix = "role_paths.";
