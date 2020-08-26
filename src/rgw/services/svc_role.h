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

class RGWRole;

class RGWSI_Role: public RGWServiceInstance
{
 public:
  RGWSI_Role(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_Role() {}

  virtual RGWSI_MetaBackend_Handler* get_be_handler() = 0;

  virtual int store_info(RGWSI_MetaBackend::Context *ctx,
			 const RGWRole& role,
			 RGWObjVersionTracker * const objv_tracker,
			 real_time * const pmtime,
			 bool exclusive,
			 std::map<std::string, bufferlist> * pattrs,
			 optional_yield y) = 0;

  virtual int store_name(RGWSI_MetaBackend::Context *ctx,
			 const std::string& name,
			 RGWObjVersionTracker * const objv_tracker,
			 real_time * const pmtime,
			 bool exclusive,
			 optional_yield y) = 0;

  virtual int store_path(RGWSI_MetaBackend::Context *ctx,
			 const std::string& path,
			 RGWObjVersionTracker * const objv_tracker,
			 real_time * const pmtime,
			 bool exclusive,
			 optional_yield y) = 0;

  virtual int read_info(RGWSI_MetaBackend::Context *ctx,
			RGWRole *role,
			RGWObjVersionTracker * const objv_tracker,
			real_time * const pmtime,
			std::map<std::string, bufferlist> * pattrs,
			optional_yield y) = 0;

  virtual int read_name(RGWSI_MetaBackend::Context *ctx,
			std::string& name,
			RGWObjVersionTracker * const objv_tracker,
			real_time * const pmtime,
			optional_yield y) = 0;

  virtual int read_path(RGWSI_MetaBackend::Context *ctx,
			std::string& path,
			RGWObjVersionTracker * const objv_tracker,
			real_time * const pmtime,
			optional_yield y) = 0;

  virtual int delete_info(RGWSI_MetaBackend::Context *ctx,
			  const std::string& name,
			  RGWObjVersionTracker * const objv_tracker,
			  optional_yield y) = 0;
};
