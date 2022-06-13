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

class RGWSI_Role_RADOS: public RGWServiceInstance
{
 public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Meta *meta{nullptr};
    RGWSI_MetaBackend *meta_be{nullptr};
    RGWSI_SysObj *sysobj{nullptr};
  } svc;

  RGWSI_Role_RADOS(CephContext *cct) : RGWServiceInstance(cct) {}
  ~RGWSI_Role_RADOS() {}

  void init(RGWSI_Zone *_zone_svc,
	    RGWSI_Meta *_meta_svc,
	    RGWSI_MetaBackend *_meta_be_svc,
	    RGWSI_SysObj *_sysobj_svc);

  RGWSI_MetaBackend_Handler * get_be_handler();
  int do_start(optional_yield y, const DoutPrefixProvider *dpp) override;

private:
  RGWSI_MetaBackend_Handler *be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;
};

static const std::string role_name_oid_prefix = "role_names.";
static const std::string role_oid_prefix = "roles.";
static const std::string role_path_oid_prefix = "role_paths.";