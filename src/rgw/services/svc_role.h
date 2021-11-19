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
};

static const std::string role_name_oid_prefix = "role_names.";
static const std::string role_oid_prefix = "roles.";
static const std::string role_path_oid_prefix = "role_paths.";
