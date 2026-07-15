// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_rest.h"

// Serves the s3control api under the path /v20180820
class RGWRESTMgr_S3Control : public RGWRESTMgr {
  friend class RGWRESTMgr_S3; // for protected get_resource_mgr()
 public:
  RGWRESTMgr_S3Control();
};
