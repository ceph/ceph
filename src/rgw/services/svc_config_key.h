

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "rgw/rgw_service.h"

class RGWSI_ConfigKey : public RGWServiceInstance
{
public:
  RGWSI_ConfigKey(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_ConfigKey() {}

  virtual int get(const string& key, bool secure, bufferlist *result) = 0;
};

