
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

class RGWBucketInfo;
struct RGWBucketEnt;


class RGWSI_BucketIndex : public RGWServiceInstance
{
public:
  RGWSI_BucketIndex(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_BucketIndex() {}

  virtual int init_index(RGWBucketInfo& bucket_info) = 0;
  virtual int clean_index(RGWBucketInfo& bucket_info) = 0;

  virtual int read_stats(const RGWBucketInfo& bucket_info,
                         RGWBucketEnt *stats) = 0;
};

