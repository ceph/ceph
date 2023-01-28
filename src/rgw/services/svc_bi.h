
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

#include "rgw_service.h"

class RGWBucketInfo;
struct RGWBucketEnt;


class RGWSI_BucketIndex : public RGWServiceInstance
{
public:
  RGWSI_BucketIndex(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_BucketIndex() {}

  virtual int init_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout) = 0;
  virtual int clean_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout) = 0;

  virtual int read_stats(const DoutPrefixProvider *dpp,
                         const RGWBucketInfo& bucket_info,
                         RGWBucketEnt *stats,
                         optional_yield y) = 0;

  virtual int handle_overwrite(const DoutPrefixProvider *dpp,
                               const RGWBucketInfo& info,
                               const RGWBucketInfo& orig_info,
                               optional_yield y) = 0;
};
