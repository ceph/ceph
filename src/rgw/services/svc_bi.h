
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include "driver/rados/rgw_service.h"

class RGWBucketInfo;
struct RGWBucketEnt;

namespace rgw { namespace rados { class BIndexer; } }


class RGWSI_BucketIndex : public RGWServiceInstance
{
public:
  RGWSI_BucketIndex(CephContext *cct) : RGWServiceInstance(cct) {}
  virtual ~RGWSI_BucketIndex() {}

  virtual int init_index(const DoutPrefixProvider *dpp, optional_yield y,
                         std::unique_ptr<rgw::rados::BIndexer>& bindexer,
                         std::map<std::string, bufferlist>* binfo_map_data,
                         bool judge_support_logrecord = false) = 0;
#if 1 // OBI deprecate??
  virtual int clean_index(const DoutPrefixProvider *dpp, optional_yield y,
                          const RGWBucketInfo& bucket_info,
                          const rgw::bucket_index_layout_generation& idx_layout) = 0;
#endif
  virtual int clean_index(const DoutPrefixProvider *dpp, optional_yield y,
                          std::unique_ptr<rgw::rados::BIndexer>& bindexer) = 0;

  virtual int read_stats(const DoutPrefixProvider *dpp,
                         const RGWBucketInfo& bucket_info,
                         RGWBucketEnt *stats,
                         optional_yield y) = 0;

  virtual int handle_overwrite(const DoutPrefixProvider *dpp,
                               const RGWBucketInfo& info,
                               const RGWBucketInfo& orig_info,
                               optional_yield y) = 0;
};
