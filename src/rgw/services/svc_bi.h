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

#include "include/expected.hpp"

#include "rgw/rgw_service.h"

class RGWBucketInfo;
struct RGWBucketEnt;


class RGWSI_BucketIndex : public RGWServiceInstance
{
public:
  RGWSI_BucketIndex(CephContext *cct, boost::asio::io_context& ioc) :
    RGWServiceInstance(cct, ioc) {}
  virtual ~RGWSI_BucketIndex() {}

  virtual boost::system::error_code init_index(RGWBucketInfo& bucket_info,
                                               optional_yield y) = 0;
  virtual boost::system::error_code clean_index(RGWBucketInfo& bucket_info,
                                                optional_yield y) = 0;

  virtual tl::expected<RGWBucketEnt, boost::system::error_code>
  read_stats(const RGWBucketInfo& bucket_info, optional_yield y) = 0;

  virtual boost::system::error_code
  handle_overwrite(const RGWBucketInfo& info,
                   const RGWBucketInfo& orig_info,
                   optional_yield y) = 0;
};
