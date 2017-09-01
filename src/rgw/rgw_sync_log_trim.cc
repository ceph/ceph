// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * Author: Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw_sync_log_trim.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "trim: ")

using rgw::BucketTrimConfig;

namespace rgw {

class BucketTrimManager::Impl {
 public:
  RGWRados *const store;
  const BucketTrimConfig config;

  Impl(RGWRados *store, const BucketTrimConfig& config)
    : store(store), config(config)
  {}
};

BucketTrimManager::BucketTrimManager(RGWRados *store,
                                     const BucketTrimConfig& config)
  : impl(new Impl(store, config))
{
}
BucketTrimManager::~BucketTrimManager() = default;

} // namespace rgw
