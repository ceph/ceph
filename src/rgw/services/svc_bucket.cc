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

#include "svc_bucket.h"

#define dout_subsys ceph_subsys_rgw

string RGWSI_Bucket::get_entrypoint_meta_key(const rgw_bucket& bucket)
{
  if (bucket.bucket_id.empty()) {
    return bucket.get_key();
  }

  rgw_bucket b(bucket);
  b.bucket_id.clear();

  return b.get_key();
}

string RGWSI_Bucket::get_bi_meta_key(const rgw_bucket& bucket)
{
  return bucket.get_key();
}
