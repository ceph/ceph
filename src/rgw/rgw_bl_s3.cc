// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_user.h"
#include "rgw_bl_s3.h"


#define dout_subsys ceph_subsys_rgw

using namespace std;

bool BLLoggingEnabled_S3::xml_end(const char *el) {
  BLTargetBucket_S3 *bl_target_bucket;
  BLTargetPrefix_S3 *bl_target_prefix;

  target_bucket.clear();
  target_prefix.clear();

  bl_target_bucket = static_cast<BLTargetBucket_S3 *>(find_first("TargetBucket"));

  if (bl_target_bucket) {
    target_bucket = bl_target_bucket->get_data();
  }

  bl_target_prefix = static_cast<BLTargetPrefix_S3 *>(find_first("TargetPrefix"));

  if (bl_target_prefix) {
    target_prefix = bl_target_prefix->get_data();
  }

  return true;
}

bool RGWBucketLoggingStatus_S3::xml_end(const char *el) {
  BLLoggingEnabled_S3 *bl_enabled = static_cast<BLLoggingEnabled_S3 *>(find_first("LoggingEnabled"));

  if (bl_enabled) {
    enabled.set_true();
    enabled.set_target_bucket(bl_enabled->get_target_bucket());
    enabled.set_target_prefix(bl_enabled->get_target_prefix());
  } else {
    enabled.set_false();
  }

  return true;
}

int RGWBucketLoggingStatus_S3::rebuild(RGWRados *store, RGWBucketLoggingStatus& dest)
{
  int ret = 0;
  // FIXME(jiaying) checkout ACL rule count limits here.
  if (this->is_enabled()) {
    dest.enabled.set_true();
    dest.enabled.set_target_bucket(this->get_target_bucket());
    dest.enabled.set_target_prefix(this->get_target_prefix());
    ldout(cct, 20) << "bl is_enabled, create new bl config"  << dendl;
  } else {
    dest.enabled.set_false();
  }

  return ret;
}

XMLObj *RGWBLXMLParser_S3::alloc_obj(const char *el)
{
  XMLObj *obj = nullptr;
  if (strcmp(el, "BucketLoggingStatus") == 0) {
    obj = new RGWBucketLoggingStatus_S3(cct);
  } else if (strcmp(el, "LoggingEnabled") == 0) {
    obj = new BLLoggingEnabled_S3(cct);
  } else if (strcmp(el, "TargetBucket") == 0) {
    obj = new BLTargetBucket_S3();
  } else if (strcmp(el, "TargetPrefix") == 0) {
    obj = new BLTargetPrefix_S3();
  }
  return obj;
}
