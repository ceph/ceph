// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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
#include <string>
#include <sys/stat.h>
#include "common/ceph_time.h"

namespace rgw { namespace sal {

static const std::string RGW_ATTR_PFX = "user.rgw.";
static const std::string ATTR_PREFIX = "user.X-RGW-";

static const std::string mp_ns = "multipart";
static const std::string MP_OBJ_PART_PFX = "part-";
static const std::string MP_OBJ_HEAD_NAME = MP_OBJ_PART_PFX + "00000";

static const std::string NULL_VERSION_ID = "null";
static const std::string HIDDEN_VERSIONS_PATH = ".versions";

const int64_t READ_SIZE = 128 * 1024;
// required alignment for O_DIRECT reads/writes (rgw_nsfs_direct_io)
const int64_t DIRECT_IO_ALIGN = 4096;

static inline ceph::real_time from_statx_timestamp(const struct statx_timestamp& xts)
{
  struct timespec ts{xts.tv_sec, xts.tv_nsec};
  return ceph::real_clock::from_timespec(ts);
}

} } // namespace rgw::sal
