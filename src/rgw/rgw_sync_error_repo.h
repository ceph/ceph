// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <optional>
#include "include/rados/librados_fwd.hpp"
#include "include/buffer_fwd.h"
#include "common/ceph_time.h"

class RGWSI_RADOS;
class RGWCoroutine;
struct rgw_raw_obj;
struct rgw_bucket_shard;

namespace rgw::error_repo {

// binary-encode a bucket/shard/gen and return it as a string
std::string encode_key(const rgw_bucket_shard& bs,
                       std::optional<uint64_t> gen);

// try to decode a key. returns -EINVAL if not in binary format
int decode_key(std::string encoded,
               rgw_bucket_shard& bs,
               std::optional<uint64_t>& gen);

// decode a timestamp as a uint64_t for CMPXATTR_MODE_U64
ceph::real_time decode_value(const ceph::bufferlist& bl);

// write an omap key iff the given timestamp is newer
int write(librados::ObjectWriteOperation& op,
          const std::string& key,
          ceph::real_time timestamp);
RGWCoroutine* write_cr(RGWSI_RADOS* rados,
                       const rgw_raw_obj& obj,
                       const std::string& key,
                       ceph::real_time timestamp);

// remove an omap key iff there isn't a newer timestamp
int remove(librados::ObjectWriteOperation& op,
           const std::string& key,
           ceph::real_time timestamp);
RGWCoroutine* remove_cr(RGWSI_RADOS* rados,
                        const rgw_raw_obj& obj,
                        const std::string& key,
                        ceph::real_time timestamp);

} // namespace rgw::error_repo
