// -*- mode:C++; tab-width:8; c-basic-offset:2
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 * Copyright (C) 2025 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <optional>
#include "include/rados/librados_fwd.hpp"
#include "ops.h"
#include "include/utime.h"
#include "common/ceph_time.h"
#include "common/dout.h"

namespace cls::blake3_hash {
  [[nodiscard]] int blake3_hash_data(librados::ObjectReadOperation& op,
                                     bufferlist *blake3_state_bl,
                                     bufferlist *out,
                                     cls_blake3_flags_t flags);
} // namespace cls::blake3_hash
