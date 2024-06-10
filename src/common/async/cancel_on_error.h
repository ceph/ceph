// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <cstdint>

namespace ceph::async {

/// Error handling strategy for concurrent operations.
enum class cancel_on_error : uint8_t {
  none, //< No spawned coroutines are canceled on failure.
  after, //< Cancel coroutines spawned after the failed coroutine.
  all, //< Cancel all spawned coroutines on failure.
};

} // namespace ceph::async
