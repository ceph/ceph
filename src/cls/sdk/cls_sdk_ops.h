/*
* Ceph - scalable distributed file system
 *
 * Copyright (C) The Ceph Foundation and contributors
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include "include/rados/cls_traits.h"

namespace cls::sdk {
struct ClassId {
  static constexpr auto name = "sdk";
};
namespace method {
constexpr auto test_coverage_write = ClsMethod<RdWrTag, ClassId>("test_coverage_write");
constexpr auto test_coverage_replay = ClsMethod<RdWrTag, ClassId>("test_coverage_replay");
}
}