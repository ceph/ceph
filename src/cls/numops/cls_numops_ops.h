/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CERN
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */
#pragma once

#include "include/rados/cls_traits.h"

namespace rados::cls::numops {
struct ClassId {
  static constexpr auto name = "numops";
};
namespace method {
constexpr auto add = ClsMethod<RdWrTag, ClassId>("add");
constexpr auto mul = ClsMethod<RdWrTag, ClassId>("mul");
}
}