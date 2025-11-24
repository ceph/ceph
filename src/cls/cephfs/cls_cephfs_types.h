/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once
#include "include/rados/cls_traits.h"
namespace cls::cephfs {
struct ClassId {
  static constexpr auto name = "cephfs";
};
namespace method {
constexpr auto accumulate_inode_metadata = ClsMethod<RdWrTag, ClassId>("accumulate_inode_metadata");
}
}