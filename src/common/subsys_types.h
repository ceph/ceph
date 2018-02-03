// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_SUBSYS_TYPES_H
#define CEPH_SUBSYS_TYPES_H

enum ceph_subsys_id_t {
  ceph_subsys_,   // default
#define SUBSYS(name, log, gather) \
  ceph_subsys_##name,
#define DEFAULT_SUBSYS(log, gather)
#include "common/subsys.h"
#undef SUBSYS
#undef DEFAULT_SUBSYS
  ceph_subsys_max
};

constexpr static std::size_t ceph_subsys_get_num() {
  return static_cast<std::size_t>(ceph_subsys_max);
}

#endif // CEPH_SUBSYS_TYPES_H

