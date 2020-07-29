// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_DETAIL_CONSTRUCT_SUSPENDED_H
#define CEPH_COMMON_DETAIL_CONSTRUCT_SUSPENDED_H

namespace ceph {
  struct construct_suspended_t { };
  inline constexpr construct_suspended_t construct_suspended { };
}

#endif // CEPH_COMMON_DETAIL_CONSTRUCT_SUSPENDED_H
