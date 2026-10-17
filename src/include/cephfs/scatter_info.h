// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */
#ifndef CEPH_CEPHFS_SCATTER_INFO_H
#define CEPH_CEPHFS_SCATTER_INFO_H

#include "include/types.h" // for version_t

struct scatter_info_t {
  version_t version = 0;
};

#endif
