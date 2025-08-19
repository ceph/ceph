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
#ifndef CEPH_CEPHFS_CLUSTER_ID_H
#define CEPH_CEPHFS_CLUSTER_ID_H

#include <cstdint>

typedef int32_t fs_cluster_id_t;
constexpr fs_cluster_id_t FS_CLUSTER_ID_NONE = -1;

// The namespace ID of the anonymous default filesystem from legacy systems
constexpr fs_cluster_id_t FS_CLUSTER_ID_ANONYMOUS = 0;

#endif
