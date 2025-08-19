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
#ifndef CEPH_CEPHFS_RANK_H
#define CEPH_CEPHFS_RANK_H

#include <cstdint>

typedef int32_t mds_rank_t;
constexpr mds_rank_t MDS_RANK_NONE		= -1;
constexpr mds_rank_t MDS_RANK_EPHEMERAL_DIST	= -2;
constexpr mds_rank_t MDS_RANK_EPHEMERAL_RAND	= -3;

#endif
