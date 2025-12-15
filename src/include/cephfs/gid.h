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
#ifndef CEPH_CEPHFS_GID_H
#define CEPH_CEPHFS_GID_H

#include <boost/serialization/strong_typedef.hpp>

#include <cstdint>

BOOST_STRONG_TYPEDEF(uint64_t, mds_gid_t)
extern const mds_gid_t MDS_GID_NONE;

template <>
struct std::hash<mds_gid_t> {
  size_t operator()(const mds_gid_t& gid) const
  {
    return hash<uint64_t> {}(gid);
  }
};

#endif
