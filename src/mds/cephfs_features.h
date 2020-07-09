// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *

 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPHFS_FEATURES_H
#define CEPHFS_FEATURES_H

class feature_bitset_t;
namespace ceph {
  class Formatter;
}

// When adding a new release, please update the "current" release below, add a
// feature bit for that release, add that feature bit to CEPHFS_FEATURES_ALL,
// and update Server::update_required_client_features(). This feature bit
// is used to indicate that operator only wants clients from that release or
// later to mount CephFS.
#define CEPHFS_CURRENT_RELEASE  CEPH_RELEASE_PACIFIC

// The first 5 bits are reserved for old ceph releases.
#define CEPHFS_FEATURE_JEWEL		5
#define CEPHFS_FEATURE_KRAKEN		6
#define CEPHFS_FEATURE_LUMINOUS		7
#define CEPHFS_FEATURE_MIMIC		8
#define CEPHFS_FEATURE_REPLY_ENCODING   9
#define CEPHFS_FEATURE_RECLAIM_CLIENT	10
#define CEPHFS_FEATURE_LAZY_CAP_WANTED  11
#define CEPHFS_FEATURE_MULTI_RECONNECT  12
#define CEPHFS_FEATURE_NAUTILUS         12
#define CEPHFS_FEATURE_DELEG_INO        13
#define CEPHFS_FEATURE_OCTOPUS          13
#define CEPHFS_FEATURE_METRIC_COLLECT   14
#define CEPHFS_FEATURE_MAX		14

#define CEPHFS_FEATURES_ALL {		\
  0, 1, 2, 3, 4,			\
  CEPHFS_FEATURE_JEWEL,			\
  CEPHFS_FEATURE_KRAKEN,		\
  CEPHFS_FEATURE_LUMINOUS,		\
  CEPHFS_FEATURE_MIMIC,			\
  CEPHFS_FEATURE_REPLY_ENCODING,        \
  CEPHFS_FEATURE_RECLAIM_CLIENT,	\
  CEPHFS_FEATURE_LAZY_CAP_WANTED,	\
  CEPHFS_FEATURE_MULTI_RECONNECT,	\
  CEPHFS_FEATURE_NAUTILUS,              \
  CEPHFS_FEATURE_DELEG_INO,             \
  CEPHFS_FEATURE_OCTOPUS,               \
  CEPHFS_FEATURE_METRIC_COLLECT,        \
}

#define CEPHFS_FEATURES_MDS_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_MDS_REQUIRED {}

#define CEPHFS_FEATURES_CLIENT_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_CLIENT_REQUIRED {}

extern std::string_view cephfs_feature_name(size_t id);
extern int cephfs_feature_from_name(std::string_view name);
std::string cephfs_stringify_features(const feature_bitset_t& features);
void cephfs_dump_features(ceph::Formatter *f, const feature_bitset_t& features);

#endif
