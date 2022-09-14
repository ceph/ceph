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

#include "include/cephfs/metrics/Types.h"

class feature_bitset_t;
namespace ceph {
  class Formatter;
}

// When adding a new release, please update the "current" release below, add a
// feature bit for that release, add that feature bit to CEPHFS_FEATURES_ALL,
// and update Server::update_required_client_features(). This feature bit
// is used to indicate that operator only wants clients from that release or
// later to mount CephFS.
#define CEPHFS_CURRENT_RELEASE  CEPH_RELEASE_REEF

// The first 5 bits are reserved for old ceph releases.
#define CEPHFS_FEATURE_JEWEL                5
#define CEPHFS_FEATURE_KRAKEN               6
#define CEPHFS_FEATURE_LUMINOUS             7
#define CEPHFS_FEATURE_MIMIC                8
#define CEPHFS_FEATURE_REPLY_ENCODING       9
#define CEPHFS_FEATURE_RECLAIM_CLIENT       10
#define CEPHFS_FEATURE_LAZY_CAP_WANTED      11
#define CEPHFS_FEATURE_MULTI_RECONNECT      12
#define CEPHFS_FEATURE_NAUTILUS             12
#define CEPHFS_FEATURE_DELEG_INO            13
#define CEPHFS_FEATURE_OCTOPUS              13
#define CEPHFS_FEATURE_METRIC_COLLECT       14
#define CEPHFS_FEATURE_ALTERNATE_NAME       15
#define CEPHFS_FEATURE_NOTIFY_SESSION_STATE 16
#define CEPHFS_FEATURE_OP_GETVXATTR         17
#define CEPHFS_FEATURE_32BITS_RETRY_FWD     18
#define CEPHFS_FEATURE_NEW_SNAPREALM_INFO   19
#define CEPHFS_FEATURE_HAS_OWNER_UIDGID     20
#define CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK  21
#define CEPHFS_FEATURE_MAX                  21

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
  CEPHFS_FEATURE_ALTERNATE_NAME,        \
  CEPHFS_FEATURE_NOTIFY_SESSION_STATE,  \
  CEPHFS_FEATURE_OP_GETVXATTR,          \
  CEPHFS_FEATURE_32BITS_RETRY_FWD,      \
  CEPHFS_FEATURE_NEW_SNAPREALM_INFO,    \
  CEPHFS_FEATURE_HAS_OWNER_UIDGID,      \
  CEPHFS_FEATURE_MDS_AUTH_CAPS_CHECK    \
}

#define CEPHFS_METRIC_FEATURES_ALL {		\
    CLIENT_METRIC_TYPE_CAP_INFO,		\
    CLIENT_METRIC_TYPE_READ_LATENCY,		\
    CLIENT_METRIC_TYPE_WRITE_LATENCY,		\
    CLIENT_METRIC_TYPE_METADATA_LATENCY,	\
    CLIENT_METRIC_TYPE_DENTRY_LEASE,		\
    CLIENT_METRIC_TYPE_OPENED_FILES,		\
    CLIENT_METRIC_TYPE_PINNED_ICAPS,		\
    CLIENT_METRIC_TYPE_OPENED_INODES,		\
    CLIENT_METRIC_TYPE_READ_IO_SIZES,		\
    CLIENT_METRIC_TYPE_WRITE_IO_SIZES,		\
    CLIENT_METRIC_TYPE_AVG_READ_LATENCY,	\
    CLIENT_METRIC_TYPE_STDEV_READ_LATENCY,	\
    CLIENT_METRIC_TYPE_AVG_WRITE_LATENCY,	\
    CLIENT_METRIC_TYPE_STDEV_WRITE_LATENCY,	\
    CLIENT_METRIC_TYPE_AVG_METADATA_LATENCY,	\
    CLIENT_METRIC_TYPE_STDEV_METADATA_LATENCY,	\
}

#define CEPHFS_FEATURES_MDS_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_CLIENT_SUPPORTED CEPHFS_FEATURES_ALL

extern std::string_view cephfs_feature_name(size_t id);
extern int cephfs_feature_from_name(std::string_view name);
std::string cephfs_stringify_features(const feature_bitset_t& features);
void cephfs_dump_features(ceph::Formatter *f, const feature_bitset_t& features);

#endif
