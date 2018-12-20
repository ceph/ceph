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

// Please add feature bits for later ceph releases and update
// Server::update_required_client_features().

// The first 5 bits are reserved for old ceph releases.
#define CEPHFS_FEATURE_JEWEL		5
#define CEPHFS_FEATURE_KRAKEN		6
#define CEPHFS_FEATURE_LUMINOUS		7
#define CEPHFS_FEATURE_MIMIC		8
#define CEPHFS_FEATURE_REPLY_ENCODING   9
#define CEPHFS_FEATURE_RECLAIM_CLIENT	10
#define CEPHFS_FEATURE_LAZY_CAP_WANTED  11
#define CEPHFS_FEATURE_MULTI_RECONNECT  12

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
}

#define CEPHFS_FEATURES_MDS_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_MDS_REQUIRED {}

#define CEPHFS_FEATURES_CLIENT_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_CLIENT_REQUIRED {}

#endif
