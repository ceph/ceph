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

#define CEPHFS_FEATURE_MIMIC 	0

#define CEPHFS_FEATURES_ALL {		\
  CEPHFS_FEATURE_MIMIC,			\
}

#define CEPHFS_FEATURES_MDS_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_MDS_REQUIRED {}

#define CEPHFS_FEATURES_CLIENT_SUPPORTED CEPHFS_FEATURES_ALL
#define CEPHFS_FEATURES_CLIENT_REQUIRED {}

#endif
