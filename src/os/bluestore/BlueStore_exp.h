// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_OSD_BLUESTORE_EXP_H
#define CEPH_OSD_BLUESTORE_EXP_H

#define WITH_EXPERIMENTAL
#undef CEPH_OSD_BLUESTORE_H
#include "BlueStore.h"
// let others include regular bluestore too. Double inclusion
// of bluestore-rdr is guarded by CEPH_OSD_BLUESTORE_EXP_H.
#undef CEPH_OSD_BLUESTORE_H

#endif // CEPH_OSD_BLUESTORE_EXP_H
