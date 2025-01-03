// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2024 IBM Corporation
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 */

#ifndef CEPH_ARCH_S390X_H
#define CEPH_ARCH_S390X_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_s390x_crc32;

extern int ceph_arch_s390x_probe(void);

#ifdef __cplusplus
}
#endif

#endif
