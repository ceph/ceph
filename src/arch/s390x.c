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

#include <sys/auxv.h>
 
#include "arch/s390x.h"
#include "arch/probe.h"

/* flags we export */
int ceph_arch_s390x_crc32 = 0;

/* Supported starting from the IBM z13 */
int ceph_arch_s390x_probe(void)
{
  ceph_arch_s390x_crc32 = 0;

  if (getauxval(AT_HWCAP) & HWCAP_S390_VX) {
    ceph_arch_s390x_crc32 = 1;
  }

  return 0;
}
