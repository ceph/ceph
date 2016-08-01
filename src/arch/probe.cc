// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "arch/probe.h"

#include "arch/intel.h"
#include "arch/arm.h"

int ceph_arch_probe(void)
{
  if (ceph_arch_probed)
    return 1;

  ceph_arch_intel_probe();
  ceph_arch_arm_probe();

  ceph_arch_probed = 1;
  return 1;
}

// do this once using the magic of c++.
int ceph_arch_probed = ceph_arch_probe();
