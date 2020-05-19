// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "probe.h"
#include "intel.h"

int spec_arch_probe(void)
{
  if (spec_arch_probed)
    return 1;
#if defined(__i386__) || defined(__x86_64__)
  spec_arch_intel_probe();
#endif
  spec_arch_probed = 1;
  return 1;
}

// do this once using the magic of c++.
int spec_arch_probed = spec_arch_probe();
