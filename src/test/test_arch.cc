// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <stdio.h>

#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/arm.h"
#include "arch/ppc.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

#if (__powerpc64__)
#include <sys/auxv.h>
#include <asm/cputable.h>
#endif

#define FLAGS_SIZE 4096

TEST(Arch, all)
{
  ceph_arch_probe();
  EXPECT_TRUE(ceph_arch_probed);
  
#if (__arm__ || __aarch64__ || __x86_64__ || __powerpc64__) && __linux__
  char flags[FLAGS_SIZE];
  FILE *f = popen("grep '^\\(flags\\|Features\\)[	 ]*:' "
                  "/proc/cpuinfo | head -1", "r");
  if(f == NULL || fgets(flags, FLAGS_SIZE - 1, f) == NULL) {
    // silently do nothing if /proc/cpuinfo does exist, is not
    // readable or does not contain the expected information
    if (f)
      pclose(f);
    return;
  }
  pclose(f);
  flags[strlen(flags) - 1] = ' ';

  int expected;

#if (__arm__ || __aarch64__)

  expected = (strstr(flags, " neon ") || strstr(flags, " asimd ")) ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_neon);

#endif
#if (__aarch64__)

  expected = strstr(flags, " crc32 ") ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_aarch64_crc32);

#endif
#if (__x86_64__)

  expected = strstr(flags, " pclmulqdq ") ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_intel_pclmul);

  expected = strstr(flags, " sse4_2 ") ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_intel_sse42);

  expected = strstr(flags, " sse4_1 ") ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_intel_sse41);

  expected = (strstr(flags, " sse3 ") || strstr(flags, " ssse3 ") || strstr(flags, " pni ")) ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_intel_sse3);

  expected = strstr(flags, " ssse3 ") ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_intel_ssse3);

  expected = strstr(flags, " sse2 ") ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_intel_sse2);

#endif
#if (__powerpc64__)

  // /proc/cpuinfo is unreliable for determining crypto features on PowerPC.
  // altivec support does NOT mean POWER8 crypto support is present.
  // We check the hardware capability directly.
  unsigned long hwcap2 = getauxval(AT_HWCAP2);
  expected = (hwcap2 & PPC_FEATURE2_VEC_CRYPTO) ? 1 : 0;
  EXPECT_EQ(expected, ceph_arch_ppc_crc32);

#endif

#endif
}


/*
 * Local Variables:
 * compile-command: "cd .. ; make -j4 &&
 *   make unittest_arch &&
 *   valgrind --tool=memcheck ./unittest_arch --gtest_filter=*.*"
 * End:
 */
