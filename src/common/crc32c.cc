// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/crc32c.h"

#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/arm.h"
#include "arch/ppc.h"
#include "common/sctp_crc32.h"
#include "common/crc32c_intel_baseline.h"
#include "common/crc32c_intel_fast.h"
#include "common/crc32c_aarch64.h"
#include "common/crc32c_ppc.h"

/*
 * choose best implementation based on the CPU architecture.
 */
ceph_crc32c_func_t ceph_choose_crc32(void)
{
  // make sure we've probed cpu features; this might depend on the
  // link order of this file relative to arch/probe.cc.
  ceph_arch_probe();

  // if the CPU supports it, *and* the fast version is compiled in,
  // use that.
#if defined(__i386__) || defined(__x86_64__)
  if (ceph_arch_intel_sse42 && ceph_crc32c_intel_fast_exists()) {
    return ceph_crc32c_intel_fast;
  }
#elif defined(__arm__) || defined(__aarch64__)
  if (ceph_arch_aarch64_crc32){
    return ceph_crc32c_aarch64;
  }
#elif defined(__powerpc__) || defined(__ppc__)
  if (ceph_arch_ppc_crc32) {
    return ceph_crc32c_ppc;
  }
#endif
  // default
  return ceph_crc32c_sctp;
}

/*
 * static global
 *
 * This is a bit of a no-no for shared libraries, but we don't care.
 * It is effectively constant for the executing process as the value
 * depends on the CPU architecture.
 *
 * We initialize it during program init using the magic of C++.
 */
ceph_crc32c_func_t ceph_crc32c_func = ceph_choose_crc32();

struct crc_turbo_struct {
  uint32_t val[32][32];
};

/*
 * Look: http://crcutil.googlecode.com/files/crc-doc.1.0.pdf
 * Here is implementation that goes 1 logical step further,
 * it splits calculating CRC into jumps of length 1, 2, 4, 8, ....
 * Each jump is performed on single input bit separately, xor-ed after that.
 */
crc_turbo_struct create_turbo_table()
{
  crc_turbo_struct table;
  for (int bit = 0 ; bit < 32 ; bit++) {
    table.val[0][bit] = ceph_crc32c_sctp(1UL << bit, nullptr, 1);
  }
  for (int range = 1; range <32 ; range++) {
    for (int bit = 0 ; bit < 32 ; bit++) {
      uint32_t crc_x = table.val[range-1][bit];
      uint32_t crc_y = 0;
      for (int b = 0 ; b < 32 ; b++) {
        if ( (crc_x & (1UL << b)) != 0 ) {
          crc_y = crc_y ^ table.val[range-1][b];
        }
      }
      table.val[range][bit] = crc_y;
    }
  }
  return table;
}

static crc_turbo_struct crc_turbo_table = create_turbo_table();

uint32_t ceph_crc32c_zeros(uint32_t crc, unsigned len)
{
  int range = 0;
  unsigned remainder = len & 15;
  len = len >> 4;
  range = 4;
  while (len != 0) {
    uint32_t crc1 = 0;
    if ((len & 1) == 1) {
      uint32_t* ptr = crc_turbo_table.val[range];
      while (crc != 0) {
        uint32_t mask = ~((crc & 1) - 1);
        crc1 = crc1 ^ (mask & *ptr);
        crc = crc >> 1;
        ptr++;
      }
      crc = crc1;
    }
    len = len >> 1;
    range++;
  }
  if (remainder > 0)
    crc = ceph_crc32c(crc, nullptr, remainder);
  return crc;
}
