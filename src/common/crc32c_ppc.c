/* Copyright (C) 2017 International Business Machines Corp.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */
#define CRC_TABLE
#include "acconfig.h"
#include "include/int_types.h"
#include "crc32c_ppc_constants.h"

#include <stdlib.h>
#include <strings.h>

#define VMX_ALIGN	16
#define VMX_ALIGN_MASK	(VMX_ALIGN-1)

#ifdef REFLECT
static unsigned int crc32_align(unsigned int crc, unsigned char const *p,
                                unsigned long len)
{
  while (len--)
    crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
  return crc;
}
#else
static unsigned int crc32_align(unsigned int crc, unsigned char const *p,
                                unsigned long len)
{
  while (len--)
    crc = crc_table[((crc >> 24) ^ *p++) & 0xff] ^ (crc << 8);
  return crc;
}
#endif


#ifdef HAVE_POWER8
unsigned int __crc32_vpmsum(unsigned int crc, unsigned char const *p,
                            unsigned long len);

static uint32_t crc32_vpmsum(uint32_t crc, unsigned char const *data,
                             unsigned len)
{
  unsigned int prealign;
  unsigned int tail;

#ifdef CRC_XOR
  crc ^= 0xffffffff;
#endif

  if (len < VMX_ALIGN + VMX_ALIGN_MASK) {
    crc = crc32_align(crc, data, (unsigned long)len);
    goto out;
  }

  if ((unsigned long)data & VMX_ALIGN_MASK) {
    prealign = VMX_ALIGN - ((unsigned long)data & VMX_ALIGN_MASK);
    crc = crc32_align(crc, data, prealign);
    len -= prealign;
    data += prealign;
  }

  crc = __crc32_vpmsum(crc, data, (unsigned long)len & ~VMX_ALIGN_MASK);

  tail = len & VMX_ALIGN_MASK;
  if (tail) {
    data += len & ~VMX_ALIGN_MASK;
    crc = crc32_align(crc, data, tail);
  }

out:
#ifdef CRC_XOR
  crc ^= 0xffffffff;
#endif

  return crc;
}

/* This wrapper function works around the fact that crc32_vpmsum 
 * does not gracefully handle the case where the data pointer is NULL.  There
 * may be room for performance improvement here.
 */
uint32_t ceph_crc32c_ppc(uint32_t crc, unsigned char const *data, unsigned len)
{
  unsigned char *buf2;

  if (!data) {
    buf2 = malloc(len);
    bzero(buf2, len);
    crc = crc32_vpmsum(crc, buf2, len);
    free(buf2);
  } else {
    crc = crc32_vpmsum(crc, data, (unsigned long)len);
  }
  return crc;
}

#else /* HAVE_POWER8 */

/* This symbol has to exist on non-ppc architectures (and on legacy
 * ppc systems using power7 or below) in order to compile properly
 * there, even though it won't be called.
 */
uint32_t ceph_crc32c_ppc(uint32_t crc, unsigned char const *data, unsigned len)
{
  return 0;
}

#endif /* HAVE_POWER8 */
