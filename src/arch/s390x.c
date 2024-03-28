#include "arch/s390x.h"
#include "arch/probe.h"

/* flags we export */
int ceph_arch_s390x_crc32 = 0;

#include <stdio.h>

int ceph_arch_s390x_probe(void)
{
  ceph_arch_s390x_crc32 = 1;

  return 0;
}
