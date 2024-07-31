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
