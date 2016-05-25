#include <sys/auxv.h>
#include "acconfig.h"
int ceph_arch_power_crc32 = 0;

int ceph_arch_power_probe(void)
{
#ifdef POWER_CRC32
    ceph_arch_power_crc32 = 1;
#endif
return 0;
}

