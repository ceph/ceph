#include <inttypes.h>
#include "acconfig.h"

extern unsigned int crc32_iscsi_00(unsigned char const *buffer, int len, unsigned int crc);

#ifdef WITH_GOOD_YASM_ELF64

uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return crc32_iscsi_00(buffer, len, crc);
}

int ceph_crc32c_intel_fast_exists(void)
{
	return 1;
}

#else

int ceph_crc32c_intel_fast_exists(void)
{
	return 0;
}

uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return 0;
}

#endif
