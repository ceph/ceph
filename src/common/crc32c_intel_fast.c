#include <inttypes.h>

extern unsigned int crc32_iscsi_00(unsigned char const *buffer, int len, unsigned int crc);

uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return crc32_iscsi_00(buffer, len, crc);
}
