#include "acconfig.h"
#include "include/int_types.h"

extern unsigned int crc32_iscsi_01(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_01");
extern unsigned int crc32_iscsi_by16_10(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_by16_10");
extern unsigned int crc32_iscsi_zero_00(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_zero_00");

#ifdef HAVE_NASM_X64_AVX512_VPCLMUL

uint32_t ceph_crc32c_intel_fast_avx512_vpclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	if (!buffer)
	{
		return crc32_iscsi_zero_00(buffer, len, crc);
	}
	return crc32_iscsi_by16_10(buffer, len, crc);
}

int ceph_crc32c_intel_fast_avx512_vpclmul_exists(void)
{
	return 1;
}

#else

uint32_t ceph_crc32c_intel_fast_avx512_vpclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return 0;
}

int ceph_crc32c_intel_fast_avx512_vpclmul_exists(void)
{
	return 0;
}

#endif

#ifdef HAVE_NASM_X64

uint32_t ceph_crc32c_intel_fast_pclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	if (!buffer)
	{
	  return crc32_iscsi_zero_00(buffer, len, crc);
	}

	return crc32_iscsi_01(buffer, len, crc);
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

uint32_t ceph_crc32c_intel_fast_pclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return 0;
}

#endif
