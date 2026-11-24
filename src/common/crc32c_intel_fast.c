#include "acconfig.h"
#include "include/int_types.h"

extern unsigned int crc32_iscsi_01(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_01");
extern unsigned int crc32_iscsi_avx2(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_avx2");
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

/*
 * VPCLMULQDQ folding in 256-bit registers (ISA-L's crc32_iscsi_avx2), for
 * CPUs with AVX2, GFNI, VAES and VPCLMULQDQ but not the AVX512 variant's
 * features. Faster than crc32_iscsi_01 (the crc32 instruction, three
 * streams) on large buffers: 25 vs 16 GB/s for 1 MiB on a Xeon 6740E.
 * Built under the same condition as the AVX512 variant: both need a NASM
 * that knows VPCLMULQDQ.
 */
uint32_t ceph_crc32c_intel_fast_avx2_vpclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	if (!buffer)
	{
		return crc32_iscsi_zero_00(buffer, len, crc);
	}
	return crc32_iscsi_avx2(buffer, len, crc);
}

int ceph_crc32c_intel_fast_avx2_vpclmul_exists(void)
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

uint32_t ceph_crc32c_intel_fast_avx2_vpclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return 0;
}

int ceph_crc32c_intel_fast_avx2_vpclmul_exists(void)
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
