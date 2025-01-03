#include "acconfig.h"
#include "common/crc32c_intel_baseline.h"

extern unsigned int crc32_iscsi_00(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_00");
extern unsigned int crc32_iscsi_01(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_01");
extern unsigned int crc32_iscsi_zero_00(unsigned char const *buffer, uint64_t len, uint64_t crc) asm("crc32_iscsi_zero_00");

#ifdef HAVE_NASM_X64

uint32_t ceph_crc32c_intel_fast_pclmul(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	if (!buffer)
	{
	  return crc32_iscsi_zero_00(buffer, len, crc);
	}

	/* Unlike crc32_iscsi_00, crc32_iscsi_01 handles the case where the
	 * input buffer is less than 8 bytes in its prelude, and does not
	 * prefetch beyond said buffer.
	 */
	return crc32_iscsi_01(buffer, len, crc);
}

uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	uint32_t v;
	unsigned left;

	if (!buffer)
	{
	  return crc32_iscsi_zero_00(buffer, len, crc);
	}

	/*
	 * the crc32_iscsi_00 method reads past buffer+len (because it
	 * reads full words) which makes valgrind unhappy.  don't do
	 * that.
	 */
	if (len < 16)
		return ceph_crc32c_intel_baseline(crc, buffer, len);
	left = ((unsigned long)buffer + len) & 7;
	len -= left;
	v = crc32_iscsi_00(buffer, len, crc);
	if (left)
		v = ceph_crc32c_intel_baseline(v, buffer + len, left);
	return v;
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

uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return 0;
}

#endif
