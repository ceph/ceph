#ifndef CEPH_COMMON_CRC32C_INTEL_FAST_H
#define CEPH_COMMON_CRC32C_INTEL_FAST_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __LP64__

extern uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len);

#else

static inline uint32_t ceph_crc32c_intel_fast(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	return 0;
}

#endif

#ifdef __cplusplus
}
#endif

#endif
