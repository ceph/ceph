#ifndef CEPH_CRC32C_H
#define CEPH_CRC32C_H

#ifdef __cplusplus
extern "C" {
#endif

#include <string.h>

extern int ceph_have_crc32c_intel(void);
extern uint32_t ceph_crc32c_le_generic(uint32_t crc, unsigned char const *data, unsigned length);
extern uint32_t ceph_crc32c_le_intel(uint32_t crc, unsigned char const *data, unsigned length);

static inline uint32_t ceph_crc32c_le(uint32_t crc, unsigned char const *data, unsigned length) {
	if (ceph_have_crc32c_intel()) //__builtin_cpu_supports("sse4.2"))
		return ceph_crc32c_le_intel(crc, data, length);
	else
		return ceph_crc32c_le_generic(crc, data, length);
}

#ifdef __cplusplus
}
#endif

#endif
