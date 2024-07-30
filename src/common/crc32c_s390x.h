#ifndef CEPH_COMMON_CRC32C_S390X_H
#define CEPH_COMMON_CRC32C_S390X_H

#ifdef __cplusplus
extern "C" {
#endif
#include <sys/types.h>
#include <stdint.h>

/* Portable implementations of CRC-32 (IEEE and Castagnoli) little-endian variant */
unsigned int crc32c_le(uint32_t, unsigned char const*, unsigned);

/* Hardware-accelerated version of the above */
unsigned int ceph_crc32c_s390x(uint32_t, unsigned char const*, unsigned);

#ifdef __cplusplus
}
#endif

#endif
