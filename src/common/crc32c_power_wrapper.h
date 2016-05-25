#ifndef CEPH_COMMON_CRC32C_POWER_WRAPPER_H
#define CEPH_COMMON_CRC32C_POWER_WRAPPER_H

#include "arch/power.h"
#ifdef __cplusplus
extern "C" {
#endif

#ifdef POWER_CRC32
extern uint32_t ceph_crc32c_power(uint32_t crc, unsigned char const *buffer, unsigned len);
#else
static inline uint32_t ceph_crc32c_power(uint32_t crc, unsigned char const *buffer, unsigned len)
{
        return 0;
}
#endif
#ifdef __cplusplus
}
#endif

#endif
