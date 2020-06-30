#ifndef CEPH_COMMON_CRC32C_INTEL_BASELINE_H
#define CEPH_COMMON_CRC32C_INTEL_BASELINE_H

#include "include/int_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t ceph_crc32c_intel_baseline(uint32_t crc, unsigned char const *buffer, unsigned len);

#ifdef __cplusplus
}
#endif

#endif
