#ifndef CEPH_COMMON_CRC32C_NEON_H
#define CEPH_COMMON_CRC32C_NEON_H

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t ceph_crc32c_neon(uint32_t crc, unsigned char const *buffer, unsigned len);

#ifdef __cplusplus
}
#endif

#endif
