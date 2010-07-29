#ifndef CEPH_CRC32C_H
#define CEPH_CRC32C_H

#ifdef __cplusplus
extern "C" {
#endif

uint32_t crc32c_le(uint32_t crc, unsigned char const *data, unsigned length);

#ifdef __cplusplus
}
#endif

#endif
