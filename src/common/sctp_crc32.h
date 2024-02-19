#ifndef CEPH_COMMON_SCTP_CRC32_H
#define CEPH_COMMON_SCTP_CRC32_H

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t ceph_crc32c_sctp(uint32_t crc, unsigned char const *data, unsigned length);

#ifdef __cplusplus
}
#endif

#endif
