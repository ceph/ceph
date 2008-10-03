#ifndef __CEPH_CRC32C
#define __CEPH_CRC32C

__u32 crc32c_le(__u32 crc, unsigned char const *data, unsigned length);

#endif
