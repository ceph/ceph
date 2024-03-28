#ifndef CEPH_ENDIAN_COMPAT_H
#define CEPH_ENDIAN_COMPAT_H

#if defined(__APPLE__)

#include <machine/endian.h>

#if __DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN

#define htobe16(s)    htons ((__uint16_t)(s))
#define htole16(s)          ((__uint16_t)(s))
#define be16toh(s)    ntohs ((__uint16_t)(s))
#define le16toh(s)          ((__uint16_t)(s))
#define htobe32(l)    htonl ((__uint32_t)(l))
#define htole32(l)          ((__uint32_t)(l))
#define be32toh(l)    ntohl ((__uint32_t)(l))
#define le32toh(l)          ((__uint32_t)(l))
#define htobe64(ll)   htonll((__uint64_t)(ll))
#define htole64(ll)         ((__uint64_t)(ll))
#define be64toh(ll)   ntohll((__uint64_t)(ll))
#define le64toh(ll)         ((__uint64_t)(ll))

#endif

#else
#include <endian.h>
#endif

#endif // CEPH_ENDIAN_COMPAT_H
