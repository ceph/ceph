#ifndef CEPH_CRC32C_H
#define CEPH_CRC32C_H

#include <inttypes.h>
#include <string.h>

typedef uint32_t (*ceph_crc32c_func_t)(uint32_t crc, unsigned char const *data, unsigned length);

/*
 * this is a static global with the chosen crc32c implementation for
 * the given architecture.
 */
extern ceph_crc32c_func_t ceph_crc32c_func;

extern ceph_crc32c_func_t ceph_choose_crc32(void);

/**
 * calculate crc32c
 *
 * Note: if the data pointer is NULL, we calculate a crc value as if
 * it were zero-filled.
 *
 * @param crc initial value
 * @param data pointer to data buffer
 * @param length length of buffer
 */
static inline uint32_t ceph_crc32c(uint32_t crc, unsigned char const *data, unsigned length)
{
	return ceph_crc32c_func(crc, data, length);
}

#endif
