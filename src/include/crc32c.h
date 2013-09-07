#ifndef CEPH_CRC32C_H
#define CEPH_CRC32C_H

#include "include/int_types.h"

#include <string.h>

typedef uint32_t (*ceph_crc32c_func_t)(uint32_t crc, unsigned char const *data, unsigned length);

/*
 * this is a static global with the chosen crc32c implementation for
 * the given architecture.
 */
extern ceph_crc32c_func_t ceph_crc32c_func;

extern ceph_crc32c_func_t ceph_choose_crc32(void);

/*
 * common entry point; use this!
 */
static inline uint32_t ceph_crc32c(uint32_t crc, unsigned char const *data, unsigned length)
{
	return ceph_crc32c_func(crc, data, length);
}

#endif
