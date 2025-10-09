#ifndef CEPH_CRC32C_H
#define CEPH_CRC32C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t (*ceph_crc32c_func_t)(uint32_t crc, unsigned char const *data, unsigned length);

/*
 * this is a static global with the chosen crc32c implementation for
 * the given architecture.
 */
extern ceph_crc32c_func_t ceph_crc32c_func;

extern ceph_crc32c_func_t ceph_choose_crc32(void);

/**
 * calculate crc32c for data that is entirely 0 (ZERO)
 *
 * Note: works the same as ceph_crc32c_func for data == nullptr, 
 * but faster than the optimized assembly on certain architectures.
 * This is faster than intel optimized assembly, but not as fast as 
 * ppc64le optimized assembly.  
 *
 * @param crc initial value
 * @param length length of buffer
 */
uint32_t ceph_crc32c_zeros(uint32_t crc, unsigned length);

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
#ifndef HAVE_POWER8
  if (!data && length > 16)
    return ceph_crc32c_zeros(crc, length);
#endif /* HAVE_POWER8 */

  return ceph_crc32c_func(crc, data, length);
}

#ifdef __cplusplus
}
#endif

#endif
