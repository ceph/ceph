/* Copyright (C) 2025 sanechips Technologies Co., Ltd.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */
#ifndef CEPH_COMMON_CRC32C_RISCV_H
#define CEPH_COMMON_CRC32C_RISCV_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__riscv) && defined(HAVE_RISCV_ZVBC)
extern uint32_t ceph_crc32c_riscv(uint32_t crc, unsigned char const *buffer, unsigned len);
#endif

#if defined(__riscv) && defined(HAVE_RISCV_ZBC)
extern uint32_t ceph_crc32c_riscv_zbc(uint32_t crc, unsigned char const *buffer, unsigned len);
#endif

#ifdef __cplusplus
}
#endif

#endif
