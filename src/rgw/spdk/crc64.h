/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2023 Intel Corporation.
 *   All rights reserved.
 */

/**
 * \file
 * CRC-64 utility functions
 */

#ifndef SPDK_CRC64_H
#define SPDK_CRC64_H

#if 0
#include "spdk/stdinc.h"
#include "spdk/config.h"
#else
#include <stdint.h>
#include <sys/types.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Calculate a CRC-64 checksum (Rocksoft), for NVMe Protection Information
 *
 * \param buf Data buffer to checksum.
 * \param len Length of buf in bytes.
 * \param crc Previous CRC-64 value.
 * \return Updated CRC-64 value.
 */
uint64_t spdk_crc64_nvme(const void *buf, size_t len, uint64_t crc);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_CRC64_H */
