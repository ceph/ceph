/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2023 Intel Corporation.
 *   All rights reserved.
 */

#ifndef SPDK_CRC_INTERNAL_H
#define SPDK_CRC_INTERNAL_H

#if 0
#include "spdk/config.h"
#endif

#ifdef SPDK_CONFIG_ISAL
#define SPDK_HAVE_ISAL
#include <isa-l/include/crc.h>
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#define SPDK_HAVE_ARM_CRC
#include <arm_acle.h>
#elif defined(__x86_64__) && defined(__SSE4_2__)
#define SPDK_HAVE_SSE4_2
#include <x86intrin.h>
#endif

#endif /* SPDK_CRC_INTERNAL_H */
