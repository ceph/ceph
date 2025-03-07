/*
 * CRC-32 algorithm implemented with the z/Architecture Vector Extension
 * Facility.
 *
 * Copyright 2024 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#ifndef CEPH_COMMON_CRC32C_S390X_H
#define CEPH_COMMON_CRC32C_S390X_H

#ifdef __cplusplus
extern "C" {
#endif
#include <sys/types.h>
#include <stdint.h>

/* Portable implementations of CRC-32 (IEEE and Castagnoli) little-endian variant */
unsigned int crc32c_le(uint32_t, unsigned char const*, unsigned);

/* Hardware-accelerated version of the above */
unsigned int ceph_crc32c_s390x(uint32_t, unsigned char const*, unsigned);

#ifdef __cplusplus
}
#endif

#endif
