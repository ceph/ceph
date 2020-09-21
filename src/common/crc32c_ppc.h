/* Copyright (C) 2017 International Business Machines Corp.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */
#ifndef CEPH_COMMON_CRC32C_PPC_H
#define CEPH_COMMON_CRC32C_PPC_H

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t ceph_crc32c_ppc(uint32_t crc, unsigned char const *buffer, unsigned len);

#ifdef __cplusplus
}
#endif

#endif
