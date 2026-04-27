/*
 * Copyright 2025 sanechips Corporation
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 */

#ifndef CEPH_ARCH_RISCV_H
#define CEPH_ARCH_RISCV_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_riscv_zbc;
extern int ceph_arch_riscv_zvbc;

extern void ceph_arch_riscv_probe(void);

#ifdef __cplusplus
}
#endif

#endif

