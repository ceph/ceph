/* Copyright (C) 2026 ZTE Corporation
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */
#include "crc32c_riscv_zbc_asm.h"

/* uint32_t crc32c_zbc(unsigned char const *buf, unsigned len, uint32_t crc) */
.text
.option  arch, +zbc
.align 1
.global crc32c_zbc
.type crc32c_zbc, %function
crc32c_zbc:
        /* switch around arguments to match common crc functions */
        mv t0, a2
        mv a2, a1
        mv a1, a0
        mv a0, t0

        /* load precomputed constants */
        ld POLY, .Lpoly_refl
        ld MU, .Lmu

        /* zero-extend seed */
        slli SEED, SEED, 32
        srli SEED, SEED, 32

        /* align buffer to 128-bits, then fold */
        crc32_refl_align

        crc_fold_loop
        crc32_refl_fold_reduction

        /* handle any remaining excess */
        crc32_refl_excess

        /* sign-extend result */
        sext.w SEED, SEED
        ret

/* precomputed constants */
.Lpoly_refl:
        .dword 0x0000000105ec76f1
.Lmu:
        .dword 0x4869ec38dea713f1
.Lk1:
        .dword 0x00000000740eef02
.Lk2:
        .dword 0x000000009e4addf8
.Lk3:
        .dword 0x00000000f20c0dfe
.Lk4:
        .dword 0x000000014cd00bd6
.Lk5:
        .dword 0x00000000dd45aab8
