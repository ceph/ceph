/* Copyright (C) 2025 sanechips Technologies Co., Ltd.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "acconfig.h"
#include "include/int_types.h"

// CRC32C  polynomial constants
#define CRC32C_CONST_0     0xdd45aab8U
#define CRC32C_CONST_1     0x493c7d27U
#define CRC32C_CONST_QUO   0x0dea713f1ULL
#define CRC32C_CONST_POLY  0x105ec76f1ULL

// Folding constants for CRC32C
static const uint64_t crc32c_fold_const[4] __attribute__((aligned(16))) = {
    0x00000000740eef02ULL, 0x000000009e4addf8ULL,
    0x00000000f20c0dfeULL, 0x00000000493c7d27ULL
};

/**
 * Hardware-accelerated CRC32C using RISC-V vector crypto extensions.
 * This uses the reflected polynomial version compatible with standard CRC32C.
 */
uint32_t ceph_crc32c_riscv(uint32_t crc, unsigned char const *buf, unsigned len) {
    if (unlikely(len < 64)) {
        // Fall back to table-based implementation for small buffers
        return ceph_crc32c_sctp(crc, buf, len);
    }

    uint32_t result;
    const uint64_t *fold_consts = crc32c_fold_const;

    __asm__ __volatile__(
        // Initialize CRC
        "li             t5, 0xffffffff\n\t"
        "and            %[crc], %[crc], t5\n\t"
        "li             a3, 0\n\t"
        "li             t1, 64\n\t"

        // Set vector configuration for 128-bit elements
        "vsetivli       zero, 2, e64, m1, ta, ma\n\t"

        // Load first 64 bytes and initialize
        "mv             a4, %[buf]\n\t"
        "vle64.v        v0, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v1, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v2, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v3, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "andi           a3, %[len], ~63\n\t"
        "addi           t0, a3, -64\n\t"

        // XOR initial CRC into first vector
        "vmv.s.x        v4, zero\n\t"
        "vmv.s.x        v5, %[crc]\n\t"
        "vslideup.vi    v5, v4, 1\n\t"
        "vxor.vv        v0, v0, v5\n\t"
        "vmv.s.x        v8, zero\n\t"

        // Load folding constant
        "add            a5, a4, t0\n\t"
        "mv             t4, %[consts]\n\t"
        "vle64.v        v5, 0(t4)\n\t"

        // Check if we need main loop
        "addi           t0, %[len], -64\n\t"
        "bltu           t0, t1, 2f\n\t"

        // Main loop - process 64 bytes at a time
        "1:\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v0, v5\n\t"
        "vclmulh.vv     v0, v0, v5\n\t"
        "vredxor.vs     v0, v0, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v0, 1\n\t"
        "vxor.vv        v0, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v1, v5\n\t"
        "vclmulh.vv     v1, v1, v5\n\t"
        "vredxor.vs     v1, v1, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v1, 1\n\t"
        "vxor.vv        v1, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v2, v5\n\t"
        "vclmulh.vv     v2, v2, v5\n\t"
        "vredxor.vs     v2, v2, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v2, 1\n\t"
        "vxor.vv        v2, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v3, v5\n\t"
        "vclmulh.vv     v3, v3, v5\n\t"
        "vredxor.vs     v3, v3, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v3, 1\n\t"
        "vxor.vv        v3, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "bne            a4, a5, 1b\n\t"

        // Fold 512 bits to 128 bits
        "2:\n\t"
        "addi           t4, t4, 16\n\t"
        "vle64.v        v5, 0(t4)\n\t"
        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v1\n\t"

        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v2\n\t"

        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v3\n\t"

        // Extract 128-bit result from vector register
        "addi           sp, sp, -16\n\t"
        "vse64.v        v0, (sp)\n\t"
        "ld             t0, 0(sp)\n\t"
        "ld             t1, 8(sp)\n\t"
        "addi           sp, sp, 16\n\t"

        // Barrett reduction
        "li             t2, %[const0]\n\t"
        "and            t2, t2, t5\n\t"
        "li             t3, %[const1]\n\t"

        "clmul          t4, t0, t3\n\t"
        "clmulh         t3, t0, t3\n\t"
        "xor            t1, t1, t4\n\t"
        "and            t4, t1, t5\n\t"
        "srli           t1, t1, 32\n\t"
        "clmul          t0, t4, t2\n\t"
        "slli           t3, t3, 32\n\t"
        "xor            t3, t3, t1\n\t"
        "xor            t3, t3, t0\n\t"

        // Final Barrett reduction
        "and            t4, t3, t5\n\t"
        "li             t2, %[quo]\n\t"
        "li             t1, %[poly]\n\t"
        "clmul          t4, t4, t2\n\t"
        "and            t4, t4, t5\n\t"
        "clmul          t4, t4, t1\n\t"
        "xor            t4, t3, t4\n\t"
        "srai           %[result], t4, 32\n\t"
        "and            %[result], %[result], t5\n\t"

        : [result] "=r" (result)
        : [buf] "r" (buf), [len] "r" (len), [crc] "r" (crc), [consts] "r" (fold_consts),
          [const0] "i" (CRC32C_CONST_0), [const1] "i" (CRC32C_CONST_1),
          [quo] "i" (CRC32C_CONST_QUO), [poly] "i" (CRC32C_CONST_POLY)
        : "a3", "a4", "a5", "t0", "t1", "t2", "t3", "t4", "t5", "v0", "v1", "v2", "v3",
          "v4", "v5", "v6", "v7", "v8", "memory"
    );
    size_t tail_len = len % 64;
    if (tail_len > 0){
        result = ceph_crc32c_sctp(result, buf + len - tail_len, tail_len);
    }
    return result;
}
