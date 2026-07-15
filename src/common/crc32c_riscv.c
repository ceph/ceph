/* Copyright (C) 2025 sanechips Technologies Co., Ltd.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */
#include <stddef.h>
#include <stdint.h>
#include "common/sctp_crc32.h"
#include "common/likely.h"

#if defined(HAVE_RISCV_ZVBC)

// CRC32C  polynomial constants
#define CRC32C_CONST_0     0xdd45aab8U
#define CRC32C_CONST_1     0x493c7d27U
#define CRC32C_CONST_QUO   0x0dea713f1ULL
#define CRC32C_CONST_POLY  0x105ec76f1ULL

// Folding constants for CRC32C
static const uint64_t crc32c_fold_const[4] __attribute__((aligned(16))) = {
    0x000000006992cea2ULL, 0x000000000d3b6092ULL,
    0x00000000f20c0dfeULL, 0x00000000493c7d27ULL
};

/**
 * Hardware-accelerated CRC32C using RISC-V vector crypto extensions.
 * This uses the reflected polynomial version compatible with standard CRC32C.
 */
uint32_t ceph_crc32c_riscv(uint32_t crc, unsigned char const *buf, unsigned len) {
    if (unlikely(len < 128) || unlikely(!buf)) {
        // Fall back to table-based implementation for small buffers
        return ceph_crc32c_sctp(crc, buf, len);
    }

    const uint64_t *fold_consts = crc32c_fold_const;
    const uint8_t *buf_end_loop = buf + (len & ~127);
    uint64_t mask_data[2] = {1, 0};

    register uintptr_t r_buf    asm("a0") = (uintptr_t)buf;
    register uintptr_t r_crc    asm("a1") = (uintptr_t)crc;
    register uintptr_t r_len    asm("a2") = (uintptr_t)len;
    register uintptr_t r_end    asm("a6") = (uintptr_t)buf_end_loop;
    register uintptr_t r_consts asm("a5") = (uintptr_t)fold_consts;
    register uintptr_t r_mask   asm("t0") = (uintptr_t)mask_data;

    __asm__ __volatile__(
        ".option push\n\t"
        ".option arch, +v, +zvbc, +zbc, +zbb\n\t"

        "addi   sp, sp, -128\n\t"
        "sd     ra, 120(sp)\n\t"
        "sd     gp, 112(sp)\n\t"
        "sd     tp, 104(sp)\n\t"
        "sd     s0, 96(sp)\n\t"
        "sd     s1, 88(sp)\n\t"
        "sd     s2, 80(sp)\n\t"
        "sd     s3, 72(sp)\n\t"
        "sd     s4, 64(sp)\n\t"
        "sd     s5, 56(sp)\n\t"
        "sd     s6, 48(sp)\n\t"
        "sd     s7, 40(sp)\n\t"
        "sd     s8, 32(sp)\n\t"
        "sd     s9, 24(sp)\n\t"
        "sd     s10, 16(sp)\n\t"
        "sd     s11, 8(sp)\n\t"

        "sd     a5, 0(sp)\n\t"
        "slli   a1, a1, 32\n\t"
        "srli   a1, a1, 32\n\t"
        "li     a3, 0\n\t"
        "li     t1, 128\n\t"
        "vsetivli zero, 2, e64, m1, ta, ma\n\t"

        "vle64.v v0, (t0)\n\t"

        "addi   t0, a0, 64\n\t"
        "addi   t1, a0, 80\n\t"
        "addi   t2, a0, 96\n\t"
        "addi   t3, a0, 112\n\t"

        "ld     s0, 0(a0)\n\t"
        "ld     s1, 8(a0)\n\t"
        "ld     s2, 16(a0)\n\t"
        "ld     s3, 24(a0)\n\t"
        "ld     s4, 32(a0)\n\t"
        "ld     s5, 40(a0)\n\t"
        "ld     s6, 48(a0)\n\t"
        "ld     s7, 56(a0)\n\t"

        "vle64.v v1, (t0)\n\t"
        "vle64.v v2, (t1)\n\t"
        "vle64.v v3, (t2)\n\t"
        "vle64.v v4, (t3)\n\t"

        "addi   a0, a0, 128\n\t"
        "andi   a3, a2, ~127\n\t"
        "addi   t0, a3, -128\n\t"
        "sub    a2, a2, a3\n\t"
        "xor    s0, s0, a1\n\t"

        "ld     a3, 0(a5)\n\t"
        "ld     a4, 8(a5)\n\t"
        "vle64.v v5, 0(a5)\n\t"

        "beq    a0, a6, 2f\n\t"

        ".align 3\n\t"
        "1:\n\t"
        "ld     a5, 0(a0)\n\t"
        "clmulh t0, a3, s0\n\t"
        "clmulh t2, a3, s2\n\t"
        "ld     ra, 8(a0)\n\t"
        "clmulh t4, a3, s4\n\t"
        "clmulh t6, a3, s6\n\t"
        "ld     gp, 16(a0)\n\t"
        "clmul  t1, a4, s1\n\t"
        "clmul  t3, a4, s3\n\t"
        "ld     tp, 24(a0)\n\t"
        "clmul  t5, a4, s5\n\t"
        "clmul  a7, a4, s7\n\t"
        "ld     s8, 32(a0)\n\t"
        "clmul  s0, a3, s0\n\t"
        "clmul  s2, a3, s2\n\t"
        "ld     s9, 40(a0)\n\t"
        "clmul  s4, a3, s4\n\t"
        "clmul  s6, a3, s6\n\t"
        "ld     s10, 48(a0)\n\t"
        "clmulh s1, a4, s1\n\t"
        "clmulh s3, a4, s3\n\t"
        "ld     s11, 56(a0)\n\t"
        "clmulh s5, a4, s5\n\t"
        "clmulh s7, a4, s7\n\t"

        "addi   a0, a0, 64\n\t"
        "vle64.v v10, (a0)\n\t"
        "addi   a0, a0, 16\n\t"
        "vclmul.vv v6, v1, v5\n\t"
        "vclmulh.vv v1, v1, v5\n\t"
        "vle64.v v11, (a0)\n\t"
        "addi   a0, a0, 16\n\t"
        "vclmul.vv v7, v2, v5\n\t"
        "vclmulh.vv v2, v2, v5\n\t"
        "vle64.v v12, (a0)\n\t"
        "addi   a0, a0, 16\n\t"
        "vclmul.vv v8, v3, v5\n\t"
        "vclmulh.vv v3, v3, v5\n\t"
        "vle64.v v13, (a0)\n\t"
        "addi   a0, a0, 16\n\t"
        "vclmul.vv v9, v4, v5\n\t"
        "vclmulh.vv v4, v4, v5\n\t"

        "vmv.v.v v14, v6\n\t"
        "vmv.v.v v15, v7\n\t"
        "vmv.v.v v16, v8\n\t"
        "vmv.v.v v17, v9\n\t"

        "vslideup.vi v6, v1, 1\n\t"
        "vslideup.vi v7, v2, 1\n\t"
        "vslideup.vi v8, v3, 1\n\t"
        "vslideup.vi v9, v4, 1\n\t"

        "vsetivli zero, 2, e64, m1, ta, mu\n\t"
        "vslidedown.vi v1, v14, 1, v0.t\n\t"
        "vslidedown.vi v2, v15, 1, v0.t\n\t"
        "vslidedown.vi v3, v16, 1, v0.t\n\t"
        "vslidedown.vi v4, v17, 1, v0.t\n\t"
        "vsetivli zero, 2, e64, m1, ta, ma\n\t"

        "xor    t1, t1, s0\n\t"
        "xor    t3, t3, s2\n\t"
        "xor    t5, t5, s4\n\t"
        "xor    a7, a7, s6\n\t"
        "xor    t0, t0, s1\n\t"
        "xor    t2, t2, s3\n\t"
        "xor    t4, t4, s5\n\t"
        "xor    t6, t6, s7\n\t"

        "xor    s0, t1, a5\n\t"
        "xor    s2, t3, gp\n\t"
        "xor    s4, t5, s8\n\t"
        "xor    s6, a7, s10\n\t"
        "xor    s1, t0, ra\n\t"
        "xor    s3, t2, tp\n\t"
        "xor    s5, t4, s9\n\t"
        "xor    s7, t6, s11\n\t"

        "vxor.vv v1, v1, v6\n\t"
        "vxor.vv v2, v2, v7\n\t"
        "vxor.vv v3, v3, v8\n\t"
        "vxor.vv v4, v4, v9\n\t"
        "vxor.vv v1, v1, v10\n\t"
        "vxor.vv v2, v2, v11\n\t"
        "vxor.vv v3, v3, v12\n\t"
        "vxor.vv v4, v4, v13\n\t"

        "bne    a0, a6, 1b\n\t"

        // Folding 1024b -> 128b
        "2:\n\t"
        "ld     a5, 0(sp)\n\t"
        "ld     a3, 16(a5)\n\t"
        "ld     a4, 24(a5)\n\t"
        "mv     t6, sp\n\t"

        "addi   sp, sp, -16\n\t"
        "vse64.v v4, 0(sp)\n\t"
        "addi   sp, sp, -16\n\t"
        "vse64.v v3, 0(sp)\n\t"
        "addi   sp, sp, -16\n\t"
        "vse64.v v2, 0(sp)\n\t"
        "addi   sp, sp, -16\n\t"
        "vse64.v v1, 0(sp)\n\t"

        "clmul  t0, a3, s0\n\t"
        "clmulh t1, a3, s0\n\t"
        "clmul  t2, a4, s1\n\t"
        "clmulh t3, a4, s1\n\t"
        "xor    s0, s2, t0\n\t"
        "xor    s0, s0, t2\n\t"
        "xor    s1, s3, t1\n\t"
        "xor    s1, s1, t3\n\t"

        "clmul  t0, a3, s0\n\t"
        "clmulh t1, a3, s0\n\t"
        "clmul  t2, a4, s1\n\t"
        "clmulh t3, a4, s1\n\t"
        "xor    s0, s4, t0\n\t"
        "xor    s0, s0, t2\n\t"
        "xor    s1, s5, t1\n\t"
        "xor    s1, s1, t3\n\t"

        "clmul  t4, a3, s0\n\t"
        "clmulh t5, a3, s0\n\t"
        "clmul  t2, a4, s1\n\t"
        "clmulh t3, a4, s1\n\t"
        "xor    s0, s6, t4\n\t"
        "xor    t0, s0, t2\n\t"
        "xor    s1, s7, t5\n\t"
        "xor    t1, s1, t3\n\t"

        ".align 3\n\t"
        "3:\n\t"
        "clmul  t4, a3, t0\n\t"
        "clmulh t5, a3, t0\n\t"
        "clmul  t2, a4, t1\n\t"
        "clmulh t3, a4, t1\n\t"
        "ld     s0, 0(sp)\n\t"
        "ld     s1, 8(sp)\n\t"
        "addi   sp, sp, 16\n\t"
        "xor    t0, s0, t4\n\t"
        "xor    t0, t0, t2\n\t"
        "xor    t1, s1, t5\n\t"
        "xor    t1, t1, t3\n\t"

        "bne    sp, t6, 3b\n\t"

        "ld     ra, 120(sp)\n\t"
        "ld     gp, 112(sp)\n\t"
        "ld     tp, 104(sp)\n\t"
        "ld     s0, 96(sp)\n\t"
        "ld     s1, 88(sp)\n\t"
        "ld     s2, 80(sp)\n\t"
        "ld     s3, 72(sp)\n\t"
        "ld     s4, 64(sp)\n\t"
        "ld     s5, 56(sp)\n\t"
        "ld     s6, 48(sp)\n\t"
        "ld     s7, 40(sp)\n\t"
        "ld     s8, 32(sp)\n\t"
        "ld     s9, 24(sp)\n\t"
        "ld     s10, 16(sp)\n\t"
        "ld     s11, 8(sp)\n\t"
        "addi   sp, sp, 128\n\t"

        // 128bit -> 64bit Folding & Barrett Reduction
        "mv     t2, %[const_low]\n\t"
        "mv     t3, %[const_high]\n\t"
        "li     t6, 0xffffffff\n\t"

        "clmul  t4, t0, t3\n\t"
        "clmulh t3, t0, t3\n\t"
        "xor    t1, t1, t4\n\t"
        "slli   t4, t1, 32\n\t"
        "srli   t4, t4, 32\n\t"
        "srli   t1, t1, 32\n\t"
        "clmul  t0, t4, t2\n\t"
        "slli   t3, t3, 32\n\t"
        "xor    t3, t3, t1\n\t"
        "xor    t3, t3, t0\n\t"

        "and    t4, t3, t6\n\t"
        "mv     t2, %[const_quo]\n\t"
        "mv     t1, %[const_poly]\n\t"

        "clmul  t4, t4, t2\n\t"
        "and    t4, t4, t6\n\t"
        "clmul  t4, t4, t1\n\t"
        "xor    t4, t3, t4\n\t"
        "srli   a1, t4, 32\n\t"


        "not    a1, a1\n\t"
        "sext.w a1, a1\n\t"
        "xori   a1, a1, -1\n\t"
        ".option pop\n\t"

        : "+r" (r_buf), "+r" (r_crc), "+r" (r_len), "+r" (r_consts), "+r" (r_mask)
        : "r" (r_end),
          [const_low] "r" ((uint64_t)CRC32C_CONST_0),
          [const_high] "r" ((uint64_t)CRC32C_CONST_1),
          [const_quo] "r" ((uint64_t)CRC32C_CONST_QUO),
          [const_poly] "r" ((uint64_t)CRC32C_CONST_POLY)
        : "t1", "t2", "t3", "t4", "t5", "t6",
          "a3", "a4", "a7",
          "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9",
          "v10", "v11", "v12", "v13", "v14", "v15", "v16", "v17"
    );

    uint32_t result = (uint32_t)r_crc;

    size_t tail_len = len & 127;
    if (tail_len > 0) {
        result = ceph_crc32c_sctp(result, buf + len - tail_len, tail_len);
    }
    return result;
}

#endif

#if defined(HAVE_RISCV_ZBC)

/* External assembly function implementing CRC32C with carryless multiply */
extern uint32_t crc32c_zbc(unsigned char const *buf, unsigned len, uint32_t crc);

uint32_t ceph_crc32c_riscv_zbc(uint32_t crc, unsigned char const *buf, unsigned len) {
    if (!buf) {
            return ceph_crc32c_sctp(crc, NULL, len);
    }

    if (len == 0) {
            return crc;
    }

    /*
     * For len < 16, the fold pipeline is never entered. Instead, the assembly
     * degrades to per-chunk barrett_reduce (via crc32_refl_excess), which
     * may not outperform the sctp table lookup on all RISC-V microarchitectures.
     * 16 is the minimum chunk size for the fold-1 path (one 128-bit load pair).
     */
    if (len < 16) {
            return ceph_crc32c_sctp(crc, buf, len);
    }

    return crc32c_zbc(buf, len, crc);
}

#endif
