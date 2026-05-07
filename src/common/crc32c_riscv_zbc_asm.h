/* Copyright (C) 2026 ZTE Corporation
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */

#define SEED a0
#define BUF  a1
#define LEN  a2
#define POLY a3
#define MU   a4
#define K1   t5
#define K2   t6
#define K3   t5
#define K4   t6
#define K5   t5
#define K6   t6

#define X3HIGH t0
#define HIGH   t0
#define X3LOW  t1
#define LOW    t1

#define X2HIGH t2
#define X2LOW  a5
#define X1HIGH a6
#define X1LOW  a7
#define X0HIGH t3
#define X0LOW  t4

#define BUF3HIGH s4
#define BUF3LOW  s5
#define BUF2HIGH s6
#define BUF2LOW  s7
#define BUF1HIGH s8
#define BUF1LOW  s9
#define BUF0HIGH s10
#define BUF0LOW  s11

#define X3K1LOW  ra
#define X3K2HIGH gp
#define X2K1LOW  tp
#define X2K2HIGH s0
#define X1K1LOW  s1
#define X1K2HIGH a0
#define X0K1LOW  s2
#define X0K2HIGH s3

/* repeated fold-by-four followed by fold-by-one */
/* expects SEED (a0), BUF (a1) and LEN (a2) to hold those values */
/* expects BUF is doubleword-aligned */
/* returns 128-bit result in HIGH:LOW (t0:t1) */
/* returns updated buffer ptr & length in BUF and LEN */
/* trashes all caller-saved registers except POLY and MU (a3/a4) */
.macro crc_fold_loop

        /* does enough buffer exist for a 4-fold? */
        li t0, 128
        bltu LEN, t0, .Lfold_1

        /* push callee-saved registers to stack */
        addi sp, sp, -128
        sd a3, 120(sp)
        sd ra, 112(sp)
        sd gp, 104(sp)
        sd tp, 96(sp)
        sd s0, 88(sp)
        sd s1, 80(sp)
        sd s2, 72(sp)
        sd s3, 64(sp)
        sd s4, 56(sp)
        sd s5, 48(sp)
        sd s6, 40(sp)
        sd s7, 32(sp)
        sd s8, 24(sp)
        sd s9, 16(sp)
        sd s10, 8(sp)
        sd s11, 0(sp)

        /* load initial 4 128-bit chunks */
        ld X3HIGH, 0(BUF)
        ld X3LOW, 8(BUF)
        ld X2HIGH, 16(BUF)
        ld X2LOW, 24(BUF)
        ld X1HIGH, 32(BUF)
        ld X1LOW, 40(BUF)
        ld X0HIGH, 48(BUF)
        ld X0LOW, 56(BUF)

        addi BUF, BUF, 64
        addi LEN, LEN, -64

        /* xor in seed */
        xor X3HIGH, X3HIGH, SEED

        /* load constants */
        ld K1, .Lk1
        ld K2, .Lk2

        /* calculate how far we'll fold til and load LEN with the amount left */
        srli a3, LEN, 6
        slli a3, a3, 6
        add a3, BUF, a3
        and LEN, LEN, 0x3f

.align 3
.Lfold_4_loop:
        /* carryless multiply each high doubleword by k1, get 128-bit result */
        /* interleve fetching next 4 128-bit chunks */
        clmulh X3K1LOW, K1, X3HIGH
        ld BUF3HIGH, 0(BUF)
        clmulh X2K1LOW, K1, X2HIGH
        ld BUF3LOW, 8(BUF)
        clmulh X1K1LOW, K1, X1HIGH
        ld BUF2HIGH, 16(BUF)
        clmulh X0K1LOW, K1, X0HIGH
        ld BUF2LOW, 24(BUF)
        clmul X3HIGH, K1, X3HIGH
        ld BUF1HIGH, 32(BUF)
        clmul X2HIGH, K1, X2HIGH
        ld BUF1LOW, 40(BUF)
        clmul X1HIGH, K1, X1HIGH
        ld BUF0HIGH, 48(BUF)
        clmul X0HIGH, K1, X0HIGH
        ld BUF0LOW, 56(BUF)

        addi BUF, BUF, 64

        /* carryless multiply each low doubleword by k2 */
        clmul X3K2HIGH, K2, X3LOW
        clmul X2K2HIGH, K2, X2LOW
        clmul X1K2HIGH, K2, X1LOW
        clmul X0K2HIGH, K2, X0LOW
        clmulh X3LOW, K2, X3LOW
        clmulh X2LOW, K2, X2LOW
        clmulh X1LOW, K2, X1LOW
        clmulh X0LOW, K2, X0LOW

        /* xor results together */
        xor BUF3LOW, BUF3LOW, X3K1LOW
        xor BUF2LOW, BUF2LOW, X2K1LOW
        xor BUF1LOW, BUF1LOW, X1K1LOW
        xor BUF0LOW, BUF0LOW, X0K1LOW
        xor X3HIGH, BUF3HIGH, X3HIGH
        xor X2HIGH, BUF2HIGH, X2HIGH
        xor X1HIGH, BUF1HIGH, X1HIGH
        xor X0HIGH, BUF0HIGH, X0HIGH
        xor X3LOW, X3LOW, BUF3LOW
        xor X2LOW, X2LOW, BUF2LOW
        xor X1LOW, X1LOW, BUF1LOW
        xor X0LOW, X0LOW, BUF0LOW
        xor X3HIGH, X3K2HIGH, X3HIGH
        xor X2HIGH, X2K2HIGH, X2HIGH
        xor X1HIGH, X1K2HIGH, X1HIGH
        xor X0HIGH, X0K2HIGH, X0HIGH

        bne BUF, a3, .Lfold_4_loop

        /* we've four folded as much as we can, fold-by-one values in regs */
        /* load fold-by-one constants */
        ld K3, .Lk3
        ld K4, .Lk4

        clmul s0, K3, X3HIGH
        clmulh s1, K3, X3HIGH
        clmul s2, K4, X3LOW
        clmulh s3, K4, X3LOW
        xor HIGH, X2HIGH, s0
        xor HIGH, HIGH, s2
        xor LOW, X2LOW, s1
        xor LOW, LOW, s3

        clmul s0, K3, HIGH
        clmulh s1, K3, HIGH
        clmul s2, K4, LOW
        clmulh s3, K4, LOW
        xor HIGH, X1HIGH, s0
        xor HIGH, HIGH, s2
        xor LOW, X1LOW, s1
        xor LOW, LOW, s3

        clmul s0, K3, HIGH
        clmulh s1, K3, HIGH
        clmul s2, K4, LOW
        clmulh s3, K4, LOW
        xor HIGH, X0HIGH, s0
        xor HIGH, HIGH, s2
        xor LOW, X0LOW, s1
        xor LOW, LOW, s3

        /* pop register values saved on stack */
        ld a3, 120(sp)
        ld ra, 112(sp)
        ld gp, 104(sp)
        ld tp, 96(sp)
        ld s0, 88(sp)
        ld s1, 80(sp)
        ld s2, 72(sp)
        ld s3, 64(sp)
        ld s4, 56(sp)
        ld s5, 48(sp)
        ld s6, 40(sp)
        ld s7, 32(sp)
        ld s8, 24(sp)
        ld s9, 16(sp)
        ld s10, 8(sp)
        ld s11, 0(sp)
        addi sp, sp, 128

        /* load fold loop constant, check if any more 1-folding to do */
        li t4, 16
        bgeu LEN, t4, .Lfold_1_loop
        /* else jump straight to end */
        j .Lfold_1_cleanup

.Lfold_1:
        li t4, 16 /* kept throughout loop */
        /* handle case where not enough buffer to do any fold */
        /* .Lfold_1_done must be defined by the crc32/64 fold reduction macro */
        bltu LEN, t4, .Lfold_1_done

        /* load in initial values and xor with seed */
        ld HIGH, 0(BUF)
        xor HIGH, HIGH, SEED

        ld LOW, 8(BUF)

        addi LEN, LEN, -16
        addi BUF, BUF, 16

        bltu a2, t4, .Lfold_1_cleanup

        /* precomputed constants */
        ld K3, .Lk3
        ld K4, .Lk4
.Lfold_1_loop:
        /* multiply high and low by constants to get two 128-bit result */
        clmul t2, K3, HIGH
        clmulh t3, K3, HIGH
        clmul a5, K4, LOW
        clmulh a6, K4, LOW

        /* load next 128-bits of buffer */
        ld HIGH, 0(BUF)
        ld LOW, 8(BUF)

        addi LEN, LEN, -16
        addi BUF, BUF, 16

        /* fold in values with xor */
        xor HIGH, HIGH, t2
        xor HIGH, HIGH, a5
        xor LOW, LOW, t3
        xor LOW, LOW, a6

        bgeu LEN, t4, .Lfold_1_loop

.Lfold_1_cleanup:
.endm

/* folding reflected final reduction */
/* expects 128-bit value in HIGH:LOW (t0:t1), puts return value in SEED (a0) */
/* trashes t2, t3, a5, a6 and t5, t6 */
.macro crc32_refl_fold_reduction
        /* load precalculated constants */
        ld K4, .Lk4
        ld K5, .Lk5

        /* fold remaining 128 bits into 96 */
        clmul t3, K4, t0
        xor t1, t3, t1
        clmulh t0, K4, t0

        /* high = (low >> 32) | (high << 32) */
        slli t0, t0, 32
        srli t3, t1, 32
        or t0, t0, t3

        /* fold last 96 bits into 64 */
        slli t1, t1, 32
        srli t1, t1, 32
        clmul t1, K5, t1
        xor t1, t1, t0

        /* barrett's reduce 64 bits */
        clmul t0, MU, t1
        slli t0, t0, 32
        srli t0, t0, 32
        clmul t0, POLY, t0
        xor t0, t1, t0
        srli SEED, t0, 32

.Lfold_1_done:
.endm

/* barrett's reduction on a \bits bit-length value, returning result in seed */
/* bits must be 64, 32, 16 or 8 */
/* value and seed must be zero-extended */
.macro barrett_reduce seed:req, value:req, bits:req
        /* combine value with seed */
        xor t0, \seed, \value
.if (\bits < 64)
        slli t0, t0, (64 - \bits)
.endif

        /* multiply by mu, which is 2^96 divided by our polynomial */
        clmul t0, t0, MU

.if (\bits == 16) || (\bits == 8)
        clmulh t0, t0, POLY
        /* subtract from original for smaller sizes */
        srli t1, \seed, \bits
        xor \seed, t0, t1
.else
        clmulh \seed, t0, POLY
.endif

.endm

/* align buffer to 64-bits updating seed */
/* expects SEED (a0), BUF (a1), LEN (a2), MU (a3), POLY (a4) to hold values */
/* expects crc32_refl_excess to be called later */
/* trashes t0 and t1 */
.macro crc32_refl_align
        /* is buffer already aligned to 128-bits? */
        andi t0, BUF, 0b111
        beqz t0, .Lalign_done

.Lalign_8:
        /* is enough buffer left? */
        li t0, 1
        bltu LEN, t0, .Lexcess_done

        /* is buffer misaligned by one byte? */
        andi t0, BUF, 0b001
        beqz t0, .Lalign_16

        /* perform barrett's reduction on one byte */
        lbu t1, (BUF)
        barrett_reduce SEED, t1, 8
        addi LEN, LEN, -1
        addi BUF, BUF, 1

.Lalign_16:
        li t0, 2
        bltu LEN, t0, .Lexcess_8

        andi t0, BUF, 0b010
        beqz t0, .Lalign_32

        lhu t1, (BUF)
        barrett_reduce SEED, t1, 16
        addi LEN, LEN, -2
        addi BUF, BUF, 2

.Lalign_32:
        li t0, 4
        bltu LEN, t0, .Lexcess_16

        andi t0, BUF, 0b100
        beqz t0, .Lalign_done

        lwu t1, (BUF)
        barrett_reduce SEED, t1, 32
        addi LEN, LEN, -4
        addi BUF, BUF, 4

.Lalign_done:
.endm

/* barrett's reduce excess buffer left following fold */
/* expects SEED (a0), BUF (a1), LEN (a2), MU (a3), POLY (a4) to hold values */
/* expects less than 127 bits to be left in doubleword-aligned buffer */
/* trashes t0, t1 and t3 */
.macro crc32_refl_excess
        /* do we have any excess left? */
        beqz LEN, .Lexcess_done

        /* barret's reduce the remaining excess */
        /* at most there is 127 bytes left */
.Lexcess_64:
        andi t0, LEN, 0b1000
        beqz t0, .Lexcess_32
        ld t1, (BUF)
        barrett_reduce SEED, t1, 64
        addi BUF, BUF, 8

.Lexcess_32:
        andi t0, LEN, 0b0100
        beqz t0, .Lexcess_16
        lwu t1, (BUF)
        barrett_reduce SEED, t1, 32
        addi BUF, BUF, 4

.Lexcess_16:
        andi t0, LEN, 0b0010
        beqz t0, .Lexcess_8
        lhu t1, (BUF)
        barrett_reduce SEED, t1, 16
        addi BUF, BUF, 2

.Lexcess_8:
        andi t0, LEN, 0b0001
        beqz t0, .Lexcess_done
        lbu t1, (BUF)
        barrett_reduce SEED, t1, 8

.Lexcess_done:
.endm
