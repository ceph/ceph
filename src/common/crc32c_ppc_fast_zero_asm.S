/*
 * Use the fixed point version of Barrett reduction to compute a mod n
 * over GF(2) for n = 0x104c11db7 using POWER8 instructions. We use k = 32.
 *
 * http://en.wikipedia.org/wiki/Barrett_reduction
 *
 * Copyright (C) 2015 Anton Blanchard <anton@au.ibm.com>, IBM
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of either:
 *
 *  a) the GNU General Public License as published by the Free Software
 *     Foundation; either version 2 of the License, or (at your option)
 *     any later version, or
 *  b) the Apache License, Version 2.0
 */

#if defined (__clang__)
#ifndef __ALTIVEC__
#define __ALTIVEC__
#endif
#include "ppc-asm.h"
#else
#include <ppc-asm.h>
#endif
#include "ppc-opcode.h"

/*
 * The following line is required because toc is defined as 2 in
 * ppc-asm.h. This definition will break @toc in the assembly code,
 * hence toc should be undefined.
 */
#undef toc

/* If we do not define r2 as 2, the assembler throws errors.
 * This is because the assembler has no builtin support for
 * registers, and we should either define them ourselves or
 * use their indexes explicitly like:
 *       addis   4,2,.bit_reflected_constants@toc@ha
 */
#ifndef r2
#define r2 2
#endif

	.section	.data
.balign 16
.constants:
	/* Barrett constant m - (4^32)/n */
	.octa 0x00000000000000000000000104d101df

	/* Barrett constant n */
	.octa 0x00000000000000000000000104c11db7

.bit_reflected_constants:
	/* 33 bit reflected Barrett constant m - (4^32)/n */
	.octa 0x000000000000000000000001f7011641

	/* 33 bit reflected Barrett constant n */
	.octa 0x000000000000000000000001db710641

	.text

/* unsigned int barrett_reduction(unsigned long val) */
FUNC_START(barrett_reduction)
	addis   r4,r2,.constants@toc@ha
	addi    r4,r4,.constants@toc@l

	li	r5,16
	vxor	v1,v1,v1	/* zero v1 */

	/* Get a into v0 */
	MTVRD(v0, r3)
	vsldoi	v0,v1,v0,8	/* shift into bottom 64 bits, this is a */

	/* Load constants */
	lvx	v2,0,r4		/* m */
	lvx	v3,r5,r4	/* n */

	/*
	 * Now for the actual algorithm. The idea is to calculate q,
	 * the multiple of our polynomial that we need to subtract. By
	 * doing the computation 2x bits higher (ie 64 bits) and shifting the
	 * result back down 2x bits, we round down to the nearest multiple.
	 */
	VPMSUMD(v4,v0,v2)	/* ma */
	vsldoi	v4,v1,v4,8	/* q = floor(ma/(2^64)) */
	VPMSUMD(v4,v4,v3)	/* qn */
	vxor	v0,v0,v4	/* a - qn, subtraction is xor in GF(2) */

	/*
	 * Get the result into r3. We need to shift it left 8 bytes:
	 * V0 [ 0 1 2 X ]
	 * V0 [ 0 X 2 3 ]
	 */
	vsldoi	v0,v0,v1,8	/* shift result into top 64 bits of v0 */
	MFVRD(r3, v0)

	blr
FUNC_END(barrett_reduction)

/* unsigned int barrett_reduction_reflected(unsigned long val) */
FUNC_START(barrett_reduction_reflected)
	addis   r4,r2,.bit_reflected_constants@toc@ha
	addi    r4,r4,.bit_reflected_constants@toc@l

	li	r5,16
	vxor	v1,v1,v1	/* zero v1 */

	/* Get a into v0 */
	MTVRD(v0, r3)
	vsldoi	v0,v1,v0,8	/* shift into bottom 64 bits, this is a */

	/* Load constants */
	lvx	v2,0,r4		/* m */
	lvx	v3,r5,r4	/* n */

	vspltisw v5,-1		/* all ones */
	vsldoi	v6,v1,v5,4	/* bitmask with low 32 bits set */

	/*
	 * Now for the Barrett reduction algorithm. Instead of bit reflecting
	 * our data (which is expensive to do), we bit reflect our constants
	 * and our algorithm, which means the intermediate data in our vector
	 * registers goes from 0-63 instead of 63-0. We can reflect the
	 * algorithm because we don't carry in mod 2 arithmetic.
	 */
	vand	v4,v0,v6	/* bottom 32 bits of a */
	VPMSUMD(v4,v4,v2)	/* ma */
	vand	v4,v4,v6	/* bottom 32bits of ma */
	VPMSUMD(v4,v4,v3)	/* qn */
	vxor	v0,v0,v4	/* a - qn, subtraction is xor in GF(2) */

	/*
	 * Since we are bit reflected, the result (ie the low 32 bits) is in the
	 * high 32 bits. We just need to shift it left 4 bytes
	 * V0 [ 0 1 X 3 ]
	 * V0 [ 0 X 2 3 ]
	 */
	vsldoi	v0,v0,v1,4	/* shift result into top 64 bits of v0 */
	MFVRD(r3, v0)

	blr
FUNC_END(barrett_reduction_reflected)
