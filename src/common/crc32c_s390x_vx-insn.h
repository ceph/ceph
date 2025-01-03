/*
 * Support for Vector Instructions
 *
 * Assembler macros to generate .byte/.word code for particular vector
 * instructions that are supported by recent binutils.
 *
 * Copyright 2015 IBM Corporation
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
 *
 * Author(s): Hendrik Brueckner <brueckner@linux.vnet.ibm.com>
 */

#ifndef __ASM_S390_VX_INSN_H
#define __ASM_S390_VX_INSN_H

/* Boilerplate for function entry points */
#define ENTRY(name) \
.globl name;        \
.align 4, 0x90;     \
name:

/* Macros to generate vector instruction byte code */

#define REG_NUM_INVALID	       255

/* GR_NUM - Retrieve general-purpose register number
 *
 * @opd:	Operand to store register number
 * @r64:	String designation register in the format "%rN"
 */
.macro	GR_NUM	opd gr
    \opd = REG_NUM_INVALID
    .ifc \gr,%r0
	\opd = 0
    .endif
    .ifc \gr,%r1
	\opd = 1
    .endif
    .ifc \gr,%r2
	\opd = 2
    .endif
    .ifc \gr,%r3
	\opd = 3
    .endif
    .ifc \gr,%r4
	\opd = 4
    .endif
    .ifc \gr,%r5
	\opd = 5
    .endif
    .ifc \gr,%r6
	\opd = 6
    .endif
    .ifc \gr,%r7
	\opd = 7
    .endif
    .ifc \gr,%r8
	\opd = 8
    .endif
    .ifc \gr,%r9
	\opd = 9
    .endif
    .ifc \gr,%r10
	\opd = 10
    .endif
    .ifc \gr,%r11
	\opd = 11
    .endif
    .ifc \gr,%r12
	\opd = 12
    .endif
    .ifc \gr,%r13
	\opd = 13
    .endif
    .ifc \gr,%r14
	\opd = 14
    .endif
    .ifc \gr,%r15
	\opd = 15
    .endif
    .if \opd == REG_NUM_INVALID
	.error "Invalid general-purpose register designation: \gr"
    .endif
.endm

/* VX_R() - Macro to encode the VX_NUM into the instruction */
#define VX_R(v)		(v & 0x0F)

/* VX_NUM - Retrieve vector register number
 *
 * @opd:	Operand to store register number
 * @vxr:	String designation register in the format "%vN"
 *
 * The vector register number is used for as input number to the
 * instruction and, as well as, to compute the RXB field of the
 * instruction.  To encode the particular vector register number,
 * use the VX_R(v) macro to extract the instruction opcode.
 */
.macro	VX_NUM	opd vxr
    \opd = REG_NUM_INVALID
    .ifc \vxr,%v0
	\opd = 0
    .endif
    .ifc \vxr,%v1
	\opd = 1
    .endif
    .ifc \vxr,%v2
	\opd = 2
    .endif
    .ifc \vxr,%v3
	\opd = 3
    .endif
    .ifc \vxr,%v4
	\opd = 4
    .endif
    .ifc \vxr,%v5
	\opd = 5
    .endif
    .ifc \vxr,%v6
	\opd = 6
    .endif
    .ifc \vxr,%v7
	\opd = 7
    .endif
    .ifc \vxr,%v8
	\opd = 8
    .endif
    .ifc \vxr,%v9
	\opd = 9
    .endif
    .ifc \vxr,%v10
	\opd = 10
    .endif
    .ifc \vxr,%v11
	\opd = 11
    .endif
    .ifc \vxr,%v12
	\opd = 12
    .endif
    .ifc \vxr,%v13
	\opd = 13
    .endif
    .ifc \vxr,%v14
	\opd = 14
    .endif
    .ifc \vxr,%v15
	\opd = 15
    .endif
    .ifc \vxr,%v16
	\opd = 16
    .endif
    .ifc \vxr,%v17
	\opd = 17
    .endif
    .ifc \vxr,%v18
	\opd = 18
    .endif
    .ifc \vxr,%v19
	\opd = 19
    .endif
    .ifc \vxr,%v20
	\opd = 20
    .endif
    .ifc \vxr,%v21
	\opd = 21
    .endif
    .ifc \vxr,%v22
	\opd = 22
    .endif
    .ifc \vxr,%v23
	\opd = 23
    .endif
    .ifc \vxr,%v24
	\opd = 24
    .endif
    .ifc \vxr,%v25
	\opd = 25
    .endif
    .ifc \vxr,%v26
	\opd = 26
    .endif
    .ifc \vxr,%v27
	\opd = 27
    .endif
    .ifc \vxr,%v28
	\opd = 28
    .endif
    .ifc \vxr,%v29
	\opd = 29
    .endif
    .ifc \vxr,%v30
	\opd = 30
    .endif
    .ifc \vxr,%v31
	\opd = 31
    .endif
    .if \opd == REG_NUM_INVALID
	.error "Invalid vector register designation: \vxr"
    .endif
.endm

/* RXB - Compute most significant bit used vector registers
 *
 * @rxb:	Operand to store computed RXB value
 * @v1:		First vector register designated operand
 * @v2:		Second vector register designated operand
 * @v3:		Third vector register designated operand
 * @v4:		Fourth vector register designated operand
 */
.macro	RXB	rxb v1 v2=0 v3=0 v4=0
    \rxb = 0
    .if \v1 & 0x10
	\rxb = \rxb | 0x08
    .endif
    .if \v2 & 0x10
	\rxb = \rxb | 0x04
    .endif
    .if \v3 & 0x10
	\rxb = \rxb | 0x02
    .endif
    .if \v4 & 0x10
	\rxb = \rxb | 0x01
    .endif
.endm

/* MRXB - Generate Element Size Control and RXB value
 *
 * @m:		Element size control
 * @v1:		First vector register designated operand (for RXB)
 * @v2:		Second vector register designated operand (for RXB)
 * @v3:		Third vector register designated operand (for RXB)
 * @v4:		Fourth vector register designated operand (for RXB)
 */
.macro	MRXB	m v1 v2=0 v3=0 v4=0
    rxb = 0
    RXB	rxb, \v1, \v2, \v3, \v4
    .byte	(\m << 4) | rxb
.endm

/* MRXBOPC - Generate Element Size Control, RXB, and final Opcode fields
 *
 * @m:		Element size control
 * @opc:	Opcode
 * @v1:		First vector register designated operand (for RXB)
 * @v2:		Second vector register designated operand (for RXB)
 * @v3:		Third vector register designated operand (for RXB)
 * @v4:		Fourth vector register designated operand (for RXB)
 */
.macro	MRXBOPC	m opc v1 v2=0 v3=0 v4=0
    MRXB	\m, \v1, \v2, \v3, \v4
    .byte	\opc
.endm

/* Vector support instructions */

/* VECTOR GENERATE BYTE MASK */
.macro	VGBM	vr imm2
    VX_NUM	v1, \vr
    .word	(0xE700 | (VX_R(v1) << 4))
    .word	\imm2
    MRXBOPC	0, 0x44, v1
.endm
.macro	VZERO	vxr
    VGBM	\vxr, 0
.endm
.macro	VONE	vxr
    VGBM	\vxr, 0xFFFF
.endm

/* VECTOR LOAD VR ELEMENT FROM GR */
.macro	VLVG	v, gr, disp, m
    VX_NUM	v1, \v
    GR_NUM	b2, "%r0"
    GR_NUM	r3, \gr
    .word	0xE700 | (VX_R(v1) << 4) | r3
    .word	(b2 << 12) | (\disp)
    MRXBOPC	\m, 0x22, v1
.endm
.macro	VLVGB	v, gr, index, base
    VLVG	\v, \gr, \index, \base, 0
.endm
.macro	VLVGH	v, gr, index
    VLVG	\v, \gr, \index, 1
.endm
.macro	VLVGF	v, gr, index
    VLVG	\v, \gr, \index, 2
.endm
.macro	VLVGG	v, gr, index
    VLVG	\v, \gr, \index, 3
.endm

/* VECTOR LOAD */
.macro	VL	v, disp, index="%r0", base
    VX_NUM	v1, \v
    GR_NUM	x2, \index
    GR_NUM	b2, \base
    .word	0xE700 | (VX_R(v1) << 4) | x2
    .word	(b2 << 12) | (\disp)
    MRXBOPC 0, 0x06, v1
.endm

/* VECTOR LOAD ELEMENT */
.macro	VLEx	vr1, disp, index="%r0", base, m3, opc
    VX_NUM	v1, \vr1
    GR_NUM	x2, \index
    GR_NUM	b2, \base
    .word	0xE700 | (VX_R(v1) << 4) | x2
    .word	(b2 << 12) | (\disp)
    MRXBOPC	\m3, \opc, v1
.endm
.macro	VLEB	vr1, disp, index="%r0", base, m3
    VLEx	\vr1, \disp, \index, \base, \m3, 0x00
.endm
.macro	VLEH	vr1, disp, index="%r0", base, m3
    VLEx	\vr1, \disp, \index, \base, \m3, 0x01
.endm
.macro	VLEF	vr1, disp, index="%r0", base, m3
    VLEx	\vr1, \disp, \index, \base, \m3, 0x03
.endm
.macro	VLEG	vr1, disp, index="%r0", base, m3
    VLEx	\vr1, \disp, \index, \base, \m3, 0x02
.endm

/* VECTOR LOAD ELEMENT IMMEDIATE */
.macro	VLEIx	vr1, imm2, m3, opc
    VX_NUM	v1, \vr1
    .word	0xE700 | (VX_R(v1) << 4)
    .word	\imm2
    MRXBOPC	\m3, \opc, v1
.endm
.macro	VLEIB	vr1, imm2, index
    VLEIx	\vr1, \imm2, \index, 0x40
.endm
.macro	VLEIH	vr1, imm2, index
    VLEIx	\vr1, \imm2, \index, 0x41
.endm
.macro	VLEIF	vr1, imm2, index
    VLEIx	\vr1, \imm2, \index, 0x43
.endm
.macro	VLEIG	vr1, imm2, index
    VLEIx	\vr1, \imm2, \index, 0x42
.endm

/* VECTOR LOAD GR FROM VR ELEMENT */
.macro	VLGV	gr, vr, disp, base="%r0", m
    GR_NUM	r1, \gr
    GR_NUM	b2, \base
    VX_NUM	v3, \vr
    .word	0xE700 | (r1 << 4) | VX_R(v3)
    .word	(b2 << 12) | (\disp)
    MRXBOPC	\m, 0x21, v3
.endm
.macro	VLGVB	gr, vr, disp, base="%r0"
    VLGV	\gr, \vr, \disp, \base, 0
.endm
.macro	VLGVH	gr, vr, disp, base="%r0"
    VLGV	\gr, \vr, \disp, \base, 1
.endm
.macro	VLGVF	gr, vr, disp, base="%r0"
    VLGV	\gr, \vr, \disp, \base, 2
.endm
.macro	VLGVG	gr, vr, disp, base="%r0"
    VLGV	\gr, \vr, \disp, \base, 3
.endm

/* VECTOR LOAD MULTIPLE */
.macro	VLM	vfrom, vto, disp, base
    VX_NUM	v1, \vfrom
    VX_NUM	v3, \vto
    GR_NUM	b2, \base	    /* Base register */
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v3)
    .word	(b2 << 12) | (\disp)
    MRXBOPC	0, 0x36, v1, v3
.endm

/* VECTOR STORE MULTIPLE */
.macro	VSTM	vfrom, vto, disp, base
    VX_NUM	v1, \vfrom
    VX_NUM	v3, \vto
    GR_NUM	b2, \base	    /* Base register */
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v3)
    .word	(b2 << 12) | (\disp)
    MRXBOPC	0, 0x3E, v1, v3
.endm

/* VECTOR PERMUTE */
.macro	VPERM	vr1, vr2, vr3, vr4
    VX_NUM	v1, \vr1
    VX_NUM	v2, \vr2
    VX_NUM	v3, \vr3
    VX_NUM	v4, \vr4
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v2)
    .word	(VX_R(v3) << 12)
    MRXBOPC	VX_R(v4), 0x8C, v1, v2, v3, v4
.endm

/* VECTOR UNPACK LOGICAL LOW */
.macro	VUPLL	vr1, vr2, m3
    VX_NUM	v1, \vr1
    VX_NUM	v2, \vr2
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v2)
    .word	0x0000
    MRXBOPC	\m3, 0xD4, v1, v2
.endm
.macro	VUPLLB	vr1, vr2
    VUPLL	\vr1, \vr2, 0
.endm
.macro	VUPLLH	vr1, vr2
    VUPLL	\vr1, \vr2, 1
.endm
.macro	VUPLLF	vr1, vr2
    VUPLL	\vr1, \vr2, 2
.endm


/* Vector integer instructions */

/* VECTOR EXCLUSIVE OR */
.macro	VX	vr1, vr2, vr3
    VX_NUM	v1, \vr1
    VX_NUM	v2, \vr2
    VX_NUM	v3, \vr3
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v2)
    .word	(VX_R(v3) << 12)
    MRXBOPC	0, 0x6D, v1, v2, v3
.endm

/* VECTOR GALOIS FIELD MULTIPLY SUM */
.macro	VGFM	vr1, vr2, vr3, m4
    VX_NUM	v1, \vr1
    VX_NUM	v2, \vr2
    VX_NUM	v3, \vr3
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v2)
    .word	(VX_R(v3) << 12)
    MRXBOPC	\m4, 0xB4, v1, v2, v3
.endm
.macro	VGFMB	vr1, vr2, vr3
    VGFM	\vr1, \vr2, \vr3, 0
.endm
.macro	VGFMH	vr1, vr2, vr3
    VGFM	\vr1, \vr2, \vr3, 1
.endm
.macro	VGFMF	vr1, vr2, vr3
    VGFM	\vr1, \vr2, \vr3, 2
.endm
.macro	VGFMG	vr1, vr2, vr3
    VGFM	\vr1, \vr2, \vr3, 3
.endm

/* VECTOR GALOIS FIELD MULTIPLY SUM AND ACCUMULATE */
.macro	VGFMA	vr1, vr2, vr3, vr4, m5
    VX_NUM	v1, \vr1
    VX_NUM	v2, \vr2
    VX_NUM	v3, \vr3
    VX_NUM	v4, \vr4
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v2)
    .word	(VX_R(v3) << 12) | (\m5 << 8)
    MRXBOPC	VX_R(v4), 0xBC, v1, v2, v3, v4
.endm
.macro	VGFMAB	vr1, vr2, vr3, vr4
    VGFMA	\vr1, \vr2, \vr3, \vr4, 0
.endm
.macro	VGFMAH	vr1, vr2, vr3, vr4
    VGFMA	\vr1, \vr2, \vr3, \vr4, 1
.endm
.macro	VGFMAF	vr1, vr2, vr3, vr4
    VGFMA	\vr1, \vr2, \vr3, \vr4, 2
.endm
.macro	VGFMAG	vr1, vr2, vr3, vr4
    VGFMA	\vr1, \vr2, \vr3, \vr4, 3
.endm

/* VECTOR SHIFT RIGHT LOGICAL BY BYTE */
.macro	VSRLB	vr1, vr2, vr3
    VX_NUM	v1, \vr1
    VX_NUM	v2, \vr2
    VX_NUM	v3, \vr3
    .word	0xE700 | (VX_R(v1) << 4) | VX_R(v2)
    .word	(VX_R(v3) << 12)
    MRXBOPC	0, 0x7D, v1, v2, v3
.endm

#endif	/* __ASM_S390_VX_INSN_H */
