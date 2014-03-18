/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_int.h
 *
 * Internal code for Galois field routines.  This is not meant for 
 * users to include, but for the internal GF files to use. 
 */

#pragma once

#include "gf_complete.h"

#include <string.h>

extern void     timer_start (double *t);
extern double   timer_split (const double *t);
extern void     galois_fill_random (void *buf, int len, unsigned int seed);

#define GF_SSE2		0x01
#define GF_SSSE3	0x02
#define GF_SSE4		0x04
#define GF_SSE4_PCLMUL	0x08

typedef struct {
  int mult_type;
  int region_type;
  int divide_type;
  int w;
  uint64_t prim_poly;
  int free_me;
  int arg1;
  int arg2;
  gf_t *base_gf;
  void *private;
  uint32_t sse;
} gf_internal_t;

extern int gf_w4_init (gf_t *gf);
extern int gf_w4_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2);

extern int gf_w8_init (gf_t *gf);
extern int gf_w8_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2);

extern int gf_w16_init (gf_t *gf);
extern int gf_w16_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2);

extern int gf_w32_init (gf_t *gf);
extern int gf_w32_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2);

extern int gf_w64_init (gf_t *gf);
extern int gf_w64_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2);

extern int gf_w128_init (gf_t *gf);
extern int gf_w128_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2);

extern int gf_wgen_init (gf_t *gf);
extern int gf_wgen_scratch_size(int w, int mult_type, int region_type, int divide_type, int arg1, int arg2);

void gf_wgen_cauchy_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor);
gf_val_32_t gf_wgen_extract_word(gf_t *gf, void *start, int bytes, int index);

extern void gf_alignment_error(char *s, int a);

extern uint32_t gf_bitmatrix_inverse(uint32_t y, int w, uint32_t pp);

/* This returns the correct default for prim_poly when base is used as the base
   field for COMPOSITE.  It returns 0 if we don't have a default prim_poly. */

extern uint64_t gf_composite_get_default_poly(gf_t *base);

/* This structure lets you define a region multiply.  It helps because you can handle
   unaligned portions of the data with the procedures below, which really cleans
   up the code. */

typedef struct {
  gf_t *gf;
  void *src;
  void *dest;
  int bytes;
  uint64_t val;
  int xor;
  int align;           /* The number of bytes to which to align. */
  void *s_start;       /* The start and the top of the aligned region. */
  void *d_start;
  void *s_top;
  void *d_top;
} gf_region_data;

/* This lets you set up one of these in one call. It also sets the start/top pointers. */

void gf_set_region_data(gf_region_data *rd,
                        gf_t *gf,
                        void *src,
                        void *dest,
                        int bytes,
                        uint64_t val,
                        int xor,
                        int align);

/* This performs gf->multiply.32() on all of the unaligned bytes in the beginning of the region */

extern void gf_do_initial_region_alignment(gf_region_data *rd);

/* This performs gf->multiply.32() on all of the unaligned bytes in the end of the region */

extern void gf_do_final_region_alignment(gf_region_data *rd);

extern void gf_two_byte_region_table_multiply(gf_region_data *rd, uint16_t *base);

extern void gf_multby_zero(void *dest, int bytes, int xor);
extern void gf_multby_one(void *src, void *dest, int bytes, int xor);

typedef enum {GF_E_MDEFDIV, /* Dev != Default && Mult == Default */
              GF_E_MDEFREG, /* Reg != Default && Mult == Default */
              GF_E_MDEFARG, /* Args != Default && Mult == Default */
              GF_E_DIVCOMP, /* Mult == Composite && Div != Default */
              GF_E_CAUCOMP, /* Mult == Composite && Reg == CAUCHY */
              GF_E_DOUQUAD, /* Reg == DOUBLE && Reg == QUAD */
              GF_E_SSE__NO, /* Reg == SSE && Reg == NOSSE */
              GF_E_CAUCHYB, /* Reg == CAUCHY && Other Reg */
              GF_E_CAUGT32, /* Reg == CAUCHY && w > 32*/
              GF_E_ARG1SET, /* Arg1 != 0 && Mult \notin COMPOSITE/SPLIT/GROUP */
              GF_E_ARG2SET, /* Arg2 != 0 && Mult \notin SPLIT/GROUP */
              GF_E_MATRIXW, /* Div == MATRIX && w > 32 */
              GF_E_BAD___W, /* Illegal w */
              GF_E_DOUBLET, /* Reg == DOUBLE && Mult != TABLE */
              GF_E_DOUBLEW, /* Reg == DOUBLE && w \notin {4,8} */
              GF_E_DOUBLEJ, /* Reg == DOUBLE && other Reg */
              GF_E_DOUBLEL, /* Reg == DOUBLE & LAZY but w = 4 */
              GF_E_QUAD__T, /* Reg == QUAD && Mult != TABLE */
              GF_E_QUAD__W, /* Reg == QUAD && w != 4 */
              GF_E_QUAD__J, /* Reg == QUAD && other Reg */
              GF_E_LAZY__X, /* Reg == LAZY && not DOUBLE or QUAD*/
              GF_E_ALTSHIF, /* Mult == Shift && Reg == ALTMAP */
              GF_E_SSESHIF, /* Mult == Shift && Reg == SSE|NOSSE */
              GF_E_ALT_CFM, /* Mult == CARRY_FREE && Reg == ALTMAP */
              GF_E_SSE_CFM, /* Mult == CARRY_FREE && Reg == SSE|NOSSE */
              GF_E_PCLMULX, /* Mult == Carry_Free && No PCLMUL */
              GF_E_ALT_BY2, /* Mult == Bytwo_x && Reg == ALTMAP */
              GF_E_BY2_SSE, /* Mult == Bytwo_x && Reg == SSE && No SSE2 */
              GF_E_LOGBADW, /* Mult == LOGx, w too big*/
              GF_E_LOG___J, /* Mult == LOGx, && Reg == SSE|ALTMAP|NOSSE */
              GF_E_ZERBADW, /* Mult == LOG_ZERO, w \notin {8,16} */
              GF_E_ZEXBADW, /* Mult == LOG_ZERO_EXT, w != 8 */
              GF_E_LOGPOLY, /* Mult == LOG & poly not primitive */
              GF_E_GR_ARGX, /* Mult == GROUP, Bad arg1/2 */
              GF_E_GR_W_48, /* Mult == GROUP, w \in { 4, 8 } */
              GF_E_GR_W_16, /* Mult == GROUP, w == 16, arg1 != 4 || arg2 != 4 */
              GF_E_GR_128A, /* Mult == GROUP, w == 128, bad args */
              GF_E_GR_A_27, /* Mult == GROUP, either arg > 27 */
              GF_E_GR_AR_W, /* Mult == GROUP, either arg > w  */
              GF_E_GR____J, /* Mult == GROUP, Reg == SSE|ALTMAP|NOSSE */
              GF_E_TABLE_W, /* Mult == TABLE, w too big */
              GF_E_TAB_SSE, /* Mult == TABLE, SSE|NOSSE only apply to w == 4 */
              GF_E_TABSSE3, /* Mult == TABLE, Need SSSE3 for SSE */
              GF_E_TAB_ALT, /* Mult == TABLE, Reg == ALTMAP */
              GF_E_SP128AR, /* Mult == SPLIT, w=128, Bad arg1/arg2 */
              GF_E_SP128AL, /* Mult == SPLIT, w=128, SSE requires ALTMAP */
              GF_E_SP128AS, /* Mult == SPLIT, w=128, ALTMAP requires SSE */
              GF_E_SP128_A, /* Mult == SPLIT, w=128, SSE only with 4/128 */
              GF_E_SP128_S, /* Mult == SPLIT, w=128, ALTMAP only with 4/128 */
              GF_E_SPLIT_W, /* Mult == SPLIT, Bad w (8, 16, 32, 64, 128)  */
              GF_E_SP_16AR, /* Mult == SPLIT, w=16, Bad arg1/arg2 */
              GF_E_SP_16_A, /* Mult == SPLIT, w=16, ALTMAP only with 4/16 */
              GF_E_SP_16_S, /* Mult == SPLIT, w=16, SSE only with 4/16 */
              GF_E_SP_32AR, /* Mult == SPLIT, w=32, Bad arg1/arg2 */
              GF_E_SP_32AS, /* Mult == SPLIT, w=32, ALTMAP requires SSE */
              GF_E_SP_32_A, /* Mult == SPLIT, w=32, ALTMAP only with 4/32 */
              GF_E_SP_32_S, /* Mult == SPLIT, w=32, SSE only with 4/32 */
              GF_E_SP_64AR, /* Mult == SPLIT, w=64, Bad arg1/arg2 */
              GF_E_SP_64AS, /* Mult == SPLIT, w=64, ALTMAP requires SSE */
              GF_E_SP_64_A, /* Mult == SPLIT, w=64, ALTMAP only with 4/64 */
              GF_E_SP_64_S, /* Mult == SPLIT, w=64, SSE only with 4/64 */
              GF_E_SP_8_AR, /* Mult == SPLIT, w=8, Bad arg1/arg2 */
              GF_E_SP_8__A, /* Mult == SPLIT, w=8, no ALTMAP */
              GF_E_SP_SSE3, /* Mult == SPLIT, Need SSSE3 for SSE */
              GF_E_COMP_A2, /* Mult == COMP, arg1 must be = 2 */
              GF_E_COMP_SS, /* Mult == COMP, SSE|NOSSE */
              GF_E_COMP__W, /* Mult == COMP, Bad w. */
              GF_E_UNKFLAG, /* Unknown flag in create_from.... */
              GF_E_UNKNOWN, /* Unknown mult_type. */
              GF_E_UNK_REG, /* Unknown region_type. */
              GF_E_UNK_DIV, /* Unknown divide_type. */
              GF_E_CFM___W, /* Mult == CFM,  Bad w. */
              GF_E_CFM4POL, /* Mult == CFM & Prim Poly has high bits set. */
              GF_E_CFM8POL, /* Mult == CFM & Prim Poly has high bits set. */
              GF_E_CF16POL, /* Mult == CFM & Prim Poly has high bits set. */
              GF_E_CF32POL, /* Mult == CFM & Prim Poly has high bits set. */
              GF_E_CF64POL, /* Mult == CFM & Prim Poly has high bits set. */
              GF_E_FEWARGS, /* Too few args in argc/argv. */
              GF_E_BADPOLY, /* Bad primitive polynomial -- too many bits set. */
              GF_E_COMP_PP, /* Bad primitive polynomial -- bigger than sub-field. */
              GF_E_COMPXPP, /* Can't derive a default pp for composite field. */
              GF_E_BASE__W, /* Composite -- Base field is the wrong size. */
              GF_E_TWOMULT, /* In create_from... two -m's. */
              GF_E_TWO_DIV, /* In create_from... two -d's. */
              GF_E_POLYSPC, /* Bad numbera after -p. */
              GF_E_SPLITAR, /* Ran out of arguments in SPLIT */
              GF_E_SPLITNU, /* Arguments not integers in SPLIT. */
              GF_E_GROUPAR, /* Ran out of arguments in GROUP */
              GF_E_GROUPNU, /* Arguments not integers in GROUP. */
              GF_E_DEFAULT } gf_error_type_t;

