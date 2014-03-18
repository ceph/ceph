/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf.c
 *
 * Generic routines for Galois fields
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

int _gf_errno = GF_E_DEFAULT;

void gf_error()
{
  char *s;

  switch(_gf_errno) {
    case GF_E_DEFAULT: s = "No Error."; break;
    case GF_E_TWOMULT: s = "Cannot specify two -m's."; break;
    case GF_E_TWO_DIV: s = "Cannot specify two -d's."; break;
    case GF_E_POLYSPC: s = "-p needs to be followed by a number in hex (0x optional)."; break;
    case GF_E_GROUPAR: s = "Ran out of arguments in -m GROUP."; break;
    case GF_E_GROUPNU: s = "In -m GROUP g_s g_r -- g_s and g_r need to be numbers."; break;
    case GF_E_SPLITAR: s = "Ran out of arguments in -m SPLIT."; break;
    case GF_E_SPLITNU: s = "In -m SPLIT w_a w_b -- w_a and w_b need to be numbers."; break;
    case GF_E_FEWARGS: s = "Not enough arguments (Perhaps end with '-'?)"; break;
    case GF_E_CFM___W: s = "-m CARRY_FREE, w must be 4, 8, 16, 32, 64 or 128."; break;
    case GF_E_COMPXPP: s = "-m COMPOSITE, No poly specified, and we don't have a default for the given sub-field."; break;
    case GF_E_BASE__W: s = "-m COMPOSITE and the base field is not for w/2."; break;
    case GF_E_CFM4POL: s = "-m CARRY_FREE, w=4. (Prim-poly & 0xc) must equal 0."; break;
    case GF_E_CFM8POL: s = "-m CARRY_FREE, w=8. (Prim-poly & 0x80) must equal 0."; break;
    case GF_E_CF16POL: s = "-m CARRY_FREE, w=16. (Prim-poly & 0xe000) must equal 0."; break;
    case GF_E_CF32POL: s = "-m CARRY_FREE, w=32. (Prim-poly & 0xfe000000) must equal 0."; break;
    case GF_E_CF64POL: s = "-m CARRY_FREE, w=64. (Prim-poly & 0xfffe000000000000ULL) must equal 0."; break;
    case GF_E_MDEFDIV: s = "If multiplication method == default, can't change division."; break;
    case GF_E_MDEFREG: s = "If multiplication method == default, can't change region."; break;
    case GF_E_MDEFARG: s = "If multiplication method == default, can't use arg1/arg2."; break;
    case GF_E_DIVCOMP: s = "Cannot change the division technique with -m COMPOSITE."; break;
    case GF_E_DOUQUAD: s = "Cannot specify -r DOUBLE and -r QUAD."; break;
    case GF_E_SSE__NO: s = "Cannot specify -r SSE and -r NOSSE."; break;
    case GF_E_CAUCHYB: s = "Cannot specify -r CAUCHY and any other -r."; break;
    case GF_E_CAUCOMP: s = "Cannot specify -m COMPOSITE and -r CAUCHY."; break;
    case GF_E_CAUGT32: s = "Cannot specify -r CAUCHY with w > 32."; break;
    case GF_E_ARG1SET: s = "Only use arg1 with SPLIT, GROUP or COMPOSITE."; break;
    case GF_E_ARG2SET: s = "Only use arg2 with SPLIT or GROUP."; break;
    case GF_E_MATRIXW: s = "Cannot specify -d MATRIX with w > 32."; break;
    case GF_E_BAD___W: s = "W must be 1-32, 64 or 128."; break;
    case GF_E_DOUBLET: s = "Can only specify -r DOUBLE with -m TABLE."; break;
    case GF_E_DOUBLEW: s = "Can only specify -r DOUBLE w = 4 or w = 8."; break;
    case GF_E_DOUBLEJ: s = "Cannot specify -r DOUBLE with -r ALTMAP|SSE|NOSSE."; break;
    case GF_E_DOUBLEL: s = "Can only specify -r DOUBLE -r LAZY with w = 8"; break;
    case GF_E_QUAD__T: s = "Can only specify -r QUAD with -m TABLE."; break;
    case GF_E_QUAD__W: s = "Can only specify -r QUAD w = 4."; break;
    case GF_E_QUAD__J: s = "Cannot specify -r QUAD with -r ALTMAP|SSE|NOSSE."; break;
    case GF_E_BADPOLY: s = "Bad primitive polynomial (high bits set)."; break;
    case GF_E_COMP_PP: s = "Bad primitive polynomial -- bigger than sub-field."; break;
    case GF_E_LAZY__X: s = "If -r LAZY, then -r must be DOUBLE or QUAD."; break;
    case GF_E_ALTSHIF: s = "Cannot specify -m SHIFT and -r ALTMAP."; break;
    case GF_E_SSESHIF: s = "Cannot specify -m SHIFT and -r SSE|NOSSE."; break;
    case GF_E_ALT_CFM: s = "Cannot specify -m CARRY_FREE and -r ALTMAP."; break;
    case GF_E_SSE_CFM: s = "Cannot specify -m CARRY_FREE and -r SSE|NOSSE."; break;
    case GF_E_PCLMULX: s = "Specified -m CARRY_FREE, but PCLMUL is not supported."; break;
    case GF_E_ALT_BY2: s = "Cannot specify -m BYTWO_x and -r ALTMAP."; break;
    case GF_E_BY2_SSE: s = "Specified -m BYTWO_x -r SSE, but SSE2 is not supported."; break;
    case GF_E_LOGBADW: s = "With Log Tables, w must be <= 27."; break;
    case GF_E_LOG___J: s = "Cannot use Log tables with -r ALTMAP|SSE|NOSSE."; break;
    case GF_E_LOGPOLY: s = "Cannot use Log tables because the polynomial is not primitive."; break;
    case GF_E_ZERBADW: s = "With -m LOG_ZERO, w must be 8 or 16."; break;
    case GF_E_ZEXBADW: s = "With -m LOG_ZERO_EXT, w must be 8."; break;
    case GF_E_GR_ARGX: s = "With -m GROUP, arg1 and arg2 must be >= 0."; break;
    case GF_E_GR_W_48: s = "With -m GROUP, w cannot be 4 or 8."; break;
    case GF_E_GR_W_16: s = "With -m GROUP, w == 16, arg1 and arg2 must be 4."; break;
    case GF_E_GR_128A: s = "With -m GROUP, w == 128, arg1 must be 4, and arg2 in { 4,8,16 }."; break;
    case GF_E_GR_A_27: s = "With -m GROUP, arg1 and arg2 must be <= 27."; break;
    case GF_E_GR_AR_W: s = "With -m GROUP, arg1 and arg2 must be <= w."; break;
    case GF_E_GR____J: s = "Cannot use GROUP with -r ALTMAP|SSE|NOSSE."; break;
    case GF_E_TABLE_W: s = "With -m TABLE, w must be < 15, or == 16."; break;
    case GF_E_TAB_SSE: s = "With -m TABLE, SSE|NOSSE only applies to w=4."; break;
    case GF_E_TABSSE3: s = "With -m TABLE, -r SSE, you need SSSE3 supported."; break;
    case GF_E_TAB_ALT: s = "With -m TABLE, you cannot use ALTMAP."; break;
    case GF_E_SP128AR: s = "With -m SPLIT, w=128, bad arg1/arg2."; break;
    case GF_E_SP128AL: s = "With -m SPLIT, w=128, -r SSE requires -r ALTMAP."; break;
    case GF_E_SP128AS: s = "With -m SPLIT, w=128, ALTMAP needs SSSE3 supported."; break;
    case GF_E_SP128_A: s = "With -m SPLIT, w=128, -r SSE|NOSSE only with arg1/arg2 = 4/128."; break;
    case GF_E_SP128_S: s = "With -m SPLIT, w=128, -r ALTMAP only with arg1/arg2 = 4/128."; break;
    case GF_E_SPLIT_W: s = "With -m SPLIT, w must be in {8, 16, 32, 64, 128}."; break;
    case GF_E_SP_16AR: s = "With -m SPLIT, w=16, Bad arg1/arg2."; break;
    case GF_E_SP_16_A: s = "With -m SPLIT, w=16, -r ALTMAP only with arg1/arg2 = 4/16."; break;
    case GF_E_SP_16_S: s = "With -m SPLIT, w=16, -r SSE|NOSSE only with arg1/arg2 = 4/16."; break;
    case GF_E_SP_32AR: s = "With -m SPLIT, w=32, Bad arg1/arg2."; break;
    case GF_E_SP_32AS: s = "With -m SPLIT, w=32, -r ALTMAP needs SSSE3 supported."; break;
    case GF_E_SP_32_A: s = "With -m SPLIT, w=32, -r ALTMAP only with arg1/arg2 = 4/32."; break;
    case GF_E_SP_32_S: s = "With -m SPLIT, w=32, -r SSE|NOSSE only with arg1/arg2 = 4/32."; break;
    case GF_E_SP_64AR: s = "With -m SPLIT, w=64, Bad arg1/arg2."; break;
    case GF_E_SP_64AS: s = "With -m SPLIT, w=64, -r ALTMAP needs SSSE3 supported."; break;
    case GF_E_SP_64_A: s = "With -m SPLIT, w=64, -r ALTMAP only with arg1/arg2 = 4/64."; break;
    case GF_E_SP_64_S: s = "With -m SPLIT, w=64, -r SSE|NOSSE only with arg1/arg2 = 4/64."; break;
    case GF_E_SP_8_AR: s = "With -m SPLIT, w=8, Bad arg1/arg2."; break;
    case GF_E_SP_8__A: s = "With -m SPLIT, w=8, Can't have -r ALTMAP."; break;
    case GF_E_SP_SSE3: s = "With -m SPLIT, Need SSSE3 support for SSE."; break;
    case GF_E_COMP_A2: s = "With -m COMPOSITE, arg1 must equal 2."; break;
    case GF_E_COMP_SS: s = "With -m COMPOSITE, -r SSE and -r NOSSE do not apply."; break;
    case GF_E_COMP__W: s = "With -m COMPOSITE, w must be 8, 16, 32, 64 or 128."; break;
    case GF_E_UNKFLAG: s = "Unknown method flag - should be -m, -d, -r or -p."; break;
    case GF_E_UNKNOWN: s = "Unknown multiplication type."; break;
    case GF_E_UNK_REG: s = "Unknown region type."; break;
    case GF_E_UNK_DIV: s = "Unknown division type."; break;
    default: s = "Undefined error.";
  }

  fprintf(stderr, "%s\n", s);
}

uint64_t gf_composite_get_default_poly(gf_t *base) 
{
  gf_internal_t *h;
  int rv;

  h = (gf_internal_t *) base->scratch;
  if (h->w == 4) {
    if (h->mult_type == GF_MULT_COMPOSITE) return 0;
    if (h->prim_poly == 0x13) return 2;
    return 0;
  } 
  if (h->w == 8) {
    if (h->mult_type == GF_MULT_COMPOSITE) return 0;
    if (h->prim_poly == 0x11d) return 3;
    return 0;
  }
  if (h->w == 16) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      rv = gf_composite_get_default_poly(h->base_gf);
      if (rv != h->prim_poly) return 0;
      if (rv == 3) return 0x105;
      return 0;
    } else {
      if (h->prim_poly == 0x1100b) return 2;
      if (h->prim_poly == 0x1002d) return 7;
      return 0;
    }
  }
  if (h->w == 32) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      rv = gf_composite_get_default_poly(h->base_gf);
      if (rv != h->prim_poly) return 0;
      if (rv == 2) return 0x10005;
      if (rv == 7) return 0x10008;
      if (rv == 0x105) return 0x10002;
      return 0;
    } else {
      if (h->prim_poly == 0x400007) return 2;
      if (h->prim_poly == 0xc5) return 3;
      return 0;
    }
  }
  if (h->w == 64) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      rv = gf_composite_get_default_poly(h->base_gf);
      if (rv != h->prim_poly) return 0;
      if (rv == 3) return 0x100000009ULL;
      if (rv == 2) return 0x100000004ULL;
      if (rv == 0x10005) return 0x100000003ULL;
      if (rv == 0x10002) return 0x100000005ULL;
      if (rv == 0x10008) return 0x100000006ULL;  /* JSP: (0x0x100000003 works too, 
                                                    but I want to differentiate cases). */
      return 0;
    } else {
      if (h->prim_poly == 0x1bULL) return 2;
      return 0;
    }
  }
  return 0;
}

int gf_error_check(int w, int mult_type, int region_type, int divide_type,
                   int arg1, int arg2, uint64_t poly, gf_t *base)
{
  int sse3 = 0;
  int sse2 = 0;
  int pclmul = 0;
  int rdouble, rquad, rlazy, rsse, rnosse, raltmap, rcauchy, tmp;
  gf_internal_t *sub;

  rdouble = (region_type & GF_REGION_DOUBLE_TABLE);
  rquad   = (region_type & GF_REGION_QUAD_TABLE);
  rlazy   = (region_type & GF_REGION_LAZY);
  rsse    = (region_type & GF_REGION_SSE);
  rnosse  = (region_type & GF_REGION_NOSSE);
  raltmap = (region_type & GF_REGION_ALTMAP);
  rcauchy = (region_type & GF_REGION_CAUCHY);

  if (divide_type != GF_DIVIDE_DEFAULT &&
      divide_type != GF_DIVIDE_MATRIX && 
      divide_type != GF_DIVIDE_EUCLID) {
    _gf_errno = GF_E_UNK_DIV;
    return 0;
  }

  tmp = ( GF_REGION_DOUBLE_TABLE | GF_REGION_QUAD_TABLE | GF_REGION_LAZY |
          GF_REGION_SSE | GF_REGION_NOSSE | GF_REGION_ALTMAP | GF_REGION_CAUCHY );
  if (region_type & (~tmp)) { _gf_errno = GF_E_UNK_REG; return 0; }

#ifdef INTEL_SSE2
  sse2 = 1;
#endif

#ifdef INTEL_SSSE3
  sse3 = 1;
#endif

#ifdef INTEL_SSE4_PCLMUL
  pclmul = 1;
#endif


  if (w < 1 || (w > 32 && w != 64 && w != 128)) { _gf_errno = GF_E_BAD___W; return 0; }
    
  if (mult_type != GF_MULT_COMPOSITE && w < 64) {
    if ((poly >> (w+1)) != 0)                   { _gf_errno = GF_E_BADPOLY; return 0; }
  }

  if (mult_type == GF_MULT_DEFAULT) {
    if (divide_type != GF_DIVIDE_DEFAULT) { _gf_errno = GF_E_MDEFDIV; return 0; }
    if (region_type != GF_REGION_DEFAULT) { _gf_errno = GF_E_MDEFREG; return 0; }
    if (arg1 != 0 || arg2 != 0)           { _gf_errno = GF_E_MDEFARG; return 0; }
    return 1;
  }
  
  if (rsse && rnosse)                                { _gf_errno = GF_E_SSE__NO; return 0; }
  if (rcauchy && w > 32)                             { _gf_errno = GF_E_CAUGT32; return 0; }
  if (rcauchy && region_type != GF_REGION_CAUCHY)    { _gf_errno = GF_E_CAUCHYB; return 0; }
  if (rcauchy && mult_type == GF_MULT_COMPOSITE)     { _gf_errno = GF_E_CAUCOMP; return 0; }

  if (arg1 != 0 && mult_type != GF_MULT_COMPOSITE && 
      mult_type != GF_MULT_SPLIT_TABLE && mult_type != GF_MULT_GROUP) {
    _gf_errno = GF_E_ARG1SET;
    return 0;
  }

  if (arg2 != 0 && mult_type != GF_MULT_SPLIT_TABLE && mult_type != GF_MULT_GROUP) {
    _gf_errno = GF_E_ARG2SET;
    return 0;
  }

  if (divide_type == GF_DIVIDE_MATRIX && w > 32) { _gf_errno = GF_E_MATRIXW; return 0; }

  if (rdouble) {
    if (rquad)                      { _gf_errno = GF_E_DOUQUAD; return 0; }
    if (mult_type != GF_MULT_TABLE) { _gf_errno = GF_E_DOUBLET; return 0; }
    if (w != 4 && w != 8)           { _gf_errno = GF_E_DOUBLEW; return 0; }
    if (rsse || rnosse || raltmap)  { _gf_errno = GF_E_DOUBLEJ; return 0; }
    if (rlazy && w == 4)            { _gf_errno = GF_E_DOUBLEL; return 0; }
    return 1;
  }

  if (rquad) {
    if (mult_type != GF_MULT_TABLE) { _gf_errno = GF_E_QUAD__T; return 0; }
    if (w != 4)                     { _gf_errno = GF_E_QUAD__W; return 0; }
    if (rsse || rnosse || raltmap)  { _gf_errno = GF_E_QUAD__J; return 0; }
    return 1;
  }

  if (rlazy)                        { _gf_errno = GF_E_LAZY__X; return 0; }

  if (mult_type == GF_MULT_SHIFT) {
    if (raltmap)                    { _gf_errno = GF_E_ALTSHIF; return 0; }
    if (rsse || rnosse)             { _gf_errno = GF_E_SSESHIF; return 0; }
    return 1;
  }

  if (mult_type == GF_MULT_CARRY_FREE) {
    if (w != 4 && w != 8 && w != 16 &&
        w != 32 && w != 64 && w != 128)            { _gf_errno = GF_E_CFM___W; return 0; }
    if (w == 4 && (poly & 0xc))                    { _gf_errno = GF_E_CFM4POL; return 0; }
    if (w == 8 && (poly & 0x80))                   { _gf_errno = GF_E_CFM8POL; return 0; }
    if (w == 16 && (poly & 0xe000))                { _gf_errno = GF_E_CF16POL; return 0; }
    if (w == 32 && (poly & 0xfe000000))            { _gf_errno = GF_E_CF32POL; return 0; }
    if (w == 64 && (poly & 0xfffe000000000000ULL)) { _gf_errno = GF_E_CF64POL; return 0; }
    if (raltmap)                                   { _gf_errno = GF_E_ALT_CFM; return 0; }
    if (rsse || rnosse)                            { _gf_errno = GF_E_SSE_CFM; return 0; }
    if (!pclmul)                                   { _gf_errno = GF_E_PCLMULX; return 0; }
    return 1;
  }

  if (mult_type == GF_MULT_BYTWO_p || mult_type == GF_MULT_BYTWO_b) {
    if (raltmap)                    { _gf_errno = GF_E_ALT_BY2; return 0; }
    if (rsse && !sse2)              { _gf_errno = GF_E_BY2_SSE; return 0; }
    return 1;
  }

  if (mult_type == GF_MULT_LOG_TABLE || mult_type == GF_MULT_LOG_ZERO
                                     || mult_type == GF_MULT_LOG_ZERO_EXT ) {
    if (w > 27)                     { _gf_errno = GF_E_LOGBADW; return 0; }
    if (raltmap || rsse || rnosse)  { _gf_errno = GF_E_LOG___J; return 0; }

    if (mult_type == GF_MULT_LOG_TABLE) return 1;

    if (w != 8 && w != 16)          { _gf_errno = GF_E_ZERBADW; return 0; }

    if (mult_type == GF_MULT_LOG_ZERO) return 1;

    if (w != 8)                     { _gf_errno = GF_E_ZEXBADW; return 0; }
    return 1;
  }

  if (mult_type == GF_MULT_GROUP) {
    if (arg1 <= 0 || arg2 <= 0)                 { _gf_errno = GF_E_GR_ARGX; return 0; }
    if (w == 4 || w == 8)                       { _gf_errno = GF_E_GR_W_48; return 0; }
    if (w == 16 && (arg1 != 4 || arg2 != 4))     { _gf_errno = GF_E_GR_W_16; return 0; }
    if (w == 128 && (arg1 != 4 || 
       (arg2 != 4 && arg2 != 8 && arg2 != 16))) { _gf_errno = GF_E_GR_128A; return 0; }
    if (arg1 > 27 || arg2 > 27)                 { _gf_errno = GF_E_GR_A_27; return 0; }
    if (arg1 > w || arg2 > w)                   { _gf_errno = GF_E_GR_AR_W; return 0; }
    if (raltmap || rsse || rnosse)              { _gf_errno = GF_E_GR____J; return 0; }
    return 1;
  }
  
  if (mult_type == GF_MULT_TABLE) {
    if (w != 16 && w >= 15)                     { _gf_errno = GF_E_TABLE_W; return 0; }
    if (w != 4 && (rsse || rnosse))             { _gf_errno = GF_E_TAB_SSE; return 0; }
    if (rsse && !sse3)                          { _gf_errno = GF_E_TABSSE3; return 0; }
    if (raltmap)                                { _gf_errno = GF_E_TAB_ALT; return 0; }
    return 1;
  }

  if (mult_type == GF_MULT_SPLIT_TABLE) {
    if (arg1 > arg2) {
      tmp = arg1;
      arg1 = arg2;
      arg2 = tmp;
    }
    if (w == 8) {
      if (arg1 != 4 || arg2 != 8)               { _gf_errno = GF_E_SP_8_AR; return 0; }
      if (rsse && !sse3)                        { _gf_errno = GF_E_SP_SSE3; return 0; }
      if (raltmap)                              { _gf_errno = GF_E_SP_8__A; return 0; }
    } else if (w == 16) {
      if (arg1 == 4 && arg2 == 16) {
        if (rsse && !sse3)                      { _gf_errno = GF_E_SP_SSE3; return 0; }
      } else if (arg1 == 8 && (arg2 == 16 || arg2 == 8)) {
        if (rsse || rnosse)                     { _gf_errno = GF_E_SP_16_S; return 0; }
        if (raltmap)                            { _gf_errno = GF_E_SP_16_A; return 0; }
      } else                                    { _gf_errno = GF_E_SP_16AR; return 0; }
    } else if (w == 32) {
      if ((arg1 == 8 && arg2 == 8) ||
          (arg1 == 8 && arg2 == 32) ||
          (arg1 == 16 && arg2 == 32)) {
        if (rsse || rnosse)                     { _gf_errno = GF_E_SP_32_S; return 0; }
        if (raltmap)                            { _gf_errno = GF_E_SP_32_A; return 0; }
      } else if ((arg1 == 4 && arg2 == 32) ||
          (arg1 == 4 && arg2 == 32)) {
        if (rsse && !sse3)                      { _gf_errno = GF_E_SP_SSE3; return 0; }
        if (raltmap && arg1 != 4)               { _gf_errno = GF_E_SP_32_A; return 0; }
        if (raltmap && !sse3)                   { _gf_errno = GF_E_SP_32AS; return 0; }
        if (raltmap && rnosse)                  { _gf_errno = GF_E_SP_32AS; return 0; }
      } else                                    { _gf_errno = GF_E_SP_32AR; return 0; }
    } else if (w == 64) {
      if ((arg1 == 8 && arg2 == 8) ||
          (arg1 == 8 && arg2 == 64) ||
          (arg1 == 16 && arg2 == 64)) {
        if (rsse || rnosse)                     { _gf_errno = GF_E_SP_64_S; return 0; }
        if (raltmap)                            { _gf_errno = GF_E_SP_64_A; return 0; }
      } else if (arg1 == 4 && arg2 == 64) {
        if (rsse && !sse3)                      { _gf_errno = GF_E_SP_SSE3; return 0; }
        if (raltmap && !sse3)                   { _gf_errno = GF_E_SP_64AS; return 0; }
        if (raltmap && rnosse)                  { _gf_errno = GF_E_SP_64AS; return 0; }
      } else                                    { _gf_errno = GF_E_SP_64AR; return 0; }
    } else if (w == 128) {
      if (arg1 == 8 && arg2 == 128) {
        if (rsse || rnosse)                     { _gf_errno = GF_E_SP128_S; return 0; }
        if (raltmap)                            { _gf_errno = GF_E_SP128_A; return 0; }
      } else if (arg1 == 4 && arg2 == 128) {
        if (rsse && !sse3)                      { _gf_errno = GF_E_SP_SSE3; return 0; }
        if (raltmap && !sse3)                   { _gf_errno = GF_E_SP128AS; return 0; }
        if (raltmap && rnosse)                  { _gf_errno = GF_E_SP128AS; return 0; }
      } else                                    { _gf_errno = GF_E_SP128AR; return 0; }
    } else                                      { _gf_errno = GF_E_SPLIT_W; return 0; }
    return 1;
  }

  if (mult_type == GF_MULT_COMPOSITE) {
    if (w != 8 && w != 16 && w != 32 
               && w != 64 && w != 128)          { _gf_errno = GF_E_COMP__W; return 0; }
    if ((poly >> (w/2)) != 0)                   { _gf_errno = GF_E_COMP_PP; return 0; }
    if (divide_type != GF_DIVIDE_DEFAULT)       { _gf_errno = GF_E_DIVCOMP; return 0; }
    if (arg1 != 2)                              { _gf_errno = GF_E_COMP_A2; return 0; }
    if (rsse || rnosse)                         { _gf_errno = GF_E_COMP_SS; return 0; }
    if (base != NULL) {
      sub = (gf_internal_t *) base->scratch;
      if (sub->w != w/2)                      { _gf_errno = GF_E_BASE__W; return 0; }
      if (poly == 0) {
        if (gf_composite_get_default_poly(base) == 0) { _gf_errno = GF_E_COMPXPP; return 0; }
      }
    }
    return 1;
  }

  _gf_errno = GF_E_UNKNOWN; 
  return 0;
}

int gf_scratch_size(int w, 
                    int mult_type, 
                    int region_type, 
                    int divide_type, 
                    int arg1, 
                    int arg2)
{
  if (gf_error_check(w, mult_type, region_type, divide_type, arg1, arg2, 0, NULL) == 0) return 0;

  switch(w) {
    case 4: return gf_w4_scratch_size(mult_type, region_type, divide_type, arg1, arg2);
    case 8: return gf_w8_scratch_size(mult_type, region_type, divide_type, arg1, arg2);
    case 16: return gf_w16_scratch_size(mult_type, region_type, divide_type, arg1, arg2);
    case 32: return gf_w32_scratch_size(mult_type, region_type, divide_type, arg1, arg2);
    case 64: return gf_w64_scratch_size(mult_type, region_type, divide_type, arg1, arg2);
    case 128: return gf_w128_scratch_size(mult_type, region_type, divide_type, arg1, arg2);
    default: return gf_wgen_scratch_size(w, mult_type, region_type, divide_type, arg1, arg2);
  }
}

extern int gf_size(gf_t *gf)
{
  gf_internal_t *h;
  int s;

  s = sizeof(gf_t);
  h = (gf_internal_t *) gf->scratch;
  s += gf_scratch_size(h->w, h->mult_type, h->region_type, h->divide_type, h->arg1, h->arg2);
  if (h->mult_type == GF_MULT_COMPOSITE) s += gf_size(h->base_gf);
  return s;
}


int gf_init_easy(gf_t *gf, int w)
{
  return gf_init_hard(gf, w, GF_MULT_DEFAULT, GF_REGION_DEFAULT, GF_DIVIDE_DEFAULT, 
                      0, 0, 0, NULL, NULL);
}

/* Allen: What's going on here is this function is putting info into the
       scratch mem of gf, and then calling the relevant REAL init
       func for the word size.  Probably done this way to consolidate
       those aspects of initialization that don't rely on word size,
       and then take care of word-size-specific stuff. */

int gf_init_hard(gf_t *gf, int w, int mult_type, 
                        int region_type,
                        int divide_type,
                        uint64_t prim_poly,
                        int arg1, int arg2,
                        gf_t *base_gf,
                        void *scratch_memory) 
{
  int sz;
  gf_internal_t *h;
 
  if (gf_error_check(w, mult_type, region_type, divide_type, 
                     arg1, arg2, prim_poly, base_gf) == 0) return 0;

  sz = gf_scratch_size(w, mult_type, region_type, divide_type, arg1, arg2);
  if (sz <= 0) return 0;  /* This shouldn't happen, as all errors should get caught
                             in gf_error_check() */
  
  if (scratch_memory == NULL) {
    h = (gf_internal_t *) malloc(sz);
    h->free_me = 1;
  } else {
    h = scratch_memory;
    h->free_me = 0;
  }
  gf->scratch = (void *) h;
  h->mult_type = mult_type;
  h->region_type = region_type;
  h->divide_type = divide_type;
  h->w = w;
  h->prim_poly = prim_poly;
  h->arg1 = arg1;
  h->arg2 = arg2;
  h->base_gf = base_gf;
  h->private = (void *) gf->scratch;
  h->private = (char*)h->private + (sizeof(gf_internal_t));
  h->sse = 0x00;
#ifdef INTEL_SSE2
  h->sse |= GF_SSE2;
#endif
#ifdef INTEL_SSSE3
  h->sse |= GF_SSSE3;
#endif
#ifdef INTEL_SSE4
  h->sse |= GF_SSE4;
#endif
#ifdef INTEL_SSE4_PCLMUL
  h->sse |= GF_SSE4_PCLMUL;
#endif
  gf->extract_word.w32 = NULL;

  switch(w) {
    case 4: return gf_w4_init(gf);
    case 8: return gf_w8_init(gf);
    case 16: return gf_w16_init(gf);
    case 32: return gf_w32_init(gf);
    case 64: return gf_w64_init(gf);
    case 128: return gf_w128_init(gf);
    default: return gf_wgen_init(gf);
  }
}

int gf_free(gf_t *gf, int recursive)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  if (recursive && h->base_gf != NULL) {
    gf_free(h->base_gf, 1);
    free(h->base_gf);
  }
  if (h->free_me) free(h);
  return 0; /* Making compiler happy */
}

void gf_alignment_error(char *s, int a)
{
  fprintf(stderr, "Alignment error in %s:\n", s);
  fprintf(stderr, "   The source and destination buffers must be aligned to each other,\n");
  fprintf(stderr, "   and they must be aligned to a %d-byte address.\n", a);
  exit(1);
}

static 
void gf_invert_binary_matrix(uint32_t *mat, uint32_t *inv, int rows) {
  int cols, i, j;
  uint32_t tmp;

  cols = rows;

  for (i = 0; i < rows; i++) inv[i] = (1 << i);

  /* First -- convert into upper triangular */

  for (i = 0; i < cols; i++) {

    /* Swap rows if we ave a zero i,i element.  If we can't swap, then the
       matrix was not invertible */

    if ((mat[i] & (1 << i)) == 0) {
      for (j = i+1; j < rows && (mat[j] & (1 << i)) == 0; j++) ;
      if (j == rows) {
        fprintf(stderr, "galois_invert_matrix: Matrix not invertible!!\n");
        exit(1);
      }
      tmp = mat[i]; mat[i] = mat[j]; mat[j] = tmp;
      tmp = inv[i]; inv[i] = inv[j]; inv[j] = tmp;
    }

    /* Now for each j>i, add A_ji*Ai to Aj */
    for (j = i+1; j != rows; j++) {
      if ((mat[j] & (1 << i)) != 0) {
        mat[j] ^= mat[i];
        inv[j] ^= inv[i];
      }
    }
  }

  /* Now the matrix is upper triangular.  Start at the top and multiply down */

  for (i = rows-1; i >= 0; i--) {
    for (j = 0; j < i; j++) {
      if (mat[j] & (1 << i)) {
        /*  mat[j] ^= mat[i]; */
        inv[j] ^= inv[i];
      }
    }
  }
}

uint32_t gf_bitmatrix_inverse(uint32_t y, int w, uint32_t pp) 
{
  uint32_t mat[32], inv[32], mask;
  int i;

  mask = (w == 32) ? 0xffffffff : (1 << w) - 1;
  for (i = 0; i < w; i++) {
    mat[i] = y;

    if (y & (1 << (w-1))) {
      y = y << 1;
      y = ((y ^ pp) & mask);
    } else {
      y = y << 1;
    }
  }

  gf_invert_binary_matrix(mat, inv, w);
  return inv[0];
}

void gf_two_byte_region_table_multiply(gf_region_data *rd, uint16_t *base)
{
  uint64_t a, prod;
  int xor;
  uint64_t *s64, *d64, *top;

  s64 = rd->s_start;
  d64 = rd->d_start;
  top = rd->d_top;
  xor = rd->xor;
  
  if (xor) {
    while (d64 != top) {
      a = *s64;
      prod = base[a >> 48];
      a <<= 16;
      prod <<= 16;
      prod ^= base[a >> 48];
      a <<= 16;
      prod <<= 16;
      prod ^= base[a >> 48];
      a <<= 16;
      prod <<= 16;
      prod ^= base[a >> 48];
      prod ^= *d64;
      *d64 = prod;
      s64++;
      d64++;
    }
  } else {
    while (d64 != top) {
      a = *s64;
      prod = base[a >> 48];
      a <<= 16;
      prod <<= 16;
      prod ^= base[a >> 48];
      a <<= 16;
      prod <<= 16;
      prod ^= base[a >> 48];
      a <<= 16;
      prod <<= 16;
      prod ^= base[a >> 48];
      *d64 = prod;
      s64++;
      d64++;
    }
  }
}

static void gf_slow_multiply_region(gf_region_data *rd, void *src, void *dest, void *s_top)
{
  uint8_t *s8, *d8;
  uint16_t *s16, *d16;
  uint32_t *s32, *d32;
  uint64_t *s64, *d64;
  gf_internal_t *h;
  int wb;
  uint32_t p, a;

  h = rd->gf->scratch;
  wb = (h->w)/8;
  if (wb == 0) wb = 1;
  
  while (src < s_top) {
    switch (h->w) {
    case 8:
      s8 = (uint8_t *) src;
      d8 = (uint8_t *) dest;
      *d8 = (rd->xor) ? (*d8 ^ rd->gf->multiply.w32(rd->gf, rd->val, *s8)) : 
                      rd->gf->multiply.w32(rd->gf, rd->val, *s8);
      break;
    case 4:
      s8 = (uint8_t *) src;
      d8 = (uint8_t *) dest;
      a = *s8;
      p = rd->gf->multiply.w32(rd->gf, rd->val, a&0xf);
      p |= (rd->gf->multiply.w32(rd->gf, rd->val, a >> 4) << 4);
      if (rd->xor) p ^= *d8;
      *d8 = p;
      break;
    case 16:
      s16 = (uint16_t *) src;
      d16 = (uint16_t *) dest;
      *d16 = (rd->xor) ? (*d16 ^ rd->gf->multiply.w32(rd->gf, rd->val, *s16)) : 
                      rd->gf->multiply.w32(rd->gf, rd->val, *s16);
      break;
    case 32:
      s32 = (uint32_t *) src;
      d32 = (uint32_t *) dest;
      *d32 = (rd->xor) ? (*d32 ^ rd->gf->multiply.w32(rd->gf, rd->val, *s32)) : 
                      rd->gf->multiply.w32(rd->gf, rd->val, *s32);
      break;
    case 64:
      s64 = (uint64_t *) src;
      d64 = (uint64_t *) dest;
      *d64 = (rd->xor) ? (*d64 ^ rd->gf->multiply.w64(rd->gf, rd->val, *s64)) : 
                      rd->gf->multiply.w64(rd->gf, rd->val, *s64);
      break;
    default:
      fprintf(stderr, "Error: gf_slow_multiply_region: w=%d not implemented.\n", h->w);
      exit(1);
    }
    src = (char*)src + wb;
    dest = (char*)dest + wb;
  }
}

/* JSP - The purpose of this procedure is to error check alignment,
   and to set up the region operation so that it can best leverage
   large words.

   It stores its information in rd.

   Assuming you're not doing Cauchy coding, (see below for that),
   then w will be 4, 8, 16, 32 or 64. It can't be 128 (probably
   should change that).

   src and dest must then be aligned on ceil(w/8)-byte boundaries.
   Moreover, bytes must be a multiple of ceil(w/8).  If the variable
   align is equal to ceil(w/8), then we will set s_start = src,
   d_start = dest, s_top to (src+bytes) and d_top to (dest+bytes).
   And we return -- the implementation will go ahead and do the
   multiplication on individual words (e.g. using discrete logs).

   If align is greater than ceil(w/8), then the implementation needs
   to work on groups of "align" bytes.  For example, suppose you are
   implementing BYTWO, without SSE. Then you will be doing the region
   multiplication in units of 8 bytes, so align = 8. Or, suppose you
   are doing a Quad table in GF(2^4). You will be doing the region
   multiplication in units of 2 bytes, so align = 2. Or, suppose you
   are doing split multiplication with SSE operations in GF(2^8).
   Then align = 16. Worse yet, suppose you are doing split
   multiplication with SSE operations in GF(2^16), with or without
   ALTMAP. Then, you will be doing the multiplication on 256 bits at
   a time.  So align = 32.

   When align does not equal ceil(w/8), we split the region
   multiplication into three parts.  We are going to make s_start be
   the first address greater than or equal to src that is a multiple
   of align.  s_top is going to be the largest address >= src+bytes
   such that (s_top - s_start) is a multiple of align.  We do the
   same with d_start and d_top.  When we say that "src and dest must
   be aligned with respect to each other, we mean that s_start-src
   must equal d_start-dest.

   Now, the region multiplication is done in three parts -- the part
   between src and s_start must be done using single words.
   Similarly, the part between s_top and src+bytes must also be done
   using single words.  The part between s_start and s_top will be
   done in chunks of "align" bytes.

   One final thing -- if align > 16, then s_start and d_start will be
   aligned on a 16 byte boundary.  Perhaps we should have two
   variables: align and chunksize.  Then we'd have s_start & d_start
   aligned to "align", and have s_top-s_start be a multiple of
   chunksize.  That may be less confusing, but it would be a big
   change.

   Finally, if align = -1, then we are doing Cauchy multiplication,
   using only XOR's.  In this case, we're not going to care about
   alignment because we are just doing XOR's.  Instead, the only
   thing we care about is that bytes must be a multiple of w.

   This is not to say that alignment doesn't matter in performance
   with XOR's.  See that discussion in gf_multby_one().

   After you call gf_set_region_data(), the procedure
   gf_do_initial_region_alignment() calls gf->multiply.w32() on
   everything between src and s_start.  The procedure
   gf_do_final_region_alignment() calls gf->multiply.w32() on
   everything between s_top and src+bytes.
   */

void gf_set_region_data(gf_region_data *rd,
  gf_t *gf,
  void *src,
  void *dest,
  int bytes,
  uint64_t val,
  int xor,
  int align)
{
  gf_internal_t *h = NULL;
  int wb;
  uint32_t a;
  unsigned long uls, uld;

  if (gf == NULL) {  /* JSP - Can be NULL if you're just doing XOR's */
    wb = 1;
  } else {
    h = gf->scratch;
    wb = (h->w)/8;
    if (wb == 0) wb = 1;
  }
  
  rd->gf = gf;
  rd->src = src;
  rd->dest = dest;
  rd->bytes = bytes;
  rd->val = val;
  rd->xor = xor;
  rd->align = align;

  uls = (unsigned long) src;
  uld = (unsigned long) dest;

  a = (align <= 16) ? align : 16;

  if (align == -1) { /* JSP: This is cauchy.  Error check bytes, then set up the pointers
                        so that there are no alignment regions. */
    if (h != NULL && bytes % h->w != 0) {
      fprintf(stderr, "Error in region multiply operation.\n");
      fprintf(stderr, "The size must be a multiple of %d bytes.\n", h->w);
      exit(1);
    }
  
    rd->s_start = src;
    rd->d_start = dest;
    rd->s_top = (char*)src + bytes;
    rd->d_top = (char*)src + bytes;
    return;
  }

  if (uls % a != uld % a) {
    fprintf(stderr, "Error in region multiply operation.\n");
    fprintf(stderr, "The source & destination pointers must be aligned with respect\n");
    fprintf(stderr, "to each other along a %d byte boundary.\n", a);
    fprintf(stderr, "Src = 0x%lx.  Dest = 0x%lx\n", (unsigned long) src,
            (unsigned long) dest);
    exit(1);
  }

  if (uls % wb != 0) {
    fprintf(stderr, "Error in region multiply operation.\n");
    fprintf(stderr, "The pointers must be aligned along a %d byte boundary.\n", wb);
    fprintf(stderr, "Src = 0x%lx.  Dest = 0x%lx\n", (unsigned long) src,
            (unsigned long) dest);
    exit(1);
  }

  if (bytes % wb != 0) {
    fprintf(stderr, "Error in region multiply operation.\n");
    fprintf(stderr, "The size must be a multiple of %d bytes.\n", wb);
    exit(1);
  }

  uls %= a;
  if (uls != 0) uls = (a-uls);
  rd->s_start = (char*)rd->src + uls;
  rd->d_start = (char*)rd->dest + uls;
  bytes -= uls;
  bytes -= (bytes % align);
  rd->s_top = (char*)rd->s_start + bytes;
  rd->d_top = (char*)rd->d_start + bytes;

}

void gf_do_initial_region_alignment(gf_region_data *rd)
{
  gf_slow_multiply_region(rd, rd->src, rd->dest, rd->s_start);
}

void gf_do_final_region_alignment(gf_region_data *rd)
{
  gf_slow_multiply_region(rd, rd->s_top, rd->d_top, (char*)rd->src+rd->bytes);
}

void gf_multby_zero(void *dest, int bytes, int xor) 
{
  if (xor) return;
  bzero(dest, bytes);
  return;
}

/* JSP - gf_multby_one tries to do this in the most efficient way
   possible.  If xor = 0, then simply call memcpy() since that
   should be optimized by the system.  Otherwise, try to do the xor
   in the following order:

   If src and dest are aligned with respect to each other on 16-byte
   boundaries and you have SSE instructions, then use aligned SSE
   instructions.

   If they aren't but you still have SSE instructions, use unaligned
   SSE instructions.

   If there are no SSE instructions, but they are aligned with
   respect to each other on 8-byte boundaries, then do them with
   uint64_t's.

   Otherwise, call gf_unaligned_xor(), which does the following:
   align a destination pointer along an 8-byte boundary, and then
   memcpy 32 bytes at a time from the src pointer to an array of
   doubles.  I'm not sure if that's the best -- probably needs
   testing, but this seems like it could be a black hole.
 */

static void gf_unaligned_xor(void *src, void *dest, int bytes);

void gf_multby_one(void *src, void *dest, int bytes, int xor) 
{
#ifdef   INTEL_SSE2
  __m128i ms, md;
#endif
  unsigned long uls, uld;
  uint8_t *s8, *d8;
  uint64_t *s64, *d64, *dtop64;
  gf_region_data rd;

  if (!xor) {
    memcpy(dest, src, bytes);
    return;
  }
  uls = (unsigned long) src;
  uld = (unsigned long) dest;

#ifdef   INTEL_SSE2
  int abytes;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;
  if (uls % 16 == uld % 16) {
    gf_set_region_data(&rd, NULL, src, dest, bytes, 1, xor, 16);
    while (s8 != rd.s_start) {
      *d8 ^= *s8;
      d8++;
      s8++;
    }
    while (s8 < (uint8_t *) rd.s_top) {
      ms = _mm_load_si128 ((__m128i *)(s8));
      md = _mm_load_si128 ((__m128i *)(d8));
      md = _mm_xor_si128(md, ms);
      _mm_store_si128((__m128i *)(d8), md);
      s8 += 16;
      d8 += 16;
    }
    while (s8 != (uint8_t *) src + bytes) {
      *d8 ^= *s8;
      d8++;
      s8++;
    }
    return;
  }

  abytes = (bytes & 0xfffffff0);

  while (d8 < (uint8_t *) dest + abytes) {
    ms = _mm_loadu_si128 ((__m128i *)(s8));
    md = _mm_loadu_si128 ((__m128i *)(d8));
    md = _mm_xor_si128(md, ms);
    _mm_storeu_si128((__m128i *)(d8), md);
    s8 += 16;
    d8 += 16;
  }
  while (d8 != (uint8_t *) dest+bytes) {
    *d8 ^= *s8;
    d8++;
    s8++;
  }
  return;
#endif

  if (uls % 8 != uld % 8) {
    gf_unaligned_xor(src, dest, bytes);
    return;
  }
  
  gf_set_region_data(&rd, NULL, src, dest, bytes, 1, xor, 8);
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;
  while (d8 != rd.d_start) {
    *d8 ^= *s8;
    d8++;
    s8++;
  }
  dtop64 = (uint64_t *) rd.d_top;

  d64 = (uint64_t *) rd.d_start;
  s64 = (uint64_t *) rd.s_start;

  while (d64 < dtop64) {
    *d64 ^= *s64;
    d64++;
    s64++;
  }

  s8 = (uint8_t *) rd.s_top;
  d8 = (uint8_t *) rd.d_top;

  while (d8 != (uint8_t *) dest+bytes) {
    *d8 ^= *s8;
    d8++;
    s8++;
  }
  return;
}

#define UNALIGNED_BUFSIZE (8)

static void gf_unaligned_xor(void *src, void *dest, int bytes)
{
  uint64_t scopy[UNALIGNED_BUFSIZE], *d64;
  int i;
  gf_region_data rd;
  uint8_t *s8, *d8;

  /* JSP - call gf_set_region_data(), but use dest in both places.  This is
     because I only want to set up dest.  If I used src, gf_set_region_data()
     would fail because src and dest are not aligned to each other wrt 
     8-byte pointers.  I know this will actually align d_start to 16 bytes.
     If I change gf_set_region_data() to split alignment & chunksize, then 
     I could do this correctly. */

  gf_set_region_data(&rd, NULL, dest, dest, bytes, 1, 1, 8*UNALIGNED_BUFSIZE);
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  while (d8 < (uint8_t *) rd.d_start) {
    *d8 ^= *s8;
    d8++;
    s8++;
  }
  
  d64 = (uint64_t *) d8;
  while (d64 < (uint64_t *) rd.d_top) {
    memcpy(scopy, s8, 8*UNALIGNED_BUFSIZE);
    s8 += 8*UNALIGNED_BUFSIZE;
    for (i = 0; i < UNALIGNED_BUFSIZE; i++) {
      *d64 ^= scopy[i];
      d64++;
    }
  }
  
  d8 = (uint8_t *) d64;
  while (d8 < (uint8_t *) ((char*)dest+bytes)) {
    *d8 ^= *s8;
    d8++;
    s8++;
  }
}
