/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w8.c
 *
 * Routines for 8-bit Galois fields
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

#define GF_FIELD_WIDTH (8)
#define GF_FIELD_SIZE       (1 << GF_FIELD_WIDTH)
#define GF_HALF_SIZE       (1 << (GF_FIELD_WIDTH/2))
#define GF_MULT_GROUP_SIZE       GF_FIELD_SIZE-1

#define GF_BASE_FIELD_WIDTH (4)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)

struct gf_w8_logtable_data {
    uint8_t         log_tbl[GF_FIELD_SIZE];
    uint8_t         antilog_tbl[GF_FIELD_SIZE * 2];
    uint8_t         inv_tbl[GF_FIELD_SIZE];
};

struct gf_w8_logzero_table_data {
    short           log_tbl[GF_FIELD_SIZE];  /* Make this signed, so that we can divide easily */
    uint8_t         antilog_tbl[512+512+1];
    uint8_t         *div_tbl;
    uint8_t         *inv_tbl;
};

struct gf_w8_logzero_small_table_data {
    short           log_tbl[GF_FIELD_SIZE];  /* Make this signed, so that we can divide easily */
    uint8_t         antilog_tbl[255*3];
    uint8_t         inv_tbl[GF_FIELD_SIZE];
    uint8_t         *div_tbl;
};

struct gf_w8_composite_data {
  uint8_t *mult_table;
};

/* Don't change the order of these relative to gf_w8_half_table_data */

struct gf_w8_default_data {
  uint8_t     high[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     low[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     divtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
  uint8_t     multtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_w8_half_table_data {
  uint8_t     high[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     low[GF_FIELD_SIZE][GF_HALF_SIZE];
};

struct gf_w8_single_table_data {
  uint8_t     divtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
  uint8_t     multtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_w8_double_table_data {
    uint8_t         div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t        mult[GF_FIELD_SIZE][GF_FIELD_SIZE*GF_FIELD_SIZE];
};

struct gf_w8_double_table_lazy_data {
    uint8_t         div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t         smult[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t        mult[GF_FIELD_SIZE*GF_FIELD_SIZE];
};

struct gf_w4_logtable_data {
    uint8_t         log_tbl[GF_BASE_FIELD_SIZE];
    uint8_t         antilog_tbl[GF_BASE_FIELD_SIZE * 2];
    uint8_t         *antilog_tbl_div;
};

struct gf_w4_single_table_data {
    uint8_t         div[GF_BASE_FIELD_SIZE][GF_BASE_FIELD_SIZE];
    uint8_t         mult[GF_BASE_FIELD_SIZE][GF_BASE_FIELD_SIZE];
};

struct gf_w8_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

#define AB2(ip, am1 ,am2, b, t1, t2) {\
  t1 = (b << 1) & am1;\
  t2 = b & am2; \
  t2 = ((t2 << 1) - (t2 >> (GF_FIELD_WIDTH-1))); \
  b = (t1 ^ (t2 & ip));}

#define SSE_AB2(pp, m1 ,m2, va, t1, t2) {\
          t1 = _mm_and_si128(_mm_slli_epi64(va, 1), m1); \
          t2 = _mm_and_si128(va, m2); \
          t2 = _mm_sub_epi64 (_mm_slli_epi64(t2, 1), _mm_srli_epi64(t2, (GF_FIELD_WIDTH-1))); \
          va = _mm_xor_si128(t1, _mm_and_si128(t2, pp)); }

#define MM_PRINT(s, r) { uint8_t blah[16], ii; printf("%-12s", s); _mm_storeu_si128((__m128i *)blah, r); for (ii = 0; ii < 16; ii += 2) printf("  %02x %02x", blah[15-ii], blah[14-ii]); printf("\n"); }

static
inline
uint32_t gf_w8_inverse_from_divide (gf_t *gf, uint32_t a)
{
  return gf->divide.w32(gf, 1, a);
}

static
inline
uint32_t gf_w8_divide_from_inverse (gf_t *gf, uint32_t a, uint32_t b)
{
  b = gf->inverse.w32(gf, b);
  return gf->multiply.w32(gf, a, b);
}

static
inline
uint32_t gf_w8_euclid (gf_t *gf, uint32_t b)
{
  uint32_t e_i, e_im1, e_ip1;
  uint32_t d_i, d_im1, d_ip1;
  uint32_t y_i, y_im1, y_ip1;
  uint32_t c_i;

  if (b == 0) return -1;
  e_im1 = ((gf_internal_t *) (gf->scratch))->prim_poly;
  e_i = b;
  d_im1 = 8;
  for (d_i = d_im1; ((1 << d_i) & e_i) == 0; d_i--) ;
  y_i = 1;
  y_im1 = 0;

  while (e_i != 1) {

    e_ip1 = e_im1;
    d_ip1 = d_im1;
    c_i = 0;

    while (d_ip1 >= d_i) {
      c_i ^= (1 << (d_ip1 - d_i));
      e_ip1 ^= (e_i << (d_ip1 - d_i));
      if (e_ip1 == 0) return 0;
      while ((e_ip1 & (1 << d_ip1)) == 0) d_ip1--;
    }

    y_ip1 = y_im1 ^ gf->multiply.w32(gf, c_i, y_i);
    y_im1 = y_i;
    y_i = y_ip1;

    e_im1 = e_i;
    d_im1 = d_i;
    e_i = e_ip1;
    d_i = d_ip1;
  }

  return y_i;
}

static
gf_val_32_t gf_w8_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint8_t *r8;

  r8 = (uint8_t *) start;
  return r8[index];
}

static
gf_val_32_t gf_w8_composite_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  int sub_size;
  gf_internal_t *h;
  uint8_t *r8, *top;
  uint8_t a, b;
  gf_region_data rd;

  h = (gf_internal_t *) gf->scratch;
  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 32);
  r8 = (uint8_t *) start;
  if (r8 + index < (uint8_t *) rd.d_start) return r8[index];
  if (r8 + index >= (uint8_t *) rd.d_top) return r8[index];
  index -= (((uint8_t *) rd.d_start) - r8);
  r8 = (uint8_t *) rd.d_start;
  top = (uint8_t *) rd.d_top;
  sub_size = (top-r8)/2;

  a = h->base_gf->extract_word.w32(h->base_gf, r8, sub_size, index);
  b = h->base_gf->extract_word.w32(h->base_gf, r8+sub_size, sub_size, index);
  return (a | (b << 4));
}

static
inline
uint32_t gf_w8_matrix (gf_t *gf, uint32_t b)
{
  return gf_bitmatrix_inverse(b, 8, ((gf_internal_t *) (gf->scratch))->prim_poly);
}


static
inline
gf_val_32_t
gf_w8_clm_multiply_2 (gf_t *gf, gf_val_32_t a8, gf_val_32_t b8)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a8, 0);
  b = _mm_insert_epi32 (a, b8, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffULL));

  /* Do the initial multiply */

  result = _mm_clmulepi64_si128 (a, b, 0);

  /* Ben: Do prim_poly reduction twice. We are guaranteed that we will only
     have to do the reduction at most twice, because (w-2)/z == 2. Where
     z is equal to the number of zeros after the leading 1

     _mm_clmulepi64_si128 is the carryless multiply operation. Here
     _mm_srli_si128 shifts the result to the right by 1 byte. This allows
     us to multiply the prim_poly by the leading bits of the result. We
     then xor the result of that operation back with the result.*/

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));

#endif
  return rv;
}

static
inline
gf_val_32_t
gf_w8_clm_multiply_3 (gf_t *gf, gf_val_32_t a8, gf_val_32_t b8)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a8, 0);
  b = _mm_insert_epi32 (a, b8, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));

#endif
  return rv;
}

static
inline
gf_val_32_t
gf_w8_clm_multiply_4 (gf_t *gf, gf_val_32_t a8, gf_val_32_t b8)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a8, 0);
  b = _mm_insert_epi32 (a, b8, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));

#endif
  return rv;
}


static
void
gf_w8_multiply_region_from_single(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int
    xor)
{
  gf_region_data rd;
  uint8_t *s8;
  uint8_t *d8;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 1);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  if (xor) {
    while (d8 < ((uint8_t *) rd.d_top)) {
      *d8 ^= gf->multiply.w32(gf, val, *s8);
      d8++;
      s8++;
    }
  } else {
    while (d8 < ((uint8_t *) rd.d_top)) {
      *d8 = gf->multiply.w32(gf, val, *s8);
      d8++;
      s8++;
    }
  }
  gf_do_final_region_alignment(&rd);
}

#if defined(INTEL_SSE4_PCLMUL)
static
void
gf_w8_clm_multiply_region_from_single_2(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int
    xor)
{
  gf_region_data rd;
  uint8_t *s8;
  uint8_t *d8;

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 1);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  if (xor) {
    while (d8 < ((uint8_t *) rd.d_top)) {
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s8), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      *d8 ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d8++;
      s8++;
    }
  } else {
    while (d8 < ((uint8_t *) rd.d_top)) {
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s8), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      *d8 = ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d8++;
      s8++;
    }
  }
  gf_do_final_region_alignment(&rd);
}
#endif

#if defined(INTEL_SSE4_PCLMUL)
static
void
gf_w8_clm_multiply_region_from_single_3(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int
    xor)
{
  gf_region_data rd;
  uint8_t *s8;
  uint8_t *d8;

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 1);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  if (xor) {
    while (d8 < ((uint8_t *) rd.d_top)) {
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s8), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      *d8 ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d8++;
      s8++;
    }
  } else {
    while (d8 < ((uint8_t *) rd.d_top)) {
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s8), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      *d8 = ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d8++;
      s8++;
    }
  }
  gf_do_final_region_alignment(&rd);
}
#endif

#if defined(INTEL_SSE4_PCLMUL)
static
void
gf_w8_clm_multiply_region_from_single_4(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int
    xor)
{
  gf_region_data rd;
  uint8_t *s8;
  uint8_t *d8;

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 1);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  if (xor) {
    while (d8 < ((uint8_t *) rd.d_top)) {
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s8), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      *d8 ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d8++;
      s8++;
    }
  } else {
    while (d8 < ((uint8_t *) rd.d_top)) {
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s8), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 1), 0);
      result = _mm_xor_si128 (result, w);
      *d8 = ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d8++;
      s8++;
    }
  }
  gf_do_final_region_alignment(&rd);
}
#endif

/* ------------------------------------------------------------
IMPLEMENTATION: SHIFT:

JSP: The world's dumbest multiplication algorithm.  I only
include it for completeness.  It does have the feature that it requires no
extra memory.  
 */

static
inline
  uint32_t
gf_w8_shift_multiply (gf_t *gf, uint32_t a8, uint32_t b8)
{
  uint16_t product, i, pp, a, b;
  gf_internal_t *h;

  a = a8;
  b = b8;
  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  product = 0;

  for (i = 0; i < GF_FIELD_WIDTH; i++) { 
    if (a & (1 << i)) product ^= (b << i);
  }
  for (i = (GF_FIELD_WIDTH*2-2); i >= GF_FIELD_WIDTH; i--) {
    if (product & (1 << i)) product ^= (pp << (i-GF_FIELD_WIDTH)); 
  }
  return product;
}

static 
int gf_w8_cfm_init(gf_t *gf)
{ 
#if defined(INTEL_SSE4_PCLMUL)
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

    if ((0xe0 & h->prim_poly) == 0){
      gf->multiply.w32 = gf_w8_clm_multiply_2;
      gf->multiply_region.w32 = gf_w8_clm_multiply_region_from_single_2;
    }else if ((0xc0 & h->prim_poly) == 0){
      gf->multiply.w32 = gf_w8_clm_multiply_3;
      gf->multiply_region.w32 = gf_w8_clm_multiply_region_from_single_3;
    }else if ((0x80 & h->prim_poly) == 0){ 
      gf->multiply.w32 = gf_w8_clm_multiply_4;
      gf->multiply_region.w32 = gf_w8_clm_multiply_region_from_single_4;
    }else{
      return 0;
    }
  return 1;
#endif

  return 0;

}

static 
int gf_w8_shift_init(gf_t *gf)
{ 
  gf->multiply.w32 = gf_w8_shift_multiply;  /* The others will be set automatically */
  return 1;
}

/* ------------------------------------------------------------
IMPLEMENTATION: LOG_TABLE:

JSP: Kevin wrote this, and I'm converting it to my structure.
*/

static
inline
  uint32_t
gf_w8_logzero_multiply (gf_t *gf, uint32_t a, uint32_t b)
{
  struct gf_w8_logzero_table_data *ltd;

  ltd = (struct gf_w8_logzero_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return ltd->antilog_tbl[ltd->log_tbl[a] + ltd->log_tbl[b]];
}

static
inline
  uint32_t
gf_w8_logzero_divide (gf_t *gf, uint32_t a, uint32_t b)
{
  struct gf_w8_logzero_table_data *ltd;

  ltd = (struct gf_w8_logzero_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return ltd->div_tbl[ltd->log_tbl[a] - ltd->log_tbl[b]];
}

static
inline
  uint32_t
gf_w8_logzero_small_multiply (gf_t *gf, uint32_t a, uint32_t b)
{
  struct gf_w8_logzero_small_table_data *std;

  std = (struct gf_w8_logzero_small_table_data *) ((gf_internal_t *) gf->scratch)->private;
  if (b == 0) return 0;
  return std->antilog_tbl[std->log_tbl[a] + std->log_tbl[b]];
}

static
inline
  uint32_t
gf_w8_logzero_small_divide (gf_t *gf, uint32_t a, uint32_t b)
{
  struct gf_w8_logzero_small_table_data *std;

  std = (struct gf_w8_logzero_small_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return std->div_tbl[std->log_tbl[a] - std->log_tbl[b]];
}

static
inline
  uint32_t
gf_w8_log_multiply (gf_t *gf, uint32_t a, uint32_t b)
{
  struct gf_w8_logtable_data *ltd;

  ltd = (struct gf_w8_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  return (a == 0 || b == 0) ? 0 : ltd->antilog_tbl[(unsigned)(ltd->log_tbl[a] + ltd->log_tbl[b])];
}

static
inline
  uint32_t
gf_w8_log_divide (gf_t *gf, uint32_t a, uint32_t b)
{
  int log_sum = 0;
  struct gf_w8_logtable_data *ltd;

  if (a == 0 || b == 0) return 0;
  ltd = (struct gf_w8_logtable_data *) ((gf_internal_t *) gf->scratch)->private;

  log_sum = ltd->log_tbl[a] - ltd->log_tbl[b] + (GF_MULT_GROUP_SIZE);
  return (ltd->antilog_tbl[log_sum]);
}

static
  uint32_t
gf_w8_log_inverse (gf_t *gf, uint32_t a)
{
  struct gf_w8_logtable_data *ltd;

  ltd = (struct gf_w8_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ltd->inv_tbl[a]);
}

static
  uint32_t
gf_w8_logzero_inverse (gf_t *gf, uint32_t a)
{
  struct gf_w8_logzero_table_data *ltd;

  ltd = (struct gf_w8_logzero_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ltd->inv_tbl[a]);
}

static
  uint32_t
gf_w8_logzero_small_inverse (gf_t *gf, uint32_t a)
{
  struct gf_w8_logzero_small_table_data *std;

  std = (struct gf_w8_logzero_small_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (std->inv_tbl[a]);
}

static
  void
gf_w8_log_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  int i;
  uint8_t lv;
  uint8_t *s8, *d8;
  struct gf_w8_logtable_data *ltd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  ltd = (struct gf_w8_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  lv = ltd->log_tbl[val];

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d8[i] ^= (s8[i] == 0 ? 0 : ltd->antilog_tbl[lv + ltd->log_tbl[s8[i]]]);
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d8[i] = (s8[i] == 0 ? 0 : ltd->antilog_tbl[lv + ltd->log_tbl[s8[i]]]);
    }
  }
}

static
  void
gf_w8_logzero_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  int i;
  uint8_t lv;
  uint8_t *s8, *d8;
  struct gf_w8_logzero_table_data *ltd;
  struct gf_w8_logzero_small_table_data *std;
  short *log;
  uint8_t *alt;
  gf_internal_t *h;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;

  if (h->arg1 == 1) {
    std = (struct gf_w8_logzero_small_table_data *) h->private;
    log = std->log_tbl;
    alt = std->antilog_tbl;
  } else {
    ltd = (struct gf_w8_logzero_table_data *) h->private;
    log = ltd->log_tbl;
    alt = ltd->antilog_tbl;
  }
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  lv = log[val];

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d8[i] ^= (alt[lv + log[s8[i]]]);
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d8[i] = (alt[lv + log[s8[i]]]);
    }
  }
}

  static
int gf_w8_log_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_logtable_data *ltd = NULL;
  struct gf_w8_logzero_table_data *ztd = NULL;
  struct gf_w8_logzero_small_table_data *std = NULL;
  uint8_t *alt;
  uint8_t *inv;
  int i, b;
  int check = 0;

  h = (gf_internal_t *) gf->scratch;
  if (h->mult_type == GF_MULT_LOG_TABLE) {
    ltd = h->private;
    alt = ltd->antilog_tbl;
    inv = ltd->inv_tbl;
  } else if (h->mult_type == GF_MULT_LOG_ZERO) {
    std = h->private;
    alt = std->antilog_tbl;
    std->div_tbl = (alt + 255);
    inv = std->inv_tbl;
  } else {
    ztd = h->private;
    alt = ztd->antilog_tbl;
    ztd->inv_tbl = (alt + 512 + 256);
    ztd->div_tbl = (alt + 255);
    inv = ztd->inv_tbl;
  }

  for (i = 0; i < GF_MULT_GROUP_SIZE+1; i++) {
    if (h->mult_type == GF_MULT_LOG_TABLE)
      ltd->log_tbl[i] = 0;
    else if (h->mult_type == GF_MULT_LOG_ZERO)
      std->log_tbl[i] = 0;
    else
      ztd->log_tbl[i] = 0;
  }

  if (h->mult_type == GF_MULT_LOG_TABLE) {
    ltd->log_tbl[0] = 0;
  } else if (h->mult_type == GF_MULT_LOG_ZERO) {
    std->log_tbl[0] = 510;
  } else {
    ztd->log_tbl[0] = 512;
  }

  b = 1;
  for (i = 0; i < GF_MULT_GROUP_SIZE; i++) {
    if (h->mult_type == GF_MULT_LOG_TABLE) {
      if (ltd->log_tbl[b] != 0) check = 1;
      ltd->log_tbl[b] = i;
    } else if (h->mult_type == GF_MULT_LOG_ZERO) {
      if (std->log_tbl[b] != 0) check = 1;
      std->log_tbl[b] = i;
    } else {
      if (ztd->log_tbl[b] != 0) check = 1;
      ztd->log_tbl[b] = i;
    }
    alt[i] = b;
    alt[i+GF_MULT_GROUP_SIZE] = b;
    b <<= 1;
    if (b & GF_FIELD_SIZE) {
      b = b ^ h->prim_poly;
    }
  }
  if (check) {
    _gf_errno = GF_E_LOGPOLY;
    return 0;
  }

  if (h->mult_type == GF_MULT_LOG_ZERO) bzero(alt+510, 255);

  if (h->mult_type == GF_MULT_LOG_ZERO_EXT) {
    bzero(alt+512, 255);
    alt[512+512] = 0;
  }

  inv[0] = 0;  /* Not really, but we need to fill it with something  */
  i = 1;
  b = GF_MULT_GROUP_SIZE;
  do {
    inv[i] = alt[b];
    i <<= 1;
    if (i & (1 << 8)) i ^= h->prim_poly;
    b--;
  } while (i != 1);

  if (h->mult_type == GF_MULT_LOG_TABLE) {
    gf->inverse.w32 = gf_w8_log_inverse;
    gf->divide.w32 = gf_w8_log_divide;
    gf->multiply.w32 = gf_w8_log_multiply;
    gf->multiply_region.w32 = gf_w8_log_multiply_region;
  } else if (h->mult_type == GF_MULT_LOG_ZERO) {
    gf->inverse.w32 = gf_w8_logzero_small_inverse;
    gf->divide.w32 = gf_w8_logzero_small_divide;
    gf->multiply.w32 = gf_w8_logzero_small_multiply;
    gf->multiply_region.w32 = gf_w8_logzero_multiply_region;
  } else {
    gf->inverse.w32 = gf_w8_logzero_inverse;
    gf->divide.w32 = gf_w8_logzero_divide;
    gf->multiply.w32 = gf_w8_logzero_multiply;
    gf->multiply_region.w32 = gf_w8_logzero_multiply_region;
  }
  return 1;
}

/* ------------------------------------------------------------
IMPLEMENTATION: FULL_TABLE:

JSP: Kevin wrote this, and I'm converting it to my structure.
 */

static
  gf_val_32_t
gf_w8_table_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_single_table_data *ftd;

  ftd = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->multtable[a][b]);
}

static
  gf_val_32_t
gf_w8_table_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_single_table_data *ftd;

  ftd = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->divtable[a][b]);
}

static
  gf_val_32_t
gf_w8_default_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_default_data *ftd;

  ftd = (struct gf_w8_default_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->multtable[a][b]);
}

#ifdef INTEL_SSSE3
static
  gf_val_32_t
gf_w8_default_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_default_data *ftd;

  ftd = (struct gf_w8_default_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->divtable[a][b]);
}
#endif

static
  gf_val_32_t
gf_w8_double_table_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_double_table_data *ftd;

  ftd = (struct gf_w8_double_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->mult[a][b]);
}

static
  gf_val_32_t
gf_w8_double_table_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_double_table_data *ftd;

  ftd = (struct gf_w8_double_table_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->div[a][b]);
}

static
  void
gf_w8_double_table_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint16_t *base;
  uint32_t b, c, vc, vb;
  gf_internal_t *h;
  struct gf_w8_double_table_data  *dtd;
  struct gf_w8_double_table_lazy_data  *ltd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) (gf->scratch);
  if (h->region_type & GF_REGION_LAZY) {
    ltd = (struct gf_w8_double_table_lazy_data *) h->private;
    base = ltd->mult;
    for (b = 0; b < GF_FIELD_SIZE; b++) {
      vb = (ltd->smult[val][b] << 8);
      for (c = 0; c < GF_FIELD_SIZE; c++) {
        vc = ltd->smult[val][c];
        base[(b << 8)| c] = (vb | vc);
      }
    }

  } else {
    dtd = (struct gf_w8_double_table_data *) h->private;
    base = &(dtd->mult[val][0]);
  }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);
  gf_two_byte_region_table_multiply(&rd, base);
  gf_do_final_region_alignment(&rd);
}

static
  gf_val_32_t
gf_w8_double_table_lazy_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_double_table_lazy_data *ftd;

  ftd = (struct gf_w8_double_table_lazy_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->smult[a][b]);
}

static
  gf_val_32_t
gf_w8_double_table_lazy_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_double_table_lazy_data *ftd;

  ftd = (struct gf_w8_double_table_lazy_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ftd->div[a][b]);
}

static
  void
gf_w8_table_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8;
  struct gf_w8_single_table_data *ftd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  ftd = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d8[i] ^= ftd->multtable[s8[i]][val];
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d8[i] = ftd->multtable[s8[i]][val];
    }
  }
}

#ifdef INTEL_SSSE3
static
  void
gf_w8_split_multiply_region_sse(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint8_t *bh, *bl, *sptr, *dptr;
  __m128i  loset, t1, r, va, mth, mtl;
  struct gf_w8_half_table_data *htd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  htd = (struct gf_w8_half_table_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  bh = (uint8_t *) htd->high;
  bh += (val << 4);
  bl = (uint8_t *) htd->low;
  bl += (val << 4);

  sptr = rd.s_start;
  dptr = rd.d_start;

  mth = _mm_loadu_si128 ((__m128i *)(bh));
  mtl = _mm_loadu_si128 ((__m128i *)(bl));
  loset = _mm_set1_epi8 (0x0f);

  if (xor) {
    while (sptr < (uint8_t *) rd.s_top) {
      va = _mm_load_si128 ((__m128i *)(sptr));
      t1 = _mm_and_si128 (loset, va);
      r = _mm_shuffle_epi8 (mtl, t1);
      va = _mm_srli_epi64 (va, 4);
      t1 = _mm_and_si128 (loset, va);
      r = _mm_xor_si128 (r, _mm_shuffle_epi8 (mth, t1));
      va = _mm_load_si128 ((__m128i *)(dptr));
      r = _mm_xor_si128 (r, va);
      _mm_store_si128 ((__m128i *)(dptr), r);
      dptr += 16;
      sptr += 16;
    }
  } else {
    while (sptr < (uint8_t *) rd.s_top) {
      va = _mm_load_si128 ((__m128i *)(sptr));
      t1 = _mm_and_si128 (loset, va);
      r = _mm_shuffle_epi8 (mtl, t1);
      va = _mm_srli_epi64 (va, 4);
      t1 = _mm_and_si128 (loset, va);
      r = _mm_xor_si128 (r, _mm_shuffle_epi8 (mth, t1));
      _mm_store_si128 ((__m128i *)(dptr), r);
      dptr += 16;
      sptr += 16;
    }
  }

  gf_do_final_region_alignment(&rd);
}
#endif


/* ------------------------------------------------------------
IMPLEMENTATION: FULL_TABLE:
 */

static
  gf_val_32_t
gf_w8_split_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w8_half_table_data *htd;
  htd = (struct gf_w8_half_table_data *) ((gf_internal_t *) gf->scratch)->private;

  return htd->high[b][a>>4] ^ htd->low[b][a&0xf];
}

static
  void
gf_w8_split_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8;
  struct gf_w8_half_table_data *htd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  htd = (struct gf_w8_half_table_data *) ((gf_internal_t *) gf->scratch)->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d8[i] ^= (htd->high[val][s8[i]>>4] ^ htd->low[val][s8[i]&0xf]);
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d8[i] = (htd->high[val][s8[i]>>4] ^ htd->low[val][s8[i]&0xf]);
    }
  }
}


  static
int gf_w8_split_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_half_table_data *htd;
  int a, b;

  h = (gf_internal_t *) gf->scratch;
  htd = (struct gf_w8_half_table_data *)h->private;

  bzero(htd->high, sizeof(uint8_t)*GF_FIELD_SIZE*GF_HALF_SIZE);
  bzero(htd->low, sizeof(uint8_t)*GF_FIELD_SIZE*GF_HALF_SIZE);

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    for (b = 1; b < GF_HALF_SIZE; b++) {
      htd->low[a][b] = gf_w8_shift_multiply(gf,a,b);
      htd->high[a][b] = gf_w8_shift_multiply(gf,a,b<<4);
    }
  }

  gf->multiply.w32 = gf_w8_split_multiply;
  
  #ifdef INTEL_SSSE3
    if (h->region_type & GF_REGION_NOSSE)
      gf->multiply_region.w32 = gf_w8_split_multiply_region;
    else
      gf->multiply_region.w32 = gf_w8_split_multiply_region_sse;
  #else
    gf->multiply_region.w32 = gf_w8_split_multiply_region;
    if(h->region_type & GF_REGION_SSE)
      return 0;
  #endif

  return 1;
}

/* JSP: This is disgusting, but it is what it is.  If there is no SSE,
   then the default is equivalent to single table.  If there is SSE, then
   we use the "gf_w8_default_data" which is a hybrid of SPLIT & TABLE. */
   
static
int gf_w8_table_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_single_table_data *ftd = NULL;
  struct gf_w8_double_table_data *dtd = NULL;
  struct gf_w8_double_table_lazy_data *ltd = NULL;
  struct gf_w8_default_data *dd = NULL;
  int a, b, c, prod, scase, issse;

  h = (gf_internal_t *) gf->scratch;

  issse = 0;
#ifdef INTEL_SSSE3
  issse = 1;
#endif

  if (h->mult_type == GF_MULT_DEFAULT && issse) {
    dd = (struct gf_w8_default_data *)h->private;
    scase = 3;
    bzero(dd->high, sizeof(uint8_t) * GF_FIELD_SIZE * GF_HALF_SIZE);
    bzero(dd->low, sizeof(uint8_t) * GF_FIELD_SIZE * GF_HALF_SIZE);
    bzero(dd->divtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    bzero(dd->multtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
  } else if (h->mult_type == GF_MULT_DEFAULT || 
             h->region_type == 0 || (h->region_type & GF_REGION_CAUCHY)) {
    ftd = (struct gf_w8_single_table_data *)h->private;
    bzero(ftd->divtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    bzero(ftd->multtable, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    scase = 0;
  } else if (h->region_type == GF_REGION_DOUBLE_TABLE) {
    dtd = (struct gf_w8_double_table_data *)h->private;
    bzero(dtd->div, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    bzero(dtd->mult, sizeof(uint16_t) * GF_FIELD_SIZE * GF_FIELD_SIZE * GF_FIELD_SIZE);
    scase = 1;
  } else if (h->region_type == (GF_REGION_DOUBLE_TABLE | GF_REGION_LAZY)) {
    ltd = (struct gf_w8_double_table_lazy_data *)h->private;
    bzero(ltd->div, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    bzero(ltd->smult, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
    scase = 2;
  } else {
    fprintf(stderr, "Internal error in gf_w8_table_init\n");
    exit(0);
  }

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    for (b = 1; b < GF_FIELD_SIZE; b++) {
      prod = gf_w8_shift_multiply(gf,a,b);
      switch (scase) {
        case 0: 
          ftd->multtable[a][b] = prod;
          ftd->divtable[prod][b] = a;
          break;
        case 1:
          dtd->div[prod][b] = a;
          for (c = 0; c < GF_FIELD_SIZE; c++) {
            dtd->mult[a][(c<<8)|b] |= prod;
            dtd->mult[a][(b<<8)|c] |= (prod<<8);
          }
          break;
        case 2:
          ltd->div[prod][b] = a;
          ltd->smult[a][b] = prod;
          break;
        case 3:
          dd->multtable[a][b] = prod;
          dd->divtable[prod][b] = a;
          if ((b & 0xf) == b) { dd->low[a][b] = prod; }
          if ((b & 0xf0) == b) { dd->high[a][b>>4] = prod; }
          break;
      }
    }
  }

  gf->inverse.w32 = NULL; /* Will set from divide */
  switch (scase) {
    case 0: 
      gf->divide.w32 = gf_w8_table_divide;
      gf->multiply.w32 = gf_w8_table_multiply;
      gf->multiply_region.w32 = gf_w8_table_multiply_region;
      break;
    case 1:
      gf->divide.w32 = gf_w8_double_table_divide;
      gf->multiply.w32 = gf_w8_double_table_multiply;
      gf->multiply_region.w32 = gf_w8_double_table_multiply_region;
      break;
    case 2:
      gf->divide.w32 = gf_w8_double_table_lazy_divide;
      gf->multiply.w32 = gf_w8_double_table_lazy_multiply;
      gf->multiply_region.w32 = gf_w8_double_table_multiply_region;
      break;
    case 3:
#ifdef INTEL_SSSE3
      gf->divide.w32 = gf_w8_default_divide;
      gf->multiply.w32 = gf_w8_default_multiply;
      gf->multiply_region.w32 = gf_w8_split_multiply_region_sse;
#endif
      break;
  }
  return 1;
}

static
  void
gf_w8_composite_multiply_region_alt(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t val0 = val & 0x0f;
  uint8_t val1 = (val & 0xf0) >> 4;
  gf_region_data rd;
  int sub_reg_size;

  if (val == 0) {
    if (xor) return;
    bzero(dest, bytes);
    return;
  }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);

  sub_reg_size = ((char*)rd.d_top - (char*)rd.d_start) / 2;

  base_gf->multiply_region.w32(base_gf, rd.s_start, rd.d_start, val0, sub_reg_size, xor);
  base_gf->multiply_region.w32(base_gf, (char*)rd.s_start+sub_reg_size, rd.d_start, val1, sub_reg_size, 1);
  base_gf->multiply_region.w32(base_gf, rd.s_start, (char*)rd.d_start+sub_reg_size, val1, sub_reg_size, xor);
  base_gf->multiply_region.w32(base_gf, (char*)rd.s_start+sub_reg_size, (char*)rd.d_start+sub_reg_size, val0, sub_reg_size, 1);
  base_gf->multiply_region.w32(base_gf, (char*)rd.s_start+sub_reg_size, (char*)rd.d_start+sub_reg_size, base_gf->multiply.w32(base_gf, h->prim_poly, val1), sub_reg_size, 1);

   gf_do_final_region_alignment(&rd);
}

static
gf_val_32_t
gf_w8_composite_multiply_recursive(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t b0 = b & 0x0f; 
  uint8_t b1 = (b & 0xf0) >> 4; 
  uint8_t a0 = a & 0x0f; 
  uint8_t a1 = (a & 0xf0) >> 4; 
  uint8_t a1b1;

  a1b1 = base_gf->multiply.w32(base_gf, a1, b1);

  return ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) | 
          ((base_gf->multiply.w32(base_gf, a1, b0) ^ 
           base_gf->multiply.w32(base_gf, a0, b1) ^ 
           base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 4));
}

static
gf_val_32_t
gf_w8_composite_multiply_inline(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  uint8_t b0 = b & 0x0f; 
  uint8_t b1 = (b & 0xf0) >> 4; 
  uint8_t a0 = a & 0x0f; 
  uint8_t a1 = (a & 0xf0) >> 4; 
  uint8_t a1b1, *mt;
  struct gf_w8_composite_data *cd;

  cd = (struct gf_w8_composite_data *) h->private;
  mt = cd->mult_table;

  a1b1 = GF_W4_INLINE_MULTDIV(mt, a1, b1);

  return ((GF_W4_INLINE_MULTDIV(mt, a0, b0) ^ a1b1) | 
          ((GF_W4_INLINE_MULTDIV(mt, a1, b0) ^ 
           GF_W4_INLINE_MULTDIV(mt, a0, b1) ^ 
           GF_W4_INLINE_MULTDIV(mt, a1b1, h->prim_poly)) << 4));
}

/*
 * Composite field division trick (explained in 2007 tech report) 
 *
 * Compute a / b = a*b^-1, where p(x) = x^2 + sx + 1 
 * 
 * let c = b^-1
 *
 * c*b = (s*b1c1+b1c0+b0c1)x+(b1c1+b0c0)
 * 
 * want (s*b1c1+b1c0+b0c1) = 0 and (b1c1+b0c0) = 1 
 *
 * let d = b1c1 and d+1 = b0c0
 *
 * solve s*b1c1+b1c0+b0c1 = 0
 *
 * solution: d = (b1b0^-1)(b1b0^-1+b0b1^-1+s)^-1
 *
 * c0 = (d+1)b0^-1
 * c1 = d*b1^-1
 * 
 * a / b = a * c
 */

static
gf_val_32_t
gf_w8_composite_inverse(gf_t *gf, gf_val_32_t a)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t a0 = a & 0x0f; 
  uint8_t a1 = (a & 0xf0) >> 4; 
  uint8_t c0, c1, c, d, tmp;
  uint8_t a0inv, a1inv; 

  if (a0 == 0) {
    a1inv = base_gf->inverse.w32(base_gf, a1) & 0xf;
    c0 = base_gf->multiply.w32(base_gf, a1inv, h->prim_poly);
    c1 = a1inv;
  } else if (a1 == 0) {
    c0 = base_gf->inverse.w32(base_gf, a0);
    c1 = 0;
  } else {
    a1inv = base_gf->inverse.w32(base_gf, a1) & 0xf;
    a0inv = base_gf->inverse.w32(base_gf, a0) & 0xf;

    d = base_gf->multiply.w32(base_gf, a1, a0inv) & 0xf;

    tmp = (base_gf->multiply.w32(base_gf, a1, a0inv) ^ base_gf->multiply.w32(base_gf, a0, a1inv) ^ h->prim_poly) & 0xf;
    tmp = base_gf->inverse.w32(base_gf, tmp) & 0xf;

    d = base_gf->multiply.w32(base_gf, d, tmp) & 0xf;

    c0 = base_gf->multiply.w32(base_gf, (d^1), a0inv) & 0xf; 
    c1 = base_gf->multiply.w32(base_gf, d, a1inv) & 0xf; 
  }

  c = c0 | (c1 << 4);

  return c;
}

static
void
gf_w8_composite_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t b0 = val & 0x0f; 
  uint8_t b1 = (val & 0xf0) >> 4; 
  uint8_t *s8;
  uint8_t *d8; 
  uint8_t *mt;
  uint8_t a0, a1, a1b1;
  struct gf_w8_composite_data *cd;

  cd = (struct gf_w8_composite_data *) h->private;

  if (val == 0) {
    if (xor) return;
    bzero(dest, bytes);
    return;
  }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 1);
  gf_do_initial_region_alignment(&rd);
  
  
  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  mt = cd->mult_table;
  if (mt == NULL) {
    if (xor) {
      while (d8 < (uint8_t *) rd.d_top) {
        a0 = *s8 & 0x0f; 
        a1 = (*s8 & 0xf0) >> 4; 
        a1b1 = base_gf->multiply.w32(base_gf, a1, b1);
  
        *d8 ^= ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) | 
               ((base_gf->multiply.w32(base_gf, a1, b0) ^ 
                 base_gf->multiply.w32(base_gf, a0, b1) ^ 
                 base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 4));
        s8++;
        d8++;
      }
    } else {
      while (d8 < (uint8_t *) rd.d_top) {
        a0 = *s8 & 0x0f; 
        a1 = (*s8 & 0xf0) >> 4; 
        a1b1 = base_gf->multiply.w32(base_gf, a1, b1);
  
        *d8 = ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) | 
              ((base_gf->multiply.w32(base_gf, a1, b0) ^ 
                base_gf->multiply.w32(base_gf, a0, b1) ^ 
                base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 4));
        s8++;
        d8++;
      }
    }
  } else {
    if (xor) {
      while (d8 < (uint8_t *) rd.d_top) {
        a0 = *s8 & 0x0f; 
        a1 = (*s8 & 0xf0) >> 4; 
        a1b1 = GF_W4_INLINE_MULTDIV(mt, a1, b1);
  
        *d8 ^= ((GF_W4_INLINE_MULTDIV(mt, a0, b0) ^ a1b1) | 
               ((GF_W4_INLINE_MULTDIV(mt, a1, b0) ^ 
                 GF_W4_INLINE_MULTDIV(mt, a0, b1) ^ 
                 GF_W4_INLINE_MULTDIV(mt, a1b1, h->prim_poly)) << 4));
        s8++;
        d8++;
      }
    } else {
      while (d8 < (uint8_t *) rd.d_top) {
        a0 = *s8 & 0x0f; 
        a1 = (*s8 & 0xf0) >> 4; 
        a1b1 = GF_W4_INLINE_MULTDIV(mt, a1, b1);
  
        *d8 = ((GF_W4_INLINE_MULTDIV(mt, a0, b0) ^ a1b1) | 
              ((GF_W4_INLINE_MULTDIV(mt, a1, b0) ^ 
                GF_W4_INLINE_MULTDIV(mt, a0, b1) ^ 
                GF_W4_INLINE_MULTDIV(mt, a1b1, h->prim_poly)) << 4));
        s8++;
        d8++;
      }
    }
  }
  gf_do_final_region_alignment(&rd);
  return;
}

static
int gf_w8_composite_init(gf_t *gf)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  struct gf_w8_composite_data *cd;

  if (h->base_gf == NULL) return 0;

  cd = (struct gf_w8_composite_data *) h->private;
  cd->mult_table = gf_w4_get_mult_table(h->base_gf);

  if (h->region_type & GF_REGION_ALTMAP) {
    gf->multiply_region.w32 = gf_w8_composite_multiply_region_alt;
  } else {
    gf->multiply_region.w32 = gf_w8_composite_multiply_region;
  }

  if (cd->mult_table == NULL) {
    gf->multiply.w32 = gf_w8_composite_multiply_recursive;
  } else {
    gf->multiply.w32 = gf_w8_composite_multiply_inline;
  }
  gf->divide.w32 = NULL;
  gf->inverse.w32 = gf_w8_composite_inverse;

  return 1;
}

static
inline
  gf_val_32_t
gf_w8_bytwo_p_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, pmask, amask;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;


  prod = 0;
  pmask = 0x80;
  amask = 0x80;

  while (amask != 0) {
    if (prod & pmask) {
      prod = ((prod << 1) ^ pp);
    } else {
      prod <<= 1;
    }
    if (a & amask) prod ^= b;
    amask >>= 1;
  }
  return prod;
}

static
inline
  gf_val_32_t
gf_w8_bytwo_b_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, bmask;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  prod = 0;
  bmask = 0x80;

  while (1) {
    if (a & 1) prod ^= b;
    a >>= 1;
    if (a == 0) return prod;
    if (b & bmask) {
      b = ((b << 1) ^ pp);
    } else {
      b <<= 1;
    }
  }
}

static
  void 
gf_w8_bytwo_p_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, prod, amask;
  gf_region_data rd;
  struct gf_w8_bytwo_data *btd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_w8_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;

  if (xor) {
    while (s64 < (uint64_t *) rd.s_top) {
      prod = 0;
      amask = 0x80;
      ta = *s64;
      while (amask != 0) {
        AB2(btd->prim_poly, btd->mask1, btd->mask2, prod, t1, t2);
        if (val & amask) prod ^= ta;
        amask >>= 1;
      }
      *d64 ^= prod;
      d64++;
      s64++;
    }
  } else { 
    while (s64 < (uint64_t *) rd.s_top) {
      prod = 0;
      amask = 0x80;
      ta = *s64;
      while (amask != 0) {
        AB2(btd->prim_poly, btd->mask1, btd->mask2, prod, t1, t2);
        if (val & amask) prod ^= ta;
        amask >>= 1;
      }
      *d64 = prod;
      d64++;
      s64++;
    }
  }
  gf_do_final_region_alignment(&rd);
}

#define BYTWO_P_ONESTEP {\
  SSE_AB2(pp, m1 ,m2, prod, t1, t2); \
  t1 = _mm_and_si128(v, one); \
  t1 = _mm_sub_epi8(t1, one); \
  t1 = _mm_and_si128(t1, ta); \
  prod = _mm_xor_si128(prod, t1); \
  v = _mm_srli_epi64(v, 1); }

#ifdef INTEL_SSE2
static
  void 
gf_w8_bytwo_p_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8;
  uint8_t vrev;
  __m128i pp, m1, m2, ta, prod, t1, t2, tp, one, v;
  struct gf_w8_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_w8_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  vrev = 0;
  for (i = 0; i < 8; i++) {
    vrev <<= 1;
    if (!(val & (1 << i))) vrev |= 1;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
  m2 = _mm_set1_epi8((btd->mask2)&0xff);
  one = _mm_set1_epi8(1);

  while (d8 < (uint8_t *) rd.d_top) {
    prod = _mm_setzero_si128();
    v = _mm_set1_epi8(vrev);
    ta = _mm_load_si128((__m128i *) s8);
    tp = (!xor) ? _mm_setzero_si128() : _mm_load_si128((__m128i *) d8);
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP;
    _mm_store_si128((__m128i *) d8, _mm_xor_si128(prod, tp));
    d8 += 16;
    s8 += 16;
  }
  gf_do_final_region_alignment(&rd);
}
#endif

#ifdef INTEL_SSE2
static
  void
gf_w8_bytwo_b_sse_region_2_noxor(gf_region_data *rd, struct gf_w8_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
  m2 = _mm_set1_epi8((btd->mask2)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, m2, va, t1, t2);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static
  void
gf_w8_bytwo_b_sse_region_2_xor(gf_region_data *rd, struct gf_w8_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
  m2 = _mm_set1_epi8((btd->mask2)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, m2, va, t1, t2);
    vb = _mm_load_si128 ((__m128i *)(d8));
    vb = _mm_xor_si128(vb, va);
    _mm_store_si128((__m128i *)d8, vb);
    d8 += 16;
    s8 += 16;
  }
}
#endif


#ifdef INTEL_SSE2
static
  void 
gf_w8_bytwo_b_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int itb;
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va, vb;
  struct gf_w8_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_w8_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  if (val == 2) {
    if (xor) {
      gf_w8_bytwo_b_sse_region_2_xor(&rd, btd);
    } else {
      gf_w8_bytwo_b_sse_region_2_noxor(&rd, btd);
    }
    gf_do_final_region_alignment(&rd);
    return;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
  m2 = _mm_set1_epi8((btd->mask2)&0xff);

  while (d8 < (uint8_t *) rd.d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = (!xor) ? _mm_setzero_si128() : _mm_load_si128 ((__m128i *)(d8));
    itb = val;
    while (1) {
      if (itb & 1) vb = _mm_xor_si128(vb, va);
      itb >>= 1;
      if (itb == 0) break;
      SSE_AB2(pp, m1, m2, va, t1, t2);
    }
    _mm_store_si128((__m128i *)d8, vb);
    d8 += 16;
    s8 += 16;
  }

  gf_do_final_region_alignment(&rd);
}
#endif

static
  void 
gf_w8_bytwo_b_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, tb, prod;
  struct gf_w8_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_w8_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;
  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;

  switch (val) {
    case 2:
      if (xor) {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 ^= ta;
          d64++;
          s64++;
        }
      } else {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 = ta;
          d64++;
          s64++;
        }
      }
      break; 
    case 3:
      if (xor) {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          prod = ta;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 ^= (ta ^ prod);
          d64++;
          s64++;
        }
      } else {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          prod = ta;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 = (ta ^ prod);
          d64++;
          s64++;
        }
      }
      break; 
    case 4:
      if (xor) {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 ^= ta;
          d64++;
          s64++;
        }
      } else {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 = ta;
          d64++;
          s64++;
        }
      }
      break; 
    case 5:
      if (xor) {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          prod = ta;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 ^= (ta ^ prod);
          d64++;
          s64++;
        }
      } else {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          prod = ta;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 = ta ^ prod;
          d64++;
          s64++;
        }
      }
    case 6:
      if (xor) {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          prod = ta;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 ^= (ta ^ prod);
          d64++;
          s64++;
        }
      } else {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          prod = ta;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 = ta ^ prod;
          d64++;
          s64++;
        }
      }
      /*
         case 7:
         if (xor) {
         while (d64 < (uint64_t *) rd.d_top) {
         ta = *s64;
         prod = ta;
         AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
         prod ^= ta;
         AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 ^= (ta ^ prod);
       d64++;
       s64++;
       }
       } else {
       while (d64 < (uint64_t *) rd.d_top) {
       ta = *s64;
       prod = ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       prod ^= ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 = ta ^ prod;
       d64++;
       s64++;
       }
       }
       break; 
       */
    case 8:
      if (xor) {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 ^= ta;
          d64++;
          s64++;
        }
      } else {
        while (d64 < (uint64_t *) rd.d_top) {
          ta = *s64;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
          *d64 = ta;
          d64++;
          s64++;
        }
      }
      break; 
      /*
         case 9:
         if (xor) {
         while (d64 < (uint64_t *) rd.d_top) {
         ta = *s64;
         prod = ta;
         AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
         AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
         AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 ^= (ta ^ prod);
       d64++;
       s64++;
       }
       } else {
       while (d64 < (uint64_t *) rd.d_top) {
       ta = *s64;
       prod = ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 = (ta ^ prod);
       d64++;
       s64++;
       }
       }
       break; 
       case 10:
       if (xor) {
       while (d64 < (uint64_t *) rd.d_top) {
       ta = *s64;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       prod = ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 ^= (ta ^ prod);
       d64++;
       s64++;
       }
       } else {
       while (d64 < (uint64_t *) rd.d_top) {
       ta = *s64;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       prod = ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 = (ta ^ prod);
       d64++;
       s64++;
       }
       }
       break; 
       case 11:
       if (xor) {
       while (d64 < (uint64_t *) rd.d_top) {
       ta = *s64;
       prod = ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       prod ^= ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       *d64 ^= (ta ^ prod);
       d64++;
       s64++;
       }
       } else {
       while (d64 < (uint64_t *) rd.d_top) {
       ta = *s64;
       prod = ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
       prod ^= ta;
       AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 = (ta ^ prod);
      d64++;
      s64++;
      }
  }
  break; 
    case 12:
  if (xor) {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 ^= (ta ^ prod);
      d64++;
      s64++;
    }
  } else {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 = (ta ^ prod);
      d64++;
      s64++;
    }
  }
  break; 
    case 13:
  if (xor) {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 ^= (ta ^ prod);
      d64++;
      s64++;
    }
  } else {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 = (ta ^ prod);
      d64++;
      s64++;
    }
  }
  break; 
    case 14:
  if (xor) {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 ^= (ta ^ prod);
      d64++;
      s64++;
    }
  } else {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 = (ta ^ prod);
      d64++;
      s64++;
    }
  }
  break; 
    case 15:
  if (xor) {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 ^= (ta ^ prod);
      d64++;
      s64++;
    }
  } else {
    while (d64 < (uint64_t *) rd.d_top) {
      ta = *s64;
      prod = ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      prod ^= ta;
      AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
      *d64 = (ta ^ prod);
      d64++;
      s64++;
    }
  }
  break; 
  */
    default:
    if (xor) {
      while (d64 < (uint64_t *) rd.d_top) {
        prod = *d64 ;
        ta = *s64;
        tb = val;
        while (1) {
          if (tb & 1) prod ^= ta;
          tb >>= 1;
          if (tb == 0) break;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
        }
        *d64 = prod;
        d64++;
        s64++;
      }
    } else {
      while (d64 < (uint64_t *) rd.d_top) {
        prod = 0 ;
        ta = *s64;
        tb = val;
        while (1) {
          if (tb & 1) prod ^= ta;
          tb >>= 1;
          if (tb == 0) break;
          AB2(btd->prim_poly, btd->mask1, btd->mask2, ta, t1, t2);
        }
        *d64 = prod;
        d64++;
        s64++;
      }
    }
    break;
  }
  gf_do_final_region_alignment(&rd);
}

  static
int gf_w8_bytwo_init(gf_t *gf)
{
  gf_internal_t *h;
  uint64_t ip, m1, m2;
  struct gf_w8_bytwo_data *btd;

  h = (gf_internal_t *) gf->scratch;
  btd = (struct gf_w8_bytwo_data *) (h->private);
  ip = h->prim_poly & 0xff;
  m1 = 0xfe;
  m2 = 0x80;
  btd->prim_poly = 0;
  btd->mask1 = 0;
  btd->mask2 = 0;

  while (ip != 0) {
    btd->prim_poly |= ip;
    btd->mask1 |= m1;
    btd->mask2 |= m2;
    ip <<= GF_FIELD_WIDTH;
    m1 <<= GF_FIELD_WIDTH;
    m2 <<= GF_FIELD_WIDTH;
  }

  if (h->mult_type == GF_MULT_BYTWO_p) {
    gf->multiply.w32 = gf_w8_bytwo_p_multiply;
#ifdef INTEL_SSE2
    if (h->region_type & GF_REGION_NOSSE)
      gf->multiply_region.w32 = gf_w8_bytwo_p_nosse_multiply_region;
    else
      gf->multiply_region.w32 = gf_w8_bytwo_p_sse_multiply_region;
#else
    gf->multiply_region.w32 = gf_w8_bytwo_p_nosse_multiply_region;
    if(h->region_type & GF_REGION_SSE)
      return 0;
#endif
  } else {
    gf->multiply.w32 = gf_w8_bytwo_b_multiply;
#ifdef INTEL_SSE2
    if (h->region_type & GF_REGION_NOSSE)
      gf->multiply_region.w32 = gf_w8_bytwo_b_nosse_multiply_region;
    else
      gf->multiply_region.w32 = gf_w8_bytwo_b_sse_multiply_region;
#else
    gf->multiply_region.w32 = gf_w8_bytwo_b_nosse_multiply_region;
    if(h->region_type & GF_REGION_SSE)
      return 0;
#endif
  }
  return 1;
}


/* ------------------------------------------------------------
   General procedures.
   You don't need to error check here on in init, because it's done
   for you in gf_error_check().
 */

int gf_w8_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2)
{
  switch(mult_type)
  {
    case GF_MULT_DEFAULT:
#ifdef INTEL_SSSE3
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_default_data) + 64;
#endif
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_single_table_data) + 64;
    case GF_MULT_TABLE:
      if (region_type == GF_REGION_CAUCHY) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w8_single_table_data) + 64;
      }

      if (region_type == GF_REGION_DEFAULT) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w8_single_table_data) + 64;
      } 
      if (region_type & GF_REGION_DOUBLE_TABLE) {
        if (region_type == GF_REGION_DOUBLE_TABLE) {
          return sizeof(gf_internal_t) + sizeof(struct gf_w8_double_table_data) + 64;
        } else if (region_type == (GF_REGION_DOUBLE_TABLE | GF_REGION_LAZY)) {
          return sizeof(gf_internal_t) + sizeof(struct gf_w8_double_table_lazy_data) + 64;
        } else {
          return 0;
        }
      }
      return 0;
      break;
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_bytwo_data);
      break;
    case GF_MULT_SPLIT_TABLE:
      if ((arg1 == 4 && arg2 == 8) || (arg1 == 8 && arg2 == 4)) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w8_half_table_data) + 64;
      }
      break;
    case GF_MULT_LOG_TABLE:
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_logtable_data) + 64;
      break;
    case GF_MULT_LOG_ZERO:
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_logzero_small_table_data) + 64;
      break;
    case GF_MULT_LOG_ZERO_EXT:
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_logzero_table_data) + 64;
      break;
    case GF_MULT_CARRY_FREE:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_SHIFT:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_COMPOSITE:
      return sizeof(gf_internal_t) + sizeof(struct gf_w8_composite_data) + 64;
    default:
      return 0;
  }
  return 0;
}

int gf_w8_init(gf_t *gf)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

  /* Allen: set default primitive polynomial / irreducible polynomial if needed */

  if (h->prim_poly == 0) {
    if (h->mult_type == GF_MULT_COMPOSITE) { 
      h->prim_poly = gf_composite_get_default_poly(h->base_gf);
      if (h->prim_poly == 0) return 0;   /* JSP: This shouldn't happen, but just in case. */
    } else {             
      h->prim_poly = 0x11d;
    } 
  }
  if (h->mult_type != GF_MULT_COMPOSITE) { 
    h->prim_poly |= 0x100;
  }

  gf->multiply.w32 = NULL;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = NULL;
  gf->extract_word.w32 = gf_w8_extract_word;

  switch(h->mult_type) {
    case GF_MULT_DEFAULT:      
    case GF_MULT_TABLE:        if (gf_w8_table_init(gf) == 0) return 0; break;
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:      if (gf_w8_bytwo_init(gf) == 0) return 0; break;
    case GF_MULT_LOG_ZERO:
    case GF_MULT_LOG_ZERO_EXT:
    case GF_MULT_LOG_TABLE:    if (gf_w8_log_init(gf) == 0) return 0; break;
    case GF_MULT_CARRY_FREE:   if (gf_w8_cfm_init(gf) == 0) return 0; break;
    case GF_MULT_SHIFT:        if (gf_w8_shift_init(gf) == 0) return 0; break;
    case GF_MULT_SPLIT_TABLE:  if (gf_w8_split_init(gf) == 0) return 0; break;
    case GF_MULT_COMPOSITE:    if (gf_w8_composite_init(gf) == 0) return 0; break;
    default: return 0;
  }

  if (h->divide_type == GF_DIVIDE_EUCLID) {
    gf->divide.w32 = gf_w8_divide_from_inverse;
    gf->inverse.w32 = gf_w8_euclid;
  } else if (h->divide_type == GF_DIVIDE_MATRIX) {
    gf->divide.w32 = gf_w8_divide_from_inverse;
    gf->inverse.w32 = gf_w8_matrix;
  }

  if (gf->divide.w32 == NULL) {
    gf->divide.w32 = gf_w8_divide_from_inverse;
    if (gf->inverse.w32 == NULL) gf->inverse.w32 = gf_w8_euclid;
  }

  if (gf->inverse.w32 == NULL)  gf->inverse.w32 = gf_w8_inverse_from_divide;

  if (h->mult_type == GF_MULT_COMPOSITE && (h->region_type & GF_REGION_ALTMAP)) {
    gf->extract_word.w32 = gf_w8_composite_extract_word;
  }

  if (h->region_type == GF_REGION_CAUCHY) {
    gf->multiply_region.w32 = gf_wgen_cauchy_region;
    gf->extract_word.w32 = gf_wgen_extract_word;
  }

  if (gf->multiply_region.w32 == NULL) {
    gf->multiply_region.w32 = gf_w8_multiply_region_from_single;
  }

  return 1;
}


/* Inline setup functions */

uint8_t *gf_w8_get_mult_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w8_default_data *ftd;
  struct gf_w8_single_table_data *std;

  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w8_default_multiply) {
    ftd = (struct gf_w8_default_data *) h->private;
    return (uint8_t *) ftd->multtable;
  } else if (gf->multiply.w32 == gf_w8_table_multiply) {
    std = (struct gf_w8_single_table_data *) h->private;
    return (uint8_t *) std->multtable;
  }
  return NULL;
}

uint8_t *gf_w8_get_div_table(gf_t *gf)
{
  struct gf_w8_default_data *ftd;
  struct gf_w8_single_table_data *std;

  if (gf->multiply.w32 == gf_w8_default_multiply) {
    ftd = (struct gf_w8_default_data *) ((gf_internal_t *) gf->scratch)->private;
    return (uint8_t *) ftd->divtable;
  } else if (gf->multiply.w32 == gf_w8_table_multiply) {
    std = (struct gf_w8_single_table_data *) ((gf_internal_t *) gf->scratch)->private;
    return (uint8_t *) std->divtable;
  }
  return NULL;
}
