/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w16.c
 *
 * Routines for 16-bit Galois fields
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

#define GF_FIELD_WIDTH (16)
#define GF_FIELD_SIZE (1 << GF_FIELD_WIDTH)
#define GF_MULT_GROUP_SIZE GF_FIELD_SIZE-1

#define GF_BASE_FIELD_WIDTH (8)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)

struct gf_w16_logtable_data {
    uint16_t      log_tbl[GF_FIELD_SIZE];
    uint16_t      antilog_tbl[GF_FIELD_SIZE * 2];
    uint16_t      inv_tbl[GF_FIELD_SIZE];
    uint16_t      *d_antilog;
};

struct gf_w16_zero_logtable_data {
    int           log_tbl[GF_FIELD_SIZE];
    uint16_t      _antilog_tbl[GF_FIELD_SIZE * 4];
    uint16_t      *antilog_tbl;
    uint16_t      inv_tbl[GF_FIELD_SIZE];
};

struct gf_w16_lazytable_data {
    uint16_t      log_tbl[GF_FIELD_SIZE];
    uint16_t      antilog_tbl[GF_FIELD_SIZE * 2];
    uint16_t      inv_tbl[GF_FIELD_SIZE];
    uint16_t      *d_antilog;
    uint16_t      lazytable[GF_FIELD_SIZE];
};

struct gf_w16_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

struct gf_w16_split_8_8_data {
    uint16_t      tables[3][256][256];
};

struct gf_w16_group_4_4_data {
    uint16_t reduce[16];
    uint16_t shift[16];
};

struct gf_w16_composite_data {
  uint8_t *mult_table;
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

#define GF_FIRST_BIT (1 << 15)
#define GF_MULTBY_TWO(p) (((p) & GF_FIRST_BIT) ? (((p) << 1) ^ h->prim_poly) : (p) << 1)

static
inline
gf_val_32_t gf_w16_inverse_from_divide (gf_t *gf, gf_val_32_t a)
{
  return gf->divide.w32(gf, 1, a);
}

static
inline
gf_val_32_t gf_w16_divide_from_inverse (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  b = gf->inverse.w32(gf, b);
  return gf->multiply.w32(gf, a, b);
}

static
void
gf_w16_multiply_region_from_single(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint16_t *s16;
  uint16_t *d16;
  
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;

  if (xor) {
    while (d16 < ((uint16_t *) rd.d_top)) {
      *d16 ^= gf->multiply.w32(gf, val, *s16);
      d16++;
      s16++;
    } 
  } else {
    while (d16 < ((uint16_t *) rd.d_top)) {
      *d16 = gf->multiply.w32(gf, val, *s16);
      d16++;
      s16++;
    } 
  }
  gf_do_final_region_alignment(&rd);
}

#if defined(INTEL_SSE4_PCLMUL)
static
void
gf_w16_clm_multiply_region_from_single_2(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint16_t *s16;
  uint16_t *d16;
  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;
  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);
  
  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;

  if (xor) {
    while (d16 < ((uint16_t *) rd.d_top)) {

      /* see gf_w16_clm_multiply() to see explanation of method */
      
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s16), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);

      *d16 ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d16++;
      s16++;
    } 
  } else {
    while (d16 < ((uint16_t *) rd.d_top)) {
      
      /* see gf_w16_clm_multiply() to see explanation of method */
      
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s16), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      
      *d16 = ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d16++;
      s16++;
    } 
  }
  gf_do_final_region_alignment(&rd);
}
#endif

#if defined(INTEL_SSE4_PCLMUL)
static
void
gf_w16_clm_multiply_region_from_single_3(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint16_t *s16;
  uint16_t *d16;

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;
  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);
  
  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;

  if (xor) {
    while (d16 < ((uint16_t *) rd.d_top)) {
      
      /* see gf_w16_clm_multiply() to see explanation of method */
      
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s16), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);

      *d16 ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d16++;
      s16++;
    } 
  } else {
    while (d16 < ((uint16_t *) rd.d_top)) {
      
      /* see gf_w16_clm_multiply() to see explanation of method */
      
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s16), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      
      *d16 = ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d16++;
      s16++;
    } 
  }
  gf_do_final_region_alignment(&rd);
}
#endif

#if defined(INTEL_SSE4_PCLMUL)
static
void
gf_w16_clm_multiply_region_from_single_4(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint16_t *s16;
  uint16_t *d16;

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;
  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);
  
  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;

  if (xor) {
    while (d16 < ((uint16_t *) rd.d_top)) {
      
      /* see gf_w16_clm_multiply() to see explanation of method */
      
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s16), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);

      *d16 ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d16++;
      s16++;
    } 
  } else {
    while (d16 < ((uint16_t *) rd.d_top)) {
      
      /* see gf_w16_clm_multiply() to see explanation of method */
      
      b = _mm_insert_epi32 (a, (gf_val_32_t)(*s16), 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
      result = _mm_xor_si128 (result, w);
      
      *d16 = ((gf_val_32_t)_mm_extract_epi32(result, 0));
      d16++;
      s16++;
    } 
  }
  gf_do_final_region_alignment(&rd);
}
#endif

static
inline
gf_val_32_t gf_w16_euclid (gf_t *gf, gf_val_32_t b)
{
  gf_val_32_t e_i, e_im1, e_ip1;
  gf_val_32_t d_i, d_im1, d_ip1;
  gf_val_32_t y_i, y_im1, y_ip1;
  gf_val_32_t c_i;

  if (b == 0) return -1;
  e_im1 = ((gf_internal_t *) (gf->scratch))->prim_poly;
  e_i = b;
  d_im1 = 16;
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
gf_val_32_t gf_w16_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint16_t *r16, rv;

  r16 = (uint16_t *) start;
  rv = r16[index];
  return rv;
}

static
gf_val_32_t gf_w16_composite_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  int sub_size;
  gf_internal_t *h;
  uint8_t *r8, *top;
  uint16_t a, b, *r16;
  gf_region_data rd;

  h = (gf_internal_t *) gf->scratch;
  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 32);
  r16 = (uint16_t *) start;
  if (r16 + index < (uint16_t *) rd.d_start) return r16[index];
  if (r16 + index >= (uint16_t *) rd.d_top) return r16[index];
  index -= (((uint16_t *) rd.d_start) - r16);
  r8 = (uint8_t *) rd.d_start;
  top = (uint8_t *) rd.d_top;
  sub_size = (top-r8)/2;

  a = h->base_gf->extract_word.w32(h->base_gf, r8, sub_size, index);
  b = h->base_gf->extract_word.w32(h->base_gf, r8+sub_size, sub_size, index);
  return (a | (b << 8));
}

static
gf_val_32_t gf_w16_split_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint16_t *r16, rv;
  uint8_t *r8;
  gf_region_data rd;

  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 32);
  r16 = (uint16_t *) start;
  if (r16 + index < (uint16_t *) rd.d_start) return r16[index];
  if (r16 + index >= (uint16_t *) rd.d_top) return r16[index];
  index -= (((uint16_t *) rd.d_start) - r16);
  r8 = (uint8_t *) rd.d_start;
  r8 += ((index & 0xfffffff0)*2);
  r8 += (index & 0xf);
  rv = (*r8 << 8);
  r8 += 16;
  rv |= *r8;
  return rv;
}

static
inline
gf_val_32_t gf_w16_matrix (gf_t *gf, gf_val_32_t b)
{
  return gf_bitmatrix_inverse(b, 16, ((gf_internal_t *) (gf->scratch))->prim_poly);
}

/* JSP: GF_MULT_SHIFT: The world's dumbest multiplication algorithm.  I only
   include it for completeness.  It does have the feature that it requires no
   extra memory.  
 */

static
inline
gf_val_32_t
gf_w16_clm_multiply_2 (gf_t *gf, gf_val_32_t a16, gf_val_32_t b16)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a16, 0);
  b = _mm_insert_epi32 (a, b16, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  /* Ben: Do prim_poly reduction twice. We are guaranteed that we will only
     have to do the reduction at most twice, because (w-2)/z == 2. Where
     z is equal to the number of zeros after the leading 1

     _mm_clmulepi64_si128 is the carryless multiply operation. Here
     _mm_srli_si128 shifts the result to the right by 2 bytes. This allows
     us to multiply the prim_poly by the leading bits of the result. We
     then xor the result of that operation back with the result.*/

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));


#endif
  return rv;
}

static
inline
gf_val_32_t
gf_w16_clm_multiply_3 (gf_t *gf, gf_val_32_t a16, gf_val_32_t b16)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a16, 0);
  b = _mm_insert_epi32 (a, b16, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));


#endif
  return rv;
}

static
inline
gf_val_32_t
gf_w16_clm_multiply_4 (gf_t *gf, gf_val_32_t a16, gf_val_32_t b16)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a16, 0);
  b = _mm_insert_epi32 (a, b16, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1ffffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 2), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));


#endif
  return rv;
}


static
inline
 gf_val_32_t
gf_w16_shift_multiply (gf_t *gf, gf_val_32_t a16, gf_val_32_t b16)
{
  gf_val_32_t product, i, pp, a, b;
  gf_internal_t *h;

  a = a16;
  b = b16;
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
int gf_w16_shift_init(gf_t *gf)
{
  gf->multiply.w32 = gf_w16_shift_multiply;
  return 1;
}

static 
int gf_w16_cfm_init(gf_t *gf)
{
#if defined(INTEL_SSE4_PCLMUL)
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  
  /*Ben: Determining how many reductions to do */
  
  if ((0xfe00 & h->prim_poly) == 0) {
    gf->multiply.w32 = gf_w16_clm_multiply_2;
    gf->multiply_region.w32 = gf_w16_clm_multiply_region_from_single_2;
  } else if((0xf000 & h->prim_poly) == 0) {
    gf->multiply.w32 = gf_w16_clm_multiply_3;
    gf->multiply_region.w32 = gf_w16_clm_multiply_region_from_single_3;
  } else if ((0xe000 & h->prim_poly) == 0) {
    gf->multiply.w32 = gf_w16_clm_multiply_4;
    gf->multiply_region.w32 = gf_w16_clm_multiply_region_from_single_4;
  } else {
    return 0;
  } 
  return 1;
#endif

  return 0;
}

/* KMG: GF_MULT_LOGTABLE: */

static
void
gf_w16_log_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint16_t *s16, *d16;
  int lv;
  struct gf_w16_logtable_data *ltd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  ltd = (struct gf_w16_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;

  lv = ltd->log_tbl[val];

  if (xor) {
    while (d16 < (uint16_t *) rd.d_top) {
      *d16 ^= (*s16 == 0 ? 0 : ltd->antilog_tbl[lv + ltd->log_tbl[*s16]]);
      d16++;
      s16++;
    }
  } else {
    while (d16 < (uint16_t *) rd.d_top) {
      *d16 = (*s16 == 0 ? 0 : ltd->antilog_tbl[lv + ltd->log_tbl[*s16]]);
      d16++;
      s16++;
    }
  }
  gf_do_final_region_alignment(&rd);
}

static
inline
gf_val_32_t
gf_w16_log_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w16_logtable_data *ltd;

  ltd = (struct gf_w16_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  return (a == 0 || b == 0) ? 0 : ltd->antilog_tbl[(int) ltd->log_tbl[a] + (int) ltd->log_tbl[b]];
}

static
inline
gf_val_32_t
gf_w16_log_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int log_sum = 0;
  struct gf_w16_logtable_data *ltd;

  if (a == 0 || b == 0) return 0;
  ltd = (struct gf_w16_logtable_data *) ((gf_internal_t *) gf->scratch)->private;

  log_sum = (int) ltd->log_tbl[a] - (int) ltd->log_tbl[b];
  return (ltd->d_antilog[log_sum]);
}

static
gf_val_32_t
gf_w16_log_inverse(gf_t *gf, gf_val_32_t a)
{
  struct gf_w16_logtable_data *ltd;

  ltd = (struct gf_w16_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ltd->inv_tbl[a]);
}

static
int gf_w16_log_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w16_logtable_data *ltd;
  int i, b;
  int check = 0;

  h = (gf_internal_t *) gf->scratch;
  ltd = h->private;
  
  for (i = 0; i < GF_MULT_GROUP_SIZE+1; i++)
    ltd->log_tbl[i] = 0;
  ltd->d_antilog = ltd->antilog_tbl + GF_MULT_GROUP_SIZE;

  b = 1;
  for (i = 0; i < GF_MULT_GROUP_SIZE; i++) {
      if (ltd->log_tbl[b] != 0) check = 1;
      ltd->log_tbl[b] = i;
      ltd->antilog_tbl[i] = b;
      ltd->antilog_tbl[i+GF_MULT_GROUP_SIZE] = b;
      b <<= 1;
      if (b & GF_FIELD_SIZE) {
          b = b ^ h->prim_poly;
      }
  }

  /* If you can't construct the log table, there's a problem.  This code is used for
     some other implementations (e.g. in SPLIT), so if the log table doesn't work in 
     that instance, use CARRY_FREE / SHIFT instead. */

  if (check) {
    if (h->mult_type != GF_MULT_LOG_TABLE) {

#if defined(INTEL_SSE4_PCLMUL)
      return gf_w16_cfm_init(gf);
#endif
      return gf_w16_shift_init(gf);
    } else {
      _gf_errno = GF_E_LOGPOLY;
      return 0;
    }
  }

  ltd->inv_tbl[0] = 0;  /* Not really, but we need to fill it with something  */
  ltd->inv_tbl[1] = 1;
  for (i = 2; i < GF_FIELD_SIZE; i++) {
    ltd->inv_tbl[i] = ltd->antilog_tbl[GF_MULT_GROUP_SIZE-ltd->log_tbl[i]];
  }

  gf->inverse.w32 = gf_w16_log_inverse;
  gf->divide.w32 = gf_w16_log_divide;
  gf->multiply.w32 = gf_w16_log_multiply;
  gf->multiply_region.w32 = gf_w16_log_multiply_region;

  return 1;
}

/* JSP: GF_MULT_SPLIT_TABLE: Using 8 multiplication tables to leverage SSE instructions.
*/


/* Ben: Does alternate mapping multiplication using a split table in the
 lazy method without sse instructions*/

static 
void
gf_w16_split_4_16_lazy_nosse_altmap_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t i, j, c, prod;
  uint8_t *s8, *d8, *top;
  uint16_t table[4][16];
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);    

  /*Ben: Constructs lazy multiplication table*/

  for (j = 0; j < 16; j++) {
    for (i = 0; i < 4; i++) {
      c = (j << (i*4));
      table[i][j] = gf->multiply.w32(gf, c, val);
    }
  }

  /*Ben: s8 is the start of source, d8 is the start of dest, top is end of dest region. */
  
  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;
  top = (uint8_t *) rd.d_top;


  while (d8 < top) {
    
    /*Ben: Multiplies across 16 two byte quantities using alternate mapping 
       high bits are on the left, low bits are on the right. */
  
    for (j=0;j<16;j++) {
    
      /*Ben: If the xor flag is set, the product should include what is in dest */
      prod = (xor) ? ((uint16_t)(*d8)<<8) ^ *(d8+16) : 0;

      /*Ben: xors all 4 table lookups into the product variable*/
      
      prod ^= ((table[0][*(s8+16)&0xf]) ^
          (table[1][(*(s8+16)&0xf0)>>4]) ^
          (table[2][*(s8)&0xf]) ^
          (table[3][(*(s8)&0xf0)>>4]));

      /*Ben: Stores product in the destination and moves on*/
      
      *d8 = (uint8_t)(prod >> 8);
      *(d8+16) = (uint8_t)(prod & 0x00ff);
      s8++;
      d8++;
    }
    s8+=16;
    d8+=16;
  }
  gf_do_final_region_alignment(&rd);
}

static
  void
gf_w16_split_4_16_lazy_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t i, j, a, c, prod;
  uint16_t *s16, *d16, *top;
  uint16_t table[4][16];
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);    

  for (j = 0; j < 16; j++) {
    for (i = 0; i < 4; i++) {
      c = (j << (i*4));
      table[i][j] = gf->multiply.w32(gf, c, val);
    }
  }

  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;
  top = (uint16_t *) rd.d_top;

  while (d16 < top) {
    a = *s16;
    prod = (xor) ? *d16 : 0;
    for (i = 0; i < 4; i++) {
      prod ^= table[i][a&0xf];
      a >>= 4;
    }
    *d16 = prod;
    s16++;
    d16++;
  }
}

static
void
gf_w16_split_8_16_lazy_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t j, k, v, a, prod, *s64, *d64, *top64;
  gf_internal_t *h;
  uint64_t htable[256], ltable[256];
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);
  
  h = (gf_internal_t *) gf->scratch;

  v = val;
  ltable[0] = 0;
  for (j = 1; j < 256; j <<= 1) {
    for (k = 0; k < j; k++) ltable[k^j] = (v ^ ltable[k]);
    v = GF_MULTBY_TWO(v);
  }
  htable[0] = 0;
  for (j = 1; j < 256; j <<= 1) {
    for (k = 0; k < j; k++) htable[k^j] = (v ^ htable[k]);
    v = GF_MULTBY_TWO(v);
  }

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top64 = (uint64_t *) rd.d_top;
  
/* Does Unrolling Matter?  -- Doesn't seem to.
  while (d64 != top64) {
    a = *s64;

    prod = htable[a >> 56];
    a <<= 8;
    prod ^= ltable[a >> 56];
    a <<= 8;
    prod <<= 16;

    prod ^= htable[a >> 56];
    a <<= 8;
    prod ^= ltable[a >> 56];
    a <<= 8;
    prod <<= 16;

    prod ^= htable[a >> 56];
    a <<= 8;
    prod ^= ltable[a >> 56];
    a <<= 8;
    prod <<= 16;

    prod ^= htable[a >> 56];
    a <<= 8;
    prod ^= ltable[a >> 56];
    prod ^= ((xor) ? *d64 : 0); 
    *d64 = prod;
    s64++;
    d64++;
  }
*/
  
  while (d64 != top64) {
    a = *s64;

    prod = 0;
    for (j = 0; j < 4; j++) {
      prod <<= 16;
      prod ^= htable[a >> 56];
      a <<= 8;
      prod ^= ltable[a >> 56];
      a <<= 8;
    }

    //JSP: We can move the conditional outside the while loop, but we need to fully test it to understand which is better.
   
    prod ^= ((xor) ? *d64 : 0); 
    *d64 = prod;
    s64++;
    d64++;
  }
  gf_do_final_region_alignment(&rd);
}

static void
gf_w16_table_lazy_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t c;
  gf_internal_t *h;
  struct gf_w16_lazytable_data *ltd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);

  h = (gf_internal_t *) gf->scratch;
  ltd = (struct gf_w16_lazytable_data *) h->private;

  ltd->lazytable[0] = 0;

  /*
  a = val;
  c = 1;
  pp = h->prim_poly;

  do {
    ltd->lazytable[c] = a;
    c <<= 1;
    if (c & (1 << GF_FIELD_WIDTH)) c ^= pp;
    a <<= 1;
    if (a & (1 << GF_FIELD_WIDTH)) a ^= pp;
  } while (c != 1);
  */

  for (c = 1; c < GF_FIELD_SIZE; c++) {
    ltd->lazytable[c] = gf_w16_shift_multiply(gf, c, val);
  }
   
  gf_two_byte_region_table_multiply(&rd, ltd->lazytable);
  gf_do_final_region_alignment(&rd);
}

static
void
gf_w16_split_4_16_lazy_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
#ifdef INTEL_SSSE3
  uint64_t i, j, *s64, *d64, *top64;;
  uint64_t c, prod;
  uint8_t low[4][16];
  uint8_t high[4][16];
  gf_region_data rd;

  __m128i  mask, ta, tb, ti, tpl, tph, tlow[4], thigh[4], tta, ttb, lmask;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);

  for (j = 0; j < 16; j++) {
    for (i = 0; i < 4; i++) {
      c = (j << (i*4));
      prod = gf->multiply.w32(gf, c, val);
      low[i][j] = (prod & 0xff);
      high[i][j] = (prod >> 8);
    }
  }

  for (i = 0; i < 4; i++) {
    tlow[i] = _mm_loadu_si128((__m128i *)low[i]);
    thigh[i] = _mm_loadu_si128((__m128i *)high[i]);
  }

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top64 = (uint64_t *) rd.d_top;

  mask = _mm_set1_epi8 (0x0f);
  lmask = _mm_set1_epi16 (0xff);

  if (xor) {
    while (d64 != top64) {
      
      ta = _mm_load_si128((__m128i *) s64);
      tb = _mm_load_si128((__m128i *) (s64+2));

      tta = _mm_srli_epi16(ta, 8);
      ttb = _mm_srli_epi16(tb, 8);
      tpl = _mm_and_si128(tb, lmask);
      tph = _mm_and_si128(ta, lmask);

      tb = _mm_packus_epi16(tpl, tph);
      ta = _mm_packus_epi16(ttb, tta);

      ti = _mm_and_si128 (mask, tb);
      tph = _mm_shuffle_epi8 (thigh[0], ti);
      tpl = _mm_shuffle_epi8 (tlow[0], ti);
  
      tb = _mm_srli_epi16(tb, 4);
      ti = _mm_and_si128 (mask, tb);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[1], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[1], ti), tph);

      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[2], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[2], ti), tph);
  
      ta = _mm_srli_epi16(ta, 4);
      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[3], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[3], ti), tph);

      ta = _mm_unpackhi_epi8(tpl, tph);
      tb = _mm_unpacklo_epi8(tpl, tph);

      tta = _mm_load_si128((__m128i *) d64);
      ta = _mm_xor_si128(ta, tta);
      ttb = _mm_load_si128((__m128i *) (d64+2));
      tb = _mm_xor_si128(tb, ttb); 
      _mm_store_si128 ((__m128i *)d64, ta);
      _mm_store_si128 ((__m128i *)(d64+2), tb);

      d64 += 4;
      s64 += 4;
      
    }
  } else {
    while (d64 != top64) {
      
      ta = _mm_load_si128((__m128i *) s64);
      tb = _mm_load_si128((__m128i *) (s64+2));

      tta = _mm_srli_epi16(ta, 8);
      ttb = _mm_srli_epi16(tb, 8);
      tpl = _mm_and_si128(tb, lmask);
      tph = _mm_and_si128(ta, lmask);

      tb = _mm_packus_epi16(tpl, tph);
      ta = _mm_packus_epi16(ttb, tta);

      ti = _mm_and_si128 (mask, tb);
      tph = _mm_shuffle_epi8 (thigh[0], ti);
      tpl = _mm_shuffle_epi8 (tlow[0], ti);
  
      tb = _mm_srli_epi16(tb, 4);
      ti = _mm_and_si128 (mask, tb);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[1], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[1], ti), tph);

      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[2], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[2], ti), tph);
  
      ta = _mm_srli_epi16(ta, 4);
      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[3], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[3], ti), tph);

      ta = _mm_unpackhi_epi8(tpl, tph);
      tb = _mm_unpacklo_epi8(tpl, tph);

      _mm_store_si128 ((__m128i *)d64, ta);
      _mm_store_si128 ((__m128i *)(d64+2), tb);

      d64 += 4;
      s64 += 4;
    }
  }

  gf_do_final_region_alignment(&rd);
#endif
}

static
void
gf_w16_split_4_16_lazy_sse_altmap_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
#ifdef INTEL_SSSE3
  uint64_t i, j, *s64, *d64, *top64;;
  uint64_t c, prod;
  uint8_t low[4][16];
  uint8_t high[4][16];
  gf_region_data rd;
  __m128i  mask, ta, tb, ti, tpl, tph, tlow[4], thigh[4];

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);

  for (j = 0; j < 16; j++) {
    for (i = 0; i < 4; i++) {
      c = (j << (i*4));
      prod = gf->multiply.w32(gf, c, val);
      low[i][j] = (prod & 0xff);
      high[i][j] = (prod >> 8);
    }
  }

  for (i = 0; i < 4; i++) {
    tlow[i] = _mm_loadu_si128((__m128i *)low[i]);
    thigh[i] = _mm_loadu_si128((__m128i *)high[i]);
  }

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top64 = (uint64_t *) rd.d_top;

  mask = _mm_set1_epi8 (0x0f);

  if (xor) {
    while (d64 != top64) {

      ta = _mm_load_si128((__m128i *) s64);
      tb = _mm_load_si128((__m128i *) (s64+2));

      ti = _mm_and_si128 (mask, tb);
      tph = _mm_shuffle_epi8 (thigh[0], ti);
      tpl = _mm_shuffle_epi8 (tlow[0], ti);
  
      tb = _mm_srli_epi16(tb, 4);
      ti = _mm_and_si128 (mask, tb);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[1], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[1], ti), tph);

      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[2], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[2], ti), tph);
  
      ta = _mm_srli_epi16(ta, 4);
      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[3], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[3], ti), tph);

      ta = _mm_load_si128((__m128i *) d64);
      tph = _mm_xor_si128(tph, ta);
      _mm_store_si128 ((__m128i *)d64, tph);
      tb = _mm_load_si128((__m128i *) (d64+2));
      tpl = _mm_xor_si128(tpl, tb);
      _mm_store_si128 ((__m128i *)(d64+2), tpl);

      d64 += 4;
      s64 += 4;
    }
  } else {
    while (d64 != top64) {

      ta = _mm_load_si128((__m128i *) s64);
      tb = _mm_load_si128((__m128i *) (s64+2));

      ti = _mm_and_si128 (mask, tb);
      tph = _mm_shuffle_epi8 (thigh[0], ti);
      tpl = _mm_shuffle_epi8 (tlow[0], ti);
  
      tb = _mm_srli_epi16(tb, 4);
      ti = _mm_and_si128 (mask, tb);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[1], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[1], ti), tph);

      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[2], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[2], ti), tph);
  
      ta = _mm_srli_epi16(ta, 4);
      ti = _mm_and_si128 (mask, ta);
      tpl = _mm_xor_si128(_mm_shuffle_epi8 (tlow[3], ti), tpl);
      tph = _mm_xor_si128(_mm_shuffle_epi8 (thigh[3], ti), tph);

      _mm_store_si128 ((__m128i *)d64, tph);
      _mm_store_si128 ((__m128i *)(d64+2), tpl);

      d64 += 4;
      s64 += 4;
      
    }
  }
  gf_do_final_region_alignment(&rd);

#endif
}

uint32_t 
gf_w16_split_8_8_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t alow, blow;
  struct gf_w16_split_8_8_data *d8;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  d8 = (struct gf_w16_split_8_8_data *) h->private;

  alow = a & 0xff;
  blow = b & 0xff;
  a >>= 8;
  b >>= 8;

  return d8->tables[0][alow][blow] ^
         d8->tables[1][alow][b] ^
         d8->tables[1][a][blow] ^
         d8->tables[2][a][b];
}

static 
int gf_w16_split_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w16_split_8_8_data *d8;
  int i, j, exp, issse3;
  uint32_t p, basep;

  h = (gf_internal_t *) gf->scratch;

issse3 = 0;
#ifdef INTEL_SSSE3
  issse3 = 1;
#endif

  if (h->arg1 == 8 && h->arg2 == 8) {
    d8 = (struct gf_w16_split_8_8_data *) h->private;
    basep = 1;
    for (exp = 0; exp < 3; exp++) {
      for (j = 0; j < 256; j++) d8->tables[exp][0][j] = 0;
      for (i = 0; i < 256; i++) d8->tables[exp][i][0] = 0;
      d8->tables[exp][1][1] = basep;
      for (i = 2; i < 256; i++) {
        if (i&1) {
          p = d8->tables[exp][i^1][1];
          d8->tables[exp][i][1] = p ^ basep;
        } else {
          p = d8->tables[exp][i>>1][1];
          d8->tables[exp][i][1] = GF_MULTBY_TWO(p);
        }
      }
      for (i = 1; i < 256; i++) {
        p = d8->tables[exp][i][1];
        for (j = 1; j < 256; j++) {
          if (j&1) {
            d8->tables[exp][i][j] = d8->tables[exp][i][j^1] ^ p;
          } else {
            d8->tables[exp][i][j] = GF_MULTBY_TWO(d8->tables[exp][i][j>>1]);
          }
        }
      }
      for (i = 0; i < 8; i++) basep = GF_MULTBY_TWO(basep);
    }
    gf->multiply.w32 = gf_w16_split_8_8_multiply;
    gf->multiply_region.w32 = gf_w16_split_8_16_lazy_multiply_region;
    return 1;

  }

  /* We'll be using LOG for multiplication, unless the pp isn't primitive.
     In that case, we'll be using SHIFT. */

  gf_w16_log_init(gf);

  /* Defaults */

  if (issse3) {
    gf->multiply_region.w32 = gf_w16_split_4_16_lazy_sse_multiply_region;
  } else {
    gf->multiply_region.w32 = gf_w16_split_8_16_lazy_multiply_region;
  }


  if ((h->arg1 == 8 && h->arg2 == 16) || (h->arg2 == 8 && h->arg1 == 16)) {
    gf->multiply_region.w32 = gf_w16_split_8_16_lazy_multiply_region;

  } else if ((h->arg1 == 4 && h->arg2 == 16) || (h->arg2 == 4 && h->arg1 == 16)) {
    if (issse3) {
      if(h->region_type & GF_REGION_ALTMAP && h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w16_split_4_16_lazy_nosse_altmap_multiply_region;
      else if(h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w16_split_4_16_lazy_multiply_region;
      else if(h->region_type & GF_REGION_ALTMAP)
        gf->multiply_region.w32 = gf_w16_split_4_16_lazy_sse_altmap_multiply_region;
    } else {
      if(h->region_type & GF_REGION_SSE)
        return 0;
      else if(h->region_type & GF_REGION_ALTMAP)
        gf->multiply_region.w32 = gf_w16_split_4_16_lazy_nosse_altmap_multiply_region;
      else
        gf->multiply_region.w32 = gf_w16_split_4_16_lazy_multiply_region;
    }
  }

  return 1;
}

static 
int gf_w16_table_init(gf_t *gf)
{
  gf_w16_log_init(gf);

  gf->multiply_region.w32 = gf_w16_table_lazy_multiply_region; 
  return 1;
}

static
void
gf_w16_log_zero_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint16_t lv;
  int i;
  uint16_t *s16, *d16, *top16;
  struct gf_w16_zero_logtable_data *ltd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  ltd = (struct gf_w16_zero_logtable_data*) ((gf_internal_t *) gf->scratch)->private;
  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;
  top16 = (uint16_t *) rd.d_top;
  bytes = top16 - d16;

  lv = ltd->log_tbl[val];

  if (xor) {
    for (i = 0; i < bytes; i++) {
      d16[i] ^= (ltd->antilog_tbl[lv + ltd->log_tbl[s16[i]]]);
    }
  } else {
    for (i = 0; i < bytes; i++) {
      d16[i] = (ltd->antilog_tbl[lv + ltd->log_tbl[s16[i]]]);
    }
  }

  /* This isn't necessary. */
  
  gf_do_final_region_alignment(&rd);
}

/* Here -- double-check Kevin */

static
inline
gf_val_32_t
gf_w16_log_zero_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_w16_zero_logtable_data *ltd;

  ltd = (struct gf_w16_zero_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  return ltd->antilog_tbl[ltd->log_tbl[a] + ltd->log_tbl[b]];
}

static
inline
gf_val_32_t
gf_w16_log_zero_divide (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int log_sum = 0;
  struct gf_w16_zero_logtable_data *ltd;

  if (a == 0 || b == 0) return 0;
  ltd = (struct gf_w16_zero_logtable_data *) ((gf_internal_t *) gf->scratch)->private;

  log_sum = ltd->log_tbl[a] - ltd->log_tbl[b] + (GF_MULT_GROUP_SIZE);
  return (ltd->antilog_tbl[log_sum]);
}

static
gf_val_32_t
gf_w16_log_zero_inverse (gf_t *gf, gf_val_32_t a)
{
  struct gf_w16_zero_logtable_data *ltd;

  ltd = (struct gf_w16_zero_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
  return (ltd->inv_tbl[a]);
}

static
inline
gf_val_32_t
gf_w16_bytwo_p_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, pmask, amask;
  gf_internal_t *h;
  
  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  
  prod = 0;
  pmask = 0x8000;
  amask = 0x8000;

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
gf_w16_bytwo_b_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, bmask;
  gf_internal_t *h;
  
  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  prod = 0;
  bmask = 0x8000;

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
gf_w16_bytwo_p_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, prod, amask;
  gf_region_data rd;
  struct gf_w16_bytwo_data *btd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_w16_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;

  if (xor) {
    while (s64 < (uint64_t *) rd.s_top) {
      prod = 0;
      amask = 0x8000;
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
      amask = 0x8000;
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
      t1 = _mm_sub_epi16(t1, one); \
      t1 = _mm_and_si128(t1, ta); \
      prod = _mm_xor_si128(prod, t1); \
      v = _mm_srli_epi64(v, 1); }

#ifdef INTEL_SSE2
static
void 
gf_w16_bytwo_p_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8;
  uint32_t vrev;
  __m128i pp, m1, m2, ta, prod, t1, t2, tp, one, v;
  struct gf_w16_bytwo_data *btd;
  gf_region_data rd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_w16_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  vrev = 0;
  for (i = 0; i < 16; i++) {
    vrev <<= 1;
    if (!(val & (1 << i))) vrev |= 1;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi16(btd->prim_poly&0xffff);
  m1 = _mm_set1_epi16((btd->mask1)&0xffff);
  m2 = _mm_set1_epi16((btd->mask2)&0xffff);
  one = _mm_set1_epi16(1);

  while (d8 < (uint8_t *) rd.d_top) {
    prod = _mm_setzero_si128();
    v = _mm_set1_epi16(vrev);
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
gf_w16_bytwo_b_sse_region_2_noxor(gf_region_data *rd, struct gf_w16_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi16(btd->prim_poly&0xffff);
  m1 = _mm_set1_epi16((btd->mask1)&0xffff);
  m2 = _mm_set1_epi16((btd->mask2)&0xffff);

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
gf_w16_bytwo_b_sse_region_2_xor(gf_region_data *rd, struct gf_w16_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi16(btd->prim_poly&0xffff);
  m1 = _mm_set1_epi16((btd->mask1)&0xffff);
  m2 = _mm_set1_epi16((btd->mask2)&0xffff);

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
gf_w16_bytwo_b_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int itb;
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va, vb;
  struct gf_w16_bytwo_data *btd;
  gf_region_data rd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_w16_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  if (val == 2) {
    if (xor) {
      gf_w16_bytwo_b_sse_region_2_xor(&rd, btd);
    } else {
      gf_w16_bytwo_b_sse_region_2_noxor(&rd, btd);
    }
    gf_do_final_region_alignment(&rd);
    return;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi16(btd->prim_poly&0xffff);
  m1 = _mm_set1_epi16((btd->mask1)&0xffff);
  m2 = _mm_set1_epi16((btd->mask2)&0xffff);

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
gf_w16_bytwo_b_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, tb, prod;
  struct gf_w16_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_w16_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;
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
int gf_w16_bytwo_init(gf_t *gf)
{
  gf_internal_t *h;
  uint64_t ip, m1, m2;
  struct gf_w16_bytwo_data *btd;

  h = (gf_internal_t *) gf->scratch;
  btd = (struct gf_w16_bytwo_data *) (h->private);
  ip = h->prim_poly & 0xffff;
  m1 = 0xfffe;
  m2 = 0x8000;
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
    gf->multiply.w32 = gf_w16_bytwo_p_multiply;
    #ifdef INTEL_SSE2
      if (h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w16_bytwo_p_nosse_multiply_region;
      else
        gf->multiply_region.w32 = gf_w16_bytwo_p_sse_multiply_region;
    #else
      gf->multiply_region.w32 = gf_w16_bytwo_p_nosse_multiply_region;
      if(h->region_type & GF_REGION_SSE)
        return 0;
    #endif
  } else {
    gf->multiply.w32 = gf_w16_bytwo_b_multiply;
    #ifdef INTEL_SSE2
      if (h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w16_bytwo_b_nosse_multiply_region;
      else
        gf->multiply_region.w32 = gf_w16_bytwo_b_sse_multiply_region;
    #else
      gf->multiply_region.w32 = gf_w16_bytwo_b_nosse_multiply_region;
      if(h->region_type & GF_REGION_SSE)
        return 0;
    #endif
  }

  return 1;
}

static
int gf_w16_log_zero_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w16_zero_logtable_data *ltd;
  int i, b;

  h = (gf_internal_t *) gf->scratch;
  ltd = h->private;

  ltd->log_tbl[0] = (-GF_MULT_GROUP_SIZE) + 1;

  bzero(&(ltd->_antilog_tbl[0]), sizeof(ltd->_antilog_tbl));

  ltd->antilog_tbl = &(ltd->_antilog_tbl[GF_FIELD_SIZE * 2]);

  b = 1;
  for (i = 0; i < GF_MULT_GROUP_SIZE; i++) {
      ltd->log_tbl[b] = (uint16_t)i;
      ltd->antilog_tbl[i] = (uint16_t)b;
      ltd->antilog_tbl[i+GF_MULT_GROUP_SIZE] = (uint16_t)b;
      b <<= 1;
      if (b & GF_FIELD_SIZE) {
          b = b ^ h->prim_poly;
      }
  }
  ltd->inv_tbl[0] = 0;  /* Not really, but we need to fill it with something  */
  ltd->inv_tbl[1] = 1;
  for (i = 2; i < GF_FIELD_SIZE; i++) {
    ltd->inv_tbl[i] = ltd->antilog_tbl[GF_MULT_GROUP_SIZE-ltd->log_tbl[i]];
  }

  gf->inverse.w32 = gf_w16_log_zero_inverse;
  gf->divide.w32 = gf_w16_log_zero_divide;
  gf->multiply.w32 = gf_w16_log_zero_multiply;
  gf->multiply_region.w32 = gf_w16_log_zero_multiply_region;
  return 1;
}

static
gf_val_32_t
gf_w16_composite_multiply_recursive(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t b0 = b & 0x00ff;
  uint8_t b1 = (b & 0xff00) >> 8;
  uint8_t a0 = a & 0x00ff;
  uint8_t a1 = (a & 0xff00) >> 8;
  uint8_t a1b1;
  uint16_t rv;

  a1b1 = base_gf->multiply.w32(base_gf, a1, b1);

  rv = ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) | ((base_gf->multiply.w32(base_gf, a1, b0) ^ base_gf->multiply.w32(base_gf, a0, b1) ^ base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 8));
  return rv;
}

static
gf_val_32_t
gf_w16_composite_multiply_inline(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  uint8_t b0 = b & 0x00ff;
  uint8_t b1 = (b & 0xff00) >> 8;
  uint8_t a0 = a & 0x00ff;
  uint8_t a1 = (a & 0xff00) >> 8;
  uint8_t a1b1, *mt;
  uint16_t rv;
  struct gf_w16_composite_data *cd;

  cd = (struct gf_w16_composite_data *) h->private;
  mt = cd->mult_table;

  a1b1 = GF_W8_INLINE_MULTDIV(mt, a1, b1);

  rv = ((GF_W8_INLINE_MULTDIV(mt, a0, b0) ^ a1b1) | ((GF_W8_INLINE_MULTDIV(mt, a1, b0) ^ GF_W8_INLINE_MULTDIV(mt, a0, b1) ^ GF_W8_INLINE_MULTDIV(mt, a1b1, h->prim_poly)) << 8));
  return rv;
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
gf_w16_composite_inverse(gf_t *gf, gf_val_32_t a)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t a0 = a & 0x00ff;
  uint8_t a1 = (a & 0xff00) >> 8;
  uint8_t c0, c1, d, tmp;
  uint16_t c;
  uint8_t a0inv, a1inv;

  if (a0 == 0) {
    a1inv = base_gf->inverse.w32(base_gf, a1);
    c0 = base_gf->multiply.w32(base_gf, a1inv, h->prim_poly);
    c1 = a1inv;
  } else if (a1 == 0) {
    c0 = base_gf->inverse.w32(base_gf, a0);
    c1 = 0;
  } else {
    a1inv = base_gf->inverse.w32(base_gf, a1);
    a0inv = base_gf->inverse.w32(base_gf, a0);

    d = base_gf->multiply.w32(base_gf, a1, a0inv);

    tmp = (base_gf->multiply.w32(base_gf, a1, a0inv) ^ base_gf->multiply.w32(base_gf, a0, a1inv) ^ h->prim_poly);
    tmp = base_gf->inverse.w32(base_gf, tmp);

    d = base_gf->multiply.w32(base_gf, d, tmp);

    c0 = base_gf->multiply.w32(base_gf, (d^1), a0inv);
    c1 = base_gf->multiply.w32(base_gf, d, a1inv);
  }

  c = c0 | (c1 << 8);

  return c;
}

static
void
gf_w16_composite_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t b0 = val & 0x00ff;
  uint8_t b1 = (val & 0xff00) >> 8;
  uint16_t *s16, *d16, *top;
  uint8_t a0, a1, a1b1, *mt;
  gf_region_data rd;
  struct gf_w16_composite_data *cd;

  cd = (struct gf_w16_composite_data *) h->private;
  mt = cd->mult_table;
  
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);

  s16 = rd.s_start;
  d16 = rd.d_start;
  top = rd.d_top;

  if (mt == NULL) {
    if (xor) {
      while (d16 < top) {
        a0 = (*s16) & 0x00ff;
        a1 = ((*s16) & 0xff00) >> 8;
        a1b1 = base_gf->multiply.w32(base_gf, a1, b1);
  
        (*d16) ^= ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) |
                  ((base_gf->multiply.w32(base_gf, a1, b0) ^ 
                    base_gf->multiply.w32(base_gf, a0, b1) ^ 
                    base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 8));
        s16++;
        d16++;
      }
    } else {
      while (d16 < top) {
        a0 = (*s16) & 0x00ff;
        a1 = ((*s16) & 0xff00) >> 8;
        a1b1 = base_gf->multiply.w32(base_gf, a1, b1);
  
        (*d16) = ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) |
                  ((base_gf->multiply.w32(base_gf, a1, b0) ^ 
                    base_gf->multiply.w32(base_gf, a0, b1) ^ 
                    base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 8));
        s16++;
        d16++;
      }
    }
  } else {
    if (xor) {
      while (d16 < top) {
        a0 = (*s16) & 0x00ff;
        a1 = ((*s16) & 0xff00) >> 8;
        a1b1 = GF_W8_INLINE_MULTDIV(mt, a1, b1);
  
        (*d16) ^= ((GF_W8_INLINE_MULTDIV(mt, a0, b0) ^ a1b1) |
                  ((GF_W8_INLINE_MULTDIV(mt, a1, b0) ^ 
                    GF_W8_INLINE_MULTDIV(mt, a0, b1) ^ 
                    GF_W8_INLINE_MULTDIV(mt, a1b1, h->prim_poly)) << 8));
        s16++;
        d16++;
      }
    } else {
      while (d16 < top) {
        a0 = (*s16) & 0x00ff;
        a1 = ((*s16) & 0xff00) >> 8;
        a1b1 = GF_W8_INLINE_MULTDIV(mt, a1, b1);
  
        (*d16) = ((GF_W8_INLINE_MULTDIV(mt, a0, b0) ^ a1b1) |
                  ((GF_W8_INLINE_MULTDIV(mt, a1, b0) ^ 
                    GF_W8_INLINE_MULTDIV(mt, a0, b1) ^ 
                    GF_W8_INLINE_MULTDIV(mt, a1b1, h->prim_poly)) << 8));
        s16++;
        d16++;
      }
    }
  }
}

static
void
gf_w16_composite_multiply_region_alt(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint8_t val0 = val & 0x00ff;
  uint8_t val1 = (val & 0xff00) >> 8;
  gf_region_data rd;
  int sub_reg_size;
  uint8_t *slow, *shigh;
  uint8_t *dlow, *dhigh, *top;;

  /* JSP: I want the two pointers aligned wrt each other on 16 byte 
     boundaries.  So I'm going to make sure that the area on 
     which the two operate is a multiple of 32. Of course, that 
     junks up the mapping, but so be it -- that's why we have extract_word.... */

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);

  slow = (uint8_t *) rd.s_start;
  dlow = (uint8_t *) rd.d_start;
  top = (uint8_t *)  rd.d_top;
  sub_reg_size = (top - dlow)/2;
  shigh = slow + sub_reg_size;
  dhigh = dlow + sub_reg_size;

  base_gf->multiply_region.w32(base_gf, slow, dlow, val0, sub_reg_size, xor);
  base_gf->multiply_region.w32(base_gf, shigh, dlow, val1, sub_reg_size, 1);
  base_gf->multiply_region.w32(base_gf, slow, dhigh, val1, sub_reg_size, xor);
  base_gf->multiply_region.w32(base_gf, shigh, dhigh, val0, sub_reg_size, 1);
  base_gf->multiply_region.w32(base_gf, shigh, dhigh, base_gf->multiply.w32(base_gf, h->prim_poly, val1), sub_reg_size, 1);

  gf_do_final_region_alignment(&rd);
}

static
int gf_w16_composite_init(gf_t *gf)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  struct gf_w16_composite_data *cd;

  if (h->base_gf == NULL) return 0;

  cd = (struct gf_w16_composite_data *) h->private;
  cd->mult_table = gf_w8_get_mult_table(h->base_gf);

  if (h->region_type & GF_REGION_ALTMAP) {
    gf->multiply_region.w32 = gf_w16_composite_multiply_region_alt;
  } else {
    gf->multiply_region.w32 = gf_w16_composite_multiply_region;
  }

  if (cd->mult_table == NULL) {
    gf->multiply.w32 = gf_w16_composite_multiply_recursive;
  } else {
    gf->multiply.w32 = gf_w16_composite_multiply_inline;
  }
  gf->divide.w32 = NULL;
  gf->inverse.w32 = gf_w16_composite_inverse;

  return 1;
}

static
void
gf_w16_group_4_set_shift_tables(uint16_t *shift, uint16_t val, gf_internal_t *h)
{
  int i, j;

  shift[0] = 0;
  for (i = 0; i < 16; i += 2) {
    j = (shift[i>>1] << 1);
    if (j & (1 << 16)) j ^= h->prim_poly;
    shift[i] = j;
    shift[i^1] = j^val;
  }
}

static
inline
gf_val_32_t
gf_w16_group_4_4_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint16_t p, l, ind, r, a16;

  struct gf_w16_group_4_4_data *d44;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;

  d44 = (struct gf_w16_group_4_4_data *) h->private;
  gf_w16_group_4_set_shift_tables(d44->shift, b, h);

  a16 = a;
  ind = a16 >> 12;
  a16 <<= 4;
  p = d44->shift[ind];
  r = p & 0xfff;
  l = p >> 12;
  ind = a16 >> 12;
  a16 <<= 4;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (r << 4));
  r = p & 0xfff;
  l = p >> 12;
  ind = a16 >> 12;
  a16 <<= 4;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (r << 4));
  r = p & 0xfff;
  l = p >> 12;
  ind = a16 >> 12;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (r << 4));
  return p;
}

static
void gf_w16_group_4_4_region_multiply(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint16_t p, l, ind, r, a16, p16;
  struct gf_w16_group_4_4_data *d44;
  gf_region_data rd;
  uint16_t *s16, *d16, *top;
  
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  d44 = (struct gf_w16_group_4_4_data *) h->private;
  gf_w16_group_4_set_shift_tables(d44->shift, val, h);

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 2);
  gf_do_initial_region_alignment(&rd);

  s16 = (uint16_t *) rd.s_start;
  d16 = (uint16_t *) rd.d_start;
  top = (uint16_t *) rd.d_top;

  while (d16 < top) {
    p = 0;
    a16 = *s16;
    p16 = (xor) ? *d16 : 0;
    ind = a16 >> 12;
    a16 <<= 4;
    p = d44->shift[ind];
    r = p & 0xfff;
    l = p >> 12;
    ind = a16 >> 12;
    a16 <<= 4;
    p = (d44->shift[ind] ^ d44->reduce[l] ^ (r << 4));
    r = p & 0xfff;
    l = p >> 12;
    ind = a16 >> 12;
    a16 <<= 4;
    p = (d44->shift[ind] ^ d44->reduce[l] ^ (r << 4));
    r = p & 0xfff;
    l = p >> 12;
    ind = a16 >> 12;
    p = (d44->shift[ind] ^ d44->reduce[l] ^ (r << 4));
    p ^= p16;
    *d16 = p;
    d16++;
    s16++;
  }
  gf_do_final_region_alignment(&rd);
}

static
int gf_w16_group_init(gf_t *gf)
{
  int i, j, p;
  struct gf_w16_group_4_4_data *d44;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;

  d44 = (struct gf_w16_group_4_4_data *) h->private;
  d44->reduce[0] = 0;
  for (i = 0; i < 16; i++) {
    p = 0;
    for (j = 0; j < 4; j++) {
      if (i & (1 << j)) p ^= (h->prim_poly << j);
    }
    d44->reduce[p>>16] = (p&0xffff);
  }

  gf->multiply.w32 = gf_w16_group_4_4_multiply;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = gf_w16_group_4_4_region_multiply;

  return 1;
}

int gf_w16_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2)
{
  switch(mult_type)
  {
    case GF_MULT_TABLE:
      return sizeof(gf_internal_t) + sizeof(struct gf_w16_lazytable_data) + 64;
      break;
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:
      return sizeof(gf_internal_t) + sizeof(struct gf_w16_bytwo_data);
      break;
    case GF_MULT_LOG_ZERO:
      return sizeof(gf_internal_t) + sizeof(struct gf_w16_zero_logtable_data) + 64;
      break;
    case GF_MULT_LOG_TABLE:
      return sizeof(gf_internal_t) + sizeof(struct gf_w16_logtable_data) + 64;
      break;
    case GF_MULT_DEFAULT:
    case GF_MULT_SPLIT_TABLE: 
      if (arg1 == 8 && arg2 == 8) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w16_split_8_8_data) + 64;
      } else if ((arg1 == 8 && arg2 == 16) || (arg2 == 8 && arg1 == 16)) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w16_logtable_data) + 64;
      } else if (mult_type == GF_MULT_DEFAULT || 
                 (arg1 == 4 && arg2 == 16) || (arg2 == 4 && arg1 == 16)) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w16_logtable_data) + 64;
      }
      return 0;
      break;
    case GF_MULT_GROUP:     
      return sizeof(gf_internal_t) + sizeof(struct gf_w16_group_4_4_data) + 64;
      break;
    case GF_MULT_CARRY_FREE:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_SHIFT:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_COMPOSITE:
      return sizeof(gf_internal_t) + sizeof(struct gf_w16_composite_data) + 64;
      break;

    default:
      return 0;
   }
   return 0;
}

int gf_w16_init(gf_t *gf)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

  /* Allen: set default primitive polynomial / irreducible polynomial if needed */

  if (h->prim_poly == 0) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      h->prim_poly = gf_composite_get_default_poly(h->base_gf);
      if (h->prim_poly == 0) return 0;
    } else { 

     /* Allen: use the following primitive polynomial to make 
               carryless multiply work more efficiently for GF(2^16).

        h->prim_poly = 0x1002d;

        The following is the traditional primitive polynomial for GF(2^16) */

      h->prim_poly = 0x1100b;
    } 
  }

  if (h->mult_type != GF_MULT_COMPOSITE) h->prim_poly |= (1 << 16);

  gf->multiply.w32 = NULL;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = NULL;

  switch(h->mult_type) {
    case GF_MULT_LOG_ZERO:    if (gf_w16_log_zero_init(gf) == 0) return 0; break;
    case GF_MULT_LOG_TABLE:   if (gf_w16_log_init(gf) == 0) return 0; break;
    case GF_MULT_DEFAULT: 
    case GF_MULT_SPLIT_TABLE: if (gf_w16_split_init(gf) == 0) return 0; break;
    case GF_MULT_TABLE:       if (gf_w16_table_init(gf) == 0) return 0; break;
    case GF_MULT_CARRY_FREE:  if (gf_w16_cfm_init(gf) == 0) return 0; break;
    case GF_MULT_SHIFT:       if (gf_w16_shift_init(gf) == 0) return 0; break;
    case GF_MULT_COMPOSITE:   if (gf_w16_composite_init(gf) == 0) return 0; break;
    case GF_MULT_BYTWO_p: 
    case GF_MULT_BYTWO_b:     if (gf_w16_bytwo_init(gf) == 0) return 0; break;
    case GF_MULT_GROUP:       if (gf_w16_group_init(gf) == 0) return 0; break;
    default: return 0;
  }
  if (h->divide_type == GF_DIVIDE_EUCLID) {
    gf->divide.w32 = gf_w16_divide_from_inverse;
    gf->inverse.w32 = gf_w16_euclid;
  } else if (h->divide_type == GF_DIVIDE_MATRIX) {
    gf->divide.w32 = gf_w16_divide_from_inverse;
    gf->inverse.w32 = gf_w16_matrix;
  }

  if (gf->divide.w32 == NULL) {
    gf->divide.w32 = gf_w16_divide_from_inverse;
    if (gf->inverse.w32 == NULL) gf->inverse.w32 = gf_w16_euclid;
  }

  if (gf->inverse.w32 == NULL)  gf->inverse.w32 = gf_w16_inverse_from_divide;

  if (h->region_type & GF_REGION_ALTMAP) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      gf->extract_word.w32 = gf_w16_composite_extract_word;
    } else {
      gf->extract_word.w32 = gf_w16_split_extract_word;
    }
  } else if (h->region_type == GF_REGION_CAUCHY) {
    gf->multiply_region.w32 = gf_wgen_cauchy_region;
    gf->extract_word.w32 = gf_wgen_extract_word;
  } else {
    gf->extract_word.w32 = gf_w16_extract_word;
  }
  if (gf->multiply_region.w32 == NULL) {
    gf->multiply_region.w32 = gf_w16_multiply_region_from_single;
  }
  return 1;
}

/* Inline setup functions */

uint16_t *gf_w16_get_log_table(gf_t *gf)
{
  struct gf_w16_logtable_data *ltd;

  if (gf->multiply.w32 == gf_w16_log_multiply) {
    ltd = (struct gf_w16_logtable_data *) ((gf_internal_t *) gf->scratch)->private;
    return (uint16_t *) ltd->log_tbl;
  }
  return NULL;
}

uint16_t *gf_w16_get_mult_alog_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w16_logtable_data *ltd;

  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w16_log_multiply) {
    ltd = (struct gf_w16_logtable_data *) h->private;
    return (uint16_t *) ltd->antilog_tbl;
  }
  return NULL;
}

uint16_t *gf_w16_get_div_alog_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_w16_logtable_data *ltd;

  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w16_log_multiply) {
    ltd = (struct gf_w16_logtable_data *) h->private;
    return (uint16_t *) ltd->d_antilog;
  }
  return NULL;
}
