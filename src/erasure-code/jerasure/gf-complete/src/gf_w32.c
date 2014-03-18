/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w32.c
 *
 * Routines for 32-bit Galois fields
 */


#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

#define GF_FIELD_WIDTH (32)
#define GF_FIRST_BIT (1 << 31)

#define GF_BASE_FIELD_WIDTH (16)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)
#define GF_BASE_FIELD_GROUP_SIZE  GF_BASE_FIELD_SIZE-1
#define GF_MULTBY_TWO(p) (((p) & GF_FIRST_BIT) ? (((p) << 1) ^ h->prim_poly) : (p) << 1)

struct gf_split_2_32_lazy_data {
    uint32_t      tables[16][4];
    uint32_t      last_value;
};

struct gf_w32_split_8_8_data {
    uint32_t      tables[7][256][256];
    uint32_t      region_tables[4][256];
    uint32_t      last_value;
};

struct gf_w32_group_data {
    uint32_t *reduce;
    uint32_t *shift;
    int      tshift;
    uint64_t rmask;
    uint32_t *memory;
};

struct gf_split_16_32_lazy_data {
    uint32_t      tables[2][(1<<16)];
    uint32_t      last_value;
};

struct gf_split_8_32_lazy_data {
    uint32_t      tables[4][256];
    uint32_t      last_value;
};

struct gf_split_4_32_lazy_data {
    uint32_t      tables[8][16];
    uint32_t      last_value;
};

struct gf_w32_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

struct gf_w32_composite_data {
  uint16_t *log;
  uint16_t *alog;
};

#define MM_PRINT32(s, r) { uint8_t blah[16], ii; printf("%-12s", s); _mm_storeu_si128((__m128i *)blah, r); for (ii = 0; ii < 16; ii += 4) printf(" %02x%02x%02x%02x", blah[15-ii], blah[14-ii], blah[13-ii], blah[12-ii]); printf("\n"); }

#define MM_PRINT8(s, r) { uint8_t blah[16], ii; printf("%-12s", s); _mm_storeu_si128((__m128i *)blah, r); for (ii = 0; ii < 16; ii += 1) printf("%s%02x", (ii%4==0) ? "   " : " ", blah[15-ii]); printf("\n"); }

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

static
inline
uint32_t gf_w32_inverse_from_divide (gf_t *gf, uint32_t a)
{
  return gf->divide.w32(gf, 1, a);
}

static
inline
uint32_t gf_w32_divide_from_inverse (gf_t *gf, uint32_t a, uint32_t b)
{
  b = gf->inverse.w32(gf, b);
  return gf->multiply.w32(gf, a, b);
}

static
void
gf_w32_multiply_region_from_single(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int 
xor)
{
  int i;
  uint32_t *s32;
  uint32_t *d32;
   
  s32 = (uint32_t *) src;
  d32 = (uint32_t *) dest; 
 
  if (xor) {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      d32[i] ^= gf->multiply.w32(gf, val, s32[i]);
    } 
  } else {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      d32[i] = gf->multiply.w32(gf, val, s32[i]);
    } 
  }
}

#if defined(INTEL_SSE4_PCLMUL)

static 
void
gf_w32_clm_multiply_region_from_single_2(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{

  int i;
  uint32_t *s32;
  uint32_t *d32;
  
  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;
  
  prim_poly = _mm_set_epi32(0, 0, 1, (uint32_t)(h->prim_poly & 0xffffffffULL));
   
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);
  s32 = (uint32_t *) src;
  d32 = (uint32_t *) dest; 
 
  if (xor) {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      b = _mm_insert_epi32 (a, s32[i], 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      d32[i] ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
    } 
  } else {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      b = _mm_insert_epi32 (a, s32[i], 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      d32[i] = ((gf_val_32_t)_mm_extract_epi32(result, 0));
    } 
  }
}
#endif

#if defined(INTEL_SSE4_PCLMUL) 

static 
void
gf_w32_clm_multiply_region_from_single_3(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{

  int i;
  uint32_t *s32;
  uint32_t *d32;
  
  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;
  
  prim_poly = _mm_set_epi32(0, 0, 1, (uint32_t)(h->prim_poly & 0xffffffffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  
  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);
  
  s32 = (uint32_t *) src;
  d32 = (uint32_t *) dest; 
 
  if (xor) {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      b = _mm_insert_epi32 (a, s32[i], 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      d32[i] ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
    } 
  } else {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      b = _mm_insert_epi32 (a, s32[i], 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      d32[i] = ((gf_val_32_t)_mm_extract_epi32(result, 0));
    } 
  }
}
#endif

#if defined(INTEL_SSE4_PCLMUL)
static 
void
gf_w32_clm_multiply_region_from_single_4(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  int i;
  uint32_t *s32;
  uint32_t *d32;
  
  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;
  
  prim_poly = _mm_set_epi32(0, 0, 1, (uint32_t)(h->prim_poly & 0xffffffffULL));

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  
  a = _mm_insert_epi32 (_mm_setzero_si128(), val, 0);
  
  s32 = (uint32_t *) src;
  d32 = (uint32_t *) dest; 
 
  if (xor) {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      b = _mm_insert_epi32 (a, s32[i], 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      d32[i] ^= ((gf_val_32_t)_mm_extract_epi32(result, 0));
    } 
  } else {
    for (i = 0; i < bytes/sizeof(uint32_t); i++) {
      b = _mm_insert_epi32 (a, s32[i], 0);
      result = _mm_clmulepi64_si128 (a, b, 0);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
      result = _mm_xor_si128 (result, w);
      d32[i] = ((gf_val_32_t)_mm_extract_epi32(result, 0));
    } 
  }
}
#endif

static
inline
uint32_t gf_w32_euclid (gf_t *gf, uint32_t b)
{
  uint32_t e_i, e_im1, e_ip1;
  uint32_t d_i, d_im1, d_ip1;
  uint32_t y_i, y_im1, y_ip1;
  uint32_t c_i;

  if (b == 0) return -1;
  e_im1 = ((gf_internal_t *) (gf->scratch))->prim_poly; 
  e_i = b;
  d_im1 = 32;
  for (d_i = d_im1-1; ((1 << d_i) & e_i) == 0; d_i--) ;
  y_i = 1;
  y_im1 = 0;

  while (e_i != 1) {

    e_ip1 = e_im1;
    d_ip1 = d_im1;
    c_i = 0;

    while (d_ip1 >= d_i) {
      c_i ^= (1 << (d_ip1 - d_i));
      e_ip1 ^= (e_i << (d_ip1 - d_i));
      d_ip1--;
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
gf_val_32_t gf_w32_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint32_t *r32, rv;

  r32 = (uint32_t *) start;
  rv = r32[index];
  return rv;
}

static
gf_val_32_t gf_w32_composite_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  int sub_size;
  gf_internal_t *h;
  uint8_t *r8, *top;
  uint32_t a, b, *r32;
  gf_region_data rd;

  h = (gf_internal_t *) gf->scratch;
  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 32);
  r32 = (uint32_t *) start;
  if (r32 + index < (uint32_t *) rd.d_start) return r32[index];
  if (r32 + index >= (uint32_t *) rd.d_top) return r32[index];
  index -= (((uint32_t *) rd.d_start) - r32);
  r8 = (uint8_t *) rd.d_start;
  top = (uint8_t *) rd.d_top;
  sub_size = (top-r8)/2;

  a = h->base_gf->extract_word.w32(h->base_gf, r8, sub_size, index);
  b = h->base_gf->extract_word.w32(h->base_gf, r8+sub_size, sub_size, index);
  return (a | (b << 16));
}

static
gf_val_32_t gf_w32_split_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  int i;
  uint32_t *r32, rv;
  uint8_t *r8;
  gf_region_data rd;

  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 64);
  r32 = (uint32_t *) start;
  if (r32 + index < (uint32_t *) rd.d_start) return r32[index];
  if (r32 + index >= (uint32_t *) rd.d_top) return r32[index];
  index -= (((uint32_t *) rd.d_start) - r32);
  r8 = (uint8_t *) rd.d_start;
  r8 += ((index & 0xfffffff0)*4);
  r8 += (index & 0xf);
  r8 += 48;
  rv =0;
  for (i = 0; i < 4; i++) {
    rv <<= 8;
    rv |= *r8;
    r8 -= 16;
  }
  return rv;
}


static
inline
uint32_t gf_w32_matrix (gf_t *gf, uint32_t b)
{
  return gf_bitmatrix_inverse(b, 32, ((gf_internal_t *) (gf->scratch))->prim_poly);
}

/* JSP: GF_MULT_SHIFT: The world's dumbest multiplication algorithm.  I only
   include it for completeness.  It does have the feature that it requires no
   extra memory.  
*/




static
inline
gf_val_32_t
gf_w32_clm_multiply_2 (gf_t *gf, gf_val_32_t a32, gf_val_32_t b32)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;


  a = _mm_insert_epi32 (_mm_setzero_si128(), a32, 0);
  b = _mm_insert_epi32 (a, b32, 0);
  
  prim_poly = _mm_set_epi32(0, 0, 1, (uint32_t)(h->prim_poly & 0xffffffffULL));
  
  /* Do the initial multiply */

  result = _mm_clmulepi64_si128 (a, b, 0);

  /* Ben: Do prim_poly reduction twice. We are guaranteed that we will only
     have to do the reduction at most twice, because (w-2)/z == 2. Where
     z is equal to the number of zeros after the leading 1 

   _mm_clmulepi64_si128 is the carryless multiply operation. Here
   _mm_srli_si128 shifts the result to the right by 4 bytes. This allows
   us to multiply the prim_poly by the leading bits of the result. We
   then xor the result of that operation back with the result.*/

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));
#endif
  return rv;
}
static
inline
gf_val_32_t
gf_w32_clm_multiply_3 (gf_t *gf, gf_val_32_t a32, gf_val_32_t b32)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;


  a = _mm_insert_epi32 (_mm_setzero_si128(), a32, 0);
  b = _mm_insert_epi32 (a, b32, 0);

  prim_poly = _mm_set_epi32(0, 0, 1, (uint32_t)(h->prim_poly & 0xffffffffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));
#endif
  return rv;
}

static
inline
gf_val_32_t
gf_w32_clm_multiply_4 (gf_t *gf, gf_val_32_t a32, gf_val_32_t b32)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;


  a = _mm_insert_epi32 (_mm_setzero_si128(), a32, 0);
  b = _mm_insert_epi32 (a, b32, 0);

  prim_poly = _mm_set_epi32(0, 0, 1, (uint32_t)(h->prim_poly & 0xffffffffULL));

  /* Do the initial multiply */
  
  result = _mm_clmulepi64_si128 (a, b, 0);

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);
  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_si128 (result, 4), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */
  
  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));
#endif
  return rv;
}


static
inline
uint32_t
gf_w32_shift_multiply (gf_t *gf, uint32_t a32, uint32_t b32)
{
  uint64_t product, i, pp, a, b, one;
  gf_internal_t *h;

  a = a32;
  b = b32;
  h = (gf_internal_t *) gf->scratch;
  one = 1;
  pp = h->prim_poly | (one << 32);

  product = 0;

  for (i = 0; i < GF_FIELD_WIDTH; i++) { 
    if (a & (one << i)) product ^= (b << i);
  }
  for (i = (GF_FIELD_WIDTH*2-2); i >= GF_FIELD_WIDTH; i--) {
    if (product & (one << i)) product ^= (pp << (i-GF_FIELD_WIDTH)); 
  }
  return product;
}

  static 
int gf_w32_cfm_init(gf_t *gf)
{
  gf->inverse.w32 = gf_w32_euclid;
  gf->multiply_region.w32 = gf_w32_multiply_region_from_single;
  
  /*Ben: We also check to see if the prim poly will work for pclmul */
  /*Ben: Check to see how many reduction steps it will take*/

#if defined(INTEL_SSE4_PCLMUL)
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

  if ((0xfffe0000 & h->prim_poly) == 0){ 
    gf->multiply.w32 = gf_w32_clm_multiply_2;
    gf->multiply_region.w32 = gf_w32_clm_multiply_region_from_single_2;
  }else if ((0xffc00000 & h->prim_poly) == 0){
    gf->multiply.w32 = gf_w32_clm_multiply_3;
    gf->multiply_region.w32 = gf_w32_clm_multiply_region_from_single_3;
  }else if ((0xfe000000 & h->prim_poly) == 0){
    gf->multiply.w32 = gf_w32_clm_multiply_4;
    gf->multiply_region.w32 = gf_w32_clm_multiply_region_from_single_4;
  } else {
    return 0;
  }
  return 1;
  #endif

  return 0;
}

  static 
int gf_w32_shift_init(gf_t *gf)
{
  gf->inverse.w32 = gf_w32_euclid;
  gf->multiply_region.w32 = gf_w32_multiply_region_from_single;
  gf->multiply.w32 = gf_w32_shift_multiply;
  return 1;
}

static
  void
gf_w32_group_set_shift_tables(uint32_t *shift, uint32_t val, gf_internal_t *h)
{
  int i;
  uint32_t j;

  shift[0] = 0;

  for (i = 1; i < (1 << h->arg1); i <<= 1) {
    for (j = 0; j < i; j++) shift[i|j] = shift[j]^val;
    if (val & GF_FIRST_BIT) {
      val <<= 1;
      val ^= h->prim_poly;
    } else {
      val <<= 1;
    }
  }
}

  static
void gf_w32_group_s_equals_r_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int leftover, rs;
  uint32_t p, l, ind, a32;
  int bits_left;
  int g_s;
  gf_region_data rd;
  uint32_t *s32, *d32, *top;
  struct gf_w32_group_data *gd;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gd = (struct gf_w32_group_data *) h->private;
  g_s = h->arg1;
  gf_w32_group_set_shift_tables(gd->shift, val, h);

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  gf_do_initial_region_alignment(&rd);

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;

  leftover = 32 % g_s;
  if (leftover == 0) leftover = g_s;

  while (d32 < top) {
    rs = 32 - leftover;
    a32 = *s32;
    ind = a32 >> rs;
    a32 <<= leftover;
    p = gd->shift[ind];

    bits_left = rs;
    rs = 32 - g_s;

    while (bits_left > 0) {
      bits_left -= g_s;
      ind = a32 >> rs;
      a32 <<= g_s;
      l = p >> rs;
      p = (gd->shift[ind] ^ gd->reduce[l] ^ (p << g_s));
    }
    if (xor) p ^= *d32;
    *d32 = p;
    d32++;
    s32++;
  }
  gf_do_final_region_alignment(&rd);
}

  static
void gf_w32_group_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint32_t *s32, *d32, *top;
  int i;
  int leftover;
  uint64_t p, l, r;
  uint32_t a32, ind;
  int g_s, g_r;
  struct gf_w32_group_data *gd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  g_s = h->arg1;
  g_r = h->arg2;
  gd = (struct gf_w32_group_data *) h->private;
  gf_w32_group_set_shift_tables(gd->shift, val, h);

  leftover = GF_FIELD_WIDTH % g_s;
  if (leftover == 0) leftover = g_s;

  gd = (struct gf_w32_group_data *) h->private;
  gf_w32_group_set_shift_tables(gd->shift, val, h);

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  gf_do_initial_region_alignment(&rd);

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;

  while (d32 < top) {
    a32 = *s32;
    ind = a32 >> (GF_FIELD_WIDTH - leftover);
    p = gd->shift[ind];
    p <<= g_s;
    a32 <<= leftover;
  
    i = (GF_FIELD_WIDTH - leftover);
    while (i > g_s) {
      ind = a32 >> (GF_FIELD_WIDTH-g_s);
      p ^= gd->shift[ind];
      a32 <<= g_s;
      p <<= g_s;
      i -= g_s;
    }
  
    ind = a32 >> (GF_FIELD_WIDTH-g_s);
    p ^= gd->shift[ind];
  
    for (i = gd->tshift ; i >= 0; i -= g_r) {
      l = p & (gd->rmask << i);
      r = gd->reduce[l >> (i+32)];
      r <<= (i);
      p ^= r;
    }

    if (xor) p ^= *d32;
    *d32 = p;
    d32++;
    s32++;
  }
  gf_do_final_region_alignment(&rd);
}

static
inline
gf_val_32_t
gf_w32_group_s_equals_r_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int leftover, rs;
  uint32_t p, l, ind, a32;
  int bits_left;
  int g_s;

  struct gf_w32_group_data *gd;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  g_s = h->arg1;

  gd = (struct gf_w32_group_data *) h->private;
  gf_w32_group_set_shift_tables(gd->shift, b, h);

  leftover = 32 % g_s;
  if (leftover == 0) leftover = g_s;

  rs = 32 - leftover;
  a32 = a;
  ind = a32 >> rs;
  a32 <<= leftover;
  p = gd->shift[ind];

  bits_left = rs;
  rs = 32 - g_s;

  while (bits_left > 0) {
    bits_left -= g_s;
    ind = a32 >> rs;
    a32 <<= g_s;
    l = p >> rs;
    p = (gd->shift[ind] ^ gd->reduce[l] ^ (p << g_s));
  }
  return p;
}

static
inline
gf_val_32_t
gf_w32_group_4_4_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t p, l, ind, a32;

  struct gf_w32_group_data *d44;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;

  d44 = (struct gf_w32_group_data *) h->private;
  gf_w32_group_set_shift_tables(d44->shift, b, h);

  p = 0;
  a32 = a;
  ind = a32 >> 28;
  a32 <<= 4;
  p = d44->shift[ind];
  ind = a32 >> 28;
  a32 <<= 4;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  ind = a32 >> 28;
  a32 <<= 4;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  ind = a32 >> 28;
  a32 <<= 4;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  ind = a32 >> 28;
  a32 <<= 4;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  ind = a32 >> 28;
  a32 <<= 4;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  ind = a32 >> 28;
  a32 <<= 4;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  ind = a32 >> 28;
  l = p >> 28;
  p = (d44->shift[ind] ^ d44->reduce[l] ^ (p << 4));
  return p;
}

static
inline
gf_val_32_t
gf_w32_group_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int i;
  int leftover;
  uint64_t p, l, r;
  uint32_t a32, ind;
  int g_s, g_r;
  struct gf_w32_group_data *gd;

  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  g_s = h->arg1;
  g_r = h->arg2;
  gd = (struct gf_w32_group_data *) h->private;
  gf_w32_group_set_shift_tables(gd->shift, b, h);

  leftover = GF_FIELD_WIDTH % g_s;
  if (leftover == 0) leftover = g_s;

  a32 = a;
  ind = a32 >> (GF_FIELD_WIDTH - leftover);
  p = gd->shift[ind];
  p <<= g_s;
  a32 <<= leftover;

  i = (GF_FIELD_WIDTH - leftover);
  while (i > g_s) {
    ind = a32 >> (GF_FIELD_WIDTH-g_s);
    p ^= gd->shift[ind];
    a32 <<= g_s;
    p <<= g_s;
    i -= g_s;
  }

  ind = a32 >> (GF_FIELD_WIDTH-g_s);
  p ^= gd->shift[ind];

  for (i = gd->tshift ; i >= 0; i -= g_r) {
    l = p & (gd->rmask << i);
    r = gd->reduce[l >> (i+32)];
    r <<= (i);
    p ^= r;
  }
  return p;
}

static
inline
gf_val_32_t
gf_w32_bytwo_b_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, bmask;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  prod = 0;
  bmask = 0x80000000;

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
inline
gf_val_32_t
gf_w32_bytwo_p_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, pmask, amask;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;


  prod = 0;
  pmask = 0x80000000;
  amask = 0x80000000;

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
void
gf_w32_bytwo_p_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, prod, amask;
  gf_region_data rd;
  struct gf_w32_bytwo_data *btd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_w32_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;

  if (xor) {
    while (s64 < (uint64_t *) rd.s_top) {
      prod = 0;
      amask = 0x80000000;
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
      amask = 0x80000000;
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
      t1 = _mm_sub_epi32(t1, one); \
      t1 = _mm_and_si128(t1, ta); \
      prod = _mm_xor_si128(prod, t1); \
      v = _mm_srli_epi64(v, 1); }

#ifdef INTEL_SSE2
static
void
gf_w32_bytwo_p_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8;
  uint32_t vrev;
  __m128i pp, m1, m2, ta, prod, t1, t2, tp, one, v;
  struct gf_w32_bytwo_data *btd;
  gf_region_data rd;
   
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_w32_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  vrev = 0;
  for (i = 0; i < 32; i++) {
    vrev <<= 1;
    if (!(val & (1 << i))) vrev |= 1;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi32(btd->prim_poly&0xffffffff);
  m1 = _mm_set1_epi32((btd->mask1)&0xffffffff);
  m2 = _mm_set1_epi32((btd->mask2)&0xffffffff);
  one = _mm_set1_epi32(1);

  while (d8 < (uint8_t *) rd.d_top) {
    prod = _mm_setzero_si128();
    v = _mm_set1_epi32(vrev);
    ta = _mm_load_si128((__m128i *) s8);
    tp = (!xor) ? _mm_setzero_si128() : _mm_load_si128((__m128i *) d8);
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP; BYTWO_P_ONESTEP;
    _mm_store_si128((__m128i *) d8, _mm_xor_si128(prod, tp));
    d8 += 16;
    s8 += 16;
  }
  gf_do_final_region_alignment(&rd);
}
#endif

static
void
gf_w32_bytwo_b_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, tb, prod;
  struct gf_w32_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_w32_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;
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

#ifdef INTEL_SSE2
static
void
gf_w32_bytwo_b_sse_region_2_noxor(gf_region_data *rd, struct gf_w32_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi32(btd->prim_poly&0xffffffff);
  m1 = _mm_set1_epi32((btd->mask1)&0xffffffff);
  m2 = _mm_set1_epi32((btd->mask2)&0xffffffff);

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
gf_w32_bytwo_b_sse_region_2_xor(gf_region_data *rd, struct gf_w32_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi32(btd->prim_poly&0xffffffff);
  m1 = _mm_set1_epi32((btd->mask1)&0xffffffff);
  m2 = _mm_set1_epi32((btd->mask2)&0xffffffff);

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
gf_w32_bytwo_b_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint32_t itb;
  uint8_t *d8, *s8;
  __m128i pp, m1, m2, t1, t2, va, vb;
  struct gf_w32_bytwo_data *btd;
  gf_region_data rd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_w32_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  if (val == 2) {
    if (xor) {
      gf_w32_bytwo_b_sse_region_2_xor(&rd, btd);
    } else {
      gf_w32_bytwo_b_sse_region_2_noxor(&rd, btd);
    }
    gf_do_final_region_alignment(&rd);
    return;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi32(btd->prim_poly&0xffffffff);
  m1 = _mm_set1_epi32((btd->mask1)&0xffffffff);
  m2 = _mm_set1_epi32((btd->mask2)&0xffffffff);

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
int gf_w32_bytwo_init(gf_t *gf)
{
  gf_internal_t *h;
  uint64_t ip, m1, m2;
  struct gf_w32_bytwo_data *btd;

  h = (gf_internal_t *) gf->scratch;
  btd = (struct gf_w32_bytwo_data *) (h->private);
  ip = h->prim_poly & 0xffffffff;
  m1 = 0xfffffffe;
  m2 = 0x80000000;
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
    gf->multiply.w32 = gf_w32_bytwo_p_multiply;
    #ifdef INTEL_SSE2
      if (h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w32_bytwo_p_nosse_multiply_region; 
      else
        gf->multiply_region.w32 = gf_w32_bytwo_p_sse_multiply_region; 
    #else
      gf->multiply_region.w32 = gf_w32_bytwo_p_nosse_multiply_region; 
      if(h->region_type & GF_REGION_SSE)
        return 0;
    #endif
  } else {
    gf->multiply.w32 = gf_w32_bytwo_b_multiply; 
    #ifdef INTEL_SSE2
      if (h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w32_bytwo_b_nosse_multiply_region; 
      else
        gf->multiply_region.w32 = gf_w32_bytwo_b_sse_multiply_region; 
    #else
      gf->multiply_region.w32 = gf_w32_bytwo_b_nosse_multiply_region; 
      if(h->region_type & GF_REGION_SSE)
        return 0;
    #endif
  }

  gf->inverse.w32 = gf_w32_euclid;
  return 1;
}

static
inline
uint32_t
gf_w32_split_8_8_multiply (gf_t *gf, uint32_t a32, uint32_t b32)
{
  uint32_t product, i, j, mask, tb;
  gf_internal_t *h;
  struct gf_w32_split_8_8_data *d8;
  
  h = (gf_internal_t *) gf->scratch;
  d8 = (struct gf_w32_split_8_8_data *) h->private;
  product = 0;
  mask = 0xff;

  for (i = 0; i < 4; i++) {
    tb = b32;
    for (j = 0; j < 4; j++) {
      product ^= d8->tables[i+j][a32&mask][tb&mask];
      tb >>= 8;
    }
    a32 >>= 8;
  }
  return product;
}

static
inline
void
gf_w32_split_8_32_lazy_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h;
  uint32_t *s32, *d32, *top, p, a, v;
  struct gf_split_8_32_lazy_data *d8;
  struct gf_w32_split_8_8_data *d88;
  uint32_t *t[4];
  int i, j, k, change;
  uint32_t pp;
  gf_region_data rd;
  
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;
  if (h->arg1 == 32 || h->arg2 == 32 || h->mult_type == GF_MULT_DEFAULT) {
    d8 = (struct gf_split_8_32_lazy_data *) h->private;
    for (i = 0; i < 4; i++) t[i] = d8->tables[i];
    change = (val != d8->last_value);
    if (change) d8->last_value = val;
  } else {
    d88 = (struct gf_w32_split_8_8_data *) h->private;
    for (i = 0; i < 4; i++) t[i] = d88->region_tables[i];
    change = (val != d88->last_value);
    if (change) d88->last_value = val;
  }
  pp = h->prim_poly;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  gf_do_initial_region_alignment(&rd);

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;
  
  if (change) {
    v = val;
    for (i = 0; i < 4; i++) {
      t[i][0] = 0;
      for (j = 1; j < 256; j <<= 1) {
        for (k = 0; k < j; k++) {
          t[i][k^j] = (v ^ t[i][k]);
        }
        v = (v & GF_FIRST_BIT) ? ((v << 1) ^ pp) : (v << 1);
      }
    }
  } 

  while (d32 < top) {
    p = (xor) ? *d32 : 0;
    a = *s32;
    i = 0;
    while (a != 0) {
      v = (a & 0xff);
      p ^= t[i][v];
      a >>= 8;
      i++;
    }
    *d32 = p;
    d32++;
    s32++;
  }
  gf_do_final_region_alignment(&rd);
}

static
inline
void
gf_w32_split_16_32_lazy_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h;
  uint32_t *s32, *d32, *top, p, a, v;
  struct gf_split_16_32_lazy_data *d16;
  uint32_t *t[2];
  int i, j, k, change;
  uint32_t pp;
  gf_region_data rd;
  
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;
  d16 = (struct gf_split_16_32_lazy_data *) h->private;
  for (i = 0; i < 2; i++) t[i] = d16->tables[i];
  change = (val != d16->last_value);
  if (change) d16->last_value = val;

  pp = h->prim_poly;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  gf_do_initial_region_alignment(&rd);

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;
  
  if (change) {
    v = val;
    for (i = 0; i < 2; i++) {
      t[i][0] = 0;
      for (j = 1; j < (1 << 16); j <<= 1) {
        for (k = 0; k < j; k++) {
          t[i][k^j] = (v ^ t[i][k]);
        }
        v = (v & GF_FIRST_BIT) ? ((v << 1) ^ pp) : (v << 1);
      }
    }
  } 

  while (d32 < top) {
    p = (xor) ? *d32 : 0;
    a = *s32;
    i = 0;
    while (a != 0) {
      v = (a & 0xffff);
      p ^= t[i][v];
      a >>= 16;
      i++;
    }
    *d32 = p;
    d32++;
    s32++;
  }
  gf_do_final_region_alignment(&rd);
}

static
void
gf_w32_split_2_32_lazy_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h;
  struct gf_split_2_32_lazy_data *ld;
  int i;
  uint32_t pp, v, v2, s, *s32, *d32, *top;
  gf_region_data rd;
 
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  gf_do_initial_region_alignment(&rd);

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  ld = (struct gf_split_2_32_lazy_data *) h->private;
  
  if (ld->last_value != val) {
    v = val;
    for (i = 0; i < 16; i++) {
      v2 = (v << 1);
      if (v & GF_FIRST_BIT) v2 ^= pp;
      ld->tables[i][0] = 0;
      ld->tables[i][1] = v;
      ld->tables[i][2] = v2;
      ld->tables[i][3] = (v2 ^ v);
      v = (v2 << 1);
      if (v2 & GF_FIRST_BIT) v ^= pp;
    }
  }
  ld->last_value = val;

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;

  while (d32 != top) {
    v = (xor) ? *d32 : 0;
    s = *s32;
    i = 0;
    while (s != 0) {
      v ^= ld->tables[i][s&3];
      s >>= 2;
      i++;
    }
    *d32 = v;
    d32++;
    s32++;
  }
  gf_do_final_region_alignment(&rd);
}

#ifdef INTEL_SSSE3
static
void
gf_w32_split_2_32_lazy_sse_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h;
  int i, tindex;
  uint32_t pp, v, v2, *s32, *d32, *top;
  __m128i vi, si, pi, shuffler, tables[16], adder, xi, mask1, mask2;
  gf_region_data rd;
 
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 32);
  gf_do_initial_region_alignment(&rd);

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;
  
  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;
  
  v = val;
  for (i = 0; i < 16; i++) {
    v2 = (v << 1);
    if (v & GF_FIRST_BIT) v2 ^= pp;
    tables[i] = _mm_set_epi32(v2 ^ v, v2, v, 0);
    v = (v2 << 1);
    if (v2 & GF_FIRST_BIT) v ^= pp;
  }

  shuffler = _mm_set_epi8(0xc, 0xc, 0xc, 0xc, 8, 8, 8, 8, 4, 4, 4, 4, 0, 0, 0, 0);
  adder = _mm_set_epi8(3, 2, 1, 0, 3, 2, 1, 0, 3, 2, 1, 0, 3, 2, 1, 0);
  mask1 = _mm_set1_epi8(0x3);
  mask2 = _mm_set1_epi8(0xc);

  while (d32 != top) {
    pi = (xor) ? _mm_load_si128 ((__m128i *) d32) : _mm_setzero_si128();
    vi = _mm_load_si128((__m128i *) s32);
 
    tindex = 0;
    for (i = 0; i < 4; i++) {
      si = _mm_shuffle_epi8(vi, shuffler);

      xi = _mm_and_si128(si, mask1);
      xi = _mm_slli_epi16(xi, 2);
      xi = _mm_xor_si128(xi, adder);
      pi = _mm_xor_si128(pi, _mm_shuffle_epi8(tables[tindex], xi));
      tindex++;

      xi = _mm_and_si128(si, mask2);
      xi = _mm_xor_si128(xi, adder);
      pi = _mm_xor_si128(pi, _mm_shuffle_epi8(tables[tindex], xi));
      si = _mm_srli_epi16(si, 2);
      tindex++;

      xi = _mm_and_si128(si, mask2);
      xi = _mm_xor_si128(xi, adder);
      pi = _mm_xor_si128(pi, _mm_shuffle_epi8(tables[tindex], xi));
      si = _mm_srli_epi16(si, 2);
      tindex++;

      xi = _mm_and_si128(si, mask2);
      xi = _mm_xor_si128(xi, adder);
      pi = _mm_xor_si128(pi, _mm_shuffle_epi8(tables[tindex], xi));
      si = _mm_srli_epi16(si, 2);
      tindex++;
      
      vi = _mm_srli_epi32(vi, 8);
    }
    _mm_store_si128((__m128i *) d32, pi);
    d32 += 4;
    s32 += 4;
  }

  gf_do_final_region_alignment(&rd);

}
#endif

static
void
gf_w32_split_4_32_lazy_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h;
  struct gf_split_4_32_lazy_data *ld;
  int i, j, k;
  uint32_t pp, v, s, *s32, *d32, *top;
  gf_region_data rd;
 
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  ld = (struct gf_split_4_32_lazy_data *) h->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  gf_do_initial_region_alignment(&rd);
  
  if (ld->last_value != val) {
    v = val;
    for (i = 0; i < 8; i++) {
      ld->tables[i][0] = 0;
      for (j = 1; j < 16; j <<= 1) {
        for (k = 0; k < j; k++) {
          ld->tables[i][k^j] = (v ^ ld->tables[i][k]);
        }
        v = (v & GF_FIRST_BIT) ? ((v << 1) ^ pp) : (v << 1);
      }
    }
  }
  ld->last_value = val;

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;

  while (d32 != top) {
    v = (xor) ? *d32 : 0;
    s = *s32;
    i = 0;
    while (s != 0) {
      v ^= ld->tables[i][s&0xf];
      s >>= 4;
      i++;
    }
    *d32 = v;
    d32++;
    s32++;
  }
  gf_do_final_region_alignment(&rd);
}

static
void
gf_w32_split_4_32_lazy_sse_altmap_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
#ifdef INTEL_SSSE3
  gf_internal_t *h;
  int i, j, k;
  uint32_t pp, v, *s32, *d32, *top;
  __m128i si, tables[8][4], p0, p1, p2, p3, mask1, v0, v1, v2, v3;
  struct gf_split_4_32_lazy_data *ld;
  uint8_t btable[16];
  gf_region_data rd;
 
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;
  
  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 64);
  gf_do_initial_region_alignment(&rd);

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;
  
  ld = (struct gf_split_4_32_lazy_data *) h->private;
 
  v = val;
  for (i = 0; i < 8; i++) {
    ld->tables[i][0] = 0;
    for (j = 1; j < 16; j <<= 1) {
      for (k = 0; k < j; k++) {
        ld->tables[i][k^j] = (v ^ ld->tables[i][k]);
      }
      v = (v & GF_FIRST_BIT) ? ((v << 1) ^ pp) : (v << 1);
    }
    for (j = 0; j < 4; j++) {
      for (k = 0; k < 16; k++) {
        btable[k] = (uint8_t) ld->tables[i][k];
        ld->tables[i][k] >>= 8;
      }
      tables[i][j] = _mm_loadu_si128((__m128i *) btable);
    }
  }

  mask1 = _mm_set1_epi8(0xf);

  if (xor) {
    while (d32 != top) {
      p0 = _mm_load_si128 ((__m128i *) d32);
      p1 = _mm_load_si128 ((__m128i *) (d32+4));
      p2 = _mm_load_si128 ((__m128i *) (d32+8));
      p3 = _mm_load_si128 ((__m128i *) (d32+12));
  
      v0 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v1 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v2 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v3 = _mm_load_si128((__m128i *) s32); s32 += 4;
  
      si = _mm_and_si128(v0, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[0][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[0][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[0][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[0][3], si));
      
      v0 = _mm_srli_epi32(v0, 4);
      si = _mm_and_si128(v0, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[1][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[1][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[1][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[1][3], si));
  
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[2][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[2][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[2][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[2][3], si));
      
      v1 = _mm_srli_epi32(v1, 4);
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[3][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[3][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[3][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[3][3], si));
  
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[4][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[4][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[4][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[4][3], si));
      
      v2 = _mm_srli_epi32(v2, 4);
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[5][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[5][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[5][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[5][3], si));
  
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[6][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[6][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[6][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[6][3], si));
      
      v3 = _mm_srli_epi32(v3, 4);
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[7][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[7][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[7][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[7][3], si));
  
      _mm_store_si128((__m128i *) d32, p0);
      _mm_store_si128((__m128i *) (d32+4), p1);
      _mm_store_si128((__m128i *) (d32+8), p2);
      _mm_store_si128((__m128i *) (d32+12), p3);
      d32 += 16;
    } 
  } else {
    while (d32 != top) {
  
      v0 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v1 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v2 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v3 = _mm_load_si128((__m128i *) s32); s32 += 4;

      si = _mm_and_si128(v0, mask1);
      p0 = _mm_shuffle_epi8(tables[0][0], si);
      p1 = _mm_shuffle_epi8(tables[0][1], si);
      p2 = _mm_shuffle_epi8(tables[0][2], si);
      p3 = _mm_shuffle_epi8(tables[0][3], si);
      
      v0 = _mm_srli_epi32(v0, 4);
      si = _mm_and_si128(v0, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[1][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[1][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[1][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[1][3], si));
  
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[2][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[2][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[2][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[2][3], si));
      
      v1 = _mm_srli_epi32(v1, 4);
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[3][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[3][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[3][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[3][3], si));
  
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[4][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[4][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[4][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[4][3], si));
      
      v2 = _mm_srli_epi32(v2, 4);
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[5][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[5][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[5][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[5][3], si));
  
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[6][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[6][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[6][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[6][3], si));
      
      v3 = _mm_srli_epi32(v3, 4);
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[7][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[7][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[7][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[7][3], si));
  
      _mm_store_si128((__m128i *) d32, p0);
      _mm_store_si128((__m128i *) (d32+4), p1);
      _mm_store_si128((__m128i *) (d32+8), p2);
      _mm_store_si128((__m128i *) (d32+12), p3);
      d32 += 16;
    } 
  }

  gf_do_final_region_alignment(&rd);

#endif
}


static
void
gf_w32_split_4_32_lazy_sse_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
#ifdef INTEL_SSSE3
  gf_internal_t *h;
  int i, j, k;
  uint32_t pp, v, *s32, *d32, *top, tmp_table[16];
  __m128i si, tables[8][4], p0, p1, p2, p3, mask1, v0, v1, v2, v3, mask8;
  __m128i tv1, tv2, tv3, tv0;
  uint8_t btable[16];
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;
  
  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 64);
  gf_do_initial_region_alignment(&rd);

  s32 = (uint32_t *) rd.s_start;
  d32 = (uint32_t *) rd.d_start;
  top = (uint32_t *) rd.d_top;

  v = val;
  for (i = 0; i < 8; i++) {
    tmp_table[0] = 0;
    for (j = 1; j < 16; j <<= 1) {
      for (k = 0; k < j; k++) {
        tmp_table[k^j] = (v ^ tmp_table[k]);
      }
      v = (v & GF_FIRST_BIT) ? ((v << 1) ^ pp) : (v << 1);
    }
    for (j = 0; j < 4; j++) {
      for (k = 0; k < 16; k++) {
        btable[k] = (uint8_t) tmp_table[k];
        tmp_table[k] >>= 8;
      }
      tables[i][j] = _mm_loadu_si128((__m128i *) btable);
    }
  }

  mask1 = _mm_set1_epi8(0xf);
  mask8 = _mm_set1_epi16(0xff);

  if (xor) {
    while (d32 != top) {
      v0 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v1 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v2 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v3 = _mm_load_si128((__m128i *) s32); s32 += 4;
  
      p0 = _mm_srli_epi16(v0, 8);
      p1 = _mm_srli_epi16(v1, 8);
      p2 = _mm_srli_epi16(v2, 8);
      p3 = _mm_srli_epi16(v3, 8);

      tv0 = _mm_and_si128(v0, mask8);
      tv1 = _mm_and_si128(v1, mask8);
      tv2 = _mm_and_si128(v2, mask8);
      tv3 = _mm_and_si128(v3, mask8);

      v0 = _mm_packus_epi16(p1, p0);
      v1 = _mm_packus_epi16(tv1, tv0);
      v2 = _mm_packus_epi16(p3, p2);
      v3 = _mm_packus_epi16(tv3, tv2);

      p0 = _mm_srli_epi16(v0, 8);
      p1 = _mm_srli_epi16(v1, 8);
      p2 = _mm_srli_epi16(v2, 8);
      p3 = _mm_srli_epi16(v3, 8);

      tv0 = _mm_and_si128(v0, mask8);
      tv1 = _mm_and_si128(v1, mask8);
      tv2 = _mm_and_si128(v2, mask8);
      tv3 = _mm_and_si128(v3, mask8);

      v0 = _mm_packus_epi16(p2, p0);
      v1 = _mm_packus_epi16(p3, p1);
      v2 = _mm_packus_epi16(tv2, tv0);
      v3 = _mm_packus_epi16(tv3, tv1);

      si = _mm_and_si128(v0, mask1);
      p0 = _mm_shuffle_epi8(tables[6][0], si);
      p1 = _mm_shuffle_epi8(tables[6][1], si);
      p2 = _mm_shuffle_epi8(tables[6][2], si);
      p3 = _mm_shuffle_epi8(tables[6][3], si);
      
      v0 = _mm_srli_epi32(v0, 4);
      si = _mm_and_si128(v0, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[7][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[7][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[7][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[7][3], si));
  
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[4][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[4][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[4][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[4][3], si));
      
      v1 = _mm_srli_epi32(v1, 4);
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[5][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[5][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[5][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[5][3], si));
  
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[2][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[2][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[2][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[2][3], si));
      
      v2 = _mm_srli_epi32(v2, 4);
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[3][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[3][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[3][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[3][3], si));
  
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[0][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[0][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[0][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[0][3], si));
      
      v3 = _mm_srli_epi32(v3, 4);
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[1][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[1][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[1][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[1][3], si));
  
      tv0 = _mm_unpackhi_epi8(p1, p3);
      tv1 = _mm_unpackhi_epi8(p0, p2);
      tv2 = _mm_unpacklo_epi8(p1, p3);
      tv3 = _mm_unpacklo_epi8(p0, p2);

      p0 = _mm_unpackhi_epi8(tv1, tv0);
      p1 = _mm_unpacklo_epi8(tv1, tv0);
      p2 = _mm_unpackhi_epi8(tv3, tv2);
      p3 = _mm_unpacklo_epi8(tv3, tv2);

      v0 = _mm_load_si128 ((__m128i *) d32);
      v1 = _mm_load_si128 ((__m128i *) (d32+4));
      v2 = _mm_load_si128 ((__m128i *) (d32+8));
      v3 = _mm_load_si128 ((__m128i *) (d32+12));
  
      p0 = _mm_xor_si128(p0, v0);
      p1 = _mm_xor_si128(p1, v1);
      p2 = _mm_xor_si128(p2, v2);
      p3 = _mm_xor_si128(p3, v3);

      _mm_store_si128((__m128i *) d32, p0);
      _mm_store_si128((__m128i *) (d32+4), p1);
      _mm_store_si128((__m128i *) (d32+8), p2);
      _mm_store_si128((__m128i *) (d32+12), p3);
      d32 += 16;
    } 
  } else {
    while (d32 != top) {
      v0 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v1 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v2 = _mm_load_si128((__m128i *) s32); s32 += 4;
      v3 = _mm_load_si128((__m128i *) s32); s32 += 4;
 
      p0 = _mm_srli_epi16(v0, 8);
      p1 = _mm_srli_epi16(v1, 8);
      p2 = _mm_srli_epi16(v2, 8);
      p3 = _mm_srli_epi16(v3, 8);
      
      tv0 = _mm_and_si128(v0, mask8);
      tv1 = _mm_and_si128(v1, mask8);
      tv2 = _mm_and_si128(v2, mask8);
      tv3 = _mm_and_si128(v3, mask8);
      
      v0 = _mm_packus_epi16(p1, p0);
      v1 = _mm_packus_epi16(tv1, tv0);
      v2 = _mm_packus_epi16(p3, p2);
      v3 = _mm_packus_epi16(tv3, tv2);
      
      p0 = _mm_srli_epi16(v0, 8);
      p1 = _mm_srli_epi16(v1, 8);
      p2 = _mm_srli_epi16(v2, 8);
      p3 = _mm_srli_epi16(v3, 8);
     
      tv0 = _mm_and_si128(v0, mask8);
      tv1 = _mm_and_si128(v1, mask8);
      tv2 = _mm_and_si128(v2, mask8);
      tv3 = _mm_and_si128(v3, mask8);
      
      v0 = _mm_packus_epi16(p2, p0);
      v1 = _mm_packus_epi16(p3, p1);
      v2 = _mm_packus_epi16(tv2, tv0);
      v3 = _mm_packus_epi16(tv3, tv1);
      
      p0 = v0;
      p1 = v1;
      p2 = v2;
      p3 = v3;

      si = _mm_and_si128(v0, mask1);
      p0 = _mm_shuffle_epi8(tables[6][0], si);
      p1 = _mm_shuffle_epi8(tables[6][1], si);
      p2 = _mm_shuffle_epi8(tables[6][2], si);
      p3 = _mm_shuffle_epi8(tables[6][3], si);
      
      v0 = _mm_srli_epi32(v0, 4);
      si = _mm_and_si128(v0, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[7][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[7][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[7][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[7][3], si));
  
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[4][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[4][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[4][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[4][3], si));
      
      v1 = _mm_srli_epi32(v1, 4);
      si = _mm_and_si128(v1, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[5][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[5][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[5][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[5][3], si));
  
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[2][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[2][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[2][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[2][3], si));
      
      v2 = _mm_srli_epi32(v2, 4);
      si = _mm_and_si128(v2, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[3][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[3][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[3][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[3][3], si));
  
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[0][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[0][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[0][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[0][3], si));
      
      v3 = _mm_srli_epi32(v3, 4);
      si = _mm_and_si128(v3, mask1);
      p0 = _mm_xor_si128(p0, _mm_shuffle_epi8(tables[1][0], si));
      p1 = _mm_xor_si128(p1, _mm_shuffle_epi8(tables[1][1], si));
      p2 = _mm_xor_si128(p2, _mm_shuffle_epi8(tables[1][2], si));
      p3 = _mm_xor_si128(p3, _mm_shuffle_epi8(tables[1][3], si)); 
  
      tv0 = _mm_unpackhi_epi8(p1, p3);
      tv1 = _mm_unpackhi_epi8(p0, p2);
      tv2 = _mm_unpacklo_epi8(p1, p3);
      tv3 = _mm_unpacklo_epi8(p0, p2);
      
      p0 = _mm_unpackhi_epi8(tv1, tv0);
      p1 = _mm_unpacklo_epi8(tv1, tv0);
      p2 = _mm_unpackhi_epi8(tv3, tv2);
      p3 = _mm_unpacklo_epi8(tv3, tv2);
      
      _mm_store_si128((__m128i *) d32, p0);
      _mm_store_si128((__m128i *) (d32+4), p1);
      _mm_store_si128((__m128i *) (d32+8), p2);
      _mm_store_si128((__m128i *) (d32+12), p3);
      d32 += 16;
    } 
  }
  gf_do_final_region_alignment(&rd);

#endif
}

static 
int gf_w32_split_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_split_2_32_lazy_data *ld2;
  struct gf_split_4_32_lazy_data *ld4;
  struct gf_w32_split_8_8_data *d8;
  struct gf_split_8_32_lazy_data *d32;
  struct gf_split_16_32_lazy_data *d16;
  uint32_t p, basep;
  int i, j, exp, ispclmul, issse3;

  ispclmul = 0;
#if defined(INTEL_SSE4_PCLMUL)
  ispclmul = 1;
#endif

  issse3 = 0;
#ifdef INTEL_SSSE3
  issse3 = 1;
#endif

  h = (gf_internal_t *) gf->scratch;

  /* Defaults */
  
  gf->inverse.w32 = gf_w32_euclid;

  /* JSP: First handle single multiplication:  
     If args == 8, then we're doing split 8 8.  
     Otherwise, if PCLMUL, we use that.
     Otherwise, we use bytwo_p.
   */

  if (h->arg1 == 8 && h->arg2 == 8) {
    gf->multiply.w32 = gf_w32_split_8_8_multiply;
  } else if (ispclmul) {
    if ((0xfffe0000 & h->prim_poly) == 0){
      gf->multiply.w32 = gf_w32_clm_multiply_2;
    } else if ((0xffc00000 & h->prim_poly) == 0){
      gf->multiply.w32 = gf_w32_clm_multiply_3;
    } else if ((0xfe000000 & h->prim_poly) == 0){
     gf->multiply.w32 = gf_w32_clm_multiply_4;
    }
  } else {
    gf->multiply.w32 = gf_w32_bytwo_p_multiply;
  }

  /* Easy cases: 16/32 and 2/32 */

  if ((h->arg1 == 16 && h->arg2 == 32) || (h->arg1 == 32 && h->arg2 == 16)) {
    d16 = (struct gf_split_16_32_lazy_data *) h->private;
    d16->last_value = 0;
    gf->multiply_region.w32 = gf_w32_split_16_32_lazy_multiply_region;
    return 1;
  }

  if ((h->arg1 == 2 && h->arg2 == 32) || (h->arg1 == 32 && h->arg2 == 2)) {
    ld2 = (struct gf_split_2_32_lazy_data *) h->private;
    ld2->last_value = 0;
    #ifdef INTEL_SSSE3
      if (!(h->region_type & GF_REGION_NOSSE))
        gf->multiply_region.w32 = gf_w32_split_2_32_lazy_sse_multiply_region;
      else
        gf->multiply_region.w32 = gf_w32_split_2_32_lazy_multiply_region;
    #else
      gf->multiply_region.w32 = gf_w32_split_2_32_lazy_multiply_region;
      if(h->region_type & GF_REGION_SSE) return 0;
    #endif
    return 1;
  } 

  /* 4/32 or Default + SSE - There is no ALTMAP/NOSSE. */

  if ((h->arg1 == 4 && h->arg2 == 32) || (h->arg1 == 32 && h->arg2 == 4) ||
      (issse3 && h->mult_type == GF_REGION_DEFAULT)) {
    ld4 = (struct gf_split_4_32_lazy_data *) h->private;
    ld4->last_value = 0;
    if ((h->region_type & GF_REGION_NOSSE) || !issse3) {
      gf->multiply_region.w32 = gf_w32_split_4_32_lazy_multiply_region;
    } else if (h->region_type & GF_REGION_ALTMAP) {
      gf->multiply_region.w32 = gf_w32_split_4_32_lazy_sse_altmap_multiply_region;
    } else {
      gf->multiply_region.w32 = gf_w32_split_4_32_lazy_sse_multiply_region;
    }
    return 1;
  } 

  /* 8/32 or Default + no SSE */

  if ((h->arg1 == 8 && h->arg2 == 32) || (h->arg1 == 32 && h->arg2 == 8) || 
       h->mult_type == GF_MULT_DEFAULT) {
    d32 = (struct gf_split_8_32_lazy_data *) h->private;
    d32->last_value = 0;
    gf->multiply_region.w32 = gf_w32_split_8_32_lazy_multiply_region;
    return 1;
  }

  /* Finally, if args == 8, then we have to set up the tables here. */

  if (h->arg1 == 8 && h->arg2 == 8) {
    d8 = (struct gf_w32_split_8_8_data *) h->private;
    d8->last_value = 0;
    gf->multiply.w32 = gf_w32_split_8_8_multiply;
    gf->multiply_region.w32 = gf_w32_split_8_32_lazy_multiply_region;
    basep = 1;
    for (exp = 0; exp < 7; exp++) {
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
    return 1;
  }

  /* If we get here, then the arguments were bad. */

  return 0;
}

static
int gf_w32_group_init(gf_t *gf)
{
  uint32_t i, j, p, index;
  struct gf_w32_group_data *gd;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  int g_r, g_s;

  g_s = h->arg1;
  g_r = h->arg2;

  gd = (struct gf_w32_group_data *) h->private;
  gd->shift = (uint32_t *) (&(gd->memory));
  gd->reduce = gd->shift + (1 << g_s);

  gd->rmask = (1 << g_r) - 1;
  gd->rmask <<= 32;

  gd->tshift = 32 % g_s;
  if (gd->tshift == 0) gd->tshift = g_s;
  gd->tshift = (32 - gd->tshift);
  gd->tshift = ((gd->tshift-1)/g_r) * g_r;

  gd->reduce[0] = 0;
  for (i = 0; i < (1 << g_r); i++) {
    p = 0;
    index = 0;
    for (j = 0; j < g_r; j++) {
      if (i & (1 << j)) {
        p ^= (h->prim_poly << j);
        index ^= (1 << j);
        index ^= (h->prim_poly >> (32-j));
      }
    }
    gd->reduce[index] = p;
  }

  if (g_s == g_r) {
    gf->multiply.w32 = gf_w32_group_s_equals_r_multiply;
    gf->multiply_region.w32 = gf_w32_group_s_equals_r_multiply_region; 
  } else {
    gf->multiply.w32 = gf_w32_group_multiply;
    gf->multiply_region.w32 = gf_w32_group_multiply_region;
  }
  gf->divide.w32 = NULL;
  gf->inverse.w32 = gf_w32_euclid;

  return 1;
}


static
uint32_t
gf_w32_composite_multiply_recursive(gf_t *gf, uint32_t a, uint32_t b)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint32_t b0 = b & 0x0000ffff;
  uint32_t b1 = (b & 0xffff0000) >> 16;
  uint32_t a0 = a & 0x0000ffff;
  uint32_t a1 = (a & 0xffff0000) >> 16;
  uint32_t a1b1;
  uint32_t rv;
  a1b1 = base_gf->multiply.w32(base_gf, a1, b1);

  rv = ((base_gf->multiply.w32(base_gf, a1, b0) ^ base_gf->multiply.w32(base_gf, a0, b1) ^ base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 16) | (base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1);
  return rv;
}

/* JSP: This could be made faster. Someday, when I'm bored. */

static
uint32_t
gf_w32_composite_multiply_inline(gf_t *gf, uint32_t a, uint32_t b)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  uint32_t b0 = b & 0x0000ffff;
  uint32_t b1 = b >> 16;
  uint32_t a0 = a & 0x0000ffff;
  uint32_t a1 = a >> 16;
  uint32_t a1b1, prod;
  uint16_t *log, *alog;
  struct gf_w32_composite_data *cd;

  cd = (struct gf_w32_composite_data *) h->private;
  log = cd->log;
  alog = cd->alog;

  a1b1 = GF_W16_INLINE_MULT(log, alog, a1, b1);
  prod = GF_W16_INLINE_MULT(log, alog, a1, b0);
  prod ^= GF_W16_INLINE_MULT(log, alog, a0, b1);
  prod ^= GF_W16_INLINE_MULT(log, alog, a1b1, h->prim_poly);
  prod <<= 16;
  prod ^= GF_W16_INLINE_MULT(log, alog, a0, b0);
  prod ^= a1b1;
  return prod;
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
uint32_t
gf_w32_composite_inverse(gf_t *gf, uint32_t a)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint16_t a0 = a & 0x0000ffff;
  uint16_t a1 = (a & 0xffff0000) >> 16;
  uint16_t c0, c1, d, tmp;
  uint32_t c;
  uint16_t a0inv, a1inv;

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

  c = c0 | (c1 << 16);

  return c;
}

static
void
gf_w32_composite_multiply_region(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint32_t b0 = val & 0x0000ffff;
  uint32_t b1 = (val & 0xffff0000) >> 16;
  uint32_t *s32, *d32, *top;
  uint16_t a0, a1, a1b1, *log, *alog;
  uint32_t prod;
  gf_region_data rd;
  struct gf_w32_composite_data *cd;

  cd = (struct gf_w32_composite_data *) h->private;
  log = cd->log;
  alog = cd->alog;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 4);
  
  s32 = rd.s_start;
  d32 = rd.d_start;
  top = rd.d_top;

  if (log == NULL) {
    if (xor) {
      while (d32 < top) {
        a0 = *s32 & 0x0000ffff;
        a1 = (*s32 & 0xffff0000) >> 16;
        a1b1 = base_gf->multiply.w32(base_gf, a1, b1);
  
        *d32 ^= ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) |
                  ((base_gf->multiply.w32(base_gf, a1, b0) ^ base_gf->multiply.w32(base_gf, a0, b1) ^ base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 16)); 
        s32++;
        d32++;
      }
    } else {
      while (d32 < top) {
        a0 = *s32 & 0x0000ffff;
        a1 = (*s32 & 0xffff0000) >> 16;
        a1b1 = base_gf->multiply.w32(base_gf, a1, b1);
  
        *d32 = ((base_gf->multiply.w32(base_gf, a0, b0) ^ a1b1) |
                  ((base_gf->multiply.w32(base_gf, a1, b0) ^ base_gf->multiply.w32(base_gf, a0, b1) ^ base_gf->multiply.w32(base_gf, a1b1, h->prim_poly)) << 16)); 
        s32++;
        d32++;
      }
    }
  } else {
    if (xor) {
      while (d32 < top) {
        a0 = *s32 & 0x0000ffff;
        a1 = (*s32 & 0xffff0000) >> 16;
        a1b1 = GF_W16_INLINE_MULT(log, alog, a1, b1);

        prod = GF_W16_INLINE_MULT(log, alog, a1, b0);
        prod ^= GF_W16_INLINE_MULT(log, alog, a0, b1);
        prod ^= GF_W16_INLINE_MULT(log, alog, a1b1, h->prim_poly);
        prod <<= 16;
        prod ^= GF_W16_INLINE_MULT(log, alog, a0, b0);
        prod ^= a1b1;
        *d32 ^= prod;
        s32++;
        d32++;
      }
    } else {
      while (d32 < top) {
        a0 = *s32 & 0x0000ffff;
        a1 = (*s32 & 0xffff0000) >> 16;
        a1b1 = GF_W16_INLINE_MULT(log, alog, a1, b1);
  
        prod = GF_W16_INLINE_MULT(log, alog, a1, b0);
        prod ^= GF_W16_INLINE_MULT(log, alog, a0, b1);
        prod ^= GF_W16_INLINE_MULT(log, alog, a1b1, h->prim_poly);
        prod <<= 16;
        prod ^= GF_W16_INLINE_MULT(log, alog, a0, b0);
        prod ^= a1b1;
        
        *d32 = prod;
        s32++;
        d32++;
      }
    }
  }
}

static
void
gf_w32_composite_multiply_region_alt(gf_t *gf, void *src, void *dest, uint32_t val, int bytes, int xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint16_t    val0 = val & 0x0000ffff;
  uint16_t    val1 = (val & 0xffff0000) >> 16;
  gf_region_data rd;
  int sub_reg_size;
  uint8_t *slow, *shigh;
  uint8_t *dlow, *dhigh, *top;

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
int gf_w32_composite_init(gf_t *gf)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  struct gf_w32_composite_data *cd;

  if (h->base_gf == NULL) return 0;

  cd = (struct gf_w32_composite_data *) h->private;
  cd->log = gf_w16_get_log_table(h->base_gf);
  cd->alog = gf_w16_get_mult_alog_table(h->base_gf);

  if (h->region_type & GF_REGION_ALTMAP) {
    gf->multiply_region.w32 = gf_w32_composite_multiply_region_alt;
  } else {
    gf->multiply_region.w32 = gf_w32_composite_multiply_region;
  }

  if (cd->log == NULL) {
    gf->multiply.w32 = gf_w32_composite_multiply_recursive;
  } else {
    gf->multiply.w32 = gf_w32_composite_multiply_inline; 
  }
  gf->divide.w32 = NULL;
  gf->inverse.w32 = gf_w32_composite_inverse;

  return 1;
}



int gf_w32_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2)
{
  int issse3 = 0;

#ifdef INTEL_SSSE3
  issse3 = 1;
#endif

  switch(mult_type)
  {
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:
      return sizeof(gf_internal_t) + sizeof(struct gf_w32_bytwo_data) + 64;
      break;
    case GF_MULT_GROUP: 
      return sizeof(gf_internal_t) + sizeof(struct gf_w32_group_data) +
               sizeof(uint32_t) * (1 << arg1) +
               sizeof(uint32_t) * (1 << arg2) + 64;
      break;
    case GF_MULT_DEFAULT:

    case GF_MULT_SPLIT_TABLE: 
        if (arg1 == 8 && arg2 == 8){
          return sizeof(gf_internal_t) + sizeof(struct gf_w32_split_8_8_data) + 64;
        }
        if ((arg1 == 16 && arg2 == 32) || (arg2 == 16 && arg1 == 32)) {
          return sizeof(gf_internal_t) + sizeof(struct gf_split_16_32_lazy_data) + 64;
        }
        if ((arg1 == 2 && arg2 == 32) || (arg2 == 2 && arg1 == 32)) {
          return sizeof(gf_internal_t) + sizeof(struct gf_split_2_32_lazy_data) + 64;
        }
        if ((arg1 == 8 && arg2 == 32) || (arg2 == 8 && arg1 == 32) || 
             (mult_type == GF_MULT_DEFAULT && !issse3)) {
          return sizeof(gf_internal_t) + sizeof(struct gf_split_8_32_lazy_data) + 64;
        }
        if ((arg1 == 4 && arg2 == 32) || 
            (arg2 == 4 && arg1 == 32) ||
            mult_type == GF_MULT_DEFAULT) {
          return sizeof(gf_internal_t) + sizeof(struct gf_split_4_32_lazy_data) + 64;
        }
        return 0;
    case GF_MULT_CARRY_FREE:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_SHIFT:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_COMPOSITE:
      return sizeof(gf_internal_t) + sizeof(struct gf_w32_composite_data) + 64;
      break;

    default:
      return 0;
   }
   return 0;
}

int gf_w32_init(gf_t *gf)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  
  /* Allen: set default primitive polynomial / irreducible polynomial if needed */

  if (h->prim_poly == 0) {
    if (h->mult_type == GF_MULT_COMPOSITE) { 
      h->prim_poly = gf_composite_get_default_poly(h->base_gf);
      if (h->prim_poly == 0) return 0; /* This shouldn't happen */
    } else { 

      /* Allen: use the following primitive polynomial to make carryless multiply work more efficiently for GF(2^32).*/

      /* h->prim_poly = 0xc5; */

      /* Allen: The following is the traditional primitive polynomial for GF(2^32) */

      h->prim_poly = 0x400007;
    } 
  }

  /* No leading one */

  if(h->mult_type != GF_MULT_COMPOSITE) h->prim_poly &= 0xffffffff;
    
  gf->multiply.w32 = NULL;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = NULL;

  switch(h->mult_type) {
    case GF_MULT_CARRY_FREE:  if (gf_w32_cfm_init(gf) == 0) return 0; break;
    case GF_MULT_SHIFT:       if (gf_w32_shift_init(gf) == 0) return 0; break;
    case GF_MULT_COMPOSITE:   if (gf_w32_composite_init(gf) == 0) return 0; break;
    case GF_MULT_DEFAULT: 
    case GF_MULT_SPLIT_TABLE: if (gf_w32_split_init(gf) == 0) return 0; break;
    case GF_MULT_GROUP:       if (gf_w32_group_init(gf) == 0) return 0; break;
    case GF_MULT_BYTWO_p:   
    case GF_MULT_BYTWO_b:     if (gf_w32_bytwo_init(gf) == 0) return 0; break;
    default: return 0;
  }
  if (h->divide_type == GF_DIVIDE_EUCLID) {
    gf->divide.w32 = gf_w32_divide_from_inverse;
    gf->inverse.w32 = gf_w32_euclid;
  } else if (h->divide_type == GF_DIVIDE_MATRIX) {
    gf->divide.w32 = gf_w32_divide_from_inverse;
    gf->inverse.w32 = gf_w32_matrix;
  }

  if (gf->inverse.w32 != NULL && gf->divide.w32 == NULL) {
    gf->divide.w32 = gf_w32_divide_from_inverse;
  }
  if (gf->inverse.w32 == NULL && gf->divide.w32 != NULL) {
    gf->inverse.w32 = gf_w32_inverse_from_divide;
  }
  if (h->region_type == GF_REGION_CAUCHY) {
    gf->extract_word.w32 = gf_wgen_extract_word;
    gf->multiply_region.w32 = gf_wgen_cauchy_region;
  } else if (h->region_type & GF_REGION_ALTMAP) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      gf->extract_word.w32 = gf_w32_composite_extract_word;
    } else {
      gf->extract_word.w32 = gf_w32_split_extract_word;
    }
  } else {
    gf->extract_word.w32 = gf_w32_extract_word;
  }
  return 1;
}
