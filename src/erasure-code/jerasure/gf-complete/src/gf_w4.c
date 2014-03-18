/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w4.c
 *
 * Routines for 4-bit Galois fields
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

#define GF_FIELD_WIDTH      4
#define GF_DOUBLE_WIDTH     (GF_FIELD_WIDTH*2)
#define GF_FIELD_SIZE       (1 << GF_FIELD_WIDTH)
#define GF_MULT_GROUP_SIZE       (GF_FIELD_SIZE-1)

/* ------------------------------------------------------------
   JSP: Each implementation has its own data, which is allocated
   at one time as part of the handle. For that reason, it 
   shouldn't be hierarchical -- i.e. one should be able to 
   allocate it with one call to malloc. */

struct gf_logtable_data {
    uint8_t      log_tbl[GF_FIELD_SIZE];
    uint8_t      antilog_tbl[GF_FIELD_SIZE * 2];
    uint8_t      *antilog_tbl_div;
};

struct gf_single_table_data {
    uint8_t      mult[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_double_table_data {
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t      mult[GF_FIELD_SIZE][GF_FIELD_SIZE*GF_FIELD_SIZE];
};
struct gf_quad_table_data {
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t     mult[GF_FIELD_SIZE][(1<<16)];
};

struct gf_quad_table_lazy_data {
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t      smult[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t     mult[(1 << 16)];
};

struct gf_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

#define AB2(ip, am1 ,am2, b, t1, t2) {\
  t1 = (b << 1) & am1;\
  t2 = b & am2; \
  t2 = ((t2 << 1) - (t2 >> (GF_FIELD_WIDTH-1))); \
  b = (t1 ^ (t2 & ip));}

#define SSE_AB2(pp, m1, va, t1, t2) {\
          t1 = _mm_and_si128(_mm_slli_epi64(va, 1), m1); \
          t2 = _mm_and_si128(va, _mm_set1_epi8(0x88)); \
          t2 = _mm_sub_epi64 (_mm_slli_epi64(t2, 1), _mm_srli_epi64(t2, (GF_FIELD_WIDTH-1))); \
          va = _mm_xor_si128(t1, _mm_and_si128(t2, pp)); }

/* ------------------------------------------------------------
   JSP: These are basic and work from multiple implementations.
 */

static
inline
gf_val_32_t gf_w4_inverse_from_divide (gf_t *gf, gf_val_32_t a)
{
  return gf->divide.w32(gf, 1, a);
}

static
inline
gf_val_32_t gf_w4_divide_from_inverse (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  b = gf->inverse.w32(gf, b);
  return gf->multiply.w32(gf, a, b);
}

static
inline
gf_val_32_t gf_w4_euclid (gf_t *gf, gf_val_32_t b)
{
  gf_val_32_t e_i, e_im1, e_ip1;
  gf_val_32_t d_i, d_im1, d_ip1;
  gf_val_32_t y_i, y_im1, y_ip1;
  gf_val_32_t c_i;

  if (b == 0) return -1;
  e_im1 = ((gf_internal_t *) (gf->scratch))->prim_poly;
  e_i = b;
  d_im1 = 4;
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
gf_val_32_t gf_w4_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint8_t *r8, v;

  r8 = (uint8_t *) start;
  v = r8[index/2];
  if (index%2) {
    return v >> 4;
  } else {
    return v&0xf;
  }
}


static
inline
gf_val_32_t gf_w4_matrix (gf_t *gf, gf_val_32_t b)
{
  return gf_bitmatrix_inverse(b, 4, ((gf_internal_t *) (gf->scratch))->prim_poly);
}


static
inline
gf_val_32_t
gf_w4_shift_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint8_t product, i, pp;
  gf_internal_t *h;
  
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

/* Ben: This function works, but it is 33% slower than the normal shift mult */

static
inline
gf_val_32_t
gf_w4_clm_multiply (gf_t *gf, gf_val_32_t a4, gf_val_32_t b4)
{
  gf_val_32_t rv = 0;

#if defined(INTEL_SSE4_PCLMUL)

  __m128i         a, b;
  __m128i         result;
  __m128i         prim_poly;
  __m128i         w;
  gf_internal_t * h = gf->scratch;

  a = _mm_insert_epi32 (_mm_setzero_si128(), a4, 0);
  b = _mm_insert_epi32 (a, b4, 0);

  prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)(h->prim_poly & 0x1fULL));

  /* Do the initial multiply */

  result = _mm_clmulepi64_si128 (a, b, 0);

  /* Ben/JSP: Do prim_poly reduction once. We are guaranteed that we will only
     have to do the reduction only once, because (w-2)/z == 1. Where
     z is equal to the number of zeros after the leading 1.

     _mm_clmulepi64_si128 is the carryless multiply operation. Here
     _mm_srli_epi64 shifts the result to the right by 4 bits. This allows
     us to multiply the prim_poly by the leading bits of the result. We
     then xor the result of that operation back with the result. */

  w = _mm_clmulepi64_si128 (prim_poly, _mm_srli_epi64 (result, 4), 0);
  result = _mm_xor_si128 (result, w);

  /* Extracts 32 bit value from result. */

  rv = ((gf_val_32_t)_mm_extract_epi32(result, 0));
#endif
  return rv;
}

static
void
gf_w4_multiply_region_from_single(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int 
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
      *d8 ^= (gf->multiply.w32(gf, val, (*s8 & 0xf)) | 
             ((gf->multiply.w32(gf, val, (*s8 >> 4))) << 4));
      d8++;
      s8++;
    }
  } else {
    while (d8 < ((uint8_t *) rd.d_top)) {
      *d8 = (gf->multiply.w32(gf, val, (*s8 & 0xf)) | 
             ((gf->multiply.w32(gf, val, (*s8 >> 4))) << 4));
      d8++;
      s8++;
    }
  }
  gf_do_final_region_alignment(&rd);
}

/* ------------------------------------------------------------
  IMPLEMENTATION: LOG_TABLE: 

  JSP: This is a basic log-antilog implementation.  
       I'm not going to spend any time optimizing it because the
       other techniques are faster for both single and region
       operations. 
 */

static
inline
gf_val_32_t
gf_w4_log_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_logtable_data *ltd;
    
  ltd = (struct gf_logtable_data *) ((gf_internal_t *) (gf->scratch))->private;
  return (a == 0 || b == 0) ? 0 : ltd->antilog_tbl[(unsigned)(ltd->log_tbl[a] + ltd->log_tbl[b])];
}

static
inline
gf_val_32_t
gf_w4_log_divide (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int log_sum = 0;
  struct gf_logtable_data *ltd;
    
  if (a == 0 || b == 0) return 0;
  ltd = (struct gf_logtable_data *) ((gf_internal_t *) (gf->scratch))->private;

  log_sum = ltd->log_tbl[a] - ltd->log_tbl[b];
  return (ltd->antilog_tbl_div[log_sum]);
}

static
void 
gf_w4_log_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t lv, b, c;
  uint8_t *s8, *d8;
  
  struct gf_logtable_data *ltd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  ltd = (struct gf_logtable_data *) ((gf_internal_t *) (gf->scratch))->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  lv = ltd->log_tbl[val];

  for (i = 0; i < bytes; i++) {
    c = (xor) ? d8[i] : 0;
    b = (s8[i] >> GF_FIELD_WIDTH);
    c ^= (b == 0) ? 0 : (ltd->antilog_tbl[lv + ltd->log_tbl[b]] << GF_FIELD_WIDTH);
    b = (s8[i] & 0xf);
    c ^= (b == 0) ? 0 : ltd->antilog_tbl[lv + ltd->log_tbl[b]];
    d8[i] = c;
  }
}

static 
int gf_w4_log_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_logtable_data *ltd;
  int i, b;

  h = (gf_internal_t *) gf->scratch;
  ltd = h->private;

  for (i = 0; i < GF_FIELD_SIZE; i++)
    ltd->log_tbl[i]=0;

  ltd->antilog_tbl_div = ltd->antilog_tbl + (GF_FIELD_SIZE-1);
  b = 1;
  i = 0;
  do {
    if (ltd->log_tbl[b] != 0 && i != 0) {
      fprintf(stderr, "Cannot construct log table: Polynomial is not primitive.\n\n");
      return 0;
    }
    ltd->log_tbl[b] = i;
    ltd->antilog_tbl[i] = b;
    ltd->antilog_tbl[i+GF_FIELD_SIZE-1] = b;
    b <<= 1;
    i++;
    if (b & GF_FIELD_SIZE) b = b ^ h->prim_poly;
  } while (b != 1);

  if (i != GF_FIELD_SIZE - 1) {
    _gf_errno = GF_E_LOGPOLY;
    return 0;
  }
    
  gf->inverse.w32 = gf_w4_inverse_from_divide;
  gf->divide.w32 = gf_w4_log_divide;
  gf->multiply.w32 = gf_w4_log_multiply;
  gf->multiply_region.w32 = gf_w4_log_multiply_region;
  return 1;
}

/* ------------------------------------------------------------
  IMPLEMENTATION: SINGLE TABLE: JSP. 
 */

static
inline
gf_val_32_t
gf_w4_single_table_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_single_table_data *std;
    
  std = (struct gf_single_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->mult[a][b];
}

static
inline
gf_val_32_t
gf_w4_single_table_divide (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_single_table_data *std;
    
  std = (struct gf_single_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->div[a][b];
}

static
void 
gf_w4_single_table_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t b, c;
  uint8_t *s8, *d8;
  
  struct gf_single_table_data *std;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  std = (struct gf_single_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;

  for (i = 0; i < bytes; i++) {
    c = (xor) ? d8[i] : 0;
    b = (s8[i] >> GF_FIELD_WIDTH);
    c ^= (std->mult[val][b] << GF_FIELD_WIDTH);
    b = (s8[i] & 0xf);
    c ^= (std->mult[val][b]);
    d8[i] = c;
  }
}

#define MM_PRINT(s, r) { uint8_t blah[16]; printf("%-12s", s); _mm_storeu_si128((__m128i *)blah, r); for (i = 0; i < 16; i++) printf(" %02x", blah[i]); printf("\n"); }

#ifdef INTEL_SSSE3
static
void 
gf_w4_single_table_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_region_data rd;
  uint8_t *base, *sptr, *dptr, *top;
  __m128i  tl, loset, r, va, th;
  
  struct gf_single_table_data *std;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);

  std = (struct gf_single_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  base = (uint8_t *) std->mult;
  base += (val << GF_FIELD_WIDTH);

  gf_do_initial_region_alignment(&rd);

  tl = _mm_loadu_si128((__m128i *)base);
  th = _mm_slli_epi64(tl, 4);
  loset = _mm_set1_epi8 (0x0f);

  sptr = rd.s_start;
  dptr = rd.d_start;
  top = rd.s_top;

  while (sptr < (uint8_t *) top) {
    va = _mm_load_si128 ((__m128i *)(sptr));
    r = _mm_and_si128 (loset, va);
    r = _mm_shuffle_epi8 (tl, r);
    va = _mm_srli_epi64 (va, 4);
    va = _mm_and_si128 (loset, va);
    va = _mm_shuffle_epi8 (th, va);
    r = _mm_xor_si128 (r, va);
    va = (xor) ? _mm_load_si128 ((__m128i *)(dptr)) : _mm_setzero_si128(); 
    r = _mm_xor_si128 (r, va);
    _mm_store_si128 ((__m128i *)(dptr), r);
    dptr += 16;
    sptr += 16;
  }
  gf_do_final_region_alignment(&rd);

}
#endif

static 
int gf_w4_single_table_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_single_table_data *std;
  int a, b, prod;


  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_single_table_data *)h->private;

  bzero(std->mult, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
  bzero(std->div, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    for (b = 1; b < GF_FIELD_SIZE; b++) {
      prod = gf_w4_shift_multiply(gf, a, b);
      std->mult[a][b] = prod;
      std->div[prod][b] = a;
    }
  }

  gf->inverse.w32 = NULL;
  gf->divide.w32 = gf_w4_single_table_divide;
  gf->multiply.w32 = gf_w4_single_table_multiply;
  #ifdef INTEL_SSSE3
    if(h->region_type & (GF_REGION_NOSSE | GF_REGION_CAUCHY))
      gf->multiply_region.w32 = gf_w4_single_table_multiply_region;
    else
      gf->multiply_region.w32 = gf_w4_single_table_sse_multiply_region;
  #else
    gf->multiply_region.w32 = gf_w4_single_table_multiply_region;
    if (h->region_type & GF_REGION_SSE) return 0;
  #endif

  return 1;
}

/* ------------------------------------------------------------
  IMPLEMENTATION: DOUBLE TABLE: JSP. 
 */

static
inline
gf_val_32_t
gf_w4_double_table_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_double_table_data *std;
    
  std = (struct gf_double_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->mult[a][b];
}

static
inline
gf_val_32_t
gf_w4_double_table_divide (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_double_table_data *std;
    
  std = (struct gf_double_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->div[a][b];
}

static
void 
gf_w4_double_table_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8, *base;
  gf_region_data rd;
  struct gf_double_table_data *std;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);

  std = (struct gf_double_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  s8 = (uint8_t *) src;
  d8 = (uint8_t *) dest;
  base = (uint8_t *) std->mult;
  base += (val << GF_DOUBLE_WIDTH);

  if (xor) {
    for (i = 0; i < bytes; i++) d8[i] ^= base[s8[i]];
  } else {
    for (i = 0; i < bytes; i++) d8[i] = base[s8[i]];
  }
}

static 
int gf_w4_double_table_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_double_table_data *std;
  int a, b, c, prod, ab;
  uint8_t mult[GF_FIELD_SIZE][GF_FIELD_SIZE];

  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_double_table_data *)h->private;

  bzero(mult, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
  bzero(std->div, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    for (b = 1; b < GF_FIELD_SIZE; b++) {
      prod = gf_w4_shift_multiply(gf, a, b);
      mult[a][b] = prod;
      std->div[prod][b] = a;
    }
  }
  bzero(std->mult, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE * GF_FIELD_SIZE);
  for (a = 0; a < GF_FIELD_SIZE; a++) {
    for (b = 0; b < GF_FIELD_SIZE; b++) {
      ab = mult[a][b];
      for (c = 0; c < GF_FIELD_SIZE; c++) {
        std->mult[a][(b << 4) | c] = ((ab << 4) | mult[a][c]);
      }
    }
  }

  gf->inverse.w32 = NULL;
  gf->divide.w32 = gf_w4_double_table_divide;
  gf->multiply.w32 = gf_w4_double_table_multiply;
  gf->multiply_region.w32 = gf_w4_double_table_multiply_region;
  return 1;
}


static
inline
gf_val_32_t
gf_w4_quad_table_lazy_divide (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_quad_table_lazy_data *std;
    
  std = (struct gf_quad_table_lazy_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->div[a][b];
}

static
inline
gf_val_32_t
gf_w4_quad_table_lazy_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_quad_table_lazy_data *std;
    
  std = (struct gf_quad_table_lazy_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->smult[a][b];
}

static
inline
gf_val_32_t
gf_w4_quad_table_divide (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_quad_table_data *std;
    
  std = (struct gf_quad_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  return std->div[a][b];
}

static
inline
gf_val_32_t
gf_w4_quad_table_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  struct gf_quad_table_data *std;
  uint16_t v;
    
  std = (struct gf_quad_table_data *) ((gf_internal_t *) (gf->scratch))->private;
  v = std->mult[a][b];
  return v;
}

static
void 
gf_w4_quad_table_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint16_t *base;
  gf_region_data rd;
  struct gf_quad_table_data *std;
  struct gf_quad_table_lazy_data *ltd;
  gf_internal_t *h;
  int a, b, c, d, va, vb, vc, vd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) (gf->scratch);
  if (h->region_type & GF_REGION_LAZY) {
    ltd = (struct gf_quad_table_lazy_data *) ((gf_internal_t *) (gf->scratch))->private;
    base = ltd->mult;
    for (a = 0; a < 16; a++) {
      va = (ltd->smult[val][a] << 12);
      for (b = 0; b < 16; b++) {
        vb = (ltd->smult[val][b] << 8);
        for (c = 0; c < 16; c++) {
          vc = (ltd->smult[val][c] << 4);
          for (d = 0; d < 16; d++) {
            vd = ltd->smult[val][d];
            base[(a << 12) | (b << 8) | (c << 4) | d ] = (va | vb | vc | vd);
          }
        }
      }
    }
  } else {
    std = (struct gf_quad_table_data *) ((gf_internal_t *) (gf->scratch))->private;
    base = &(std->mult[val][0]);
  }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);
  gf_two_byte_region_table_multiply(&rd, base);
  gf_do_final_region_alignment(&rd);
}

static 
int gf_w4_quad_table_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_quad_table_data *std;
  int prod, val, a, b, c, d, va, vb, vc, vd;
  uint8_t mult[GF_FIELD_SIZE][GF_FIELD_SIZE];

  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_quad_table_data *)h->private;

  bzero(mult, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
  bzero(std->div, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    for (b = 1; b < GF_FIELD_SIZE; b++) {
      prod = gf_w4_shift_multiply(gf, a, b);
      mult[a][b] = prod;
      std->div[prod][b] = a;
    }
  }

  for (val = 0; val < 16; val++) {
    for (a = 0; a < 16; a++) {
      va = (mult[val][a] << 12);
      for (b = 0; b < 16; b++) {
        vb = (mult[val][b] << 8);
        for (c = 0; c < 16; c++) {
          vc = (mult[val][c] << 4);
          for (d = 0; d < 16; d++) {
            vd = mult[val][d];
            std->mult[val][(a << 12) | (b << 8) | (c << 4) | d ] = (va | vb | vc | vd);
          }
        }
      }
    }
  }

  gf->inverse.w32 = NULL;
  gf->divide.w32 = gf_w4_quad_table_divide;
  gf->multiply.w32 = gf_w4_quad_table_multiply;
  gf->multiply_region.w32 = gf_w4_quad_table_multiply_region;
  return 1;
}
static 
int gf_w4_quad_table_lazy_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_quad_table_lazy_data *std;
  int a, b, prod, loga, logb;
  uint8_t log_tbl[GF_FIELD_SIZE];
  uint8_t antilog_tbl[GF_FIELD_SIZE*2];

  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_quad_table_lazy_data *)h->private;

  b = 1;
  for (a = 0; a < GF_MULT_GROUP_SIZE; a++) {
      log_tbl[b] = a;
      antilog_tbl[a] = b;
      antilog_tbl[a+GF_MULT_GROUP_SIZE] = b;
      b <<= 1;
      if (b & GF_FIELD_SIZE) {
          b = b ^ h->prim_poly;
      }
  }

  bzero(std->smult, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);
  bzero(std->div, sizeof(uint8_t) * GF_FIELD_SIZE * GF_FIELD_SIZE);

  for (a = 1; a < GF_FIELD_SIZE; a++) {
    loga = log_tbl[a];
    for (b = 1; b < GF_FIELD_SIZE; b++) {
      logb = log_tbl[b];
      prod = antilog_tbl[loga+logb];
      std->smult[a][b] = prod;
      std->div[prod][b] = a;
    }
  }

  gf->inverse.w32 = NULL;
  gf->divide.w32 = gf_w4_quad_table_lazy_divide;
  gf->multiply.w32 = gf_w4_quad_table_lazy_multiply;
  gf->multiply_region.w32 = gf_w4_quad_table_multiply_region;
  return 1;
}

static 
int gf_w4_table_init(gf_t *gf)
{
  int rt;
  gf_internal_t *h;
  int issse3 = 0;

#ifdef INTEL_SSSE3
  issse3 = 1;
#endif

  h = (gf_internal_t *) gf->scratch;
  rt = (h->region_type);

  if (h->mult_type == GF_MULT_DEFAULT && !issse3) rt |= GF_REGION_DOUBLE_TABLE;

  if (rt & GF_REGION_DOUBLE_TABLE) {
    return gf_w4_double_table_init(gf);
  } else if (rt & GF_REGION_QUAD_TABLE) {
    if (rt & GF_REGION_LAZY) {
      return gf_w4_quad_table_lazy_init(gf);
    } else {
      return gf_w4_quad_table_init(gf);
    }
    return gf_w4_double_table_init(gf);
  } else {
    return gf_w4_single_table_init(gf);
  }
  return 0;
}

/* ------------------------------------------------------------
   JSP: GF_MULT_BYTWO_p and _b: See the paper.
*/

static
inline
gf_val_32_t
gf_w4_bytwo_p_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, pmask, amask;
  gf_internal_t *h;
  
  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  
  prod = 0;
  pmask = 0x8;
  amask = 0x8;

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
gf_w4_bytwo_b_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, bmask;
  gf_internal_t *h;
  
  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  prod = 0;
  bmask = 0x8;

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
gf_w4_bytwo_p_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, prod, amask;
  gf_region_data rd;
  struct gf_bytwo_data *btd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 8);
  gf_do_initial_region_alignment(&rd);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;

  if (xor) {
    while (s64 < (uint64_t *) rd.s_top) {
      prod = 0;
      amask = 0x8;
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
      amask = 0x8;
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
      SSE_AB2(pp, m1, prod, t1, t2); \
      t1 = _mm_and_si128(v, one); \
      t1 = _mm_sub_epi8(t1, one); \
      t1 = _mm_and_si128(t1, ta); \
      prod = _mm_xor_si128(prod, t1); \
      v = _mm_srli_epi64(v, 1); }

#ifdef INTEL_SSE2
static
void
gf_w4_bytwo_p_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  int i;
  uint8_t *s8, *d8;
  uint8_t vrev;
  __m128i pp, m1, ta, prod, t1, t2, tp, one, v;
  struct gf_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  btd = (struct gf_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  vrev = 0;
  for (i = 0; i < 4; i++) {
    vrev <<= 1;
    if (!(val & (1 << i))) vrev |= 1;
  }

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
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
    _mm_store_si128((__m128i *) d8, _mm_xor_si128(prod, tp));
    d8 += 16;
    s8 += 16;
  }
  gf_do_final_region_alignment(&rd);
}
#endif

/*
static
void 
gf_w4_bytwo_b_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
#ifdef INTEL_SSE2
  uint8_t *d8, *s8, tb;
  __m128i pp, m1, m2, t1, t2, va, vb;
  struct gf_bytwo_data *btd;
  gf_region_data rd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  btd = (struct gf_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
  m2 = _mm_set1_epi8((btd->mask2)&0xff);

  if (xor) {
    while (d8 < (uint8_t *) rd.d_top) {
      va = _mm_load_si128 ((__m128i *)(s8));
      vb = _mm_load_si128 ((__m128i *)(d8));
      tb = val;
      while (1) {
        if (tb & 1) vb = _mm_xor_si128(vb, va);
        tb >>= 1;
        if (tb == 0) break;
        SSE_AB2(pp, m1, m2, va, t1, t2);
      }
      _mm_store_si128((__m128i *)d8, vb);
      d8 += 16;
      s8 += 16;
    }
  } else {
    while (d8 < (uint8_t *) rd.d_top) {
      va = _mm_load_si128 ((__m128i *)(s8));
      vb = _mm_setzero_si128 ();
      tb = val;
      while (1) {
        if (tb & 1) vb = _mm_xor_si128(vb, va);
        tb >>= 1;
        if (tb == 0) break;
        t1 = _mm_and_si128(_mm_slli_epi64(va, 1), m1);
        t2 = _mm_and_si128(va, m2);
        t2 = _mm_sub_epi64 (
          _mm_slli_epi64(t2, 1), _mm_srli_epi64(t2, (GF_FIELD_WIDTH-1)));
        va = _mm_xor_si128(t1, _mm_and_si128(t2, pp));
      }
      _mm_store_si128((__m128i *)d8, vb);
      d8 += 16;
      s8 += 16;
    }
  }
  gf_do_final_region_alignment(&rd);
#endif
}
*/

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_2_noxor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, va, t1, t2);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_2_xor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, va, t1, t2);
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
gf_w4_bytwo_b_sse_region_4_noxor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, va, t1, t2);
    SSE_AB2(pp, m1, va, t1, t2);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_4_xor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, va, t1, t2);
    SSE_AB2(pp, m1, va, t1, t2);
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
gf_w4_bytwo_b_sse_region_3_noxor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = va;
    SSE_AB2(pp, m1, va, t1, t2);
    va = _mm_xor_si128(va, vb);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_3_xor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = _mm_xor_si128(_mm_load_si128 ((__m128i *)(d8)), va);
    SSE_AB2(pp, m1, va, t1, t2);
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
gf_w4_bytwo_b_sse_region_5_noxor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = va;
    SSE_AB2(pp, m1, va, t1, t2);
    SSE_AB2(pp, m1, va, t1, t2);
    va = _mm_xor_si128(va, vb);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_5_xor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = _mm_xor_si128(_mm_load_si128 ((__m128i *)(d8)), va);
    SSE_AB2(pp, m1, va, t1, t2);
    SSE_AB2(pp, m1, va, t1, t2);
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
gf_w4_bytwo_b_sse_region_7_noxor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = va;
    SSE_AB2(pp, m1, va, t1, t2);
    vb = _mm_xor_si128(va, vb);
    SSE_AB2(pp, m1, va, t1, t2);
    va = _mm_xor_si128(va, vb);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_7_xor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    vb = _mm_xor_si128(_mm_load_si128 ((__m128i *)(d8)), va);
    SSE_AB2(pp, m1, va, t1, t2);
    vb = _mm_xor_si128(vb, va);
    SSE_AB2(pp, m1, va, t1, t2);
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
gf_w4_bytwo_b_sse_region_6_noxor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, va, t1, t2);
    vb = va;
    SSE_AB2(pp, m1, va, t1, t2);
    va = _mm_xor_si128(va, vb);
    _mm_store_si128((__m128i *)d8, va);
    d8 += 16;
    s8 += 16;
  }
}
#endif

#ifdef INTEL_SSE2
static 
void
gf_w4_bytwo_b_sse_region_6_xor(gf_region_data *rd, struct gf_bytwo_data *btd)
{
  uint8_t *d8, *s8;
  __m128i pp, m1, t1, t2, va, vb;

  s8 = (uint8_t *) rd->s_start;
  d8 = (uint8_t *) rd->d_start;

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);

  while (d8 < (uint8_t *) rd->d_top) {
    va = _mm_load_si128 ((__m128i *)(s8));
    SSE_AB2(pp, m1, va, t1, t2);
    vb = _mm_xor_si128(_mm_load_si128 ((__m128i *)(d8)), va);
    SSE_AB2(pp, m1, va, t1, t2);
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
gf_w4_bytwo_b_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint8_t *d8, *s8, tb;
  __m128i pp, m1, m2, t1, t2, va, vb;
  struct gf_bytwo_data *btd;
  gf_region_data rd;
    
  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  s8 = (uint8_t *) rd.s_start;
  d8 = (uint8_t *) rd.d_start;

  btd = (struct gf_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;

  switch (val) {
    case 2:
      if (!xor) {
        gf_w4_bytwo_b_sse_region_2_noxor(&rd, btd);
      } else {
        gf_w4_bytwo_b_sse_region_2_xor(&rd, btd);
      }
      gf_do_final_region_alignment(&rd);
      return;
    case 3:
      if (!xor) {
        gf_w4_bytwo_b_sse_region_3_noxor(&rd, btd);
      } else {
        gf_w4_bytwo_b_sse_region_3_xor(&rd, btd);
      }
      gf_do_final_region_alignment(&rd);
      return;
    case 4:
      if (!xor) {
        gf_w4_bytwo_b_sse_region_4_noxor(&rd, btd);
      } else {
        gf_w4_bytwo_b_sse_region_4_xor(&rd, btd);
      }
      gf_do_final_region_alignment(&rd);
      return;
    case 5:
      if (!xor) {
        gf_w4_bytwo_b_sse_region_5_noxor(&rd, btd);
      } else {
        gf_w4_bytwo_b_sse_region_5_xor(&rd, btd);
      }
      gf_do_final_region_alignment(&rd);
      return;
    case 6:
      if (!xor) {
        gf_w4_bytwo_b_sse_region_6_noxor(&rd, btd);
      } else {
        gf_w4_bytwo_b_sse_region_6_xor(&rd, btd);
      }
      gf_do_final_region_alignment(&rd);
      return;
    case 7:
      if (!xor) {
        gf_w4_bytwo_b_sse_region_7_noxor(&rd, btd);
      } else {
        gf_w4_bytwo_b_sse_region_7_xor(&rd, btd);
      }
      gf_do_final_region_alignment(&rd);
      return;
  }

  pp = _mm_set1_epi8(btd->prim_poly&0xff);
  m1 = _mm_set1_epi8((btd->mask1)&0xff);
  m2 = _mm_set1_epi8((btd->mask2)&0xff);

  if (xor) {
    while (d8 < (uint8_t *) rd.d_top) {
      va = _mm_load_si128 ((__m128i *)(s8));
      vb = _mm_load_si128 ((__m128i *)(d8));
      tb = val;
      while (1) {
        if (tb & 1) vb = _mm_xor_si128(vb, va);
        tb >>= 1;
        if (tb == 0) break;
        SSE_AB2(pp, m1, va, t1, t2);
      }
      _mm_store_si128((__m128i *)d8, vb);
      d8 += 16;
      s8 += 16;
    }
  } else {
    while (d8 < (uint8_t *) rd.d_top) {
      va = _mm_load_si128 ((__m128i *)(s8));
      vb = _mm_setzero_si128 ();
      tb = val;
      while (1) {
        if (tb & 1) vb = _mm_xor_si128(vb, va);
        tb >>= 1;
        if (tb == 0) break;
        t1 = _mm_and_si128(_mm_slli_epi64(va, 1), m1);
        t2 = _mm_and_si128(va, m2);
        t2 = _mm_sub_epi64 (
          _mm_slli_epi64(t2, 1), _mm_srli_epi64(t2, (GF_FIELD_WIDTH-1)));
        va = _mm_xor_si128(t1, _mm_and_si128(t2, pp));
      }
      _mm_store_si128((__m128i *)d8, vb);
      d8 += 16;
      s8 += 16;
    }
  }
  gf_do_final_region_alignment(&rd);
}
#endif

static
void 
gf_w4_bytwo_b_nosse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  uint64_t *s64, *d64, t1, t2, ta, tb, prod;
  struct gf_bytwo_data *btd;
  gf_region_data rd;

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, 16);
  gf_do_initial_region_alignment(&rd);

  btd = (struct gf_bytwo_data *) ((gf_internal_t *) (gf->scratch))->private;
  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;

  switch (val) {
  case 1:
    if (xor) {
      while (d64 < (uint64_t *) rd.d_top) {
        *d64 ^= *s64;
        d64++;
        s64++;
      }
    } else {
      while (d64 < (uint64_t *) rd.d_top) {
        *d64 = *s64;
        d64++;
        s64++;
      }
    }
    break;
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
int gf_w4_bytwo_init(gf_t *gf)
{
  gf_internal_t *h;
  uint64_t ip, m1, m2;
  struct gf_bytwo_data *btd;

  h = (gf_internal_t *) gf->scratch;
  btd = (struct gf_bytwo_data *) (h->private);
  ip = h->prim_poly & 0xf;
  m1 = 0xe;
  m2 = 0x8;
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
    gf->multiply.w32 = gf_w4_bytwo_p_multiply;
    #ifdef INTEL_SSE2
      if (h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w4_bytwo_p_nosse_multiply_region;
      else
        gf->multiply_region.w32 = gf_w4_bytwo_p_sse_multiply_region;
    #else
      gf->multiply_region.w32 = gf_w4_bytwo_p_nosse_multiply_region;
      if (h->region_type & GF_REGION_SSE)
        return 0;
    #endif
  } else {
    gf->multiply.w32 = gf_w4_bytwo_b_multiply;
    #ifdef INTEL_SSE2
      if (h->region_type & GF_REGION_NOSSE)
        gf->multiply_region.w32 = gf_w4_bytwo_b_nosse_multiply_region;
      else
        gf->multiply_region.w32 = gf_w4_bytwo_b_sse_multiply_region;
    #else
      gf->multiply_region.w32 = gf_w4_bytwo_b_nosse_multiply_region;
      if (h->region_type & GF_REGION_SSE)
        return 0;
    #endif
  }
  return 1;
}


static 
int gf_w4_cfm_init(gf_t *gf)
{
#if defined(INTEL_SSE4_PCLMUL)
  gf->multiply.w32 = gf_w4_clm_multiply;
  return 1;
#endif
  return 0;
}

static 
int gf_w4_shift_init(gf_t *gf)
{
  gf->multiply.w32 = gf_w4_shift_multiply;
  return 1;
}

/* JSP: I'm putting all error-checking into gf_error_check(), so you don't 
   have to do error checking in scratch_size or in init */

int gf_w4_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2)
{
  int issse3 = 0;

#ifdef INTEL_SSSE3
  issse3 = 1;
#endif

  switch(mult_type)
  {
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:
      return sizeof(gf_internal_t) + sizeof(struct gf_bytwo_data);
      break;
    case GF_MULT_DEFAULT:
    case GF_MULT_TABLE:
      if (region_type == GF_REGION_CAUCHY) {
        return sizeof(gf_internal_t) + sizeof(struct gf_single_table_data) + 64;
      }

      if (mult_type == GF_MULT_DEFAULT && !issse3) region_type = GF_REGION_DOUBLE_TABLE;

      if (region_type & GF_REGION_DOUBLE_TABLE) {
        return sizeof(gf_internal_t) + sizeof(struct gf_double_table_data) + 64;
      } else if (region_type & GF_REGION_QUAD_TABLE) {
        if ((region_type & GF_REGION_LAZY) == 0) {
          return sizeof(gf_internal_t) + sizeof(struct gf_quad_table_data) + 64;
        } else {
          return sizeof(gf_internal_t) + sizeof(struct gf_quad_table_lazy_data) + 64;
        }
      } else {
        return sizeof(gf_internal_t) + sizeof(struct gf_single_table_data) + 64;
      }
      break;

    case GF_MULT_LOG_TABLE:
      return sizeof(gf_internal_t) + sizeof(struct gf_logtable_data) + 64;
      break;
    case GF_MULT_CARRY_FREE:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_SHIFT:
      return sizeof(gf_internal_t);
      break;
    default:
      return 0;
   }
  return 0;
}

int
gf_w4_init (gf_t *gf)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  if (h->prim_poly == 0) h->prim_poly = 0x13;
  h->prim_poly |= 0x10;
  gf->multiply.w32 = NULL;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = NULL;
  gf->extract_word.w32 = gf_w4_extract_word;

  switch(h->mult_type) {
    case GF_MULT_CARRY_FREE: if (gf_w4_cfm_init(gf) == 0) return 0; break;
    case GF_MULT_SHIFT:      if (gf_w4_shift_init(gf) == 0) return 0; break;
    case GF_MULT_BYTWO_p:   
    case GF_MULT_BYTWO_b:    if (gf_w4_bytwo_init(gf) == 0) return 0; break;
    case GF_MULT_LOG_TABLE:  if (gf_w4_log_init(gf) == 0) return 0; break;
    case GF_MULT_DEFAULT:   
    case GF_MULT_TABLE:      if (gf_w4_table_init(gf) == 0) return 0; break;
    default: return 0;
  }

  if (h->divide_type == GF_DIVIDE_EUCLID) {
    gf->divide.w32 = gf_w4_divide_from_inverse;
    gf->inverse.w32 = gf_w4_euclid;
  } else if (h->divide_type == GF_DIVIDE_MATRIX) {
    gf->divide.w32 = gf_w4_divide_from_inverse;
    gf->inverse.w32 = gf_w4_matrix;
  }

  if (gf->divide.w32 == NULL) {
    gf->divide.w32 = gf_w4_divide_from_inverse;
    if (gf->inverse.w32 == NULL) gf->inverse.w32 = gf_w4_euclid;
  }

  if (gf->inverse.w32 == NULL)  gf->inverse.w32 = gf_w4_inverse_from_divide;

  if (h->region_type == GF_REGION_CAUCHY) {
    gf->multiply_region.w32 = gf_wgen_cauchy_region;
    gf->extract_word.w32 = gf_wgen_extract_word;
  }

  if (gf->multiply_region.w32 == NULL) {
    gf->multiply_region.w32 = gf_w4_multiply_region_from_single;
  }

  return 1;
}

/* Inline setup functions */

uint8_t *gf_w4_get_mult_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_single_table_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w4_single_table_multiply) {
    std = (struct gf_single_table_data *) h->private;
    return (uint8_t *) std->mult;
  } 
  return NULL;
}
    
uint8_t *gf_w4_get_div_table(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_single_table_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  if (gf->multiply.w32 == gf_w4_single_table_multiply) {
    std = (struct gf_single_table_data *) h->private;
    return (uint8_t *) std->div;
  } 
  return NULL;
}

