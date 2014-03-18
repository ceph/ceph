/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w128.c
 *
 * Routines for 128-bit Galois fields
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

#define GF_FIELD_WIDTH (128)

#define two_x(a) {\
  a[0] <<= 1; \
  if (a[1] & 1ULL << 63) a[0] ^= 1; \
  a[1] <<= 1; }
  
#define a_get_b(a, i, b, j) {\
  a[i] = b[j]; \
  a[i + 1] = b[j + 1];}

#define set_zero(a, i) {\
  a[i] = 0; \
  a[i + 1] = 0;}

struct gf_w128_split_4_128_data {
  uint64_t last_value[2];
  uint64_t tables[2][32][16];
};

struct gf_w128_split_8_128_data {
  uint64_t last_value[2];
  uint64_t tables[2][16][256];
};

typedef struct gf_group_tables_s {
  gf_val_128_t m_table;
  gf_val_128_t r_table;
} gf_group_tables_t;

#define MM_PRINT8(s, r) { uint8_t blah[16], ii; printf("%-12s", s); _mm_storeu_si128((__m128i *)blah, r); for (ii = 0; ii < 16; ii += 1) printf("%s%02x", (ii%4==0) ? "   " : " ", blah[15-ii]); printf("\n"); }

static
void
gf_w128_multiply_region_from_single(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes,
int xor)
{
    int i;
    gf_val_128_t s128;
    gf_val_128_t d128;
    uint64_t c128[2];
    gf_region_data rd;

    /* We only do this to check on alignment. */
    gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);

    if (val[0] == 0) {
      if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
      if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
    }

    set_zero(c128, 0);

    s128 = (gf_val_128_t) src;
    d128 = (gf_val_128_t) dest;

    if (xor) {
      for (i = 0; i < bytes/sizeof(gf_val_64_t); i += 2) {
        gf->multiply.w128(gf, &s128[i], val, c128);
        d128[i] ^= c128[0];
        d128[i+1] ^= c128[1];
      }
    } else {
      for (i = 0; i < bytes/sizeof(gf_val_64_t); i += 2) {
        gf->multiply.w128(gf, &s128[i], val, &d128[i]);
      }
    }
}

static
void
gf_w128_clm_multiply_region_from_single(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes,
int xor)
{
    gf_internal_t * h = gf->scratch;
    if ((h->sse & GF_SSE4_PCLMUL) == 0)
      return;
#if defined(INTEL_SSE4_PCLMUL)
    int i;
    gf_val_128_t s128;
    gf_val_128_t d128;
    gf_region_data rd;
    __m128i     a,b;
    __m128i     result0,result1;
    __m128i     prim_poly;
    __m128i     c,d,e,f;
    prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)h->prim_poly);
    /* We only do this to check on alignment. */
    gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);

    if (val[0] == 0) {
      if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
      if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
    }

    s128 = (gf_val_128_t) src;
    d128 = (gf_val_128_t) dest;

    if (xor) {
      for (i = 0; i < bytes/sizeof(gf_val_64_t); i += 2) {
        a = _mm_insert_epi64 (_mm_setzero_si128(), s128[i+1], 0);
        b = _mm_insert_epi64 (a, val[1], 0);
        a = _mm_insert_epi64 (a, s128[i], 1);
        b = _mm_insert_epi64 (b, val[0], 1);
    
        c = _mm_clmulepi64_si128 (a, b, 0x00); /*low-low*/
        f = _mm_clmulepi64_si128 (a, b, 0x01); /*high-low*/
        e = _mm_clmulepi64_si128 (a, b, 0x10); /*low-high*/
        d = _mm_clmulepi64_si128 (a, b, 0x11); /*high-high*/

        /* now reusing a and b as temporary variables*/
        result0 = _mm_setzero_si128();
        result1 = result0;

        result0 = _mm_xor_si128 (result0, _mm_insert_epi64 (d, 0, 0));
        a = _mm_xor_si128 (_mm_srli_si128 (e, 8), _mm_insert_epi64 (d, 0, 1));
        result0 = _mm_xor_si128 (result0, _mm_xor_si128 (_mm_srli_si128 (f, 8), a));

        a = _mm_xor_si128 (_mm_slli_si128 (e, 8), _mm_insert_epi64 (c, 0, 0));
        result1 = _mm_xor_si128 (result1, _mm_xor_si128 (_mm_slli_si128 (f, 8), a));
        result1 = _mm_xor_si128 (result1, _mm_insert_epi64 (c, 0, 1));
        /* now we have constructed our 'result' with result0 being the carry bits, and we have to reduce. */

        a = _mm_srli_si128 (result0, 8);
        b = _mm_clmulepi64_si128 (a, prim_poly, 0x00);
        result0 = _mm_xor_si128 (result0, _mm_srli_si128 (b, 8));
        result1 = _mm_xor_si128 (result1, _mm_slli_si128 (b, 8));

        a = _mm_insert_epi64 (result0, 0, 1);
        b = _mm_clmulepi64_si128 (a, prim_poly, 0x00);
        result1 = _mm_xor_si128 (result1, b); 
        d128[i] ^= (uint64_t)_mm_extract_epi64(result1,1);
        d128[i+1] ^= (uint64_t)_mm_extract_epi64(result1,0);
      }
    } else {
      for (i = 0; i < bytes/sizeof(gf_val_64_t); i += 2) {
        a = _mm_insert_epi64 (_mm_setzero_si128(), s128[i+1], 0);
        b = _mm_insert_epi64 (a, val[1], 0);
        a = _mm_insert_epi64 (a, s128[i], 1);
        b = _mm_insert_epi64 (b, val[0], 1);

        c = _mm_clmulepi64_si128 (a, b, 0x00); /*low-low*/
        f = _mm_clmulepi64_si128 (a, b, 0x01); /*high-low*/
        e = _mm_clmulepi64_si128 (a, b, 0x10); /*low-high*/ 
        d = _mm_clmulepi64_si128 (a, b, 0x11); /*high-high*/ 

        /* now reusing a and b as temporary variables*/
        result0 = _mm_setzero_si128();
        result1 = result0;

        result0 = _mm_xor_si128 (result0, _mm_insert_epi64 (d, 0, 0));
        a = _mm_xor_si128 (_mm_srli_si128 (e, 8), _mm_insert_epi64 (d, 0, 1));
        result0 = _mm_xor_si128 (result0, _mm_xor_si128 (_mm_srli_si128 (f, 8), a));

        a = _mm_xor_si128 (_mm_slli_si128 (e, 8), _mm_insert_epi64 (c, 0, 0));
        result1 = _mm_xor_si128 (result1, _mm_xor_si128 (_mm_slli_si128 (f, 8), a));
        result1 = _mm_xor_si128 (result1, _mm_insert_epi64 (c, 0, 1));
        /* now we have constructed our 'result' with result0 being the carry bits, and we have to reduce.*/

        a = _mm_srli_si128 (result0, 8);
        b = _mm_clmulepi64_si128 (a, prim_poly, 0x00);
        result0 = _mm_xor_si128 (result0, _mm_srli_si128 (b, 8));
        result1 = _mm_xor_si128 (result1, _mm_slli_si128 (b, 8));

        a = _mm_insert_epi64 (result0, 0, 1);
        b = _mm_clmulepi64_si128 (a, prim_poly, 0x00);
        result1 = _mm_xor_si128 (result1, b);
        d128[i] = (uint64_t)_mm_extract_epi64(result1,1);
        d128[i+1] = (uint64_t)_mm_extract_epi64(result1,0);
      }
    }
#endif
}

/*
 * Some w128 notes:
 * --Big Endian
 * --return values allocated beforehand
 */

#define GF_W128_IS_ZERO(val) (val[0] == 0 && val[1] == 0)

void
gf_w128_shift_multiply(gf_t *gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  /* ordered highest bit to lowest l[0] l[1] r[0] r[1] */
  uint64_t pl[2], pr[2], ppl[2], ppr[2], i, a[2], bl[2], br[2], one, lbit;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

  if (GF_W128_IS_ZERO(a128) || GF_W128_IS_ZERO(b128)) {
    set_zero(c128, 0);
    return;
  }

  a_get_b(a, 0, a128, 0);
  a_get_b(br, 0, b128, 0);
  set_zero(bl, 0);

  one = 1;
  lbit = (one << 63);

  set_zero(pl, 0);
  set_zero(pr, 0);

  /* Allen: a*b for right half of a */
  for (i = 0; i < GF_FIELD_WIDTH/2; i++) {
    if (a[1] & (one << i)) {
      pl[1] ^= bl[1];
      pr[0] ^= br[0];
      pr[1] ^= br[1];
    }
    bl[1] <<= 1;
    if (br[0] & lbit) bl[1] ^= 1;
    br[0] <<= 1;
    if (br[1] & lbit) br[0] ^= 1;
    br[1] <<= 1;
  }

  /* Allen: a*b for left half of a */
  for (i = 0; i < GF_FIELD_WIDTH/2; i++) {
    if (a[0] & (one << i)) {
      pl[0] ^= bl[0];
      pl[1] ^= bl[1];
      pr[0] ^= br[0];
    }
    bl[0] <<= 1;
    if (bl[1] & lbit) bl[0] ^= 1;
    bl[1] <<= 1;
    if (br[0] & lbit) bl[1] ^= 1;
    br[0] <<= 1;
  }

  /* Allen: do first half of reduction (based on left quarter of initial product) */
  one = lbit >> 1;
  ppl[0] = one; /* Allen: introduce leading one of primitive polynomial */
  ppl[1] = h->prim_poly >> 2;
  ppr[0] = h->prim_poly << (GF_FIELD_WIDTH/2-2);
  ppr[1] = 0;
  while (one != 0) {
    if (pl[0] & one) {
      pl[0] ^= ppl[0];
      pl[1] ^= ppl[1];
      pr[0] ^= ppr[0];
      pr[1] ^= ppr[1];
    }
    one >>= 1;
    ppr[1] >>= 1;
    if (ppr[0] & 1) ppr[1] ^= lbit;
    ppr[0] >>= 1;
    if (ppl[1] & 1) ppr[0] ^= lbit;
    ppl[1] >>= 1;
    if (ppl[0] & 1) ppl[1] ^= lbit;
    ppl[0] >>= 1;
  }

  /* Allen: final half of reduction */
  one = lbit;
  while (one != 0) {
    if (pl[1] & one) {
      pl[1] ^= ppl[1];
      pr[0] ^= ppr[0];
      pr[1] ^= ppr[1];
    }
    one >>= 1;
    ppr[1] >>= 1;
    if (ppr[0] & 1) ppr[1] ^= lbit;
    ppr[0] >>= 1;
    if (ppl[1] & 1) ppr[0] ^= lbit;
    ppl[1] >>= 1;
  }

  /* Allen: if we really want to optimize this we can just be using c128 instead of pr all along */
  c128[0] = pr[0];
  c128[1] = pr[1];

  return;
}

void
gf_w128_clm_multiply(gf_t *gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
    gf_internal_t * h = gf->scratch;
    if ((h->sse & GF_SSE4_PCLMUL) == 0)
      return;
#if defined(INTEL_SSE4_PCLMUL)

    __m128i     a,b;
    __m128i     result0,result1;
    __m128i     prim_poly;
    __m128i     c,d,e,f;
    
    a = _mm_insert_epi64 (_mm_setzero_si128(), a128[1], 0);
    b = _mm_insert_epi64 (a, b128[1], 0);
    a = _mm_insert_epi64 (a, a128[0], 1);
    b = _mm_insert_epi64 (b, b128[0], 1);

    prim_poly = _mm_set_epi32(0, 0, 0, (uint32_t)h->prim_poly);

    /* we need to test algorithm 2 later*/
    c = _mm_clmulepi64_si128 (a, b, 0x00); /*low-low*/
    f = _mm_clmulepi64_si128 (a, b, 0x01); /*high-low*/
    e = _mm_clmulepi64_si128 (a, b, 0x10); /*low-high*/
    d = _mm_clmulepi64_si128 (a, b, 0x11); /*high-high*/
    
    /* now reusing a and b as temporary variables*/
    result0 = _mm_setzero_si128();
    result1 = result0;

    result0 = _mm_xor_si128 (result0, _mm_insert_epi64 (d, 0, 0));
    a = _mm_xor_si128 (_mm_srli_si128 (e, 8), _mm_insert_epi64 (d, 0, 1));
    result0 = _mm_xor_si128 (result0, _mm_xor_si128 (_mm_srli_si128 (f, 8), a));

    a = _mm_xor_si128 (_mm_slli_si128 (e, 8), _mm_insert_epi64 (c, 0, 0));
    result1 = _mm_xor_si128 (result1, _mm_xor_si128 (_mm_slli_si128 (f, 8), a));
    result1 = _mm_xor_si128 (result1, _mm_insert_epi64 (c, 0, 1));
    /* now we have constructed our 'result' with result0 being the carry bits, and we have to reduce.*/
    
    a = _mm_srli_si128 (result0, 8);
    b = _mm_clmulepi64_si128 (a, prim_poly, 0x00);
    result0 = _mm_xor_si128 (result0, _mm_srli_si128 (b, 8));
    result1 = _mm_xor_si128 (result1, _mm_slli_si128 (b, 8));
    
    a = _mm_insert_epi64 (result0, 0, 1);
    b = _mm_clmulepi64_si128 (a, prim_poly, 0x00);
    result1 = _mm_xor_si128 (result1, b);

    c128[0] = (uint64_t)_mm_extract_epi64(result1,1);
    c128[1] = (uint64_t)_mm_extract_epi64(result1,0);
#endif
return;
}

void
gf_w128_bytwo_p_multiply(gf_t *gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  uint64_t amask[2], pmask, pp, prod[2]; /*John: pmask is always the highest bit set, and the rest zeros. amask changes, it's a countdown.*/
  uint64_t topbit; /* this is used as a boolean value */
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;
  prod[0] = 0;
  prod[1] = 0;
  pmask = 0x8000000000000000ULL;
  amask[0] = 0x8000000000000000ULL;
  amask[1] = 0;

  while (amask[1] != 0 || amask[0] != 0) {
    topbit = (prod[0] & pmask);
    prod[0] <<= 1;
    if (prod[1] & pmask) prod[0] ^= 1;
    prod[1] <<= 1;
    if (topbit) prod[1] ^= pp;
    if ((a128[0] & amask[0]) || (a128[1] & amask[1])) {
      prod[0] ^= b128[0];
      prod[1] ^= b128[1];
    }
    amask[1] >>= 1;
    if (amask[0] & 1) amask[1] ^= pmask;
    amask[0] >>= 1;
  }
  c128[0] = prod [0];
  c128[1] = prod [1];
  return;
}

void
gf_w128_sse_bytwo_p_multiply(gf_t *gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  gf_internal_t * h = gf->scratch;
  if ((h->sse & GF_SSE4) == 0)
    return;
#if defined(INTEL_SSE4)
  int i;
  __m128i a, b, pp, prod, amask, u_middle_one; 
  /*John: pmask is always the highest bit set, and the rest zeros. amask changes, it's a countdown.*/
  uint32_t topbit, middlebit, pmask; /* this is used as a boolean value */


  h = (gf_internal_t *) gf->scratch;
  pp = _mm_set_epi32(0, 0, 0, (uint32_t)h->prim_poly);
  prod = _mm_setzero_si128();
  a = _mm_insert_epi64(prod, a128[1], 0x0);
  a = _mm_insert_epi64(a, a128[0], 0x1);
  b = _mm_insert_epi64(prod, b128[1], 0x0);
  b = _mm_insert_epi64(b, b128[0], 0x1);
  pmask = 0x80000000;
  amask = _mm_insert_epi32(prod, 0x80000000, 0x3);
  u_middle_one = _mm_insert_epi32(prod, 1, 0x2);
  
  for (i = 0; i < 64; i++) {
    topbit = (_mm_extract_epi32(prod, 0x3) & pmask);
    middlebit = (_mm_extract_epi32(prod, 0x1) & pmask);
    prod = _mm_slli_epi64(prod, 1); /* this instruction loses the middle bit */
    if (middlebit) {
      prod = _mm_xor_si128(prod, u_middle_one);
    }
    if (topbit) {
      prod = _mm_xor_si128(prod, pp);
    }
    if (((uint64_t)_mm_extract_epi64(_mm_and_si128(a, amask), 1))) {
      prod = _mm_xor_si128(prod, b);
    }
    amask = _mm_srli_epi64(amask, 1); /*so does this one, but we can just replace after loop*/
  }
  amask = _mm_insert_epi32(amask, 1 << 31, 0x1);
  for (i = 64; i < 128; i++) {
    topbit = (_mm_extract_epi32(prod, 0x3) & pmask);
    middlebit = (_mm_extract_epi32(prod, 0x1) & pmask);
    prod = _mm_slli_epi64(prod, 1);
    if (middlebit) prod = _mm_xor_si128(prod, u_middle_one);
    if (topbit) prod = _mm_xor_si128(prod, pp);
    if (((uint64_t)_mm_extract_epi64(_mm_and_si128(a, amask), 0))) {
      prod = _mm_xor_si128(prod, b);
    }
    amask = _mm_srli_epi64(amask, 1);
  }
  c128[0] = (uint64_t)_mm_extract_epi64(prod, 1);
  c128[1] = (uint64_t)_mm_extract_epi64(prod, 0);
#endif
  return;
}


/* Ben: This slow function implements sse instrutions for bytwo_b because why not */
void
gf_w128_sse_bytwo_b_multiply(gf_t *gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  gf_internal_t * h = gf->scratch;
  if ((h->sse & GF_SSE4) == 0)
    return;
#if defined(INTEL_SSE4)
  __m128i a, b, lmask, hmask, pp, c, middle_one;
  uint64_t topbit, middlebit;

  
  c = _mm_setzero_si128();
  lmask = _mm_insert_epi64(c, 1ULL << 63, 0);
  hmask = _mm_insert_epi64(c, 1ULL << 63, 1);
  b = _mm_insert_epi64(c, a128[0], 1);
  b = _mm_insert_epi64(b, a128[1], 0);
  a = _mm_insert_epi64(c, b128[0], 1);
  a = _mm_insert_epi64(a, b128[1], 0);
  pp = _mm_insert_epi64(c, h->prim_poly, 0);
  middle_one = _mm_insert_epi64(c, 1, 0x1);

  while (1) {
    if (_mm_extract_epi32(a, 0x0) & 1) {
      c = _mm_xor_si128(c, b);
    }
    middlebit = (_mm_extract_epi32(a, 0x2) & 1);
    a = _mm_srli_epi64(a, 1);
    if (middlebit) a = _mm_xor_si128(a, lmask);
    if ((_mm_extract_epi64(a, 0x1) == 0ULL) && (_mm_extract_epi64(a, 0x0) == 0ULL)){
      c128[0] = _mm_extract_epi64(c, 0x1);
      c128[1] = _mm_extract_epi64(c, 0x0);
      return;
    }
    topbit = (_mm_extract_epi64(_mm_and_si128(b, hmask), 1));
    middlebit = (_mm_extract_epi64(_mm_and_si128(b, lmask), 0));
    b = _mm_slli_epi64(b, 1);
    if (middlebit) b = _mm_xor_si128(b, middle_one);
    if (topbit) b = _mm_xor_si128(b, pp);
  }
#endif
}

void
gf_w128_bytwo_b_multiply(gf_t *gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  uint64_t bmask, pp;
  gf_internal_t *h;
  uint64_t a[2], b[2], c[2];

  h = (gf_internal_t *) gf->scratch;

  bmask = (1ULL << 63);
  set_zero(c, 0);
  b[0] = a128[0];
  b[1] = a128[1];
  a[0] = b128[0];
  a[1] = b128[1];
  
  while (1) {
    if (a[1] & 1) {
      c[0] ^= b[0];
      c[1] ^= b[1];
    }
    a[1] >>= 1;
    if (a[0] & 1) a[1] ^= bmask;
    a[0] >>= 1;
    if (a[1] == 0 && a[0] == 0) {
      c128[0] = c[0];
      c128[1] = c[1];
      return;
    }
    pp = (b[0] & bmask);
    b[0] <<= 1;
    if (b[1] & bmask) b[0] ^= 1;
    b[1] <<= 1;
    if (pp) b[1] ^= h->prim_poly;
  }
}

static
void
gf_w128_split_4_128_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  int i, j, k;
  uint64_t pp;
  gf_internal_t *h;
  uint64_t *s64, *d64, *top;
  gf_region_data rd;
  uint64_t v[2], s;
  struct gf_w128_split_4_128_data *ld;

  /* We only do this to check on alignment. */
  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);

  if (val[0] == 0) {
    if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
    if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  }
    
  h = (gf_internal_t *) gf->scratch;
  ld = (struct gf_w128_split_4_128_data *) h->private;

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top = (uint64_t *) rd.d_top;

  if (val[0] != ld->last_value[0] || val[1] != ld->last_value[1]) {
    v[0] = val[0];
    v[1] = val[1];
    for (i = 0; i < 32; i++) {
      ld->tables[0][i][0] = 0;
      ld->tables[1][i][0] = 0;
      for (j = 1; j < 16; j <<= 1) {
        for (k = 0; k < j; k++) {
          ld->tables[0][i][k^j] = (v[0] ^ ld->tables[0][i][k]);
          ld->tables[1][i][k^j] = (v[1] ^ ld->tables[1][i][k]);
        }
        pp = (v[0] & (1ULL << 63));
        v[0] <<= 1;
        if (v[1] & (1ULL << 63)) v[0] ^= 1;
        v[1] <<= 1;
        if (pp) v[1] ^= h->prim_poly;
      }
    }
  }
  ld->last_value[0] = val[0];
  ld->last_value[1] = val[1];

/*
  for (i = 0; i < 32; i++) {
    for (j = 0; j < 16; j++) {
      printf("%2d %2d %016llx %016llx\n", i, j, ld->tables[0][i][j], ld->tables[1][i][j]);
    }
    printf("\n");
  }
 */
  i = 0;
  while (d64 < top) {
    v[0] = (xor) ? d64[0] : 0;
    v[1] = (xor) ? d64[1] : 0;
    s = s64[1];
    i = 0;
    while (s != 0) {
      v[0] ^= ld->tables[0][i][s&0xf];
      v[1] ^= ld->tables[1][i][s&0xf];
      s >>= 4;
      i++;
    }
    s = s64[0];
    i = 16;
    while (s != 0) {
      v[0] ^= ld->tables[0][i][s&0xf];
      v[1] ^= ld->tables[1][i][s&0xf];
      s >>= 4;
      i++;
    }
    d64[0] = v[0];
    d64[1] = v[1];
    s64 += 2;
    d64 += 2;
  }
}

static
void
gf_w128_split_4_128_sse_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  gf_internal_t * h = gf->scratch;
  if ((h->sse & GF_SSSE3) == 0)
    return;

#ifdef INTEL_SSSE3
  int i, j, k;
  uint64_t pp, v[2], s, *s64, *d64, *top;
  __m128i p, tables[32][16];
  struct gf_w128_split_4_128_data *ld;
  gf_region_data rd;

  if (val[0] == 0) {
    if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
    if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  }

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;
  
  /* We only do this to check on alignment. */
  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 16);

  /* Doing this instead of gf_do_initial_region_alignment() because that doesn't hold 128-bit vals */

  gf_w128_multiply_region_from_single(gf, src, dest, val, ((char*)rd.s_start-(char*)src), xor);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top = (uint64_t *) rd.d_top;
 
  ld = (struct gf_w128_split_4_128_data *) h->private;

  if (val[0] != ld->last_value[0] || val[1] != ld->last_value[1]) {
    v[0] = val[0];
    v[1] = val[1];
    for (i = 0; i < 32; i++) {
      ld->tables[0][i][0] = 0;
      ld->tables[1][i][0] = 0;
      for (j = 1; j < 16; j <<= 1) {
        for (k = 0; k < j; k++) {
          ld->tables[0][i][k^j] = (v[0] ^ ld->tables[0][i][k]);
          ld->tables[1][i][k^j] = (v[1] ^ ld->tables[1][i][k]);
        }
        pp = (v[0] & (1ULL << 63));
        v[0] <<= 1;
        if (v[1] & (1ULL << 63)) v[0] ^= 1;
        v[1] <<= 1;
        if (pp) v[1] ^= h->prim_poly;
      }
    }
  }

  ld->last_value[0] = val[0];
  ld->last_value[1] = val[1];

  for (i = 0; i < 32; i++) {
    for (j = 0; j < 16; j++) {
      v[0] = ld->tables[0][i][j];
      v[1] = ld->tables[1][i][j];
      tables[i][j] = _mm_loadu_si128((__m128i *) v);

/*
      printf("%2d %2d: ", i, j);
      MM_PRINT8("", tables[i][j]); */
    }
  }

  while (d64 != top) {

    if (xor) {
      p = _mm_load_si128 ((__m128i *) d64);
    } else {
      p = _mm_setzero_si128();
    }
    s = *s64;
    s64++;
    for (i = 0; i < 16; i++) {
      j = (s&0xf);
      s >>= 4;
      p = _mm_xor_si128(p, tables[16+i][j]);
    }
    s = *s64;
    s64++;
    for (i = 0; i < 16; i++) {
      j = (s&0xf);
      s >>= 4;
      p = _mm_xor_si128(p, tables[i][j]);
    }
    _mm_store_si128((__m128i *) d64, p);
    d64 += 2;
  }

  /* Doing this instead of gf_do_final_region_alignment() because that doesn't hold 128-bit vals */

  gf_w128_multiply_region_from_single(gf, rd.s_top, rd.d_top, val, ((char*)src+bytes)-(char*)rd.s_top, xor);
#endif
}

static
void
gf_w128_split_4_128_sse_altmap_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  gf_internal_t * h = gf->scratch;
  if ((h->sse & GF_SSSE3) == 0)
    return;

#ifdef INTEL_SSSE3
  int i, j, k;
  uint64_t pp, v[2], *s64, *d64, *top;
  __m128i si, tables[32][16], p[16], v0, mask1;
  struct gf_w128_split_4_128_data *ld;
  uint8_t btable[16];
  gf_region_data rd;

  if (val[0] == 0) {
    if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
    if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  }

  pp = h->prim_poly;
  
  /* We only do this to check on alignment. */
  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 256);

  /* Doing this instead of gf_do_initial_region_alignment() because that doesn't hold 128-bit vals */

  gf_w128_multiply_region_from_single(gf, src, dest, val, ((char*)rd.s_start-(char*)src), xor);

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top = (uint64_t *) rd.d_top;
 
  ld = (struct gf_w128_split_4_128_data *) h->private;

  if (val[0] != ld->last_value[0] || val[1] != ld->last_value[1]) {
    v[0] = val[0];
    v[1] = val[1];
    for (i = 0; i < 32; i++) {
      ld->tables[0][i][0] = 0;
      ld->tables[1][i][0] = 0;
      for (j = 1; j < 16; j <<= 1) {
        for (k = 0; k < j; k++) {
          ld->tables[0][i][k^j] = (v[0] ^ ld->tables[0][i][k]);
          ld->tables[1][i][k^j] = (v[1] ^ ld->tables[1][i][k]);
        }
        pp = (v[0] & (1ULL << 63));
        v[0] <<= 1;
        if (v[1] & (1ULL << 63)) v[0] ^= 1;
        v[1] <<= 1;
        if (pp) v[1] ^= h->prim_poly;
      }
    }
  }

  ld->last_value[0] = val[0];
  ld->last_value[1] = val[1];

  for (i = 0; i < 32; i++) {
    for (j = 0; j < 16; j++) {
      for (k = 0; k < 16; k++) {
        btable[k] = (uint8_t) ld->tables[1-(j/8)][i][k];
        ld->tables[1-(j/8)][i][k] >>= 8;
      }
      tables[i][j] = _mm_loadu_si128((__m128i *) btable);
/*
      printf("%2d %2d: ", i, j);
      MM_PRINT8("", tables[i][j]);
 */
    }
  }


  mask1 = _mm_set1_epi8(0xf);

  while (d64 != top) {

    if (xor) {
      for (i = 0; i < 16; i++) p[i] = _mm_load_si128 ((__m128i *) (d64+i*2));
    } else {
      for (i = 0; i < 16; i++) p[i] = _mm_setzero_si128();
    }
    i = 0;
    for (k = 0; k < 16; k++) {
      v0 = _mm_load_si128((__m128i *) s64); 
      s64 += 2;
      
      si = _mm_and_si128(v0, mask1);
  
      for (j = 0; j < 16; j++) {
        p[j] = _mm_xor_si128(p[j], _mm_shuffle_epi8(tables[i][j], si));
      }
      i++;
      v0 = _mm_srli_epi32(v0, 4);
      si = _mm_and_si128(v0, mask1);
      for (j = 0; j < 16; j++) {
        p[j] = _mm_xor_si128(p[j], _mm_shuffle_epi8(tables[i][j], si));
      }
      i++;
    }
    for (i = 0; i < 16; i++) {
      _mm_store_si128((__m128i *) d64, p[i]);
      d64 += 2;
    }
  }
  /* Doing this instead of gf_do_final_region_alignment() because that doesn't hold 128-bit vals */

  gf_w128_multiply_region_from_single(gf, rd.s_top, rd.d_top, val, ((char*)src+bytes)-(char*)rd.s_top, xor);
#endif
}

static
void
gf_w128_split_8_128_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  int i, j, k;
  uint64_t pp;
  gf_internal_t *h;
  uint64_t *s64, *d64, *top;
  gf_region_data rd;
  uint64_t v[2], s;
  struct gf_w128_split_8_128_data *ld;

  /* Check on alignment. Ignore it otherwise. */
  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);

  if (val[0] == 0) {
    if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
    if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  }
    
  h = (gf_internal_t *) gf->scratch;
  ld = (struct gf_w128_split_8_128_data *) h->private;

  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top = (uint64_t *) rd.d_top;

  if (val[0] != ld->last_value[0] || val[1] != ld->last_value[1]) {
    v[0] = val[0];
    v[1] = val[1];
    for (i = 0; i < 16; i++) {
      ld->tables[0][i][0] = 0;
      ld->tables[1][i][0] = 0;
      for (j = 1; j < (1 << 8); j <<= 1) {
        for (k = 0; k < j; k++) {
          ld->tables[0][i][k^j] = (v[0] ^ ld->tables[0][i][k]);
          ld->tables[1][i][k^j] = (v[1] ^ ld->tables[1][i][k]);
        }
        pp = (v[0] & (1ULL << 63));
        v[0] <<= 1;
        if (v[1] & (1ULL << 63)) v[0] ^= 1;
        v[1] <<= 1;
        if (pp) v[1] ^= h->prim_poly;
      }
    }
  }
  ld->last_value[0] = val[0];
  ld->last_value[1] = val[1];

  while (d64 < top) {
    v[0] = (xor) ? d64[0] : 0;
    v[1] = (xor) ? d64[1] : 0;
    s = s64[1];
    i = 0;
    while (s != 0) {
      v[0] ^= ld->tables[0][i][s&0xff];
      v[1] ^= ld->tables[1][i][s&0xff];
      s >>= 8;
      i++;
    }
    s = s64[0];
    i = 8;
    while (s != 0) {
      v[0] ^= ld->tables[0][i][s&0xff];
      v[1] ^= ld->tables[1][i][s&0xff];
      s >>= 8;
      i++;
    }
    d64[0] = v[0];
    d64[1] = v[1];
    s64 += 2;
    d64 += 2;
  }
}

void
gf_w128_bytwo_b_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  uint64_t bmask, pp;
  gf_internal_t *h;
  uint64_t a[2], c[2], b[2], *s64, *d64, *top;
  gf_region_data rd;

  /* We only do this to check on alignment. */
  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);

  if (val[0] == 0) {
    if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
    if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  }
    
  h = (gf_internal_t *) gf->scratch;
  s64 = (uint64_t *) rd.s_start;
  d64 = (uint64_t *) rd.d_start;
  top = (uint64_t *) rd.d_top;
  bmask = (1ULL << 63);

  while (d64 < top) {
    set_zero(c, 0);
    b[0] = s64[0];
    b[1] = s64[1];
    a[0] = val[0];
    a[1] = val[1];

    while (a[0] != 0) {
      if (a[1] & 1) {
        c[0] ^= b[0];
        c[1] ^= b[1];
      }
      a[1] >>= 1;
      if (a[0] & 1) a[1] ^= bmask;
      a[0] >>= 1;
      pp = (b[0] & bmask);
      b[0] <<= 1;
      if (b[1] & bmask) b[0] ^= 1;    
      b[1] <<= 1;
      if (pp) b[1] ^= h->prim_poly;
    }
    while (1) {
      if (a[1] & 1) {
        c[0] ^= b[0];
        c[1] ^= b[1];
      }
      a[1] >>= 1;
      if (a[1] == 0) break;
      pp = (b[0] & bmask);
      b[0] <<= 1;
      if (b[1] & bmask) b[0] ^= 1;    
      b[1] <<= 1;
      if (pp) b[1] ^= h->prim_poly;
    }
    if (xor) {
      d64[0] ^= c[0];
      d64[1] ^= c[1];
    } else {
      d64[0] = c[0];
      d64[1] = c[1];
    }
    s64 += 2;
    d64 += 2;
  }
}

static
void gf_w128_group_m_init(gf_t *gf, gf_val_128_t b128)
{
  int i, j;
  int g_m;
  uint64_t prim_poly, lbit;
  gf_internal_t *scratch;
  gf_group_tables_t *gt;
  uint64_t a128[2];
  scratch = (gf_internal_t *) gf->scratch;
  gt = scratch->private;
  g_m = scratch->arg1;
  prim_poly = scratch->prim_poly;


  set_zero(gt->m_table, 0);
  a_get_b(gt->m_table, 2, b128, 0);
  lbit = 1;
  lbit <<= 63;

  for (i = 2; i < (1 << g_m); i <<= 1) {
    a_get_b(a128, 0, gt->m_table, 2 * (i >> 1));
    two_x(a128);
    a_get_b(gt->m_table, 2 * i, a128, 0);
    if (gt->m_table[2 * (i >> 1)] & lbit) gt->m_table[(2 * i) + 1] ^= prim_poly;
    for (j = 0; j < i; j++) {
      gt->m_table[(2 * i) + (2 * j)] = gt->m_table[(2 * i)] ^ gt->m_table[(2 * j)];
      gt->m_table[(2 * i) + (2 * j) + 1] = gt->m_table[(2 * i) + 1] ^ gt->m_table[(2 * j) + 1];
    }
  }
  return;
}

void
gf_w128_group_multiply(GFP gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  int i;
  /* index_r, index_m, total_m (if g_r > g_m) */
  int i_r, i_m, t_m;
  int mask_m, mask_r;
  int g_m, g_r;
  uint64_t p_i[2], a[2];
  gf_internal_t *scratch;
  gf_group_tables_t *gt;

  scratch = (gf_internal_t *) gf->scratch;
  gt = scratch->private;
  g_m = scratch->arg1;
  g_r = scratch->arg2;

  mask_m = (1 << g_m) - 1;
  mask_r = (1 << g_r) - 1;

  if (b128[0] != gt->m_table[2] || b128[1] != gt->m_table[3]) {
    gf_w128_group_m_init(gf, b128);
  }
  
  p_i[0] = 0;
  p_i[1] = 0;
  a[0] = a128[0];
  a[1] = a128[1];

  t_m = 0;
  i_r = 0;

  /* Top 64 bits */
  for (i = ((GF_FIELD_WIDTH / 2) / g_m) - 1; i >= 0; i--) {
    i_m = (a[0] >> (i * g_m)) & mask_m;
    i_r ^= (p_i[0] >> (64 - g_m)) & mask_r;
    p_i[0] <<= g_m;
    p_i[0] ^= (p_i[1] >> (64-g_m));
    p_i[1] <<= g_m;
    p_i[0] ^= gt->m_table[2 * i_m];
    p_i[1] ^= gt->m_table[(2 * i_m) + 1];
    t_m += g_m;
    if (t_m == g_r) {
      p_i[1] ^= gt->r_table[i_r];
      t_m = 0;
      i_r = 0;
    } else {
      i_r <<= g_m;
    }
  }

  for (i = ((GF_FIELD_WIDTH / 2) / g_m) - 1; i >= 0; i--) {
    i_m = (a[1] >> (i * g_m)) & mask_m;
    i_r ^= (p_i[0] >> (64 - g_m)) & mask_r;
    p_i[0] <<= g_m;
    p_i[0] ^= (p_i[1] >> (64-g_m));
    p_i[1] <<= g_m;
    p_i[0] ^= gt->m_table[2 * i_m];
    p_i[1] ^= gt->m_table[(2 * i_m) + 1];
    t_m += g_m;
    if (t_m == g_r) {
      p_i[1] ^= gt->r_table[i_r];
      t_m = 0;
      i_r = 0;
    } else {
      i_r <<= g_m;
    }
  }
  c128[0] = p_i[0];
  c128[1] = p_i[1];
}

static
void
gf_w128_group_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  int i;
  int i_r, i_m, t_m;
  int mask_m, mask_r;
  int g_m, g_r;
  uint64_t p_i[2], a[2];
  gf_internal_t *scratch;
  gf_group_tables_t *gt;
  gf_region_data rd;
  uint64_t *a128, *c128, *top;

  /* We only do this to check on alignment. */
  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);
      
  if (val[0] == 0) {
    if (val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }
    if (val[1] == 1) { gf_multby_one(src, dest, bytes, xor); return; }
  }
    
  scratch = (gf_internal_t *) gf->scratch;
  gt = scratch->private;
  g_m = scratch->arg1;
  g_r = scratch->arg2;

  mask_m = (1 << g_m) - 1;
  mask_r = (1 << g_r) - 1;

  if (val[0] != gt->m_table[2] || val[1] != gt->m_table[3]) {
    gf_w128_group_m_init(gf, val);
  }

  a128 = (uint64_t *) src;
  c128 = (uint64_t *) dest;
  top = (uint64_t *) rd.d_top;

  while (c128 < top) {
    p_i[0] = 0;
    p_i[1] = 0;
    a[0] = a128[0];
    a[1] = a128[1];
  
    t_m = 0;
    i_r = 0;
  
    /* Top 64 bits */
    for (i = ((GF_FIELD_WIDTH / 2) / g_m) - 1; i >= 0; i--) {
      i_m = (a[0] >> (i * g_m)) & mask_m;
      i_r ^= (p_i[0] >> (64 - g_m)) & mask_r;
      p_i[0] <<= g_m;
      p_i[0] ^= (p_i[1] >> (64-g_m));
      p_i[1] <<= g_m;
      
      p_i[0] ^= gt->m_table[2 * i_m];
      p_i[1] ^= gt->m_table[(2 * i_m) + 1];
      t_m += g_m;
      if (t_m == g_r) {
        p_i[1] ^= gt->r_table[i_r];
        t_m = 0;
        i_r = 0;
      } else {
        i_r <<= g_m;
      }
    }
    for (i = ((GF_FIELD_WIDTH / 2) / g_m) - 1; i >= 0; i--) {
      i_m = (a[1] >> (i * g_m)) & mask_m;
      i_r ^= (p_i[0] >> (64 - g_m)) & mask_r;
      p_i[0] <<= g_m;
      p_i[0] ^= (p_i[1] >> (64-g_m));
      p_i[1] <<= g_m;
      p_i[0] ^= gt->m_table[2 * i_m];
      p_i[1] ^= gt->m_table[(2 * i_m) + 1];
      t_m += g_m;
      if (t_m == g_r) {
        p_i[1] ^= gt->r_table[i_r];
        t_m = 0;
        i_r = 0;
      } else {
        i_r <<= g_m;
      }
    }
  
    if (xor) {
      c128[0] ^= p_i[0];
      c128[1] ^= p_i[1];
    } else {
      c128[0] = p_i[0];
      c128[1] = p_i[1];
    }
    a128 += 2;
    c128 += 2;
  }
}

/* a^-1 -> b */
  void
gf_w128_euclid(GFP gf, gf_val_128_t a128, gf_val_128_t b128)
{
  uint64_t e_i[2], e_im1[2], e_ip1[2];
  uint64_t d_i, d_im1, d_ip1;
  uint64_t y_i[2], y_im1[2], y_ip1[2];
  uint64_t c_i[2];
  uint64_t *b;
  uint64_t one = 1;

  /* This needs to return some sort of error (in b128?) */
  if (a128[0] == 0 && a128[1] == 0) return;

  b = (uint64_t *) b128;

  e_im1[0] = 0;
  e_im1[1] = ((gf_internal_t *) (gf->scratch))->prim_poly;
  e_i[0] = a128[0];
  e_i[1] = a128[1];
  d_im1 = 128;

  //Allen: I think d_i starts at 63 here, and checks each bit of a, starting at MSB, looking for the first nonzero bit
  //so d_i should be 0 if this half of a is all 0s, otherwise it should be the position from right of the first-from-left zero bit of this half of a.
  //BUT if d_i is 0 at end we won't know yet if the rightmost bit of this half is 1 or not

  for (d_i = (d_im1-1) % 64; ((one << d_i) & e_i[0]) == 0 && d_i > 0; d_i--) ;

  //Allen: this is testing just the first half of the stop condition above, so if it holds we know we did not find a nonzero bit yet

  if (!((one << d_i) & e_i[0])) {

    //Allen: this is doing the same thing on the other half of a. In other words, we're still searching for a nonzero bit of a.
    // but not bothering to test if d_i hits zero, which is fine because we've already tested for a=0.

    for (d_i = (d_im1-1) % 64; ((one << d_i) & e_i[1]) == 0; d_i--) ;

  } else {

    //Allen: if a 1 was found in more-significant half of a, make d_i the ACTUAL index of the first nonzero bit in the entire a.

    d_i += 64;
  }
  y_i[0] = 0;
  y_i[1] = 1;
  y_im1[0] = 0;
  y_im1[1] = 0;

  while (!(e_i[0] == 0 && e_i[1] == 1)) {

    e_ip1[0] = e_im1[0];
    e_ip1[1] = e_im1[1];
    d_ip1 = d_im1;
    c_i[0] = 0;
    c_i[1] = 0;

    while (d_ip1 >= d_i) {
      if ((d_ip1 - d_i) >= 64) {
        c_i[0] ^= (one << ((d_ip1 - d_i) - 64));
        e_ip1[0] ^= (e_i[1] << ((d_ip1 - d_i) - 64));
      } else {
        c_i[1] ^= (one << (d_ip1 - d_i));
        e_ip1[0] ^= (e_i[0] << (d_ip1 - d_i));
        if (d_ip1 - d_i > 0) e_ip1[0] ^= (e_i[1] >> (64 - (d_ip1 - d_i)));
        e_ip1[1] ^= (e_i[1] << (d_ip1 - d_i));
      }
      d_ip1--;
      if (e_ip1[0] == 0 && e_ip1[1] == 0) { b[0] = 0; b[1] = 0; return; }
      while (d_ip1 >= 64 && (e_ip1[0] & (one << (d_ip1 - 64))) == 0) d_ip1--;
      while (d_ip1 <  64 && (e_ip1[1] & (one << d_ip1)) == 0) d_ip1--;
    }
    gf->multiply.w128(gf, c_i, y_i, y_ip1);
    y_ip1[0] ^= y_im1[0];
    y_ip1[1] ^= y_im1[1];

    y_im1[0] = y_i[0];
    y_im1[1] = y_i[1];

    y_i[0] = y_ip1[0];
    y_i[1] = y_ip1[1];

    e_im1[0] = e_i[0];
    e_im1[1] = e_i[1];
    d_im1 = d_i;
    e_i[0] = e_ip1[0];
    e_i[1] = e_ip1[1];
    d_i = d_ip1;
  }

  b[0] = y_i[0];
  b[1] = y_i[1];
  return;
}

  void
gf_w128_divide_from_inverse(GFP gf, gf_val_128_t a128, gf_val_128_t b128, gf_val_128_t c128)
{
  uint64_t d[2];
  gf->inverse.w128(gf, b128, d);
  gf->multiply.w128(gf, a128, d, c128);
  return;
}

  void
gf_w128_inverse_from_divide(GFP gf, gf_val_128_t a128, gf_val_128_t b128)
{
  uint64_t one128[2];
  one128[0] = 0;
  one128[1] = 1;
  gf->divide.w128(gf, one128, a128, b128);
  return;
}


static
  void
gf_w128_composite_inverse(gf_t *gf, gf_val_128_t a, gf_val_128_t inv)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint64_t a0 = a[1];
  uint64_t a1 = a[0];
  uint64_t c0, c1, d, tmp;
  uint64_t a0inv, a1inv;

  if (a0 == 0) {
    a1inv = base_gf->inverse.w64(base_gf, a1);
    c0 = base_gf->multiply.w64(base_gf, a1inv, h->prim_poly);
    c1 = a1inv;
  } else if (a1 == 0) {
    c0 = base_gf->inverse.w64(base_gf, a0);
    c1 = 0;
  } else {
    a1inv = base_gf->inverse.w64(base_gf, a1);
    a0inv = base_gf->inverse.w64(base_gf, a0);

    d = base_gf->multiply.w64(base_gf, a1, a0inv);

    tmp = (base_gf->multiply.w64(base_gf, a1, a0inv) ^ base_gf->multiply.w64(base_gf, a0, a1inv) ^ h->prim_poly);
    tmp = base_gf->inverse.w64(base_gf, tmp);

    d = base_gf->multiply.w64(base_gf, d, tmp);

    c0 = base_gf->multiply.w64(base_gf, (d^1), a0inv);
    c1 = base_gf->multiply.w64(base_gf, d, a1inv);
  }
  inv[0] = c1;
  inv[1] = c0;
}

static
  void
gf_w128_composite_multiply(gf_t *gf, gf_val_128_t a, gf_val_128_t b, gf_val_128_t rv)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint64_t b0 = b[1];
  uint64_t b1 = b[0];
  uint64_t a0 = a[1];
  uint64_t a1 = a[0];
  uint64_t a1b1;

  a1b1 = base_gf->multiply.w64(base_gf, a1, b1);

  rv[1] = (base_gf->multiply.w64(base_gf, a0, b0) ^ a1b1);
  rv[0] = base_gf->multiply.w64(base_gf, a1, b0) ^ 
    base_gf->multiply.w64(base_gf, a0, b1) ^ 
    base_gf->multiply.w64(base_gf, a1b1, h->prim_poly);
}

static
  void
gf_w128_composite_multiply_region(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  gf_t *base_gf = h->base_gf;
  uint64_t b0 = val[1];
  uint64_t b1 = val[0];
  uint64_t *s64, *d64;
  uint64_t *top;
  uint64_t a0, a1, a1b1;
  gf_region_data rd;

  if (val[0] == 0 && val[1] == 0) { gf_multby_zero(dest, bytes, xor); return; }

  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 8);

  s64 = rd.s_start;
  d64 = rd.d_start;
  top = rd.d_top;

  if (xor) {
    while (d64 < top) {
      a1 = s64[0];
      a0 = s64[1];
      a1b1 = base_gf->multiply.w64(base_gf, a1, b1);

      d64[1] ^= (base_gf->multiply.w64(base_gf, a0, b0) ^ a1b1);
      d64[0] ^= (base_gf->multiply.w64(base_gf, a1, b0) ^ 
          base_gf->multiply.w64(base_gf, a0, b1) ^ 
          base_gf->multiply.w64(base_gf, a1b1, h->prim_poly));
      s64 += 2;
      d64 += 2;
    }
  } else {
    while (d64 < top) {
      a1 = s64[0];
      a0 = s64[1];
      a1b1 = base_gf->multiply.w64(base_gf, a1, b1);

      d64[1] = (base_gf->multiply.w64(base_gf, a0, b0) ^ a1b1);
      d64[0] = (base_gf->multiply.w64(base_gf, a1, b0) ^ 
          base_gf->multiply.w64(base_gf, a0, b1) ^ 
          base_gf->multiply.w64(base_gf, a1b1, h->prim_poly));
      s64 += 2;
      d64 += 2;
    }
  }
}

static
void
gf_w128_composite_multiply_region_alt(gf_t *gf, void *src, void *dest, gf_val_128_t val, int bytes, int 
    xor)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;  gf_t *base_gf = h->base_gf;
  gf_val_64_t val0 = val[1];
  gf_val_64_t val1 = val[0];
  uint8_t *slow, *shigh;
  uint8_t *dlow, *dhigh, *top;
  int sub_reg_size;
  gf_region_data rd;

  gf_set_region_data(&rd, gf, src, dest, bytes, 0, xor, 64);
  gf_w128_multiply_region_from_single(gf, src, dest, val, ((char*)rd.s_start-(char*)src), xor);

  slow = (uint8_t *) rd.s_start;
  dlow = (uint8_t *) rd.d_start;
  top = (uint8_t*) rd.d_top;
  sub_reg_size = (top - dlow)/2;
  shigh = slow + sub_reg_size;
  dhigh = dlow + sub_reg_size;

  base_gf->multiply_region.w64(base_gf, slow, dlow, val0, sub_reg_size, xor);
  base_gf->multiply_region.w64(base_gf, shigh, dlow, val1, sub_reg_size, 1);
  base_gf->multiply_region.w64(base_gf, slow, dhigh, val1, sub_reg_size, xor);
  base_gf->multiply_region.w64(base_gf, shigh, dhigh, val0, sub_reg_size, 1);
  base_gf->multiply_region.w64(base_gf, shigh, dhigh, base_gf->multiply.w64(base_gf, h->prim_poly, val1
        ), sub_reg_size, 1);

  gf_w128_multiply_region_from_single(gf, rd.s_top, rd.d_top, val, ((char*)src+bytes)-(char*)rd.s_top, xor);
}


  static
int gf_w128_composite_init(gf_t *gf)
{
  gf_internal_t *h = (gf_internal_t *) gf->scratch;

  if (h->region_type & GF_REGION_ALTMAP) {
    gf->multiply_region.w128 = gf_w128_composite_multiply_region_alt;   
  } else {
    gf->multiply_region.w128 = gf_w128_composite_multiply_region;
  }

  gf->multiply.w128 = gf_w128_composite_multiply;
  gf->divide.w128 = gf_w128_divide_from_inverse;
  gf->inverse.w128 = gf_w128_composite_inverse;

  return 1;
}

static
int gf_w128_cfm_init(gf_t *gf)
{
  gf_internal_t * h = gf->scratch;
  if (h->sse & GF_SSE4_PCLMUL) {
    return 0;
  } else {
    gf->inverse.w128 = gf_w128_euclid;
    gf->multiply.w128 = gf_w128_clm_multiply;
    gf->multiply_region.w128 = gf_w128_clm_multiply_region_from_single;
    return 1;
  }
}

static
int gf_w128_shift_init(gf_t *gf)
{
  gf->multiply.w128 = gf_w128_shift_multiply;
  gf->inverse.w128 = gf_w128_euclid;
  gf->multiply_region.w128 = gf_w128_multiply_region_from_single;
  return 1;
}

  static
int gf_w128_bytwo_init(gf_t *gf)
{
  gf_internal_t *h;
  h = (gf_internal_t *) gf->scratch;

  if (h->mult_type == GF_MULT_BYTWO_p) {
    gf->multiply.w128 = gf_w128_bytwo_p_multiply;
    /*gf->multiply.w128 = gf_w128_sse_bytwo_p_multiply;*/
    /* John: the sse function is slower.*/
  } else {
    gf->multiply.w128 = gf_w128_bytwo_b_multiply;
    /*gf->multiply.w128 = gf_w128_sse_bytwo_b_multiply;
Ben: This sse function is also slower. */
  }
  gf->inverse.w128 = gf_w128_euclid;
  gf->multiply_region.w128 = gf_w128_bytwo_b_multiply_region;
  return 1;
}

/*
 * Because the prim poly is only 8 bits and we are limiting g_r to 16, I do not need the high 64
 * bits in all of these numbers.
 */
  static
void gf_w128_group_r_init(gf_t *gf)
{
  int i, j;
  int g_r;
  uint64_t pp;
  gf_internal_t *scratch;
  gf_group_tables_t *gt;
  scratch = (gf_internal_t *) gf->scratch;
  gt = scratch->private;
  g_r = scratch->arg2;
  pp = scratch->prim_poly;

  gt->r_table[0] = 0;
  for (i = 1; i < (1 << g_r); i++) {
    gt->r_table[i] = 0;
    for (j = 0; j < g_r; j++) {
      if (i & (1 << j)) {
        gt->r_table[i] ^= (pp << j);
      }
    }
  }
  return;
}

  static 
int gf_w128_split_init(gf_t *gf)
{
  struct gf_w128_split_4_128_data *sd4;
  struct gf_w128_split_8_128_data *sd8;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;

  gf->multiply.w128 = gf_w128_bytwo_p_multiply;
  if((h->sse & GF_SSE4_PCLMUL) && !(h->region_type & GF_REGION_NOSSE)){
    gf->multiply.w128 = gf_w128_clm_multiply;
  }

  gf->inverse.w128 = gf_w128_euclid;

  if ((h->arg1 != 4 && h->arg2 != 4) || h->mult_type == GF_MULT_DEFAULT) {
    sd8 = (struct gf_w128_split_8_128_data *) h->private;
    sd8->last_value[0] = 0;
    sd8->last_value[1] = 0;
    gf->multiply_region.w128 = gf_w128_split_8_128_multiply_region;
  } else {
    sd4 = (struct gf_w128_split_4_128_data *) h->private;
    sd4->last_value[0] = 0;
    sd4->last_value[1] = 0;
    if((h->region_type & GF_REGION_ALTMAP))
    {
      #ifdef INTEL_SSE4
        if(!(h->region_type & GF_REGION_NOSSE))
          gf->multiply_region.w128 = gf_w128_split_4_128_sse_altmap_multiply_region;
        else
          return 0;
      #else
        return 0;
      #endif
    }
    else {
      if(h->sse & GF_SSE4) {
        if(!(h->region_type & GF_REGION_NOSSE))
          gf->multiply_region.w128 = gf_w128_split_4_128_sse_multiply_region;
        else
          gf->multiply_region.w128 = gf_w128_split_4_128_multiply_region;
      } else {
        gf->multiply_region.w128 = gf_w128_split_4_128_multiply_region;
      }
    }
  }
  return 1;
}


static
int gf_w128_group_init(gf_t *gf)
{
  gf_internal_t *scratch;
  gf_group_tables_t *gt;
  int g_r, size_r;

  scratch = (gf_internal_t *) gf->scratch;
  gt = scratch->private;
  g_r = scratch->arg2;
  size_r = (1 << g_r);

  gt->r_table = scratch->private + (2 * sizeof(uint64_t *));
  gt->m_table = gt->r_table + size_r;
  gt->m_table[2] = 0;
  gt->m_table[3] = 0;

  gf->multiply.w128 = gf_w128_group_multiply;
  gf->inverse.w128 = gf_w128_euclid;
  gf->multiply_region.w128 = gf_w128_group_multiply_region;

  gf_w128_group_r_init(gf);

  return 1;
}

void gf_w128_extract_word(gf_t *gf, void *start, int bytes, int index, gf_val_128_t rv)
{
  gf_val_128_t s;

  s = (gf_val_128_t) start;
  s += (index * 2); 
  memcpy(rv, s, 16);
}

static void gf_w128_split_extract_word(gf_t *gf, void *start, int bytes, int index, gf_val_128_t rv)
{
  int i, blocks;
  uint64_t *r64, tmp;
  uint8_t *r8;
  gf_region_data rd;

  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 256);
  r64 = (uint64_t *) start;
  if ((r64 + index*2 < (uint64_t *) rd.d_start) ||
      (r64 + index*2 >= (uint64_t *) rd.d_top)) {
    memcpy(rv, r64+(index*2), 16);
    return;
  }

  index -= (((uint64_t *) rd.d_start) - r64)/2;
  r64 = (uint64_t *) rd.d_start;

  blocks = index/16;
  r64 += (blocks*32);
  index %= 16;
  r8 = (uint8_t *) r64;
  r8 += index;
  rv[0] = 0;
  rv[1] = 0;

  for (i = 0; i < 8; i++) {
    tmp = *r8;
    rv[1] |= (tmp << (i*8));
    r8 += 16;
  }

  for (i = 0; i < 8; i++) {
    tmp = *r8;
    rv[0] |= (tmp << (i*8));
    r8 += 16;
  }
  return;
}

  static
void gf_w128_composite_extract_word(gf_t *gf, void *start, int bytes, int index, gf_val_128_t rv)
{
  int sub_size;
  gf_internal_t *h;
  uint8_t *r8, *top;
  uint64_t *r64;
  gf_region_data rd;

  h = (gf_internal_t *) gf->scratch;
  gf_set_region_data(&rd, gf, start, start, bytes, 0, 0, 64);
  r64 = (uint64_t *) start;
  if ((r64 + index*2 < (uint64_t *) rd.d_start) ||
      (r64 + index*2 >= (uint64_t *) rd.d_top)) {
    memcpy(rv, r64+(index*2), 16);
    return;
  }
  index -= (((uint64_t *) rd.d_start) - r64)/2;
  r8 = (uint8_t *) rd.d_start;
  top = (uint8_t *) rd.d_top;
  sub_size = (top-r8)/2;

  rv[1] = h->base_gf->extract_word.w64(h->base_gf, r8, sub_size, index);
  rv[0] = h->base_gf->extract_word.w64(h->base_gf, r8+sub_size, sub_size, index);
  
  return;
}

int gf_w128_scratch_size(int mult_type, int region_type, int divide_type, int arg1, int arg2)
{
  int size_m, size_r;
  if (divide_type==GF_DIVIDE_MATRIX) return 0;

  switch(mult_type)
  {
    case GF_MULT_CARRY_FREE:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_SHIFT:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_DEFAULT: 
    case GF_MULT_SPLIT_TABLE:
      if ((arg1 == 4 && arg2 == 128) || (arg1 == 128 && arg2 == 4)) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w128_split_4_128_data) + 64;
      } else if ((arg1 == 8 && arg2 == 128) || (arg1 == 128 && arg2 == 8) || mult_type == GF_MULT_DEFAULT) {
        return sizeof(gf_internal_t) + sizeof(struct gf_w128_split_8_128_data) + 64;
      }
      return 0;
      break;
    case GF_MULT_GROUP:
      /* JSP We've already error checked the arguments. */
      size_m = (1 << arg1) * 2 * sizeof(uint64_t);
      size_r = (1 << arg2) * 2 * sizeof(uint64_t);
      /* 
       * two pointers prepend the table data for structure
       * because the tables are of dynamic size
       */
      return sizeof(gf_internal_t) + size_m + size_r + 4 * sizeof(uint64_t *);
      break;
    case GF_MULT_COMPOSITE:
      if (arg1 == 2) {
        return sizeof(gf_internal_t) + 4;
      } else {
        return 0;
      }
      break;

    default:
      return 0;
   }
}

int gf_w128_init(gf_t *gf)
{
  gf_internal_t *h;
  int no_default_flag = 0;

  h = (gf_internal_t *) gf->scratch;
  
  /* Allen: set default primitive polynomial / irreducible polynomial if needed */

  if (h->prim_poly == 0) {
    if (h->mult_type == GF_MULT_COMPOSITE) {
      h->prim_poly = gf_composite_get_default_poly(h->base_gf);
      if (h->prim_poly == 0) return 0; /* This shouldn't happen */
    } else {
      h->prim_poly = 0x87; /* Omitting the leftmost 1 as in w=32 */
    }
    if (no_default_flag == 1) {
      fprintf(stderr,"Code contains no default irreducible polynomial for given base field\n");
      return 0;
    }
  }

  gf->multiply.w128 = NULL;
  gf->divide.w128 = NULL;
  gf->inverse.w128 = NULL;
  gf->multiply_region.w128 = NULL;
  switch(h->mult_type) {
    case GF_MULT_BYTWO_p:
    case GF_MULT_BYTWO_b:      if (gf_w128_bytwo_init(gf) == 0) return 0; break;
    case GF_MULT_CARRY_FREE:   if (gf_w128_cfm_init(gf) == 0) return 0; break;
    case GF_MULT_SHIFT:        if (gf_w128_shift_init(gf) == 0) return 0; break;
    case GF_MULT_GROUP:        if (gf_w128_group_init(gf) == 0) return 0; break;
    case GF_MULT_DEFAULT: 
    case GF_MULT_SPLIT_TABLE:  if (gf_w128_split_init(gf) == 0) return 0; break;
    case GF_MULT_COMPOSITE:    if (gf_w128_composite_init(gf) == 0) return 0; break;
    default: return 0;
  }

  /* Ben: Used to be h->region_type == GF_REGION_ALTMAP, but failed since there
     are multiple flags in h->region_type */
  if (h->mult_type == GF_MULT_SPLIT_TABLE && (h->region_type & GF_REGION_ALTMAP)) {
    gf->extract_word.w128 = gf_w128_split_extract_word;
  } else if (h->mult_type == GF_MULT_COMPOSITE && h->region_type == GF_REGION_ALTMAP) {
    gf->extract_word.w128 = gf_w128_composite_extract_word;
  } else {
    gf->extract_word.w128 = gf_w128_extract_word;
  }

  if (h->divide_type == GF_DIVIDE_EUCLID) {
    gf->divide.w128 = gf_w128_divide_from_inverse;
  } 

  if (gf->inverse.w128 != NULL && gf->divide.w128 == NULL) {
    gf->divide.w128 = gf_w128_divide_from_inverse;
  }
  if (gf->inverse.w128 == NULL && gf->divide.w128 != NULL) {
    gf->inverse.w128 = gf_w128_inverse_from_divide;
  }
  return 1;
}
