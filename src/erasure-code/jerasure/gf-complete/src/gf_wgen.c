/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_wgen.c
 *
 * Routines for Galois fields for general w < 32.  For specific w, 
   like 4, 8, 16, 32, 64 and 128, see the other files.
 */

#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>

struct gf_wgen_table_w8_data {
  uint8_t *mult;
  uint8_t *div;
  uint8_t base;
};

struct gf_wgen_table_w16_data {
  uint16_t *mult;
  uint16_t *div;
  uint16_t base;
};

struct gf_wgen_log_w8_data {
  uint8_t *log;
  uint8_t *anti;
  uint8_t *danti;
  uint8_t base;
};

struct gf_wgen_log_w16_data {
  uint16_t *log;
  uint16_t *anti;
  uint16_t *danti;
  uint16_t base;
};

struct gf_wgen_log_w32_data {
  uint32_t *log;
  uint32_t *anti;
  uint32_t *danti;
  uint32_t base;
};

struct gf_wgen_group_data {
    uint32_t *reduce;
    uint32_t *shift;
    uint32_t mask;
    uint64_t rmask;
    int tshift;
    uint32_t memory;
};

static
inline
gf_val_32_t gf_wgen_inverse_from_divide (gf_t *gf, gf_val_32_t a)
{
  return gf->divide.w32(gf, 1, a);
}

static
inline
gf_val_32_t gf_wgen_divide_from_inverse (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  b = gf->inverse.w32(gf, b);
  return gf->multiply.w32(gf, a, b);
}

static
inline
gf_val_32_t gf_wgen_euclid (gf_t *gf, gf_val_32_t b)
{
  
  gf_val_32_t e_i, e_im1, e_ip1;
  gf_val_32_t d_i, d_im1, d_ip1;
  gf_val_32_t y_i, y_im1, y_ip1;
  gf_val_32_t c_i;

  if (b == 0) return -1;
  e_im1 = ((gf_internal_t *) (gf->scratch))->prim_poly;
  e_i = b;
  d_im1 = ((gf_internal_t *) (gf->scratch))->w;
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

gf_val_32_t gf_wgen_extract_word(gf_t *gf, void *start, int bytes, int index)
{
  uint8_t *ptr;
  uint32_t rv;
  int rs;
  int byte, bit, i;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  rs = bytes / h->w;
  byte = index/8;
  bit = index%8;

  ptr = (uint8_t *) start;
  ptr += bytes;
  ptr -= rs;
  ptr += byte;

  rv = 0;
  for (i = 0; i < h->w; i++) {
    rv <<= 1;
    if ((*ptr) & (1 << bit)) rv |= 1;
    ptr -= rs;
  }
  
  return rv;
}

static
inline
gf_val_32_t gf_wgen_matrix (gf_t *gf, gf_val_32_t b)
{
  return gf_bitmatrix_inverse(b, ((gf_internal_t *) (gf->scratch))->w, 
              ((gf_internal_t *) (gf->scratch))->prim_poly);
}

static
inline
uint32_t
gf_wgen_shift_multiply (gf_t *gf, uint32_t a32, uint32_t b32)
{
  uint64_t product, i, pp, a, b, one;
  gf_internal_t *h;
 
  a = a32;
  b = b32;
  h = (gf_internal_t *) gf->scratch;
  one = 1;
  pp = h->prim_poly | (one << h->w);

  product = 0;

  for (i = 0; i < h->w; i++) {
    if (a & (one << i)) product ^= (b << i);
  }
  for (i = h->w*2-1; i >= h->w; i--) {
    if (product & (one << i)) product ^= (pp << (i-h->w));
  }
  return product;
}

static 
int gf_wgen_shift_init(gf_t *gf)
{
  gf->multiply.w32 = gf_wgen_shift_multiply;
  gf->inverse.w32 = gf_wgen_euclid;
  return 1;
}

static
gf_val_32_t
gf_wgen_bytwo_b_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, bmask;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  prod = 0;
  bmask = (1 << (h->w-1));

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
int gf_wgen_bytwo_b_init(gf_t *gf)
{
  gf->multiply.w32 = gf_wgen_bytwo_b_multiply;
  gf->inverse.w32 = gf_wgen_euclid;
  return 1;
}

static
inline
gf_val_32_t
gf_wgen_bytwo_p_multiply (gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  uint32_t prod, pp, pmask, amask;
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  pp = h->prim_poly;

  prod = 0;
  pmask = (1 << ((h->w)-1)); /*Ben: Had an operator precedence warning here*/
  amask = pmask;

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
int gf_wgen_bytwo_p_init(gf_t *gf)
{
  gf->multiply.w32 = gf_wgen_bytwo_p_multiply;
  gf->inverse.w32 = gf_wgen_euclid;
  return 1;
}

static
void
gf_wgen_group_set_shift_tables(uint32_t *shift, uint32_t val, gf_internal_t *h)
{
  int i;
  uint32_t j;
  int g_s;

  if (h->mult_type == GF_MULT_DEFAULT) {
    g_s = 2;
  } else {
    g_s = h->arg1;
  }

  shift[0] = 0;

  for (i = 1; i < (1 << g_s); i <<= 1) {
    for (j = 0; j < i; j++) shift[i|j] = shift[j]^val;
    if (val & (1 << (h->w-1))) {
      val <<= 1;
      val ^= h->prim_poly;
    } else {
      val <<= 1;
    }
  }
}

static
inline
gf_val_32_t
gf_wgen_group_s_equals_r_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int leftover, rs;
  uint32_t p, l, ind, a32;
  int bits_left;
  int g_s;
  int w;

  struct gf_wgen_group_data *gd;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  g_s = h->arg1;
  w = h->w;

  gd = (struct gf_wgen_group_data *) h->private;
  gf_wgen_group_set_shift_tables(gd->shift, b, h);

  leftover = w % g_s;
  if (leftover == 0) leftover = g_s;

  rs = w - leftover;
  a32 = a;
  ind = a32 >> rs;
  a32 <<= leftover;
  a32 &= gd->mask;
  p = gd->shift[ind];

  bits_left = rs;
  rs = w - g_s;

  while (bits_left > 0) {
    bits_left -= g_s;
    ind = a32 >> rs;
    a32 <<= g_s;
    a32 &= gd->mask;
    l = p >> rs;
    p = (gd->shift[ind] ^ gd->reduce[l] ^ (p << g_s)) & gd->mask;
  }
  return p;
}

char *bits(uint32_t v)
{
  char *rv;
  int i, j;

  rv = malloc(30);
  j = 0;
  for (i = 27; i >= 0; i--) {
    rv[j] = '0' + ((v & (1 << i)) ? 1 : 0);
    j++;
  }
  rv[j] = '\0';
  return rv;
}
char *bits_56(uint64_t v)
{
  char *rv;
  int i, j;
  uint64_t one;

  one = 1;

  rv = malloc(60);
  j = 0;
  for (i = 55; i >= 0; i--) {
    rv[j] = '0' + ((v & (one << i)) ? 1 : 0);
    j++;
  }
  rv[j] = '\0';
  return rv;
}

static
inline
gf_val_32_t
gf_wgen_group_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  int i;
  int leftover;
  uint64_t p, l, r;
  uint32_t a32, ind;
  int g_s, g_r;
  struct gf_wgen_group_data *gd;
  int w;

  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  if (h->mult_type == GF_MULT_DEFAULT) {
    g_s = 2;
    g_r = 8;
  } else {
    g_s = h->arg1;
    g_r = h->arg2;
  }
  w = h->w;
  gd = (struct gf_wgen_group_data *) h->private;
  gf_wgen_group_set_shift_tables(gd->shift, b, h);

  leftover = w % g_s;
  if (leftover == 0) leftover = g_s;

  a32 = a;
  ind = a32 >> (w - leftover);
  p = gd->shift[ind];
  p <<= g_s;
  a32 <<= leftover;
  a32 &= gd->mask;

  i = (w - leftover);
  while (i > g_s) {
    ind = a32 >> (w-g_s);
    p ^= gd->shift[ind];
    a32 <<= g_s;
    a32 &= gd->mask;
    p <<= g_s;
    i -= g_s;
  }

  ind = a32 >> (h->w-g_s);
  p ^= gd->shift[ind];

  for (i = gd->tshift ; i >= 0; i -= g_r) {
    l = p & (gd->rmask << i);
    r = gd->reduce[l >> (i+w)];
    r <<= (i);
    p ^= r;
  }
  return p & gd->mask;
}

static
int gf_wgen_group_init(gf_t *gf)
{
  uint32_t i, j, p, index;
  struct gf_wgen_group_data *gd;
  gf_internal_t *h = (gf_internal_t *) gf->scratch;
  int g_s, g_r;

  if (h->mult_type == GF_MULT_DEFAULT) {
    g_s = 2;
    g_r = 8;
  } else {
    g_s = h->arg1;
    g_r = h->arg2;
  }
  gd = (struct gf_wgen_group_data *) h->private;
  gd->shift = &(gd->memory);
  gd->reduce = gd->shift + (1 << g_s);
  gd->mask = (h->w != 31) ? ((1 << h->w)-1) : 0x7fffffff;

  gd->rmask = (1 << g_r) - 1;
  gd->rmask <<= h->w;

  gd->tshift = h->w % g_s;
  if (gd->tshift == 0) gd->tshift = g_s;
  gd->tshift = (h->w - gd->tshift);
  gd->tshift = ((gd->tshift-1)/g_r) * g_r;

  gd->reduce[0] = 0;
  for (i = 0; i < (1 << g_r); i++) {
    p = 0;
    index = 0;
    for (j = 0; j < g_r; j++) {
      if (i & (1 << j)) {
        p ^= (h->prim_poly << j);
        index ^= (h->prim_poly >> (h->w-j));
      }
    }
    gd->reduce[index] = (p & gd->mask);
  }

  if (g_s == g_r) {
    gf->multiply.w32 = gf_wgen_group_s_equals_r_multiply;
  } else {
    gf->multiply.w32 = gf_wgen_group_multiply; 
  }
  gf->divide.w32 = NULL;
  gf->divide.w32 = NULL;
  return 1;
}


static
gf_val_32_t
gf_wgen_table_8_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_table_w8_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_table_w8_data *) h->private;

  return (std->mult[(a<<h->w)+b]);
}

static
gf_val_32_t
gf_wgen_table_8_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_table_w8_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_table_w8_data *) h->private;

  return (std->div[(a<<h->w)+b]);
}

static 
int gf_wgen_table_8_init(gf_t *gf)
{
  gf_internal_t *h;
  int w;
  struct gf_wgen_table_w8_data *std;
  uint32_t a, b, p;
  
  h = (gf_internal_t *) gf->scratch;
  w = h->w;
  std = (struct gf_wgen_table_w8_data *) h->private;
  
  std->mult = &(std->base);
  std->div = std->mult + ((1<<h->w)*(1<<h->w));
  
  for (a = 0; a < (1 << w); a++) {
    std->mult[a] = 0;
    std->mult[a<<w] = 0;
    std->div[a] = 0;
    std->div[a<<w] = 0;
  }
    
  for (a = 1; a < (1 << w); a++) {
    for (b = 1; b < (1 << w); b++) {
      p = gf_wgen_shift_multiply(gf, a, b);
      std->mult[(a<<w)|b] = p;
      std->div[(p<<w)|a] = b;
    }
  }

  gf->multiply.w32 = gf_wgen_table_8_multiply;
  gf->divide.w32 = gf_wgen_table_8_divide;
  return 1;
}

static
gf_val_32_t
gf_wgen_table_16_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_table_w16_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_table_w16_data *) h->private;

  return (std->mult[(a<<h->w)+b]);
}

static
gf_val_32_t
gf_wgen_table_16_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_table_w16_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_table_w16_data *) h->private;

  return (std->div[(a<<h->w)+b]);
}

static 
int gf_wgen_table_16_init(gf_t *gf)
{
  gf_internal_t *h;
  int w;
  struct gf_wgen_table_w16_data *std;
  uint32_t a, b, p;
  
  h = (gf_internal_t *) gf->scratch;
  w = h->w;
  std = (struct gf_wgen_table_w16_data *) h->private;
  
  std->mult = &(std->base);
  std->div = std->mult + ((1<<h->w)*(1<<h->w));
  
  for (a = 0; a < (1 << w); a++) {
    std->mult[a] = 0;
    std->mult[a<<w] = 0;
    std->div[a] = 0;
    std->div[a<<w] = 0;
  }
  
  for (a = 1; a < (1 << w); a++) {
    for (b = 1; b < (1 << w); b++) {
      p = gf_wgen_shift_multiply(gf, a, b);
      std->mult[(a<<w)|b] = p;
      std->div[(p<<w)|a] = b;
    }
  }

  gf->multiply.w32 = gf_wgen_table_16_multiply;
  gf->divide.w32 = gf_wgen_table_16_divide;
  return 1;
}

static 
int gf_wgen_table_init(gf_t *gf)
{
  gf_internal_t *h;
  
  h = (gf_internal_t *) gf->scratch;
  if (h->w <= 8) return gf_wgen_table_8_init(gf);
  if (h->w <= 14) return gf_wgen_table_16_init(gf);

  /* Returning zero to make the compiler happy, but this won't get 
     executed, because it is tested in _scratch_space. */

  return 0;
}

static
gf_val_32_t
gf_wgen_log_8_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_log_w8_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_log_w8_data *) h->private;

  if (a == 0 || b == 0) return 0;
  return (std->anti[std->log[a]+std->log[b]]);
}

static
gf_val_32_t
gf_wgen_log_8_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_log_w8_data *std;
  int index;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_log_w8_data *) h->private;

  if (a == 0 || b == 0) return 0;
  index = std->log[a];
  index -= std->log[b];

  return (std->danti[index]);
}

static 
int gf_wgen_log_8_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_wgen_log_w8_data *std;
  int w;
  uint32_t a, i;
  int check = 0;
  
  h = (gf_internal_t *) gf->scratch;
  w = h->w;
  std = (struct gf_wgen_log_w8_data *) h->private;
  
  std->log = &(std->base);
  std->anti = std->log + (1<<h->w);
  std->danti = std->anti + (1<<h->w)-1;
  
  for (i = 0; i < (1 << w); i++)
    std->log[i] = 0;

  a = 1;
  for(i=0; i < (1<<w)-1; i++)
  {
    if (std->log[a] != 0) check = 1;
    std->log[a] = i;
    std->anti[i] = a;
    std->danti[i] = a;
    a <<= 1;
    if(a & (1<<w))
      a ^= h->prim_poly;
    //a &= ((1 << w)-1);
  }

  if (check != 0) {
    _gf_errno = GF_E_LOGPOLY;
    return 0;
  }

  gf->multiply.w32 = gf_wgen_log_8_multiply;
  gf->divide.w32 = gf_wgen_log_8_divide;
  return 1;
}

static
gf_val_32_t
gf_wgen_log_16_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_log_w16_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_log_w16_data *) h->private;

  if (a == 0 || b == 0) return 0;
  return (std->anti[std->log[a]+std->log[b]]);
}

static
gf_val_32_t
gf_wgen_log_16_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_log_w16_data *std;
  int index;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_log_w16_data *) h->private;

  if (a == 0 || b == 0) return 0;
  index = std->log[a];
  index -= std->log[b];

  return (std->danti[index]);
}

static 
int gf_wgen_log_16_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_wgen_log_w16_data *std;
  int w;
  uint32_t a, i;
  int check = 0;
  
  h = (gf_internal_t *) gf->scratch;
  w = h->w;
  std = (struct gf_wgen_log_w16_data *) h->private;
  
  std->log = &(std->base);
  std->anti = std->log + (1<<h->w);
  std->danti = std->anti + (1<<h->w)-1;
 
  for (i = 0; i < (1 << w); i++)
    std->log[i] = 0;

  a = 1;
  for(i=0; i < (1<<w)-1; i++)
  {
    if (std->log[a] != 0) check = 1;
    std->log[a] = i;
    std->anti[i] = a;
    std->danti[i] = a;
    a <<= 1;
    if(a & (1<<w))
      a ^= h->prim_poly;
    //a &= ((1 << w)-1);
  }

  if (check) {
    if (h->mult_type != GF_MULT_LOG_TABLE) return gf_wgen_shift_init(gf);
    _gf_errno = GF_E_LOGPOLY;
    return 0;
  }
  
  gf->multiply.w32 = gf_wgen_log_16_multiply;
  gf->divide.w32 = gf_wgen_log_16_divide;
  return 1;
}

static
gf_val_32_t
gf_wgen_log_32_multiply(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_log_w32_data *std;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_log_w32_data *) h->private;

  if (a == 0 || b == 0) return 0;
  return (std->anti[std->log[a]+std->log[b]]);
}

static
gf_val_32_t
gf_wgen_log_32_divide(gf_t *gf, gf_val_32_t a, gf_val_32_t b)
{
  gf_internal_t *h;
  struct gf_wgen_log_w32_data *std;
  int index;
  
  h = (gf_internal_t *) gf->scratch;
  std = (struct gf_wgen_log_w32_data *) h->private;

  if (a == 0 || b == 0) return 0;
  index = std->log[a];
  index -= std->log[b];

  return (std->danti[index]);
}

static 
int gf_wgen_log_32_init(gf_t *gf)
{
  gf_internal_t *h;
  struct gf_wgen_log_w32_data *std;
  int w;
  uint32_t a, i;
  int check = 0;

  h = (gf_internal_t *) gf->scratch;
  w = h->w;
  std = (struct gf_wgen_log_w32_data *) h->private;
  
  std->log = &(std->base);
  std->anti = std->log + (1<<h->w);
  std->danti = std->anti + (1<<h->w)-1;
  
  for (i = 0; i < (1 << w); i++)
    std->log[i] = 0;

  a = 1;
  for(i=0; i < (1<<w)-1; i++)
  {
    if (std->log[a] != 0) check = 1;
    std->log[a] = i;
    std->anti[i] = a;
    std->danti[i] = a;
    a <<= 1;
    if(a & (1<<w))
      a ^= h->prim_poly;
    //a &= ((1 << w)-1);
  }

  if (check != 0) {
    _gf_errno = GF_E_LOGPOLY;
    return 0;
  }

  gf->multiply.w32 = gf_wgen_log_32_multiply;
  gf->divide.w32 = gf_wgen_log_32_divide;
  return 1;
}

static 
int gf_wgen_log_init(gf_t *gf)
{
  gf_internal_t *h;
  
  h = (gf_internal_t *) gf->scratch;
  if (h->w <= 8) return gf_wgen_log_8_init(gf);
  if (h->w <= 16) return gf_wgen_log_16_init(gf);
  if (h->w <= 32) return gf_wgen_log_32_init(gf); 

  /* Returning zero to make the compiler happy, but this won't get 
     executed, because it is tested in _scratch_space. */

  return 0;
}

int gf_wgen_scratch_size(int w, int mult_type, int region_type, int divide_type, int arg1, int arg2)
{

  switch(mult_type)
  {
    case GF_MULT_DEFAULT: 
      if (w <= 8) {
          return sizeof(gf_internal_t) + sizeof(struct gf_wgen_table_w8_data) +
               sizeof(uint8_t)*(1 << w)*(1<<w)*2 + 64;
      } else if (w <= 16) {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_log_w16_data) +
               sizeof(uint16_t)*(1 << w)*3;
      } else {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_group_data) +
               sizeof(uint32_t) * (1 << 2) +
               sizeof(uint32_t) * (1 << 8) + 64;
      }
    case GF_MULT_SHIFT:
    case GF_MULT_BYTWO_b:
    case GF_MULT_BYTWO_p:
      return sizeof(gf_internal_t);
      break;
    case GF_MULT_GROUP:
      return sizeof(gf_internal_t) + sizeof(struct gf_wgen_group_data) +
               sizeof(uint32_t) * (1 << arg1) +
               sizeof(uint32_t) * (1 << arg2) + 64;
      break;

    case GF_MULT_TABLE: 
      if (w <= 8) {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_table_w8_data) +
               sizeof(uint8_t)*(1 << w)*(1<<w)*2 + 64;
      } else if (w < 15) {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_table_w16_data) +
               sizeof(uint16_t)*(1 << w)*(1<<w)*2 + 64;
      } 
      return 0;
    case GF_MULT_LOG_TABLE: 
      if (w <= 8) {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_log_w8_data) +
               sizeof(uint8_t)*(1 << w)*3;
      } else if (w <= 16) {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_log_w16_data) +
               sizeof(uint16_t)*(1 << w)*3;
      } else if (w <= 27) {
        return sizeof(gf_internal_t) + sizeof(struct gf_wgen_log_w32_data) +
               sizeof(uint32_t)*(1 << w)*3;
      } else 
      return 0;
    default:
      return 0;
   }
}

void
gf_wgen_cauchy_region(gf_t *gf, void *src, void *dest, gf_val_32_t val, int bytes, int xor)
{
  gf_internal_t *h;
  gf_region_data rd;
  int written;    
  int rs, i, j;

  gf_set_region_data(&rd, gf, src, dest, bytes, val, xor, -1);

  if (val == 0) { gf_multby_zero(dest, bytes, xor); return; }
  if (val == 1) { gf_multby_one(src, dest, bytes, xor); return; }

  h = (gf_internal_t *) gf->scratch;
  rs = bytes / (h->w);
  
  written = (xor) ? 0xffffffff : 0;
  for (i = 0; i < h->w; i++) {
    for (j = 0; j < h->w; j++) {
      if (val & (1 << j)) {
        gf_multby_one(src, ((char*)dest) + j*rs, rs, (written & (1 << j)));
        written |= (1 << j);
      }
    }
    src = (char*)src + rs;
    val = gf->multiply.w32(gf, val, 2);
  }
}

int gf_wgen_init(gf_t *gf)
{
  gf_internal_t *h;

  h = (gf_internal_t *) gf->scratch;
  if (h->prim_poly == 0) {
    switch (h->w) {
      case 1: h->prim_poly = 1; break;
      case 2: h->prim_poly = 7; break;
      case 3: h->prim_poly = 013; break;
      case 4: h->prim_poly = 023; break;
      case 5: h->prim_poly = 045; break;
      case 6: h->prim_poly = 0103; break;
      case 7: h->prim_poly = 0211; break;
      case 8: h->prim_poly = 0435; break;
      case 9: h->prim_poly = 01021; break;
      case 10: h->prim_poly = 02011; break;
      case 11: h->prim_poly = 04005; break;
      case 12: h->prim_poly = 010123; break;
      case 13: h->prim_poly = 020033; break;
      case 14: h->prim_poly = 042103; break;
      case 15: h->prim_poly = 0100003; break;
      case 16: h->prim_poly = 0210013; break;
      case 17: h->prim_poly = 0400011; break;
      case 18: h->prim_poly = 01000201; break;
      case 19: h->prim_poly = 02000047; break;
      case 20: h->prim_poly = 04000011; break;
      case 21: h->prim_poly = 010000005; break;
      case 22: h->prim_poly = 020000003; break;
      case 23: h->prim_poly = 040000041; break;
      case 24: h->prim_poly = 0100000207; break;
      case 25: h->prim_poly = 0200000011; break;
      case 26: h->prim_poly = 0400000107; break;
      case 27: h->prim_poly = 01000000047; break;
      case 28: h->prim_poly = 02000000011; break;
      case 29: h->prim_poly = 04000000005; break;
      case 30: h->prim_poly = 010040000007; break;
      case 31: h->prim_poly = 020000000011; break;
      case 32: h->prim_poly = 00020000007; break;
      default: fprintf(stderr, "gf_wgen_init: w not defined yet\n"); exit(1);
    }
  } else {
    if (h->w == 32) {
      h->prim_poly &= 0xffffffff;
    } else {
      h->prim_poly |= (1 << h->w);
      if (h->prim_poly & ~((1ULL<<(h->w+1))-1)) return 0;
    }
  }

  gf->multiply.w32 = NULL;
  gf->divide.w32 = NULL;
  gf->inverse.w32 = NULL;
  gf->multiply_region.w32 = gf_wgen_cauchy_region;
  gf->extract_word.w32 = gf_wgen_extract_word;

  switch(h->mult_type) {
    case GF_MULT_DEFAULT:
      if (h->w <= 8) {
        if (gf_wgen_table_init(gf) == 0) return 0; 
      } else if (h->w <= 16) {
        if (gf_wgen_log_init(gf) == 0) return 0; 
      } else {
        if (gf_wgen_bytwo_p_init(gf) == 0) return 0; 
      }
      break;
    case GF_MULT_SHIFT:     if (gf_wgen_shift_init(gf) == 0) return 0; break;
    case GF_MULT_BYTWO_b:     if (gf_wgen_bytwo_b_init(gf) == 0) return 0; break;
    case GF_MULT_BYTWO_p:     if (gf_wgen_bytwo_p_init(gf) == 0) return 0; break;
    case GF_MULT_GROUP:     if (gf_wgen_group_init(gf) == 0) return 0; break;
    case GF_MULT_TABLE:     if (gf_wgen_table_init(gf) == 0) return 0; break;
    case GF_MULT_LOG_TABLE: if (gf_wgen_log_init(gf) == 0) return 0; break;
    default: return 0;
  }
  if (h->divide_type == GF_DIVIDE_EUCLID) {
    gf->divide.w32 = gf_wgen_divide_from_inverse;
    gf->inverse.w32 = gf_wgen_euclid;
  } else if (h->divide_type == GF_DIVIDE_MATRIX) {
    gf->divide.w32 = gf_wgen_divide_from_inverse;
    gf->inverse.w32 = gf_wgen_matrix;
  }

  if (gf->inverse.w32== NULL && gf->divide.w32 == NULL) gf->inverse.w32 = gf_wgen_euclid;

  if (gf->inverse.w32 != NULL && gf->divide.w32 == NULL) {
    gf->divide.w32 = gf_wgen_divide_from_inverse;
  }
  if (gf->inverse.w32 == NULL && gf->divide.w32 != NULL) {
    gf->inverse.w32 = gf_wgen_inverse_from_divide;
  }
  return 1;
}
