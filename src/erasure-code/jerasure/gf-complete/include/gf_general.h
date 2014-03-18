/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_general.h
 *
 * This file has helper routines for doing basic GF operations with any
 * legal value of w.  The problem is that w <= 32, w=64 and w=128 all have
 * different data types, which is a pain.  The procedures in this file try
 * to alleviate that pain.  They are used in gf_unit and gf_time.
 */

#pragma once

#include <stdio.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "gf_complete.h"

typedef union {
  uint32_t w32;
  uint64_t w64;
  uint64_t w128[2];
} gf_general_t;

void gf_general_set_zero(gf_general_t *v, int w);
void gf_general_set_one(gf_general_t *v, int w);
void gf_general_set_two(gf_general_t *v, int w);

int gf_general_is_zero(gf_general_t *v, int w);
int gf_general_is_one(gf_general_t *v, int w);
int gf_general_are_equal(gf_general_t *v1, gf_general_t *v2, int w);

void gf_general_val_to_s(gf_general_t *v, int w, char *s, int hex);
int  gf_general_s_to_val(gf_general_t *v, int w, char *s, int hex);

void gf_general_set_random(gf_general_t *v, int w, int zero_ok);

void gf_general_add(gf_t *gf, gf_general_t *a, gf_general_t *b, gf_general_t *c);
void gf_general_multiply(gf_t *gf, gf_general_t *a, gf_general_t *b, gf_general_t *c);
void gf_general_divide(gf_t *gf, gf_general_t *a, gf_general_t *b, gf_general_t *c);
void gf_general_inverse(gf_t *gf, gf_general_t *a, gf_general_t *b);

void gf_general_do_region_multiply(gf_t *gf, gf_general_t *a, 
                                   void *ra, void *rb, 
                                   int bytes, int xor);

void gf_general_do_region_check(gf_t *gf, gf_general_t *a, 
                                void *orig_a, void *orig_target, void *final_target, 
                                int bytes, int xor);


/* Which is M, D or I for multiply, divide or inverse. */

void gf_general_set_up_single_timing_test(int w, void *ra, void *rb, int size);
int  gf_general_do_single_timing_test(gf_t *gf, void *ra, void *rb, int size, char which);
