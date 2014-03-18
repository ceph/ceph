/* 
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_rand.h
 *
 * Random number generation, using the "Mother of All" random number generator.  */

#pragma once
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* These are all pretty self-explanatory */
uint32_t MOA_Random_32();
uint64_t MOA_Random_64();
void     MOA_Random_128(uint64_t *x);
uint32_t MOA_Random_W(int w, int zero_ok);
void MOA_Fill_Random_Region (void *reg, int size);   /* reg should be aligned to 4 bytes, but
                                                        size can be anything. */
void     MOA_Seed(uint32_t seed);
