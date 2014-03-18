/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_rand.c -- Random number generator.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "gf_rand.h"

/* Lifted the "Mother of All" random number generator from http://www.agner.org/random/ */

static uint32_t MOA_X[5];

uint32_t MOA_Random_32() {
  uint64_t sum;
  sum = (uint64_t)2111111111UL * (uint64_t)MOA_X[3] +
     (uint64_t)1492 * (uint64_t)(MOA_X[2]) +
     (uint64_t)1776 * (uint64_t)(MOA_X[1]) +
     (uint64_t)5115 * (uint64_t)(MOA_X[0]) +
     (uint64_t)MOA_X[4];
  MOA_X[3] = MOA_X[2];  MOA_X[2] = MOA_X[1];  MOA_X[1] = MOA_X[0];
  MOA_X[4] = (uint32_t)(sum >> 32);
  MOA_X[0] = (uint32_t)sum;
  return MOA_X[0];
}

uint64_t MOA_Random_64() {
  uint64_t sum;

  sum = MOA_Random_32();
  sum <<= 32;
  sum |= MOA_Random_32();
  return sum;
}

void MOA_Random_128(uint64_t *x) {
  x[0] = MOA_Random_64();
  x[1] = MOA_Random_64();
  return;
}

uint32_t MOA_Random_W(int w, int zero_ok)
{
  uint32_t b;

  do {
    b = MOA_Random_32();
    if (w == 31) b &= 0x7fffffff;
    if (w < 31)  b %= (1 << w);
  } while (!zero_ok && b == 0);
  return b;
}

void MOA_Seed(uint32_t seed) {
  int i;
  uint32_t s = seed;
  for (i = 0; i < 5; i++) {
    s = s * 29943829 - 1;
    MOA_X[i] = s;
  }
  for (i=0; i<19; i++) MOA_Random_32();
}


void MOA_Fill_Random_Region (void *reg, int size)
{
  uint32_t *r32;
  uint8_t *r8;
  int i;

  r32 = (uint32_t *) reg;
  r8 = (uint8_t *) reg;
  for (i = 0; i < size/4; i++) r32[i] = MOA_Random_32();
  for (i *= 4; i < size; i++) r8[i] = MOA_Random_W(8, 1);
}

