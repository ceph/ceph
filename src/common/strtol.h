// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_STRTOL_H
#define CEPH_COMMON_STRTOL_H

#include <string>
#include <limits>
extern "C" {
#include <stdint.h>
}

long long strict_strtoll(const char *str, int base, std::string *err);

int strict_strtol(const char *str, int base, std::string *err);

double strict_strtod(const char *str, std::string *err);

float strict_strtof(const char *str, std::string *err);

uint64_t strict_sistrtoll(const char *str, std::string *err);

template<typename T>
T strict_si_cast(const char *str, std::string *err);

/* On enter buf points to the end of the buffer, e.g. where the least
 * significant digit of the input number will be printed. Returns pointer to
 * where the most significant digit were printed, including zero padding.
 * Does NOT add zero at the end of buffer, this is responsibility of the caller.
 */
template<typename T, const unsigned base = 10, const unsigned width = 1>
static inline
char* ritoa(T u, char *buf)
{
  static_assert(std::is_unsigned<T>::value, "signed types are not supported");
  static_assert(base <= 16, "extend character map below to support higher bases");
  unsigned digits = 0;
  while (u) {
    *--buf = "0123456789abcdef"[u % base];
    u /= base;
    digits++;
  }
  while (digits++ < width)
    *--buf = '0';
  return buf;
}

#endif
