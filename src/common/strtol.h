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

template <typename Target>
Target strict_si_cast(const char *str, std::string *err) {
  uint64_t ret = strict_sistrtoll(str, err);
  if (!err->empty())
    return ret;
  if (ret > (uint64_t)std::numeric_limits<Target>::max()) {
    err->append("The option value '");
    err->append(str);
    err->append("' seems to be too large");
    return 0;
  }
  return ret;
}

template <>
uint64_t strict_si_cast(const char *str, std::string *err);

#endif
