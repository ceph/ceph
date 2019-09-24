// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <array>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>

static inline void buf_to_hex(const unsigned char* const buf,
                              const size_t len,
                              char* const str)
{
  str[0] = '\0';
  for (size_t i = 0; i < len; i++) {
    ::sprintf(&str[i*2], "%02x", static_cast<int>(buf[i]));
  }
}

template<size_t N> static inline std::array<char, N * 2 + 1>
buf_to_hex(const std::array<unsigned char, N>& buf)
{
  static_assert(N > 0, "The input array must be at least one element long");

  std::array<char, N * 2 + 1> hex_dest;
  buf_to_hex(buf.data(), N, hex_dest.data());
  return hex_dest;
}

static inline int hexdigit(char c)
{
  if (c >= '0' && c <= '9')
    return (c - '0');
  c = toupper(c);
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 0xa;
  return -EINVAL;
}

static inline int hex_to_buf(const char *hex, char *buf, int len)
{
  int i = 0;
  const char *p = hex;
  while (*p) {
    if (i >= len)
      return -EINVAL;
    buf[i] = 0;
    int d = hexdigit(*p);
    if (d < 0)
      return d;
    buf[i] = d << 4;
    p++;
    if (!*p)
      return -EINVAL;
    d = hexdigit(*p);
    if (d < 0)
      return d;
    buf[i] += d;
    i++;
    p++;
  }
  return i;
} /* hex_to_buf */
