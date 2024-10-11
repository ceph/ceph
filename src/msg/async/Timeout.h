// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IONOS SE
 *
 * Author: Max Kellermann <max.kellermann@ionos.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_TIMEOUT_H
#define CEPH_MSG_TIMEOUT_H

#include "include/intarith.h" // for div_round_up()

#include <time.h> // for struct timeval

/**
 * Convert the given `struct timeval` to milliseconds.
 *
 * This is supposed to be used as timeout parameter to system calls
 * such as poll() and epoll_wait().
 */
constexpr int
timeout_to_milliseconds(const struct timeval &tv) noexcept
{
  /* round up to the next millisecond so we don't wake up too early */
  return tv.tv_sec * 1000 + div_round_up(tv.tv_usec, 1000);
}

/**
 * This overload makes the timeout optional; on nullptr, it returns
 * -1.
 */
constexpr int
timeout_to_milliseconds(const struct timeval *tv) noexcept
{
  return tv != nullptr ? timeout_to_milliseconds(*tv) : -1;
}

#endif
