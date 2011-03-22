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

#ifndef CEPH_COMMON_UTF8_H
#define CEPH_COMMON_UTF8_H

#ifdef __cplusplus
extern "C" {
#endif

/* Checks if a buffer is valid UTF-8.
 * Returns 0 if it is, and one plus the offset of the first invalid byte
 * if it is not.
 */
int check_utf8(const char *buf, int len);

/* Checks if a null-terminated string is valid UTF-8.
 * Returns 0 if it is, and one plus the offset of the first invalid byte
 * if it is not.
 */
int check_utf8_cstr(const char *buf);

#ifdef __cplusplus
}
#endif

#endif
