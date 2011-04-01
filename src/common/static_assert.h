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

#ifndef CEPH_COMMON_STATIC_ASSERT
#define CEPH_COMMON_STATIC_ASSERT

/* Create a compiler error if condition is false.
 * Also produce a result of value 0 and type size_t.
 * This expression can be used anywhere, even in structure initializers.
 */
#define STATIC_ASSERT(x) (sizeof(int[((x)==0) ? -1 : 0]))

#endif
