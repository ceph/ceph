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

#ifndef CEPH_COMPILER_EXTENSIONS_H
#define CEPH_COMPILER_EXTENSIONS_H

/* We should be able to take advantage of nice nonstandard features of gcc
 * and other compilers, but still maintain portability.
 */

#ifdef __GNUC__
// GCC
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
// some other compiler - just make it a no-op
#define WARN_UNUSED_RESULT
#endif

#endif
