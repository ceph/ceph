// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_CONCURRENCY_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_CONCURRENCY_H

// Experimental: backend-native composition is available through native_handle().
#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC) && defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #error "libfdb experimental concurrency requires exactly one backend"
#elif defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 #include "stdexec.h"
#elif defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #include "asio.h"
#else
 #error "enable LIBFDB_EXPERIMENTAL_CONCURRENCY=ASIO or NVIDIA_STDEXEC and link rgw_fdb_execution"
#endif

#endif
