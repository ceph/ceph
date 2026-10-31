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

#ifndef CEPH_FDB_EXECUTION_ADAPTER_H
 #define CEPH_FDB_EXECUTION_ADAPTER_H

#include "fdb/experimental/concurrency.h"

#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 #include "fdb/experimental/stdexec_adapter.h"
#elif defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 #include "fdb/experimental/asio_adapter.h"
#endif

#endif
