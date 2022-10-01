// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2020 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef WINSOCK_WRAPPER_H
#define WINSOCK_WRAPPER_H 1

#ifdef __cplusplus
// Boost complains if winsock2.h (or windows.h) is included before asio.hpp.
#include <boost/asio.hpp>
#endif

#include <winsock2.h>
#include <ws2ipdef.h>
#include <ws2tcpip.h>

#endif /* WINSOCK_WRAPPER_H */
