// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <netinet/tcp.h>
#include <sys/socket.h>

#include "Formatter.h"

namespace ceph {

/// Return TCP_INFO socket stats (see tcp(7)). Return true on success.
bool tcp_info(int fd, struct tcp_info& info);
/// Dump TCP_INFO socket stats to formatter. Use struct tcp_info variables
/// names as keys. Returns true on success.
bool dump_tcp_info(int fd, Formatter* f);

}  // namespace ceph
