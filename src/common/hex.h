
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

#ifndef CEPH_COMMON_HEX_H
#define CEPH_COMMON_HEX_H

#include <string>

extern void hex2str(const char *s, int len, char *buf, int dest_len);

extern std::string hexdump(std::string msg, const char *s, int len);

#endif
