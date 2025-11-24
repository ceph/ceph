/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

constexpr int CLS_METHOD_RD      = 0x1; /// method executes read operations
constexpr int CLS_METHOD_WR      = 0x2; /// method executes write operations
constexpr int CLS_METHOD_PROMOTE = 0x8; /// method cannot be proxied to base tier